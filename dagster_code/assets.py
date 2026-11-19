"""Surface-scoped asset graph for the nostrology loads.

Each webapp surface (/relays, /nutzaps, …) is ONE job/run that holds the Aurora dial once across
all of its kinds and post-steps (definitions.py). Within a surface, every kind is an
extract→table chain built by the `_kind_chain` factory (DRY over the shared run.sh seam verbs):

    <kind>_extract  --(refresh-extract-only: flush -> diff-register -> Athena UNLOAD)-->  S3 prefix
        │   (+ blocking check: the UNLOAD produced >= 1 part)
        ▼
    <kind>_table    --(refresh-load-only: fetch -> in-proc loader TRUNCATE+COPY)-->  Aurora

Surfaces that need a cross-kind aggregation add a post-step asset downstream of the table(s) it
reads — e.g. /nutzaps's `nutzaps_active_users` (the Overview-MAU counts, rebuilt from the loaded
k=9321 table). RUN_ID is derived deterministically from (dagster_run_id, kind[, into]) so the
extract and table halves address the same s3://.../k=K/<RUN_ID>/ prefix without passing metadata
between assets (which Dagster can't read as an input anyway).

The three /relays materialized views (relay_stats_mv, relay_per_user_distribution_mv,
relay_tier_user_counts_mv) are now their OWN asset (relays_mv), downstream of relays_table — lifted
out of the loader (the load runs with --no-refresh-mv) so the refresh is visible, re-runnable, and
reusable via the `_mv_refresh_asset` factory as other surfaces move serving onto MVs. The /nutzaps
MAU is likewise its own post-step asset (native pipeline/postwork.py, like the rest of the pipeline).
"""

import os
import shutil
import tempfile
from datetime import datetime, timezone

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from .pipeline import aurora, aws, pg, s3util
from .pipeline import extract as _extract_mod
from .pipeline import load as _load
from .pipeline import nectar as _nectar
from .pipeline import postwork as _postwork
from .pipeline.config import MissingConfig, PipelineConfig
from .pipeline.kinds import LIGHTNING_MVS, RELAYS_MVS
from .resources import AuroraDial

# Repo root (= /app in the image): the loader path + blocklists dir resolve against it.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Per-step ceilings. The extract's is the Athena poll ceiling (1800s, the old REFRESH_TIMEOUT — a
# wedged query is cancelled then). The load's is the loader-subprocess wall clock (killed via its
# process group if it overruns). The MAU/signer post-steps are native (pipeline/postwork.py).
_EXTRACT_TIMEOUT_S = 1800
_LOAD_TIMEOUT_S = 2400
_MAU_TIMEOUT_S = 600


def _run_id_suffix(kind: str, into: str = "") -> str:
    """The per-(kind[, into]) segment of the deterministic RUN_ID. `into` keeps a subset extract
    of a kind (9735 -> zapstore) on a DIFFERENT S3 prefix than the kind's full extract."""
    return f"k{kind}" + (f"-{into}" if into else "")


def _human_bytes(n: int) -> str:
    f = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if f < 1024 or unit == "TiB":
            return f"{f:.1f} {unit}" if unit != "B" else f"{int(f)} B"
        f /= 1024
    return f"{n} B"


def _extract_asset(
    *,
    kind: str,
    group: str,
    name: str,
    into: str = "",
    timeout: int = _EXTRACT_TIMEOUT_S,
    allow_empty: bool = False,
    after: list | None = None,
):
    """Build ONE extract asset (flush -> register -> Athena UNLOAD) - the extract half of
    `_kind_chain`, factored out so surfaces that consume the UNLOAD parts directly (the
    marketplaces compute) reuse the identical extract behavior without a loader/table half.

    `allow_empty` is for a SUBSET extract whose volume is unmeasured (the /git reactions,
    zaps and deletions): its extract_nonempty check is non-blocking and passes with a
    warning on zero parts, so an empty subset cannot wedge the surface. Every other extract
    keeps the blocking check.

    `after` adds extracts this one must follow, for a template that reads ANOTHER kind's
    partitions: run_extract flushes and registers only its own kind, and the executor's order
    among unrelated assets is incidental."""
    suffix = _run_id_suffix(kind, into)
    if allow_empty:
        check_description = (
            "The Athena UNLOAD produced >= 1 gz part. NON-blocking for this subset extract: zero "
            "parts is a legitimate empty subset, so the check passes with a warning."
        )
    else:
        check_description = (
            "The Athena UNLOAD produced >= 1 gz part (a 0-part extract is the SHORT/empty failure mode)."
        )

    @asset(
        name=name,
        group_name=group,
        deps=after or [],
        check_specs=[
            AssetCheckSpec(
                name="extract_nonempty",
                asset=name,
                description=check_description,
                blocking=not allow_empty,
            )
        ],
    )
    def _extract(context: AssetExecutionContext, aurora_dial: AuroraDial) -> MaterializeResult:
        """flush -> full per-kind diff-register -> Athena UNLOAD to the durable S3 prefix.

        Native boto3 extract (pipeline/extract.py): flush over urllib, the Glue diff-register
        (with its own completeness + read-back gates), then the Athena UNLOAD. (aurora_dial is
        required so the run-scoped dial resource initializes — the cluster is dialed up before the
        load step runs; the extract itself doesn't touch Aurora. Catalog completeness is enforced
        INSIDE register's read-back gate; this asset's blocking check guards the extract OUTPUT.)
        """
        run_id = f"{context.run_id}-{suffix}"
        stats = _extract_mod.run_extract(
            context, PipelineConfig.from_env(),
            kind=kind, run_id=run_id, into=into, repo_root=_REPO_ROOT, query_timeout_s=timeout,
        )
        parts = int(stats.get("parts", 0) or 0)
        scanned = int(stats.get("scanned_bytes", 0) or 0)
        # The workgroup's per-query cutoff (BytesScannedCutoffPerQuery, set in the infrastructure
        # code: 10 GiB, ENFORCED - Athena cancels the query past
        # it and this extract fails). Warn at 60% so the growth is seen before the cliff (review
        # 4 m7: the kind-5 (event-deletion-requests) scan feeds /git and /marketplaces alike).
        cutoff = 10 * 1024 ** 3
        if scanned * 10 > cutoff * 6:
            context.log.warning(
                f"[extract] {name} (k={kind}): the Athena query scanned {scanned} bytes "
                f"({_human_bytes(scanned)}), {100 * scanned / cutoff:.0f}% of the workgroup's "
                "10 GiB per-query cutoff (BytesScannedCutoffPerQuery, enforced) - past it Athena "
                "cancels the query and this extract fails"
            )
        check_meta: dict = {"gz_parts": MetadataValue.int(parts)}
        if parts < 1 and allow_empty:
            context.log.warning(
                f"[extract] {name}: the UNLOAD produced 0 parts - an empty subset, allowed here"
            )
            check_meta["warning"] = MetadataValue.text("0 parts: an empty subset (allowed)")
        return MaterializeResult(
            metadata={
                "run_id": MetadataValue.text(run_id),
                # a FLOAT: an int metadata value above 2^31 reads back null over GraphQL, which
                # hid exactly the heavy kinds' trend (review 4 m7)
                "athena_scanned_bytes": MetadataValue.float(float(scanned)),
                "athena_scanned": MetadataValue.text(_human_bytes(scanned)),
                "gz_parts": MetadataValue.int(parts),
            },
            check_results=[
                AssetCheckResult(
                    check_name="extract_nonempty",
                    passed=parts >= 1 or allow_empty,
                    metadata=check_meta,
                )
            ],
        )

    return _extract


def _kind_chain(
    *,
    kind: str,
    group: str,
    extract_name: str,
    table_name: str,
    into: str = "",
    extract_timeout: int = _EXTRACT_TIMEOUT_S,
    load_timeout: int = _LOAD_TIMEOUT_S,
    after: list | None = None,
):
    """Build the (extract, table) AssetsDefinition pair for one kind on the run.sh seam.

    `into` disambiguates a subset extract of the same kind (e.g. 9735 -> zapstore) so its
    S3 prefix and serving table don't collide with the kind's full extract; it is "" here.

    `after` adds table assets this kind's LOAD must follow. It exists for one case: the
    /zapstore loads of kind-30063 (zapstore-app-releases), kind-30267 (zapstore-app-stacks)
    and the kind-9735 (zap-receipts) subset each read
    events_kind_32267_app_catalog_excluded to drop what belongs to a listing that is not a
    Zapstore app, so they must run AFTER the catalog load has rewritten it. The four chains
    are otherwise independent, and without this the executor's order is incidental - it
    would silently apply YESTERDAY's exclusions, or none on a first run.
    """
    suffix = _run_id_suffix(kind, into)

    def _rid(context: AssetExecutionContext) -> str:
        return f"{context.run_id}-{suffix}"

    _extract = _extract_asset(kind=kind, group=group, name=extract_name, into=into, timeout=extract_timeout)

    @asset(name=table_name, group_name=group, deps=[_extract, *(after or [])])
    def _table(context: AssetExecutionContext, aurora_dial: AuroraDial) -> MaterializeResult:
        """fetch the extract's S3 prefix -> loader (TRUNCATE+COPY, MIN_ROWS pre-gate, --no-refresh-mv).

        Native boto3 load (pipeline/load.py): resolves the Aurora endpoint/secret, fetches the
        durable S3 prefix to real-disk scratch, runs the loader as a Python subprocess. Recomputes
        the SAME run_id as the extract (same dagster run + kind) so it loads that extract's prefix;
        run_load fails loud on an empty/missing prefix (stale RUN_ID). MV refresh is suppressed
        (--no-refresh-mv) and owned by the downstream MV asset.
        """
        run_id = _rid(context)
        stats = _load.run_load(
            context, PipelineConfig.from_env(),
            kind=kind, run_id=run_id, into=into, repo_root=_REPO_ROOT, timeout=load_timeout,
        )
        rows = int(stats.get("rows", 0) or 0)
        min_rows = int(stats.get("min_rows", 0) or 0)
        copy_s = int(stats.get("copy_s", 0) or 0)
        # Only meaningful when we actually counted rows (>0). A best-effort count hiccup (rows=0)
        # would otherwise plot a sharply-negative headroom that looks like a real anti-shrink breach.
        headroom_pct = round(100.0 * (rows - min_rows) / min_rows, 1) if (min_rows > 0 and rows > 0) else None
        meta: dict = {
            "rows_loaded": MetadataValue.int(rows),
            "min_rows_floor": MetadataValue.int(min_rows),
            "copy_duration_s": MetadataValue.int(copy_s),
            "run_id": MetadataValue.text(run_id),
        }
        if headroom_pct is not None:
            meta["anti_shrink_headroom_pct"] = MetadataValue.float(headroom_pct)
        # Only kind-32267 (zapstore-app-catalog) sets this: the listings it sorted out
        # of the catalog because they are not Zapstore apps. Without it, rows_loaded
        # just steps down and nothing on the asset says why.
        if stats.get("sorted_out") is not None:
            meta["rows_sorted_out"] = MetadataValue.int(int(stats["sorted_out"]))
        return MaterializeResult(metadata=meta)

    return _extract, _table


def _mv_refresh_asset(*, name: str, group: str, deps: list, mvs):
    """Build an asset that REFRESHes (CONCURRENTLY) a surface's materialized views, downstream of
    the table(s) they read.

    The MV refresh used to ride hidden inside the loader (kind-10002 (relay-list-metadata));
    lifting it into its own asset makes it visible (own box + per-MV refresh-duration metadata),
    independently re-runnable, and correctly lineaged - and gives the reusable shape the other surfaces adopt as they move serving
    onto MVs (e.g. /lightning's chart data). Requires aurora_dial so it runs inside the run's dial
    bracket; CONCURRENTLY keeps the page serving the prior snapshot until the refresh atomically swaps.
    """
    mvs = list(mvs)

    @asset(name=name, group_name=group, deps=deps)
    def _mv(context: AssetExecutionContext, aurora_dial: AuroraDial) -> MaterializeResult:
        cfg = PipelineConfig.from_env()
        rds = aws.client("rds", cfg.aws_region)
        sm = aws.client("secretsmanager", cfg.aws_region)
        host = aurora.resolve_endpoint(rds, cfg.aurora_cluster)
        password = aurora.load_master_password(rds, sm, cfg.aurora_cluster)
        conn = pg.connect(pg.build_dsn(host, cfg.pg_port, cfg.pg_owner, cfg.pg_database), password)
        try:
            timings = pg.refresh_materialized_views(conn, mvs, log=context.log.info)
        finally:
            conn.close()
        return MaterializeResult(
            metadata={f"{mv}_refresh_s": MetadataValue.float(secs) for mv, secs in timings.items()}
        )

    return _mv


# ── /relays ────────────────────────────────────────────────────────────────────────────────
# Single replaceable kind (NIP-65 relay lists). The 3 leaderboard MVs are refreshed by relays_mv
# downstream of relays_table (lifted out of the loader via --no-refresh-mv).
relays_extract, relays_table = _kind_chain(
    kind="10002", group="relays", extract_name="relays_extract", table_name="relays_table",
)
relays_mv = _mv_refresh_asset(
    name="relays_mv", group="relays", deps=[relays_table], mvs=RELAYS_MVS,
)

# ── /nutzaps ───────────────────────────────────────────────────────────────────────────────
# Two kinds - the regular kind-9321 (nutzaps) + the replaceable kind-10019 (nutzap-configs) - then
# the mint classification over both (nutzap_mint_classes, #9), then the Overview-MAU counts rebuilt
# from the loaded kind-9321 (nutzaps) table minus excluded mints. kind-7376 (nutzap-redemptions) is
# deliberately NOT loaded (no page reads it). Both kinds are blocklist-filtered by the loader (the blocklists
# dir is mounted into dagster-code at /app/blocklists; see the compose).
nutzaps_payments_extract, nutzaps_payments_table = _kind_chain(
    kind="9321", group="nutzaps",
    extract_name="nutzaps_payments_extract", table_name="nutzaps_payments_table",
)
nutzaps_configs_extract, nutzaps_configs_table = _kind_chain(
    kind="10019", group="nutzaps",
    extract_name="nutzaps_configs_extract", table_name="nutzaps_configs_table",
)


@asset(name="nutzaps_mint_classes", group_name="nutzaps", deps=[nutzaps_payments_table, nutzaps_configs_table])
def nutzaps_mint_classes(context: AssetExecutionContext, aurora_dial: AuroraDial) -> MaterializeResult:
    """Rebuild nutzap_mint_classes (which mints count on /nutzaps and in the paid MAU; #9).

    Classifies every mint URL in BOTH loaded tables (kind-9321 (nutzaps) `u` tags, kind-10019
    (nutzap-configs) `mint` tags), so it depends on both loads. Native (pipeline/postwork.py): runs the loader's --classify-nutzap-mints
    mode against the image-baked data/nutzaps/mint_registry.yaml.
    """
    stats = _postwork.classify_nutzap_mints(
        context, PipelineConfig.from_env(), repo_root=_REPO_ROOT, timeout=_MAU_TIMEOUT_S,
    )
    rows = int(stats.get("rows", 0) or 0)
    return MaterializeResult(metadata={"mint_urls": MetadataValue.int(rows)})


@asset(name="nutzaps_active_users", group_name="nutzaps", deps=[nutzaps_payments_table, nutzaps_mint_classes])
def nutzaps_active_users(context: AssetExecutionContext, aurora_dial: AuroraDial) -> MaterializeResult:
    """Rebuild active_user_counts_kind_9321_nutzaps (the /nutzaps Overview-MAU) from the loaded k=9321 table.

    Depends on nutzaps_payments_table and nutzaps_mint_classes - the rebuild SQL reads
    events_kind_9321_nutzaps and drops rows on excluded mints (test and private mints, #9).
    aurora_dial is required so this runs while the cluster is dialed up.

    Native (pipeline/postwork.py): runs schema/nutzap_active_users.sql in-process via psycopg2.
    """
    stats = _postwork.rebuild_nutzap_mau(context, PipelineConfig.from_env(), repo_root=_REPO_ROOT)
    rows = int(stats.get("rows", 0) or 0)
    return MaterializeResult(metadata={"mau_rows": MetadataValue.int(rows)})


# ── /lightning ─────────────────────────────────────────────────────────────────────────────
# Two kinds - the replaceable, blocklist-filtered kind-0 (profile-metadata) + the regular
# kind-9735 (zap-receipts) - plus the provider->signer map loaded from a vendored YAML (no
# Athena/S3). These are the two LARGEST loads (kind-0 (profile-metadata) ~2.2M rows,
# kind-9735 (zap-receipts) ~3.5M rows / ~2.3 GB gz extract); the fetch lands on the extract scratch, behind the load's free-space preflight (fail-loud).
lightning_profiles_extract, lightning_profiles_table = _kind_chain(
    kind="0", group="lightning",
    extract_name="lightning_profiles_extract", table_name="lightning_profiles_table",
)
lightning_zaps_extract, lightning_zaps_table = _kind_chain(
    kind="9735", group="lightning",
    extract_name="lightning_zaps_extract", table_name="lightning_zaps_table",
)


@asset(name="lightning_signers", group_name="lightning")
def lightning_signers(context: AssetExecutionContext, aurora_dial: AuroraDial) -> MaterializeResult:
    """Load lightning_provider_signers from the image-baked YAML (TRUNCATE+reinsert; the YAML is the
    source of truth). Independent of the kind loads (no S3/Athena) — applies schema so the table
    exists, then runs the loader; aurora_dial keeps it inside the run's dial bracket.

    Native (pipeline/postwork.py): applies schema in-process, then runs the loader's
    --load-lightning-signers mode as a Python subprocess (no compose, no docker socket).
    """
    stats = _postwork.load_signers(
        context, PipelineConfig.from_env(), repo_root=_REPO_ROOT, timeout=_MAU_TIMEOUT_S,
    )
    rows = int(stats.get("rows", 0) or 0)
    return MaterializeResult(metadata={"signer_rows": MetadataValue.int(rows)})


# The two /lightning MVs (provider leaderboard + daily provider grain), refreshed CONCURRENTLY
# downstream of ALL three lightning loads: lightning_provider_stats_mv reads the
# kind-0 (profile-metadata) table, and lightning_daily_provider_mv reads BOTH the
# kind-9735 (zap-receipts) table AND the signer map - so the refresh must wait for the freshest of all three. Mirrors relays_mv.
lightning_mv = _mv_refresh_asset(
    name="lightning_mv", group="lightning",
    deps=[lightning_profiles_table, lightning_zaps_table, lightning_signers], mvs=LIGHTNING_MVS,
)


# ── /zapstore ──────────────────────────────────────────────────────────────────────────────
# Three addressable app kinds - kind-32267 (zapstore-app-catalog),
# kind-30063 (zapstore-app-releases) and kind-30267 (zapstore-app-stacks) - + a derived
# kind-9735 (zap-receipts) zapstore-attributed subset (the same archive, a-tag-filtered via
# INTO=zapstore -> events_kind_9735_zaps_zapstore; tiny output, MIN_ROWS forced to 1 in run.sh).
# Decoupled from /lightning (the kind-0 (profile-metadata) rows the pages JOIN against come from
# the /lightning load - a cross-surface DATA dependency we do NOT model as an asset dep, matching the legacy verbs' decoupling).
zapstore_catalog_extract, zapstore_catalog_table = _kind_chain(
    kind="32267", group="zapstore",
    extract_name="zapstore_catalog_extract", table_name="zapstore_catalog_table",
)
# The three dependent loads follow the catalog: each drops the rows belonging to a listing
# the catalog load classified as not-a-Zapstore-app. See _kind_chain's `after`.
zapstore_releases_extract, zapstore_releases_table = _kind_chain(
    kind="30063", group="zapstore",
    extract_name="zapstore_releases_extract", table_name="zapstore_releases_table",
    after=[zapstore_catalog_table],
)
zapstore_stacks_extract, zapstore_stacks_table = _kind_chain(
    kind="30267", group="zapstore",
    extract_name="zapstore_stacks_extract", table_name="zapstore_stacks_table",
    after=[zapstore_catalog_table],
)
zapstore_zaps_extract, zapstore_zaps_table = _kind_chain(
    kind="9735", into="zapstore", group="zapstore",
    extract_name="zapstore_zaps_extract", table_name="zapstore_zaps_table",
    after=[zapstore_catalog_table],
)


# -- /marketplaces --------------------------------------------------------------------------
# The GammaMarkets marketplace surface: kind-30402 (markets-product-listings) +
# kind-30406 (markets-shipping-options) extracted with ALL versions (extract_addressable_all.sql -
# the farm detector and archive_events span every revision; the current-version selection with
# the exact NIP-01 tie-break runs in pipeline/marketplaces.py), plus the
# kind-5 (event-deletion-requests) SELF-AUTHORED request aggregation (extract_markets_deletions.sql).
# kind-30405 (markets-collections) is captured by nozzl but feeds no page metric - not loaded.
#
# Unlike the loader surfaces there is no per-kind serving table of raw events: the compute
# asset consumes the UNLOAD parts directly and writes the two serving tables itself
# (marketplaces_listings TRUNCATE+INSERT + one appended marketplaces_snapshot JSONB row, in
# ONE transaction behind the MIN_ACTIVE_LISTINGS anti-shrink gate). The methodology is the
# exact port of crosscheck/build_markets_snapshot.py, which remains the independent
# cross-check (crosscheck/compare_markets_snapshots.py) - the two must FULLY agree.
marketplaces_listings_extract = _extract_asset(
    kind="30402", into="markets", group="marketplaces", name="marketplaces_listings_extract",
)
marketplaces_shipping_extract = _extract_asset(
    kind="30406", into="markets", group="marketplaces", name="marketplaces_shipping_extract",
)
# AFTER the listings extract, because its template joins `e` tags to the listing ids, in the
# kind-30402 (markets-product-listings) partitions that extract's run registers.
marketplaces_deletions_extract = _extract_asset(
    kind="5", into="markets-deletions", group="marketplaces", name="marketplaces_deletions_extract",
    after=[marketplaces_listings_extract],
)


@asset(
    name="marketplaces_snapshot",
    group_name="marketplaces",
    deps=[marketplaces_listings_extract, marketplaces_shipping_extract, marketplaces_deletions_extract],
    check_specs=[
        AssetCheckSpec(
            name="snapshot_invariants",
            asset="marketplaces_snapshot",
            description="Cross-checks between the snapshot's own numbers (exclusions+active==coordinates, "
            "monotone cumulative series, geo/currency sums, coordinate ranges, nothing dated after the "
            "run). Recorded on success; "
            "a failure RAISES before any write, so the previous serving data stays live.",
        )
    ],
)
def marketplaces_snapshot(context: AssetExecutionContext, aurora_dial: AuroraDial) -> MaterializeResult:
    """Fetch the three UNLOAD part sets, run the marketplaces metrics compute, and persist
    the refresh (listings table + snapshot row) in one transaction.

    Fail-loud ordering: parse -> build -> invariants -> anti-shrink -> write. Any failure
    raises BEFORE the TRUNCATE, so a broken refresh can never blank the page - the web keeps
    serving the previous snapshot row (append-only) and the previous listings rows.
    """
    # Imported HERE, not at module level: the marketplaces compute drags in the geo stack
    # (markets_geolib -> reverse_geocoder + scipy/numpy + a gazetteer parse at import
    # time). At module level that made the WHOLE code location's load depend on the geo
    # deps - a broken wheel would take down every surface's schedule, not just this one.
    # Same lazy posture as pipeline/pg.py's psycopg2.
    from .pipeline import marketplaces as _marketplaces

    cfg = PipelineConfig.from_env()
    s3 = aws.client("s3", cfg.aws_region)

    prefixes = {
        "listings": f"k=30402/{context.run_id}-{_run_id_suffix('30402', 'markets')}/",
        "shipping": f"k=30406/{context.run_id}-{_run_id_suffix('30406', 'markets')}/",
        "deletions": f"k=5/{context.run_id}-{_run_id_suffix('5', 'markets-deletions')}/",
    }
    os.makedirs(cfg.extract_tmp, exist_ok=True)
    need = sum(s3util.prefix_total_bytes(s3, cfg.extracts_bucket, p) for p in prefixes.values())
    free = shutil.disk_usage(cfg.extract_tmp).free
    if need and not s3util.enough_free_space(need, free):
        raise RuntimeError(
            f"fetch dir {cfg.extract_tmp} has {free // 1048576} MiB free, but the marketplaces "
            f"UNLOADs total {need // 1048576} MiB (need ~+20% headroom)."
        )

    fetch_dir = tempfile.mkdtemp(prefix="nostrology-marketplaces.", dir=cfg.extract_tmp)
    try:
        rows = {}
        for label, prefix in prefixes.items():
            sub = os.path.join(fetch_dir, label)
            os.makedirs(sub, exist_ok=True)
            parts = s3util.download_prefix(s3, cfg.extracts_bucket, prefix, sub)
            # deletions may legitimately be small but never absent (the extract_nonempty
            # blocking check upstream already refused a 0-part extract).
            if not parts:
                raise RuntimeError(f"no .gz parts at s3://{cfg.extracts_bucket}/{prefix}")
            rows[label] = _marketplaces.parse_ndjson_gz(parts)
            context.log.info(f"[marketplaces] {label}: {len(parts)} part(s), {len(rows[label])} rows")

        # Input-sanity floors on the deletions aggregation: the anti-shrink gate is
        # one-directional, and an under-counted extract INFLATES the pool (deleted listings
        # resurface) while passing every other check.
        _marketplaces.check_deletion_floors(rows["deletions"])

        now_ts = int(datetime.now(timezone.utc).timestamp())
        recs, data = _marketplaces.build_snapshot(
            rows["listings"], rows["shipping"], rows["deletions"], now_ts
        )
        problems = _marketplaces.check_invariants(recs, data)
        if problems:
            raise RuntimeError("snapshot invariants FAILED (nothing written): " + "; ".join(problems))

        rds = aws.client("rds", cfg.aws_region)
        sm = aws.client("secretsmanager", cfg.aws_region)
        host = aurora.resolve_endpoint(rds, cfg.aurora_cluster)
        password = aurora.load_master_password(rds, sm, cfg.aurora_cluster)
        conn = pg.connect(pg.build_dsn(host, cfg.pg_port, cfg.pg_owner, cfg.pg_database), password)
        try:
            pg.apply_schema(conn, os.path.join(_REPO_ROOT, "schema", "init.sql"))
            stats = _marketplaces.write_pg(conn, recs, data, run_id=context.run_id)
        finally:
            conn.close()
    finally:
        shutil.rmtree(fetch_dir, ignore_errors=True)

    smry = data["summary"]
    context.log.info(
        f"[marketplaces] snapshot {data['generated_utc']}: {smry['active_listings']} live listings, "
        f"{smry['sellers']} sellers, {smry['countries']} countries "
        f"({stats['listings_rows']} coordinate rows)"
    )
    return MaterializeResult(
        metadata={
            "generated_utc": MetadataValue.text(data["generated_utc"]),
            "archive_events": MetadataValue.int(smry["archive_events"]),
            "current_coordinates": MetadataValue.int(smry["current_coordinates"]),
            "active_listings": MetadataValue.int(smry["active_listings"]),
            "sellers": MetadataValue.int(smry["sellers"]),
            "countries": MetadataValue.int(smry["countries"]),
            "listings_rows": MetadataValue.int(stats["listings_rows"]),
        },
        check_results=[
            AssetCheckResult(
                check_name="snapshot_invariants",
                passed=True,
                metadata={"checks": MetadataValue.text("all cross-checks passed")},
            )
        ],
    )


# -- /routstr -------------------------------------------------------------------------------
# The Routstr AI-inference marketplace surface: kind-38421 (routstr-provider-announcements),
# kind-38423 (routstr-model-catalog) and kind-38425 (routstr-provider-reviews) extracted
# with ALL versions (extract_addressable_all.sql - first-seen dates, catalog history and
# the review timeline span every revision), plus kind-38422 (routstr-analytics-snapshots)
# reduced IN Athena to the last snapshot per (pubkey, UTC day) carrying the
# routstr.analytics.snapshot.v1 schema tag (extract_routstr_analytics_daily.sql - the
# events are ~120 KB each and near-identical within a day).
#
# Same shape as /marketplaces: no per-kind serving table of raw events - the compute
# asset consumes the UNLOAD parts directly, reconstructs the per-provider activity
# ledger from the snapshots' trailing windows (pipeline/routstr.py - full-coverage +
# newest-wins bucket merge), and writes the serving tables itself: the five
# substrate tables (routstr_providers, routstr_provider_days, routstr_model_days,
# routstr_catalog_versions, routstr_reviews - TRUNCATE+INSERT; the MCP tools' source,
# docs/design/routstr-mcp-design.md) plus one appended routstr_snapshot JSONB row
# (the page's document), in ONE transaction behind the MIN_LEDGER_ROWS /
# MIN_PROVIDERS anti-shrink gates. The methodology is independently re-derived by
# crosscheck/build_routstr_snapshot.py (DuckDB over raw S3) and the two must
# FULLY agree (crosscheck/compare_routstr_snapshots.py); the substrate is tied to the
# document by check_invariants, so the tools and the page cannot diverge.
routstr_announcements_extract = _extract_asset(
    kind="38421", into="routstr", group="routstr", name="routstr_announcements_extract",
)
routstr_analytics_extract = _extract_asset(
    kind="38422", into="routstr", group="routstr", name="routstr_analytics_extract",
)
routstr_catalog_extract = _extract_asset(
    kind="38423", into="routstr", group="routstr", name="routstr_catalog_extract",
)
routstr_reviews_extract = _extract_asset(
    kind="38425", into="routstr", group="routstr", name="routstr_reviews_extract",
)


@asset(
    name="routstr_snapshot",
    group_name="routstr",
    deps=[routstr_announcements_extract, routstr_analytics_extract,
          routstr_catalog_extract, routstr_reviews_extract],
    check_specs=[
        AssetCheckSpec(
            name="snapshot_invariants",
            asset="routstr_snapshot",
            description="Cross-checks between the snapshot's own numbers (cumulative monotone + "
            "endpoint agreement, daily-series contiguity, model sums == network sums, provider "
            "table partition, catalog history tail) AND between the document and the MCP "
            "substrate tables (model days re-sum to the model rows + chart series, catalog "
            "versions re-derive the catalog history, reviews carry the timeline). Recorded on "
            "success; a failure RAISES before any write, so the previous serving data stays live.",
        )
    ],
)
def routstr_snapshot(context: AssetExecutionContext, aurora_dial: AuroraDial) -> MaterializeResult:
    """Fetch the four UNLOAD part sets, run the routstr metrics compute, and persist the
    refresh (providers + provider_days tables + one snapshot row) in one transaction.

    Fail-loud ordering: parse -> build -> invariants -> anti-shrink -> write. Any failure
    raises BEFORE the TRUNCATEs, so a broken refresh can never blank the page - the web
    keeps serving the previous snapshot row (append-only) and the previous substrate rows.
    """
    # Lazy import for symmetry with the marketplaces asset (and so a broken import in the
    # compute module can never take down the other surfaces' schedule loading).
    from .pipeline import routstr as _routstr

    cfg = PipelineConfig.from_env()
    s3 = aws.client("s3", cfg.aws_region)

    prefixes = {
        "announce": f"k=38421/{context.run_id}-{_run_id_suffix('38421', 'routstr')}/",
        "analytics": f"k=38422/{context.run_id}-{_run_id_suffix('38422', 'routstr')}/",
        "catalog": f"k=38423/{context.run_id}-{_run_id_suffix('38423', 'routstr')}/",
        "reviews": f"k=38425/{context.run_id}-{_run_id_suffix('38425', 'routstr')}/",
    }
    os.makedirs(cfg.extract_tmp, exist_ok=True)
    need = sum(s3util.prefix_total_bytes(s3, cfg.extracts_bucket, p) for p in prefixes.values())
    free = shutil.disk_usage(cfg.extract_tmp).free
    if need and not s3util.enough_free_space(need, free):
        raise RuntimeError(
            f"fetch dir {cfg.extract_tmp} has {free // 1048576} MiB free, but the routstr "
            f"UNLOADs total {need // 1048576} MiB (need ~+20% headroom)."
        )

    fetch_dir = tempfile.mkdtemp(prefix="nostrology-routstr.", dir=cfg.extract_tmp)
    try:
        rows = {}
        for label, prefix in prefixes.items():
            sub = os.path.join(fetch_dir, label)
            os.makedirs(sub, exist_ok=True)
            parts = s3util.download_prefix(s3, cfg.extracts_bucket, prefix, sub)
            if not parts:
                raise RuntimeError(f"no .gz parts at s3://{cfg.extracts_bucket}/{prefix}")
            rows[label] = _routstr.parse_ndjson_gz(parts)
            context.log.info(f"[routstr] {label}: {len(parts)} part(s), {len(rows[label])} rows")

        now_ts = int(datetime.now(timezone.utc).timestamp())
        state, data = _routstr.build_snapshot(
            rows["announce"], rows["analytics"], rows["catalog"], rows["reviews"], now_ts
        )
        problems = _routstr.check_invariants(state, data)
        if problems:
            raise RuntimeError("snapshot invariants FAILED (nothing written): " + "; ".join(problems))

        rds = aws.client("rds", cfg.aws_region)
        sm = aws.client("secretsmanager", cfg.aws_region)
        host = aurora.resolve_endpoint(rds, cfg.aurora_cluster)
        password = aurora.load_master_password(rds, sm, cfg.aurora_cluster)
        conn = pg.connect(pg.build_dsn(host, cfg.pg_port, cfg.pg_owner, cfg.pg_database), password)
        try:
            pg.apply_schema(conn, os.path.join(_REPO_ROOT, "schema", "init.sql"))
            stats = _routstr.write_pg(conn, state, data, run_id=context.run_id)
        finally:
            conn.close()
    finally:
        shutil.rmtree(fetch_dir, ignore_errors=True)

    smry = data["summary"]
    context.log.info(
        f"[routstr] snapshot {data['generated_utc']}: {smry['providers_announced']} providers "
        f"announced, {smry['providers_reporting']} reporting, "
        f"{smry['revenue_alltime_sats']} sats observed all-time "
        f"({stats['ledger_rows']} ledger rows)"
    )
    return MaterializeResult(
        metadata={
            "generated_utc": MetadataValue.text(data["generated_utc"]),
            "providers_announced": MetadataValue.int(smry["providers_announced"]),
            "providers_reporting": MetadataValue.int(smry["providers_reporting"]),
            "revenue_alltime_sats": MetadataValue.int(smry["revenue_alltime_sats"]),
            "revenue_30d_sats": MetadataValue.int(smry["revenue_30d_sats"]),
            "ledger_rows": MetadataValue.int(stats["ledger_rows"]),
            "provider_rows": MetadataValue.int(stats["provider_rows"]),
            "model_day_rows": MetadataValue.int(stats["model_day_rows"]),
            "catalog_version_rows": MetadataValue.int(stats["catalog_version_rows"]),
            "review_rows": MetadataValue.int(stats["review_rows"]),
        },
        check_results=[
            AssetCheckResult(
                check_name="snapshot_invariants",
                passed=True,
                metadata={"checks": MetadataValue.text("all cross-checks passed")},
            )
        ],
    )


# -- /git (the ngit job) ---------------------------------------------------------------------
# The Git-on-Nostr surface, NIP-34 (git collaboration). Eight extracts, all into="ngit":
# kind-30617 (repo-announcements) and kind-30618 (repo-states) with ALL versions
# (extract_addressable_all.sql - first-seen dates, the republish-loop rules and the push
# ledger read every version), kind-10317 (grasp-server-lists) with all versions
# (extract_replaceable_all.sql), the git subsets of kind-1111 (comments),
# kind-7 (reactions), kind-17 (external-content-reactions) and kind-9735 (zap-receipts)
# selected IN Athena, and the kind-5 (event-deletion-requests) reduced to its DISTINCT
# self-authored (author, coord, created_at) triples.
# Reactions, zaps and deletions are small subsets that may be empty, so their
# extract_nonempty check is a non-blocking warning (the anti-shrink gate on
# summary.archive_events catches one that empties); the roster kinds, comments and the
# external reactions keep the blocking check. The external reactions had 31 rows when the
# extract was added and the archive only grows, and the gate has no served value to compare
# with on its first run, so an empty one must stop the run rather than write zero Iris Git
# stars (pre-deploy review, 2026-10-11).
#
# Same shape as /routstr: no per-kind serving table of raw events - the compute asset
# consumes the UNLOAD parts directly (pipeline/ngit.py, the rule text is
# docs/dataset-reference/ngit-dataset-reference.md; design docs/design/ngit-design.md) and writes, in ONE
# transaction behind RELATIVE anti-shrink gates, the substrate tables ngit_repos (one row
# per announcement coordinate) and ngit_pushes (one row per state version of a kept
# repository, and since 2026-10-12 one per hashtree push, an announcement republish),
# ngit_repo_stats (one row per kept repository with its stars and zaps, since 2026-10-18 the
# source of the page's Stars and Zaps lists) plus one appended ngit_snapshot JSONB
# row (the page's document). There is no second full implementation: the independent
# cross-check (crosscheck/ngit_check.py) re-derives ngit_repos and ngit_pushes row by
# row from the reference alone; ngit_repo_stats it does not yet, and the invariants hold its
# figures to the summary, which it does.
# MEMORY: the two roster kinds are extracted with ALL versions, and the N5 announcement flooder
# and its state bot hold about two thirds of the ids (+~67k versions a month). A full parse
# made the footprint follow that version count (review 4 M2: 879 MiB peak on the 2026-09-25
# production parts), so the asset streams the two kinds through pipeline/ngit.load_roster: full tags
# only on each coordinate's current version inside the horizon and on the state rows of kept
# repositories' member coordinates, every other row reduced to (id, pubkey, created_at, d) and
# sig / content dropped. Replayed on a workstation over those parts (CPython 3.12): about 870 MiB
# peak with the full parse, about 425 MiB streamed, the document and both substrate tables
# identical. What remains follows the roster (mostly the kept repositories' ref maps);
# peak_rss_mib records it per run.
# The reduction is in-process, not in Athena: a "newest version only" cut in SQL is wrong
# unless it keeps every version of the run's own partial day (the horizon comes first).
ngit_announcements_extract = _extract_asset(
    kind="30617", into="ngit", group="ngit", name="ngit_announcements_extract",
)
ngit_states_extract = _extract_asset(
    kind="30618", into="ngit", group="ngit", name="ngit_states_extract",
)
ngit_grasp_lists_extract = _extract_asset(
    kind="10317", into="ngit", group="ngit", name="ngit_grasp_lists_extract",
)
ngit_comments_extract = _extract_asset(
    kind="1111", into="ngit", group="ngit", name="ngit_comments_extract",
)
ngit_reactions_extract = _extract_asset(
    kind="7", into="ngit", group="ngit", name="ngit_reactions_extract", allow_empty=True,
)
# Iris Git's repository stars (docs/dataset-reference/ngit-dataset-reference.md section 6.2), read
# since the nostrology-data deploy of 2026-10-11: 31 rows on the day nozzl captured the kind,
# so the blocking extract_nonempty check (see above).
ngit_external_reactions_extract = _extract_asset(
    kind="17", into="ngit", group="ngit", name="ngit_external_reactions_extract",
)
ngit_zaps_extract = _extract_asset(
    kind="9735", into="ngit", group="ngit", name="ngit_zaps_extract", allow_empty=True,
)
ngit_deletions_extract = _extract_asset(
    kind="5", into="ngit", group="ngit", name="ngit_deletions_extract", allow_empty=True,
)

# label -> (kind, may its UNLOAD part set be empty?)
_NGIT_PARTS = {
    "announce": ("30617", False),
    "states": ("30618", False),
    "grasp": ("10317", False),
    "comments": ("1111", False),
    "reactions": ("7", True),
    "external_reactions": ("17", False),
    "zaps": ("9735", True),
    "deletions": ("5", True),
}

# `::bpchar[]` matters: psycopg2 sends a Python list as text[], and against the CHAR(64)
# column Postgres would cast the COLUMN - a parallel seq scan of the kind-0 (profile-metadata)
# table every night instead of an index scan on idx_k0_pubkey (EXPLAIN in production, review 2).
_NGIT_NAMES_SQL = (
    "SELECT DISTINCT ON (pubkey) pubkey, name, display_name FROM events_kind_0_profiles "
    "WHERE pubkey = ANY(%s::bpchar[]) ORDER BY pubkey, created_at DESC"
)
# The names lookup's own ceiling (an index scan of at most NAMES_LOOKUP_MAX keys takes well
# under a second): past it Postgres cancels the query and the run continues without names.
_NGIT_NAMES_TIMEOUT = "60s"


def _ngit_display_names(context, conn, ngit_mod, rows: dict, now_ts: int) -> dict:
    """Display names for the kept maintainers (at most NAMES_LOOKUP_MAX, most kept
    coordinates first) from the kind-0 (profile-metadata) serving table the lightning job
    loads. Presentation only and outside the cross-check: ANY failure - the table missing on
    a fresh cluster, a timeout - degrades to no names with a logged warning, never to a
    failed run. The table is keyed by event id, so DISTINCT ON picks each pubkey's newest.

    The timeout is real: the owner role has no statement_timeout (review 4 n6), so the lookup
    sets its own with SET LOCAL, scoped to this read transaction (the schema was committed
    before; the rollback below ends it, and the setting with it)."""
    try:
        pubkeys = ngit_mod.kept_maintainers(
            rows["announce"], rows["states"], rows["deletions"], now_ts
        )[: ngit_mod.NAMES_LOOKUP_MAX]
        if not pubkeys:
            return {}
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = %s", (_NGIT_NAMES_TIMEOUT,))
            cur.execute(_NGIT_NAMES_SQL, (pubkeys,))
            fetched = cur.fetchall()
        conn.rollback()  # end the read transaction: no idle-in-transaction during the build
        names = {}
        for pubkey, name, display in fetched:
            v = ngit_mod.display_name(display, name)
            if v:
                names[pubkey] = v
        # none at all for a non-empty roster is a broken lookup (an empty or unloaded
        # kind-0 (profile-metadata) table), not a quiet night - the page loses every name
        log = context.log.info if names else context.log.warning
        log(f"[ngit] display names: {len(names)} of {len(pubkeys)} maintainers")
        return names
    except Exception as exc:  # noqa: BLE001 - names are presentation; degrade, never fail
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        context.log.warning(
            f"[ngit] display-name lookup failed, continuing without names: {type(exc).__name__}: {exc}"
        )
        return {}


@asset(
    name="ngit_snapshot",
    group_name="ngit",
    deps=[ngit_announcements_extract, ngit_states_extract, ngit_grasp_lists_extract,
          ngit_comments_extract, ngit_reactions_extract, ngit_external_reactions_extract,
          ngit_zaps_extract, ngit_deletions_extract],
    check_specs=[
        AssetCheckSpec(
            name="snapshot_invariants",
            asset="ngit_snapshot",
            description="Cross-checks between the document's own numbers (the ledger, hosting, "
            "recency and no-state partitions; the monthly, cumulative, weekly and comment series "
            "against their totals; sorted series never past as_of_day; every list within its cap; "
            "every top-list row on the roster) AND between the document and the substrate "
            "(ngit_repos families, repositories, leads, hosting and push counts; ngit_pushes "
            "re-summing to every push figure; ngit_repo_stats one row per repository, adding up "
            "to the summary, the document's Stars and Zaps lists its top 20). Recorded on "
            "success; a failure RAISES before any "
            "write, so the previous serving data stays live.",
            blocking=True,
        ),
        AssetCheckSpec(
            name="held_keys_absent",
            asset="ngit_snapshot",
            description="No kept announcement coordinate by a key held off the page "
            "(pipeline/ngit.py HELD_KEYS: the nostr.net catalog of other people's projects, which "
            "the ledger's N10 excludes since the operator's decision of 2026-10-12; "
            "the hold guards that rule). "
            "Recorded on success; a failure RAISES before any write, so the previous serving data "
            "stays live.",
            blocking=True,
        ),
        AssetCheckSpec(
            name="roster_growth_sane",
            asset="ngit_snapshot",
            description="NON-blocking early warning: the kept announcement coordinates first seen "
            "on as_of_day stay within max(20, 5 x the median daily count over the 30 days before "
            "it). A coordinate flood lands as repositories and never trips a version-based rule.",
            blocking=False,
        ),
        AssetCheckSpec(
            name="push_concentration_sane",
            asset="ngit_snapshot",
            description="NON-blocking early warning: no repository holds more than half of the "
            "pushes in the 7 days ending on as_of_day, once they number 100. Every push counts "
            "since the daily cap was dropped on 2026-10-02; past half, a cap is worth "
            "reconsidering (docs/design/ngit-design.md section 6).",
            blocking=False,
        ),
    ],
)
def ngit_snapshot(context: AssetExecutionContext, aurora_dial: AuroraDial) -> MaterializeResult:
    """Fetch the eight UNLOAD part sets, resolve display names, run the ngit metrics compute,
    and persist the refresh (ngit_repos + ngit_pushes + ngit_repo_stats + one ngit_snapshot
    row) in one transaction.

    Fail-loud ordering: signers -> fetch -> parse -> names -> build -> invariants ->
    anti-shrink -> write. Any failure raises BEFORE the TRUNCATEs, so a broken refresh can
    never blank the page - the web keeps serving the previous snapshot row (append-only) and
    the previous substrate.
    """
    # Lazy import, as for the other snapshot surfaces: a broken compute module can never take
    # down the other surfaces' schedule loading.
    from .pipeline import ngit as _ngit

    # ref 6.3: the verified LNURL provider signer keys, read with the /lightning loader's own
    # parser from the image-baked YAML. First, so a broken file fails the run before any fetch;
    # it raises rather than yield an empty set (which would silently zero every zap figure).
    zap_signers = _ngit.load_provider_signers()
    context.log.info(f"[ngit] zap receipt signers: {len(zap_signers)} verified LNURL provider keys")

    cfg = PipelineConfig.from_env()
    s3 = aws.client("s3", cfg.aws_region)

    prefixes = {
        label: f"k={kind}/{context.run_id}-{_run_id_suffix(kind, 'ngit')}/"
        for label, (kind, _may_be_empty) in _NGIT_PARTS.items()
    }
    os.makedirs(cfg.extract_tmp, exist_ok=True)
    need = sum(s3util.prefix_total_bytes(s3, cfg.extracts_bucket, p) for p in prefixes.values())
    free = shutil.disk_usage(cfg.extract_tmp).free
    if need and not s3util.enough_free_space(need, free):
        raise RuntimeError(
            f"fetch dir {cfg.extract_tmp} has {free // 1048576} MiB free, but the ngit "
            f"UNLOADs total {need // 1048576} MiB (need ~+20% headroom)."
        )

    fetch_dir = tempfile.mkdtemp(prefix="nostrology-ngit.", dir=cfg.extract_tmp)
    try:
        part_paths = {}
        for label, prefix in prefixes.items():
            sub = os.path.join(fetch_dir, label)
            os.makedirs(sub, exist_ok=True)
            parts = s3util.download_prefix(s3, cfg.extracts_bucket, prefix, sub)
            # The roster kinds, comments and external reactions are never empty (their
            # extract_nonempty check is blocking upstream); reactions, zaps and deletions may
            # legitimately be.
            if not parts and not _NGIT_PARTS[label][1]:
                raise RuntimeError(f"no .gz parts at s3://{cfg.extracts_bucket}/{prefix}")
            part_paths[label] = parts

        # The run instant is read BEFORE the parse, because the roster reduction applies the
        # horizon while it parses (a coordinate's current version is the newest one inside it).
        # Until 2026-09-26 it was read after the parse, so generated_utc now comes earlier by
        # the parse's duration (seconds, at most about a minute). Nothing depends on the
        # difference: as_of_day is the day before, and the cross-check pins --now to whatever
        # generated_utc the run wrote.
        now_ts = int(datetime.now(timezone.utc).timestamp())
        rows = {}
        rows["deletions"] = _ngit.parse_ndjson_gz(part_paths["deletions"])
        rows["announce"], rows["states"], roster = _ngit.load_roster(
            part_paths["announce"], part_paths["states"], rows["deletions"], now_ts)
        for label in ("grasp", "comments", "reactions", "external_reactions", "zaps"):
            rows[label] = _ngit.parse_ndjson_gz(part_paths[label])
        for label in _NGIT_PARTS:
            context.log.info(
                f"[ngit] {label}: {len(part_paths[label])} part(s), {len(rows[label])} rows")
        context.log.info(
            f"[ngit] roster reduction: {roster['announce_tagged']} of {roster['announce_rows']} "
            f"kind-30617 (repo-announcements) versions and {roster['state_tagged']} of "
            f"{roster['state_rows']} kind-30618 (repo-states) versions keep their tags "
            f"({roster['member_coords']} candidate member coordinates); peak RSS so far "
            f"{_ngit.peak_rss_mib():.0f} MiB"
        )

        rds = aws.client("rds", cfg.aws_region)
        sm = aws.client("secretsmanager", cfg.aws_region)
        host = aurora.resolve_endpoint(rds, cfg.aurora_cluster)
        password = aurora.load_master_password(rds, sm, cfg.aurora_cluster)
        conn = pg.connect(pg.build_dsn(host, cfg.pg_port, cfg.pg_owner, cfg.pg_database), password)
        try:
            pg.apply_schema(conn, os.path.join(_REPO_ROOT, "schema", "init.sql"))
            names = _ngit_display_names(context, conn, _ngit, rows, now_ts)
            state, data = _ngit.build_snapshot(
                rows["announce"], rows["states"], rows["grasp"], rows["comments"],
                rows["reactions"], rows["zaps"], rows["deletions"], names, now_ts,
                zap_signers=zap_signers, external_reaction_events=rows["external_reactions"],
            )
            problems = _ngit.check_invariants(state, data)
            if problems:
                raise RuntimeError("snapshot invariants FAILED (nothing written): " + "; ".join(problems))
            _ngit.require_no_held_keys(state)
            growth = _ngit.roster_growth(state, data)
            concentration = _ngit.push_concentration(state, data)
            stats = _ngit.write_pg(conn, state, data, run_id=context.run_id, log=context.log.warning)
        finally:
            conn.close()
    finally:
        shutil.rmtree(fetch_dir, ignore_errors=True)

    smry = data["summary"]
    # The process's peak resident set: the in_process_executor runs every step of the run in
    # this process, and the extracts are light, so this is the compute's footprint (review 4
    # M2: the trend to watch on a box shared with the nozzl producer).
    peak_rss = _ngit.peak_rss_mib()
    context.log.info(
        f"[ngit] snapshot {data['generated_utc']} (as of {data['as_of_day']}): "
        f"{smry['repos']} repositories from {smry['announcements_kept']} of "
        f"{smry['announcements_total']} announcements, {smry['maintainers']} maintainers, "
        f"{smry['repos_pushed_30d']} pushed in 30 d, {smry['pushes_total']} pushes "
        f"({smry['pushes_hashtree_30d']} of the {smry['pushes_30d']} in 30 d hashtree's), "
        f"{smry['grasp_repos']} GRASP-hosted, {smry['stars_total']} stars, {smry['stars_iris']} of "
        f"them Iris Git's from {smry['archive_events']['external_reactions']} "
        f"kind-17 (external-content-reactions) rows; {stats['repo_rows']} coordinate rows, "
        f"{stats['push_rows']} ledger rows, {stats['stats_rows']} repository stats rows; "
        f"peak RSS {peak_rss:.0f} MiB"
    )
    if not growth["passed"]:
        context.log.warning(
            f"[ngit] roster growth: {growth['first_seen_on_as_of_day']} kept coordinates first "
            f"seen on {data['as_of_day']}, above the limit of {growth['limit']} - a coordinate "
            "flood? Check ngit_repos for the new keys before the next run."
        )
    if not concentration["passed"]:
        context.log.warning(
            f"[ngit] push concentration: {concentration['top_repo']} holds "
            f"{concentration['top_pushes']} of the {concentration['pushes_7d']} pushes in the 7 days "
            f"to {data['as_of_day']} - more than half. Every push counts since 2026-10-02; "
            "consider the daily cap again (docs/design/ngit-design.md section 6)."
        )
    return MaterializeResult(
        metadata={
            "generated_utc": MetadataValue.text(data["generated_utc"]),
            "as_of_day": MetadataValue.text(data["as_of_day"]),
            "announcements_total": MetadataValue.int(smry["announcements_total"]),
            "announcements_kept": MetadataValue.int(smry["announcements_kept"]),
            "repos": MetadataValue.int(smry["repos"]),
            "maintainers": MetadataValue.int(smry["maintainers"]),
            "repos_pushed_30d": MetadataValue.int(smry["repos_pushed_30d"]),
            "pushes_total": MetadataValue.int(smry["pushes_total"]),
            "pushes_hashtree_30d": MetadataValue.int(smry["pushes_hashtree_30d"]),
            "grasp_repos": MetadataValue.int(smry["grasp_repos"]),
            "stars_total": MetadataValue.int(smry["stars_total"]),
            "stars_iris": MetadataValue.int(smry["stars_iris"]),
            "external_reactions": MetadataValue.int(smry["archive_events"]["external_reactions"]),
            "repo_rows": MetadataValue.int(stats["repo_rows"]),
            "push_rows": MetadataValue.int(stats["push_rows"]),
            "stats_rows": MetadataValue.int(stats["stats_rows"]),
            "shrink_bypassed": MetadataValue.bool(stats["shrink_bypassed"]),
            "peak_rss_mib": MetadataValue.float(float(peak_rss)),
        },
        check_results=[
            AssetCheckResult(
                check_name="snapshot_invariants",
                passed=True,
                metadata={"checks": MetadataValue.text("all cross-checks passed")},
            ),
            AssetCheckResult(
                check_name="held_keys_absent",
                passed=True,
                metadata={"held_keys": MetadataValue.int(len(_ngit.HELD_KEYS))},
            ),
            AssetCheckResult(
                check_name="roster_growth_sane",
                passed=growth["passed"],
                severity=AssetCheckSeverity.WARN,
                metadata={
                    "first_seen_on_as_of_day": MetadataValue.int(growth["first_seen_on_as_of_day"]),
                    "median_30d": MetadataValue.float(float(growth["median_30d"])),
                    "limit": MetadataValue.float(float(growth["limit"])),
                },
            ),
            AssetCheckResult(
                check_name="push_concentration_sane",
                passed=concentration["passed"],
                severity=AssetCheckSeverity.WARN,
                metadata={
                    "top_repo": MetadataValue.text(str(concentration["top_repo"])),
                    "top_pushes": MetadataValue.int(concentration["top_pushes"]),
                    "pushes_7d": MetadataValue.int(concentration["pushes_7d"]),
                    "share": MetadataValue.float(float(concentration["share"])),
                },
            ),
        ],
    )


# ── nectar (dataset publishing) ──────────────────────────────────────────────────────────────
# Publish the daily Zapstore-native app-release counts as a kind-30800 (Nectar dataset manifest)
# event (see pipeline/nectar.py). Downstream of zapstore_releases_table
# (the loaded kind-30063 (zapstore-app-releases) corpus) for LINEAGE, but in its OWN group + OWN job/schedule (definitions.py)
# so a relay hiccup or a serving blip can never red the core /zapstore refresh or hold its dial.
#
#   nectar_native_releases_parquet  --(query native counts -> Parquet -> S3 PUT)-->  $NECTAR_BUCKET
#       │   (+ blocking checks: dataset_nonempty, blob_roundtrip = serves back and hashes match)
#       ▼
#   nectar_manifest_published       --(sign the kind-30800 (Nectar dataset manifest) in-process -> publish)-->  relays
#
# The two assets hand off via a stable "latest" pointer on the Dagster scratch volume (the repo
# can't read MaterializeResult metadata back as an asset input) — so `materialize parquet+` builds
# and publishes THIS run, while `materialize manifest`-only re-signs+republishes the latest build.
_NECTAR_DEFAULT_STATE_DIR = "/mnt/nozzl-dagster/nectar"
_NECTAR_PUBLISH_TIMEOUT_S = 20


def _nectar_env(key: str, default: str) -> str:
    return os.environ.get(key, default)


@asset(
    name="nectar_native_releases_parquet",
    group_name="nectar",
    deps=[zapstore_releases_table],
    check_specs=[
        AssetCheckSpec(
            name="dataset_nonempty",
            asset="nectar_native_releases_parquet",
            description="The built dataset has >= 1 day row (a 0-row build is the empty / broken-classification failure mode).",
            blocking=True,
        ),
        AssetCheckSpec(
            name="blob_roundtrip",
            asset="nectar_native_releases_parquet",
            description="The uploaded blob fetches back from its blossom URL and hashes to the value we are about to sign (serve-before-verify gate).",
            blocking=True,
        ),
    ],
)
def nectar_native_releases_parquet(context: AssetExecutionContext, aurora_dial: AuroraDial) -> MaterializeResult:
    """Build the daily native-release-count Parquet from the loaded kind-30063 (zapstore-app-releases) table, upload it to
    the private Nectar bucket ($NECTAR_BUCKET), and verify it serves back at its blossom URL.

    aurora_dial is required so this runs inside the run's dial bracket (it reads Aurora). The two
    blocking checks gate the downstream publish: a 0-row build or a blob that doesn't serve back
    (verified byte-for-byte) stops the manifest from ever being signed.
    """
    cfg = PipelineConfig.from_env()
    bucket = _nectar_env("NECTAR_BUCKET", "")
    if not bucket:
        raise MissingConfig("set NECTAR_BUCKET: the Nectar blob bucket has no default")
    manifest_base = _nectar_env("NECTAR_BLOSSOM_BASE", _nectar.DEFAULT_BLOSSOM_BASE)
    verify_base = _nectar_env("NECTAR_VERIFY_BASE", manifest_base)  # on-box: same host, resolved via extra_hosts
    state_dir = _nectar_env("NECTAR_STATE_DIR", _NECTAR_DEFAULT_STATE_DIR)

    rds = aws.client("rds", cfg.aws_region)
    sm = aws.client("secretsmanager", cfg.aws_region)
    host = aurora.resolve_endpoint(rds, cfg.aurora_cluster)
    password = aurora.load_master_password(rds, sm, cfg.aurora_cluster)
    conn = pg.connect(pg.build_dsn(host, cfg.pg_port, cfg.pg_owner, cfg.pg_database), password)
    try:
        artifact = _nectar.build_artifact(conn, _REPO_ROOT)
    finally:
        conn.close()

    s3 = aws.client("s3", cfg.aws_region)
    _nectar.put_blob(s3, bucket, artifact)
    manifest_url = _nectar.blob_url(manifest_base, artifact.sha256)
    verify_url = _nectar.blob_url(verify_base, artifact.sha256)
    roundtrip_ok, actual = _nectar.verify_roundtrip(verify_url, artifact.sha256)
    nonempty = artifact.rows >= 1

    # Only advance the pointer the manifest asset reads when the build is good AND actually serves —
    # a bad build/upload must not leave a pointer to an unservable or empty blob for a later
    # manifest-only run to republish.
    if nonempty and roundtrip_ok:
        _nectar.write_pointer(
            state_dir, artifact, manifest_url, built_at=int(datetime.now(timezone.utc).timestamp())
        )
    context.log.info(
        f"[nectar] built {artifact.rows} rows ({artifact.day_min}..{artifact.day_max}), "
        f"{len(artifact.parquet)} B, sha256={artifact.sha256[:12]}…; "
        f"roundtrip={'OK' if roundtrip_ok else 'FAIL'} (actual={actual[:12]}…)"
    )
    return MaterializeResult(
        metadata={
            "rows": MetadataValue.int(artifact.rows),
            "day_min": MetadataValue.text(artifact.day_min),
            "day_max": MetadataValue.text(artifact.day_max),
            "bytes": MetadataValue.int(len(artifact.parquet)),
            "size": MetadataValue.text(_human_bytes(len(artifact.parquet))),
            "sha256": MetadataValue.text(artifact.sha256),
            "blob_url": MetadataValue.text(manifest_url),
        },
        check_results=[
            AssetCheckResult(
                check_name="dataset_nonempty",
                passed=nonempty,
                metadata={"rows": MetadataValue.int(artifact.rows)},
            ),
            AssetCheckResult(
                check_name="blob_roundtrip",
                passed=roundtrip_ok,
                metadata={
                    "expected_sha256": MetadataValue.text(artifact.sha256),
                    "actual_sha256": MetadataValue.text(actual),
                    "verify_url": MetadataValue.text(verify_url),
                },
            ),
        ],
    )


@asset(name="nectar_manifest_published", group_name="nectar", deps=[nectar_native_releases_parquet])
def nectar_manifest_published(context: AssetExecutionContext) -> MaterializeResult:
    """Sign the kind-30800 (Nectar dataset manifest) event (in-process; the nsec never hits argv/logs) over the
    latest built blob, and publish it to the configured relays. Succeeds if >= 1 relay accepts;
    fails loud if none do. No aurora_dial (touches no DB) so a manifest-only re-materialize doesn't
    needlessly wake the cluster; the relay publish is timeout-bounded so a hung relay can't wedge.
    """
    state_dir = _nectar_env("NECTAR_STATE_DIR", _NECTAR_DEFAULT_STATE_DIR)
    relays = [r.strip() for r in _nectar_env("NECTAR_RELAYS", ",".join(_nectar.DEFAULT_RELAYS)).split(",") if r.strip()]
    nsec = os.environ.get("NECTAR_SIGNING_NSEC")
    if not nsec:
        raise RuntimeError(
            "NECTAR_SIGNING_NSEC is not set — the signing key must be injected from the state-volume "
            ".secrets, which the deployment passes to the Dagster containers."
        )

    ptr = _nectar.read_pointer(state_dir)
    event = _nectar.build_signed_manifest(nsec, ptr["sha256"], ptr["url"])
    results = _nectar.publish_to_relays(event, relays, timeout=_NECTAR_PUBLISH_TIMEOUT_S, log=context.log.info)
    accepted = [relay for relay, res in results.items() if res["accepted"]]
    if not accepted:
        raise RuntimeError(f"no relay accepted the manifest (event {event['id'][:12]}…; results={results})")
    context.log.info(f"[nectar] published {event['id'][:12]}… to {len(accepted)}/{len(relays)} relay(s)")
    return MaterializeResult(
        metadata={
            "event_id": MetadataValue.text(event["id"]),
            "coordinate": MetadataValue.text(_nectar.coordinate(event["pubkey"])),
            "created_at": MetadataValue.int(event["created_at"]),
            "sha256": MetadataValue.text(ptr["sha256"]),
            "blob_url": MetadataValue.text(ptr["url"]),
            "relays_accepted": MetadataValue.int(len(accepted)),
            "relays_total": MetadataValue.int(len(relays)),
            "relays": MetadataValue.json(results),
        }
    )

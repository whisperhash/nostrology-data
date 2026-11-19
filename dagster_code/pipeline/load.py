"""The nativized per-kind load - a boto3 port of the old bash load, MINUS the materialized-view
refresh (lifted into the relays_mv asset).

Sequence: resolve the Aurora endpoint + master password -> build a password-free DSN -> apply the
idempotent schema (also the cluster-resume gate) -> free-space preflight -> fetch the extract's S3
prefix to real-disk scratch -> run the loader (a Python subprocess: --truncate --min-rows <floor>
--no-refresh-mv) -> count the result -> clean up. The password is fetched fresh and handed to the
loader via PGPASSWORD — never logged, never in argv.

Dagster-vs-Dagster is serialized by the QueuedRunCoordinator (max_concurrent_runs: 1), and nothing
else loads these tables any more, so there is no other lock.
"""

import glob
import os
import shutil
import sys
import tempfile
import time

from . import aurora, aws, pg, proc, s3util
from .kinds import kind_to_table, min_rows_for

# A load that splits its input across two tables, so rows_loaded alone understates
# what it read. kind-32267 (zapstore-app-catalog) sorts the listings that are not
# Zapstore apps into their own table (the rule is zapstore/relay's own Validate();
# see loader/test_zapstore_conformance.py), which is why the catalog's row count
# steps DOWN the first time this ships. Counting the companion and reporting it as
# asset metadata is what makes that step self-explaining a year from now.
_SORTED_OUT_TABLE = {
    "events_kind_32267_app_catalog": "events_kind_32267_app_catalog_excluded",
}

_LOADER = "loader/load_jsonl_exports_into_postgres.py"
# Warn if the mount is gone: these kinds' loaders filter on the blocklist, and with no lists they
# load UNFILTERED, exit 0 and a green run. Advisory, not a contract: the full kind-9735 (zap-receipts)
# load does not filter on it, and its zapstore subset does, by recipient, in the same job as
# kind-30267 (zapstore-app-stacks), which warns.
_BLOCKLIST_KINDS = {0, 1, 3, 9321, 10002, 10019, 30267}


def _blocklist_present(repo_root: str) -> bool:
    # Recursive, like the loader's own os.walk: the lists live in per-kind subdirectories,
    # so a flat glob found none and warned "loading UNFILTERED" on every load.
    return bool(glob.glob(os.path.join(repo_root, "blocklists", "**", "*.txt"), recursive=True))


def run_load(context, cfg, *, kind, run_id: str, into: str = "", repo_root: str, timeout: int | None = None) -> dict:
    """Fetch the kind's extract prefix and load it into Aurora. Returns {rows, min_rows, copy_s}.

    Raises on: insufficient scratch space, an empty/missing S3 prefix (stale RUN_ID — the
    never-wipe guard), or a non-zero loader exit (exit 2 = MIN_ROWS anti-shrink abort)."""
    k = int(kind)
    min_rows = min_rows_for(kind, into)
    table = kind_to_table(kind, into)

    rds = aws.client("rds", cfg.aws_region)
    sm = aws.client("secretsmanager", cfg.aws_region)
    host = aurora.resolve_endpoint(rds, cfg.aurora_cluster)
    password = aurora.load_master_password(rds, sm, cfg.aurora_cluster)
    dsn = pg.build_dsn(host, cfg.pg_port, cfg.pg_owner, cfg.pg_database)

    # Apply the idempotent schema first (run.sh:738). This also makes the run's FIRST connection
    # (pg.build_dsn's connect_timeout=90 says what it waits through, as the bash wait_pg_ready
    # did), and creates any newly-added table / MV / index so the TRUNCATE+load and the
    # downstream MV refresh have their objects present.
    schema_conn = pg.connect(dsn, password)
    try:
        pg.apply_schema(schema_conn, os.path.join(repo_root, "schema", "init.sql"))
    finally:
        schema_conn.close()
    context.log.info("[load] schema applied (idempotent)")

    s3 = aws.client("s3", cfg.aws_region)
    prefix = f"k={kind}/{run_id}/"

    # Free-space preflight on the real-disk scratch (NOT /tmp tmpfs) before the fetch.
    os.makedirs(cfg.extract_tmp, exist_ok=True)
    need = s3util.prefix_total_bytes(s3, cfg.extracts_bucket, prefix)
    free = shutil.disk_usage(cfg.extract_tmp).free
    if need and not s3util.enough_free_space(need, free):
        raise RuntimeError(
            f"fetch dir {cfg.extract_tmp} has {free // 1048576} MiB free, but the kind-{kind} UNLOAD "
            f"is {need // 1048576} MiB (need ~+20% headroom). Point NOSTROLOGY_EXTRACT_TMP at a "
            "larger REAL-disk dir — NOT /tmp (a ~1.9 GiB tmpfs)."
        )

    fetch_dir = tempfile.mkdtemp(prefix="nostrology-extract.", dir=cfg.extract_tmp)
    try:
        context.log.info(f"[load] fetching UNLOAD parts for k={kind} run={run_id} -> {fetch_dir}")
        parts = s3util.download_prefix(s3, cfg.extracts_bucket, prefix, fetch_dir)
        if not parts:
            raise RuntimeError(
                f"no .gz parts at s3://{cfg.extracts_bucket}/{prefix} - the extract output is "
                "missing/expired (a stale or never-run RUN_ID), or Athena's UNLOAD stopped writing "
                "gzip. Refusing to load (a TRUNCATE+0-row load over a live serving table is exactly "
                "what the never-wipe guard prevents)."
            )
        context.log.info(f"[load] fetched {len(parts)} part(s)")
        if k in _BLOCKLIST_KINDS and not _blocklist_present(repo_root):
            context.log.warning(
                f"[load] kind-{kind} applies a blocklist but {repo_root}/blocklists has no .txt files "
                "— loading UNFILTERED. Bind-mount the synced blocklist before a parity load."
            )

        # The loader is invoked as a Python subprocess (not bash): --truncate + --min-rows is the
        # anti-shrink gate (checked BEFORE TRUNCATE, exit 2 on a sub-floor); --no-refresh-mv keeps
        # MV refresh OUT of the load (the MV assets own it). PGPASSWORD (env) keeps the secret out
        # of argv; the DSN is password-free.
        argv = [
            sys.executable, _LOADER,
            "--kind", str(k), "--truncate", "--min-rows", str(min_rows), "--no-refresh-mv",
        ]
        if into:
            argv += ["--into", into]
        argv += ["--db-url", dsn, "--input", *parts]

        env = dict(os.environ)
        env["PGPASSWORD"] = password
        context.log.info(f"[load] loading kind={kind}{' into=' + into if into else ''} (min_rows={min_rows}, parts={len(parts)})")
        t0 = time.time()
        rc = proc.run_streamed(context, argv, env=env, cwd=repo_root, timeout=timeout)
        copy_s = int(time.time() - t0)
        if rc != 0:
            raise RuntimeError(
                f"loader exited {rc} (kind={kind}{' into=' + into if into else ''}) — see the step log. "
                "Exit 2 = MIN_ROWS anti-shrink abort (the live table is preserved, NOT truncated)."
            )
    finally:
        shutil.rmtree(fetch_dir, ignore_errors=True)

    rows = 0
    sorted_out = None
    if table:
        conn = pg.connect(dsn, password)
        try:
            rows = pg.count_rows(conn, table)
            companion = _SORTED_OUT_TABLE.get(table)
            if companion:
                sorted_out = pg.count_rows(conn, companion)
        finally:
            conn.close()
    extra = f" sorted_out={sorted_out}" if sorted_out is not None else ""
    context.log.info(
        f"[load] done kind={kind}{' into=' + into if into else ''} rows={rows}{extra} "
        f"copy_s={copy_s}s run={run_id}")
    stats = {"rows": rows, "min_rows": min_rows, "copy_s": copy_s}
    if sorted_out is not None:
        stats["sorted_out"] = sorted_out
    return stats

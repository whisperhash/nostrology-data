"""Definitions wiring for the nostrology code location.

Loaded by the gRPC code server as `--module-name dagster_code.definitions` (PYTHONPATH=/app).

Job model: ONE whole-subgraph job per SURFACE, each on the in_process_executor — so the
run-scoped AuroraDial resource is initialized ONCE per run and shared across every step (under
the multiprocess executor each step would re-init the resource and thrash the dial). A surface's
job is the unit that holds the Aurora dial for one bracket and runs the surface's kinds + post-
steps together. The daily schedules ship RUNNING, apart from `nectar_daily`, which ships
STOPPED: Dagster has been the refresh trigger since the Stage-2 cutover (2026-06-30), and
Stage-3 Phase 5 deleted the `refresh-*` verbs from run.sh. The break-glass is the deployment's
re-run action where the surface has one, or Materialize in the operator's Dagit UI. The schedules
serialize through the QueuedRunCoordinator (`max_concurrent_runs: 1`, dagster.yaml), so the
staggered fire times only set ordering - two surfaces never hold the Aurora dial concurrently.
"""

from dagster import (
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    DefaultScheduleStatus,
    MetadataValue,
    ScheduleDefinition,
    define_asset_job,
    in_process_executor,
)

from .assets import (
    lightning_mv,
    marketplaces_deletions_extract,
    marketplaces_listings_extract,
    marketplaces_shipping_extract,
    marketplaces_snapshot,
    lightning_profiles_extract,
    lightning_profiles_table,
    lightning_signers,
    lightning_zaps_extract,
    lightning_zaps_table,
    nectar_manifest_published,
    nectar_native_releases_parquet,
    ngit_announcements_extract,
    ngit_comments_extract,
    ngit_deletions_extract,
    ngit_external_reactions_extract,
    ngit_grasp_lists_extract,
    ngit_reactions_extract,
    ngit_snapshot,
    ngit_states_extract,
    ngit_zaps_extract,
    nutzaps_active_users,
    nutzaps_configs_extract,
    nutzaps_configs_table,
    nutzaps_mint_classes,
    nutzaps_payments_extract,
    nutzaps_payments_table,
    relays_extract,
    relays_mv,
    relays_table,
    routstr_analytics_extract,
    routstr_announcements_extract,
    routstr_catalog_extract,
    routstr_reviews_extract,
    routstr_snapshot,
    zapstore_catalog_extract,
    zapstore_catalog_table,
    zapstore_releases_extract,
    zapstore_releases_table,
    zapstore_stacks_extract,
    zapstore_stacks_table,
    zapstore_zaps_extract,
    zapstore_zaps_table,
)
from .pipeline import lineage
from .resources import AuroraDial
from .sensors import (
    last_run_failure_sensor,
    last_run_success_sensor,
    make_discord_failure_sensor,
    make_discord_recovery_sensor,
    make_freshness_sensor,
)

_RELAYS_ASSETS = [relays_extract, relays_table, relays_mv]
_NUTZAPS_ASSETS = [
    nutzaps_payments_extract,
    nutzaps_payments_table,
    nutzaps_configs_extract,
    nutzaps_configs_table,
    nutzaps_mint_classes,
    nutzaps_active_users,
]
_LIGHTNING_ASSETS = [
    lightning_profiles_extract,
    lightning_profiles_table,
    lightning_zaps_extract,
    lightning_zaps_table,
    lightning_signers,
    lightning_mv,
]
_ZAPSTORE_ASSETS = [
    zapstore_catalog_extract,
    zapstore_catalog_table,
    zapstore_releases_extract,
    zapstore_releases_table,
    zapstore_stacks_extract,
    zapstore_stacks_table,
    zapstore_zaps_extract,
    zapstore_zaps_table,
]
_MARKETPLACES_ASSETS = [
    marketplaces_listings_extract,
    marketplaces_shipping_extract,
    marketplaces_deletions_extract,
    marketplaces_snapshot,
]
_ROUTSTR_ASSETS = [
    routstr_announcements_extract,
    routstr_analytics_extract,
    routstr_catalog_extract,
    routstr_reviews_extract,
    routstr_snapshot,
]
_NGIT_ASSETS = [
    ngit_announcements_extract,
    ngit_states_extract,
    ngit_grasp_lists_extract,
    ngit_comments_extract,
    ngit_reactions_extract,
    ngit_external_reactions_extract,
    ngit_zaps_extract,
    ngit_deletions_extract,
    ngit_snapshot,
]
# Nectar publish is its OWN surface (not folded into _ZAPSTORE_ASSETS) so a relay/serving blip
# can't fail the core /zapstore data refresh or hold its Aurora dial across the network-bound
# publish. Lineage-linked to zapstore_releases_table via the parquet asset's deps.
_NECTAR_ASSETS = [
    nectar_native_releases_parquet,
    nectar_manifest_published,
]

relays_refresh_job = define_asset_job(
    name="relays_refresh",
    selection=AssetSelection.assets(*_RELAYS_ASSETS),
    executor_def=in_process_executor,
    description="Whole-surface /relays refresh: k=10002 extract -> table -> relays_mv (the 3 "
    "leaderboard MVs, refreshed CONCURRENTLY). One Aurora dial bracket per run.",
)

nutzaps_refresh_job = define_asset_job(
    name="nutzaps_refresh",
    selection=AssetSelection.assets(*_NUTZAPS_ASSETS),
    executor_def=in_process_executor,
    description="Whole-surface /nutzaps refresh: k=9321 (payments) + k=10019 (configs) extract -> table, "
    "then classify every loaded mint URL (nutzap_mint_classes: test and private mints excluded, #9), "
    "then rebuild the Overview-MAU counts from the loaded 9321 table. One Aurora dial bracket per run.",
)

lightning_refresh_job = define_asset_job(
    name="lightning_refresh",
    selection=AssetSelection.assets(*_LIGHTNING_ASSETS),
    executor_def=in_process_executor,
    description="Whole-surface /lightning refresh: k=0 (profiles) + k=9735 (zap receipts) extract -> table, "
    "plus the provider→signer map from the baked YAML, then lightning_mv (provider leaderboard + daily "
    "provider-grain MVs, refreshed CONCURRENTLY). The two largest loads. One Aurora dial bracket per run.",
)

zapstore_refresh_job = define_asset_job(
    name="zapstore_refresh",
    selection=AssetSelection.assets(*_ZAPSTORE_ASSETS),
    executor_def=in_process_executor,
    description="Whole-surface /zapstore refresh: app catalog (32267) + releases (30063) + stacks (30267) "
    "+ the 9735→zapstore zap subset (INTO=zapstore). One Aurora dial bracket per run.",
)

marketplaces_refresh_job = define_asset_job(
    name="marketplaces_refresh",
    selection=AssetSelection.assets(*_MARKETPLACES_ASSETS),
    executor_def=in_process_executor,
    description="Whole-surface /marketplaces refresh: kind-30402 (markets-product-listings) + "
    "kind-30406 (markets-shipping-options) all-versions extracts + the kind-5 (event-deletion-requests) "
    "self-authored request aggregation, then the metrics compute writing marketplaces_listings + one "
    "marketplaces_snapshot row. One Aurora dial bracket per run.",
)

routstr_refresh_job = define_asset_job(
    name="routstr_refresh",
    selection=AssetSelection.assets(*_ROUTSTR_ASSETS),
    executor_def=in_process_executor,
    description="Whole-surface /routstr refresh: kind-38421 (routstr-provider-announcements) + "
    "kind-38423 (routstr-model-catalog) + kind-38425 (routstr-provider-reviews) all-versions "
    "extracts + the kind-38422 (routstr-analytics-snapshots) last-per-(provider, day) extract, "
    "then the ledger-reconstruction compute writing routstr_providers + routstr_provider_days + "
    "one routstr_snapshot row. One Aurora dial bracket per run.",
)

ngit_refresh_job = define_asset_job(
    name="ngit_refresh",
    selection=AssetSelection.assets(*_NGIT_ASSETS),
    executor_def=in_process_executor,
    description="Whole-surface /git refresh: kind-30617 (repo-announcements) + "
    "kind-30618 (repo-states) + kind-10317 (grasp-server-lists) all-versions extracts, the "
    "git subsets of kind-1111 (comments), kind-7 (reactions), "
    "kind-17 (external-content-reactions) and kind-9735 (zap-receipts), "
    "and the kind-5 (event-deletion-requests) self-authored aggregation, then the "
    "noise-ledger + push-ledger compute writing ngit_repos + ngit_pushes + ngit_repo_stats + "
    "one ngit_snapshot row. One Aurora dial bracket per run.",
)

nectar_refresh_job = define_asset_job(
    name="nectar_refresh",
    selection=AssetSelection.assets(*_NECTAR_ASSETS),
    executor_def=in_process_executor,
    description="Nectar dataset publish: build the daily Zapstore-native app-release-count Parquet "
    "(downstream of zapstore_releases_table), upload it + verify it serves back, then sign+publish "
    "the kind-30800 (Nectar dataset manifest) event to relays. Isolated from /zapstore so a "
    "relay/serving blip can't red it.",
)

# Stage-2 cutover: schedules ship RUNNING - Dagster is the authoritative refresh trigger. Run order
# is relays -> lightning -> zapstore -> nutzaps -> marketplaces -> routstr -> ngit, staggered 30 min
# apart from 00:15 UTC to 03:15 UTC (the block ends at about 03:45 UTC).
#
# THE BLOCK MOVED PAST MIDNIGHT UTC on 2026-09-08 (it used to start 20:00 UTC). Everything these
# jobs compute and publish is "as of midnight UTC", so running BEFORE that boundary meant every
# surface was published against a day that had not closed yet. Starting at 00:15 the jobs read a
# COMPLETE just-closed UTC day. Completeness is safe to rely on because each extract asset flushes
# the nozzl WAL to S3 first and fails loud rather than reading a short slice (pipeline/flush.py),
# so the 12h chunk_max_buffer_seconds is NOT a bound on what the extract sees.
#
# 00:15 rather than 00:00 leaves margin after the deployment's backup windows (its volume
# snapshots and its database backup), which all start before midnight UTC, so every recovery
# point is PRE-load. Do NOT move relays_daily earlier without revisiting that. The stagger only sets deterministic ORDERING; the
# QueuedRunCoordinator (max_concurrent_runs: 1) is what actually serializes them, so an overrun never
# makes two surfaces contend for the dial.
# (default_status=RUNNING governs a schedule ONLY while it has no stored instigator state; ANY prior
# UI/CLI toggle — start OR stop — persists a state that then overrides the flag on reload. Verified
# 2026-06-30 via the run store: ZERO SCHEDULE rows existed (only the 3 sensors), i.e. none were ever
# toggled, so this flip starts all four cleanly. Post-deploy "all four read RUNNING" check stays
# mandatory — see the design doc's Stage-2 deploy note.)
relays_daily = ScheduleDefinition(
    name="relays_daily",
    job=relays_refresh_job,
    cron_schedule="15 0 * * *",  # 00:15 UTC, first past the midnight-UTC boundary
    default_status=DefaultScheduleStatus.RUNNING,
)
lightning_daily = ScheduleDefinition(
    name="lightning_daily",
    job=lightning_refresh_job,
    cron_schedule="45 0 * * *",  # 00:45 UTC
    default_status=DefaultScheduleStatus.RUNNING,
)
zapstore_daily = ScheduleDefinition(
    name="zapstore_daily",
    job=zapstore_refresh_job,
    cron_schedule="15 1 * * *",  # 01:15 UTC
    default_status=DefaultScheduleStatus.RUNNING,
)
nutzaps_daily = ScheduleDefinition(
    name="nutzaps_daily",
    job=nutzaps_refresh_job,
    cron_schedule="45 1 * * *",  # 01:45 UTC
    default_status=DefaultScheduleStatus.RUNNING,
)
# Marketplaces runs late (02:15 UTC), after nutzaps_daily (01:45) - same
# serialization story as the rest: the stagger sets ordering, the QueuedRunCoordinator
# (max_concurrent_runs: 1) serializes. New schedule, never toggled -> no stored instigator
# state -> default_status=RUNNING takes effect cleanly on deploy (same verified mechanism
# as the Stage-2 cutover note above).
marketplaces_daily = ScheduleDefinition(
    name="marketplaces_daily",
    job=marketplaces_refresh_job,
    cron_schedule="15 2 * * *",  # 02:15 UTC
    default_status=DefaultScheduleStatus.RUNNING,
)
# Routstr runs at 02:45 UTC, after marketplaces_daily (02:15) and before
# ngit_daily (03:15, the last of the block) - same
# serialization story: the stagger sets ordering, the QueuedRunCoordinator
# (max_concurrent_runs: 1) serializes. New schedule, never toggled -> no stored instigator
# state -> default_status=RUNNING takes effect cleanly on deploy.
routstr_daily = ScheduleDefinition(
    name="routstr_daily",
    job=routstr_refresh_job,
    cron_schedule="45 2 * * *",  # 02:45 UTC
    default_status=DefaultScheduleStatus.RUNNING,
)
# ngit runs last (03:15 UTC), after routstr_daily (02:45) - same serialization
# story: the stagger sets ordering, the QueuedRunCoordinator (max_concurrent_runs: 1)
# serializes. The nightly block therefore ends at about 03:45 UTC, which is where the "never
# deploy the Dagster stack inside the nightly window" rule now ends. Its display names come from the
# kind-0 (profile-metadata) table lightning_daily (00:45) has loaded by then. New schedule,
# never toggled -> no stored instigator state -> default_status=RUNNING takes effect cleanly
# on deploy.
ngit_daily = ScheduleDefinition(
    name="ngit_daily",
    job=ngit_refresh_job,
    cron_schedule="15 3 * * *",  # 03:15 UTC
    default_status=DefaultScheduleStatus.RUNNING,
)
# Nectar runs after zapstore_daily (01:15) so it reads the freshly-loaded
# kind-30063 (zapstore-app-releases) table. Ships STOPPED - unlike the surface schedules - because the publish targets a public serving path
# + relays: go-live is a deliberate switch, flipped to RUNNING (or started in the UI) once
# blossom.nostrolo.gy is deployed and the relay target is confirmed. The manual
# `dagster asset materialize --select nectar_native_releases_parquet+` path works regardless of this.
nectar_daily = ScheduleDefinition(
    name="nectar_daily",
    job=nectar_refresh_job,
    cron_schedule="30 1 * * *",  # 01:30 UTC, just after zapstore_daily
    default_status=DefaultScheduleStatus.STOPPED,
)


_DATA_ASSETS = [*_RELAYS_ASSETS, *_NUTZAPS_ASSETS, *_LIGHTNING_ASSETS, *_ZAPSTORE_ASSETS,
                *_MARKETPLACES_ASSETS, *_ROUTSTR_ASSETS, *_NGIT_ASSETS, *_NECTAR_ASSETS]


def _consumer_spec(c: dict) -> AssetSpec:
    """One consumer of the data (a page, a landing panel, an MCP domain) as a
    non-materializable asset downstream of what it reads, so the Dagster asset graph shows who
    reads what, and the Discord failure post can say who a failed run leaves stale."""
    meta = {"label": c["label"], "reads": ", ".join(c["reads"])}
    if c.get("url"):
        meta["url"] = MetadataValue.url(c["url"])
    return AssetSpec(
        key=AssetKey(["consumer", *c["key"]]),
        deps=[AssetKey.from_user_string(d) for d in lineage.consumer_deps(c)],
        group_name="consumers",
        kinds={c["key"][0]},
        description=f"{c['key'][0]} {c['label']}: reads what the run that owns each upstream asset writes.",
        metadata=meta,
    )


_CONSUMER_SPECS = [_consumer_spec(c) for c in lineage.CONSUMERS]

# The lineage graph the Discord sensors walk: every data asset's deps, plus every consumer's.
_UPSTREAM = {k.to_user_string(): {d.to_user_string() for d in deps}
             for a in _DATA_ASSETS for k, deps in a.asset_deps.items()}
_UPSTREAM.update(lineage.consumer_upstream())
_CONSUMER_IDS = [lineage.consumer_id(c["key"]) for c in lineage.CONSUMERS]
_SCHEDULES = [relays_daily, lightning_daily, zapstore_daily, nutzaps_daily, marketplaces_daily,
              routstr_daily, ngit_daily, nectar_daily]

defs = Definitions(
    assets=[*_DATA_ASSETS, *_CONSUMER_SPECS],
    jobs=[relays_refresh_job, nutzaps_refresh_job, lightning_refresh_job, zapstore_refresh_job,
          marketplaces_refresh_job, routstr_refresh_job, ngit_refresh_job, nectar_refresh_job],
    schedules=_SCHEDULES,
    sensors=[
        last_run_success_sensor,
        last_run_failure_sensor,
        make_discord_failure_sensor(
            _UPSTREAM, _CONSUMER_IDS, lineage.consumer_labels(),
            {s.job_name: s.cron_schedule for s in _SCHEDULES}),
        make_discord_recovery_sensor(_UPSTREAM, _CONSUMER_IDS, lineage.consumer_labels()),
        # The watchdog over the per-job last-run files: the seven RUNNING surface schedules
        # (nectar_daily ships STOPPED, so its file is legitimately absent and is not watched).
        make_freshness_sensor([
            relays_refresh_job.name, lightning_refresh_job.name, zapstore_refresh_job.name,
            nutzaps_refresh_job.name, marketplaces_refresh_job.name, routstr_refresh_job.name,
            ngit_refresh_job.name,
        ]),
    ],
    resources={"aurora_dial": AuroraDial()},
)

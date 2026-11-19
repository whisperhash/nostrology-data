# Dagster orchestration of the nostrology-data loads (the living design doc)

> **Scope note (read first).** This is the **living design doc for the whole Dagster orchestration**,
> kept current through Stage 1. **As of 2026-06-30 all four webapp surfaces are deployed and
> verified live in the deployment** as surface-scoped jobs: `relays_refresh` (k=10002),
> `nutzaps_refresh` (9321 + 10019 + MAU), `lightning_refresh` (0 + 9735 + signer map),
> `zapstore_refresh` (32267 + 30063 + 30267 + the 9735→zapstore subset). The schedules ship
> **STOPPED** - the legacy `refresh-*` path stays authoritative until the Stage-2 cutover.
> The text below was
> originally written for the Stage-0 /relays slice and generalized in place — where it still
> says "Stage 0", read it as the per-surface model the other three surfaces also follow.

This documents the Dagster migration for the nostrology-data loads, for subsequent sessions and
the operator. Its decisions were reviewed at every stage (Stage-0: three review passes,
incl. the cross-repo one that caught a `.dockerignore` blocker - the image `COPY . /app` was
shipping without `scripts/`+`deploy/`, i.e. without `run.sh` - and shaped what a deployment
provides, below; Stage-1: a fresh-context pass per surface batch).

## Why

The loads are driven by a ~1320-line bash `run.sh`, triggered by the deployment's deploy
workflow. The pain: no run history, no per-step duration trends, logs
scattered (the status JSON is last-run-only; durations only as `TIMING` log lines). We're moving
orchestration to **Dagster** (asset/data-centric — declare data artifacts + lineage), via a
**strangler-fig** path: Dagster grows *around* `run.sh` (assets shell out to it at first), with a
working pipeline at every stage, until the bash refresh verbs are dead wood (Stage 2 cutover).

Stage 0 is ONE end-to-end vertical slice - **kind-10002 (relay-list-metadata), the NIP-65
(relay list metadata) relay lists behind the `/relays` surface** - proving the whole machine.
kind-10002 (relay-list-metadata) is a replaceable kind (`extract_replaceable_latest.sql`)
with three `CONCURRENTLY`-refreshed MVs, so it exercises every shape at minimal blast radius.
The legacy `refresh-*` path stays authoritative; the Dagster schedule ships **STOPPED**.

## Architecture (Docker)

Five services, four of them from **one image** (`deploy/dagster/Dockerfile.dagster`) and the run
store on stock `postgres:16`, so version parity is trivial (the "pin all containers to one Dagster
version" rule):

| service | role | who reaches it |
|---|---|---|
| `dagster-postgres` | run/event/schedule store + the SELECT-only `dagster_ro` role (NOT Aurora: chosen when Aurora still scaled to zero, which the daemon's constant polling would have defeated) | the Dagster services; read-only accounts as `dagster_ro` |
| `dagster-code` | the gRPC code location (our `Definitions`) — the **only** image rebuilt on a DAG change; **runs EXECUTE here** (DefaultRunLauncher launches against this gRPC server's process), so it carries the full step env | internal :4000 |
| `dagster-daemon` | schedules/sensors/run-queue; **launches** runs (does NOT execute them) | internal |
| `dagster-webserver` | operator UI, mutation-enabled | operators only |
| `dagster-webserver-ro` | a read-only UI/GraphQL, `--read-only` | read-only accounts |

All Dagster artifacts live on a dedicated volume (pgdata, compute logs, `last-run.json`,
`daemon-heartbeat.json`, extract scratch). No docker socket is mounted -
the load runs the loader **in-container** (`NOSTROLOGY_LOADER_INPROC=1`).

### The asset graph (`dagster_code/`) — surface-scoped (Stage 1)

A **surface** (`/relays`, `/nutzaps`, …) is one job/run holding the Aurora dial for a single
bracket. Within a surface, each kind is an `extract → table` chain built by the `_kind_chain`
factory over the shared run.sh seam verbs; cross-kind aggregations are downstream post-step
assets.

```
/relays  (job relays_refresh)
  relays_extract(10002)  --(refresh-extract-only: flush → diff-register → Athena UNLOAD)-->  s3://…/k=10002/<RUN_ID>/
      │  + blocking asset check `extract_nonempty` (>=1 gz part)
      ▼
  relays_table(10002)    --(refresh-load-only: fetch → in-proc loader TRUNCATE+COPY → 3 MV refresh)-->  Aurora

/nutzaps  (job nutzaps_refresh)          ── one dial bracket across BOTH kinds + the MAU rebuild
  nutzaps_payments_extract(9321)  → nutzaps_payments_table(9321)  ─┐
  nutzaps_configs_extract(10019)  → nutzaps_configs_table(10019)   │
                                                                    └→ nutzaps_active_users
       (rebuild-nutzap-mau: psql -f nutzap_active_users.sql, reads ONLY events_kind_9321_nutzaps)

/lightning  (job lightning_refresh)      ── the two LARGEST loads (k=0 ~2.2M, k=9735 ~3.5M / ~2.3 GB gz)
  lightning_profiles_extract(0)    → lightning_profiles_table(0)
  lightning_zaps_extract(9735)     → lightning_zaps_table(9735)
  lightning_signers   (load-signers: YAML→lightning_provider_signers; independent, in-proc, self-applies schema)

/zapstore  (job zapstore_refresh)
  zapstore_catalog_extract(32267)  → zapstore_catalog_table(32267)
  zapstore_releases_extract(30063) → zapstore_releases_table(30063)
  zapstore_stacks_extract(30267)   → zapstore_stacks_table(30267)
  zapstore_zaps_extract(9735,into=zapstore) → zapstore_zaps_table   (a-tag-filtered subset → events_kind_9735_zaps_zapstore)
```

- **kind-9735 (zap-receipts) is loaded by two surfaces** - lightning (full -> `events_kind_9735_zaps`)
  and zapstore (`into=zapstore` subset -> `events_kind_9735_zaps_zapstore`). Distinct assets in distinct jobs;
  `_run_id_suffix` differs (`k9735` vs `k9735-zapstore`) so their S3 UNLOAD prefixes never collide.
- **The signer map (`load-signers`) gained an in-proc path** (run.sh): `NOSTROLOGY_LOADER_INPROC=1`
  runs `bin/load-signers.sh` directly (no `compose run`, no docker socket), building the DSN +
  `PGPASSWORD` like `refresh-load-only`. The signer YAML is image-baked (`data/` is not ignored).
  Stage 3 replaced this with `postwork.load_signers`, which calls the loader directly.
- **zapstore independent of lightning**: the /zapstore pages JOIN kind-0 (profile-metadata) rows
  loaded by /lightning - a cross-surface DATA dependency NOT modeled as an asset dep (matches the legacy verbs' decoupling).

- **One job per surface** (not partition-by-kind). The earlier Stage-0 design partitioned the
  generic `kind_extract`/`kind_table` assets by kind; that broke on the first multi-kind surface
  (a surface wants ONE dial bracket across its kinds, and `/nutzaps`'s MAU rebuild is a cross-kind
  post-step with no home in a one-run-per-kind model). Surface-as-job generalizes to the remaining
  multi-kind surfaces (lightning, zapstore).
- Materialized **only via the whole-surface job** (no ad-hoc partial materialization) — that, not
  a magic flag, is how the load never reuses a stale extract.
- **`RUN_ID` is deterministic** from `(dagster_run_id, kind[, into])` (`_kind_chain._rid`), so the
  extract + table halves address the **same** S3 prefix without passing metadata between assets.
- **`in_process_executor`** so the run-scoped `AuroraDial` resource initializes ONCE per run
  (under the multiprocess executor each step would re-init it and thrash the dial).
- **Metadata** plotted per asset over time: extract → `athena_scanned_bytes`, `gz_parts`;
  table → `rows_loaded`, `min_rows_floor`, `anti_shrink_headroom_pct`, `copy_duration_s`;
  MAU → `mau_rows`.
- **`last-run.json` is per-surface**: the success/failure sensors write `last-run-<job>.json`
  (the stable per-surface freshness signal a read-only account reads) plus the global
  last-write-wins `last-run.json` (back-compat with the heartbeat freshness check).

### The `run.sh` seam (the only producer-side bash change)

Historical: Stage 3 replaced this seam with `dagster_code/pipeline/` and deleted these verbs, and
`bin/` went on 2026-11-09.

`cmd_refresh` stays the **authoritative production path** until cutover. Two new verbs split it:

- `refresh-extract-only` (`KIND=`, `RUN_ID=`): flush → **full per-kind diff-register**
  (`register-partitions --kind K`, no `--since-dt` — keeps the existing ADD-only diff logic; it
  reconciles the whole kind every run, cheap, ~0 adds steady-state, and sidesteps the
  dt-immutability question — no windowing, no weekly walk) → Athena UNLOAD to the durable prefix.
- `refresh-load-only` (`KIND=`, `RUN_ID=`): re-fetch the prefix (the extract's `FETCH_DIR` is
  per-process trap-cleaned; re-fetch is correct) with the free-space preflight + empty-prefix
  guard → load. `NOSTROLOGY_LOADER_INPROC=1` runs `bin/load.sh` directly (no `compose run`, no
  docker socket); the DSN (`sslmode=require&connect_timeout=60`) is built by run.sh itself.
- Plus thin `dial-up` / `dial-down` verbs the `AuroraDial` resource shells (reusing the proven
  modify-db-cluster retry/backoff).

Both verbs: **re-acquire the same `flock` fd9** (mutual exclusion with the legacy
`cmd_refresh`/`refresh-all` — the loser aborts, so a stray old-path run during Stage 0 can't tear
the serving table); **never claim/release the dial** when `NOSTROLOGY_DIAL_HELD` is set (the
resource exports it); **emit stats to `NOSTROLOGY_STATS_OUT`** (a file), never stdout.

### MIN_ROWS — unchanged

The anti-shrink floor stays a **pre-`TRUNCATE` gate inside the loader** (it aborts before the
truncate if the row count is under the floor, preserving the live table). It is NOT a
post-materialization asset check (a check runs *after* the truncate — too late). Stage 0 shells
the loader unchanged, so the guard is inherited for free.

## Observability - operator vs read-only access

- **Operator:** the mutation-enabled Dagit UI, for operators only.
- **Read-only access:** the `--read-only` webserver (UI **and** `/graphql`), the **`dagster_ro`**
  SELECT-only Postgres grant on the run store (raw SQL only, not the `dagster` CLI), the stable
  `last-run.json` and the compute-log files on the Dagster volume, and Dagster stdout in Loki.
- OSS Dagster has **no per-user RBAC** (that's Dagster+); two server instances + the DB grant is
  the OSS-native way, and a DB grant is a harder boundary than any app-level role. The RW
  webserver is mutation-enabled = full launch/terminate/wipe control - unlike nozzl's bounded
  `/admin/flush` route - so it is **not** left to the honor system: a deployment keeps it out of
  reach of every account that is not an operator's, while leaving read-only accounts the
  read-only UI and the `dagster_ro` SELECT.
- **Daemon liveness** is out-of-band (`deploy/dagster/heartbeat/`, a systemd timer that nothing in this
  repository installs: a deployment installs it, "What a deployment provides" below) - a dead
  daemon writes no run record and the Discord failure sensor is itself daemon-hosted. A health check
  that reads `daemonHealth.allDaemonStatuses` on the read-only UI's GraphQL can DETECT it, but that is
  a pull; only an independent heartbeat catches "daemon down -> nothing fires, no alerts".

## What a deployment provides

The code assumes these and installs none of them. How one deployment provides them, with its
hosts, accounts and commands, is the deployment's runbook, not this repository's.

- **A run store of its own**: a Postgres for Dagster's run, event and schedule storage
  (`deploy/dagster/dagster.yaml`), separate from the serving database for the reason in the
  services table above. `deploy/dagster/pg-init/` creates the `dagster_ro` role on a fresh data
  directory only, so a deploy re-grants its SELECT after every migrate.
- **The five services** (the table above: four from one image, the run store on `postgres:16`),
  with the step environment on `dagster-code`, where runs execute.
- **A volume for the artifacts** (pgdata, compute logs, the last-run files, the heartbeat file,
  extract scratch), owned by the account the deploy runs as, so that the deploy can create its
  subdirectories, and traversable by read-only accounts, so that they can read the files.
- **The heartbeat timer** (the liveness bullet above): `deploy/dagster/heartbeat/` holds the
  script and a systemd unit pair that runs it every 5 minutes. A deployment installs the pair as
  root, with its own user, paths and optional Discord webhook in the service unit: copy both units
  into `/etc/systemd/system/`, then `systemctl daemon-reload` and
  `systemctl enable --now dagster-heartbeat.timer`.
- **A read-only path**: the read-only webserver and the `dagster_ro` role for read-only
  accounts, and the read-write UI out of their reach (the Observability section above).

A deploy keeps this contract: it refuses while a run is active (the queue-wedge note, below),
snapshots the run store, builds `dagster-code`, runs `dagster instance migrate` and keeps the old
stack if the migrate fails, then recreates the stack, re-grants `dagster_ro`, and fails if the
code location is unhealthy or the daemon is not live afterwards. **Bump the three Dagster pins together** (dagster,
dagster-webserver, dagster-postgres) in `dagster_code/requirements.in`, regenerate the lock
(`scripts/python-lock.sh compile dagster`, which moves dagster-graphql and dagster-pipes with
them) and redeploy (the gated migrate).

Run a surface manually (an operator triggers, a read-only account verifies): the operator clicks
**Materialize all** on the surface job in the UI (`relays_refresh` / `nutzaps_refresh` /
`lightning_refresh` / `zapstore_refresh`) - or enables its schedule. A read-only account then
verifies (`last-run-<job>.json`, the read-only GraphQL, `dagster_ro`, compute logs, and the live
tables via `nostrology_ro`), diffing the loaded tables against the verified baselines
(`/relays` -> M1; `/nutzaps` -> M4: kind-9321 (nutzaps) about 4,819 rows, kind-10019
(nutzap-configs) about 6,332).

## End-to-end verification (in a deployment)

1. Materialize `relays_refresh` in the UI → assert: one `[dial]` up + one down in the run log, both
   assets materialize with metadata, the 3 MVs refreshed, `last-run.json` written, run+step
   durations in the Dagster PG, and a read-only account can read all of it.
2. Materialize `nutzaps_refresh` → assert: ONE `[dial]` up + one down for the whole run (not per
   kind), all 5 assets materialize, `nutzaps_active_users` runs AFTER `nutzaps_payments_table`
   (`mau_rows` > 0), `last-run-nutzaps_refresh.json` written, and the loaded tables match the M4
   baseline (`events_kind_9321_nutzaps` ≈ 4,819; `events_kind_10019_nutzap_configs` ≈ 6,332).
   Confirm the blocklist took effect (the 9321 load log reports `blocklisted` > 0).
3. Materialize `lightning_refresh` → assert: ONE dial bracket; `lightning_profiles_table` (k=0,
   ~2.2M, blocklisted>0) + `lightning_zaps_table` (k=9735, ~3.5M) load; `lightning_signers` runs
   in-proc (no `compose run`, `signer_rows` > 0). **Check the artifact volume's free space
   first**: the kind-9735 (zap-receipts) fetch was about 2.7 GB compressed in 2026-11, and the
   load's free-space preflight fails loud unless 20% more than the fetch is free. Materialize `zapstore_refresh` -> all 4 chains
   load; the 9735->zapstore subset writes
   `events_kind_9735_zaps_zapstore` (NOT the full table) and uses the `k9735-zapstore` prefix.
4. Kill a run mid-flight → the process-group signal forwarding runs run.sh's EXIT trap (fetch dir
   cleaned; dial released by the resource teardown).

## Deviations from the plan (for the reviewer)

- **One image runs four of the five services** (all but the run store, on `postgres:16`; the plan
  implied stock daemon/webserver images) - chosen for trivial version parity on a single box.
- **Two assets, not four** — `kind_glue_catalog_freshen` (flush+register) and `relays_mv` are
  sub-steps *inside* the two run.sh verbs / the loader, not separately shellable at the bash seam.
  The catalog check is enforced inside `register-partitions`' read-back gate; the blocking asset
  check on `kind_extract` guards the extract OUTPUT (≥1 part). The 4-asset split is a Stage-3
  native-ization target.

## Known gaps / Stage-1+ follow-ups

- The seam verbs don't yet pass through `LATEST_DT_ONLY`/`DT` (smoke knobs) — a smoke runs a full
  extract. Add pass-through if a 1-ingest-day smoke is wanted.
- **Dial is held during the extracts**, not just the loads: the `_extract` assets require
  `aurora_dial` (to force the run-scoped resource to init before the first load), so the cluster
  sits at load capacity through flush+register+Athena UNLOAD — wasted ACU, and it ~doubles with a
  2-kind surface. Harmless (pre-existing since Stage 0). Stage-3 fix: drop `aurora_dial` from the
  extract assets (init still fires on the first table asset under `in_process_executor`).
- **The MAU rebuild (`rebuild-nutzap-mau`) takes no fd9 lock** (unlike the extract/load seam
  verbs). It's idempotent (TRUNCATE+INSERT) and reads only the 9321 table, so a concurrent legacy
  `refresh-nutzaps` at worst causes a redundant rebuild — not corruption. Matches the legacy
  standalone call's behavior. (Post-cutover the legacy scheduled path is gone — only on-box
  break-glass `refresh-*` remains — so a concurrent run is now an operator-initiated edge case.)
- `dagster_ro` SELECT is re-granted post-migrate by the deploy; the pg-init only fires on a fresh
  volume. Verify a `dagster_ro` raw-SQL read works post-deploy (raw SQL only; the
  `dagster` CLI / a full `DagsterInstance` may attempt schema-version writes a SELECT-only role rejects).
- Stage 1 (**DONE**): all four surfaces onboarded as their own jobs (/relays, /nutzaps, /lightning,
  /zapstore). Each = one job, its kinds via `_kind_chain`, plus any post-step asset (nutzaps MAU;
  lightning signer map; zapstore's 9735→zapstore subset via `into`). Status JSON retired (below).
- Stage 2 — **cutover, DONE 2026-06-30**:
  - The four `*_daily` schedules now ship `default_status=RUNNING` (`definitions.py`) — Dagster is
    the authoritative refresh trigger. They serialize through the `QueuedRunCoordinator`
    (`max_concurrent_runs: 1`, `dagster.yaml`), so the staggered fire times only set ordering; two
    surfaces never hold the Aurora dial at once even if one overruns its 30-min slot.
  - **Status JSON retired**: `cmd_refresh_all` no longer writes `nostrology-refresh-status.json`; the
    sensors' per-surface `last-run-<job>.json` own the "did it succeed?" + freshness signal, and the
    out-of-band `daemon-heartbeat.json` is the dead-daemon dead-man (designed here, and installed by the
    deployment - see the liveness bullet above).
  - **Deploy-time schedule check**: `default_status=RUNNING` governs a schedule ONLY while it has no
    stored instigator state — ANY prior UI/CLI toggle (start *or* stop, not just a stop) persists a
    state that overrides the flag on reload. **Verified 2026-06-30** via the `dagster_ro` run store:
    the `instigators` and `jobs` tables held **zero `SCHEDULE` rows** (only the 3 `DECLARED_IN_CODE`
    sensors), i.e. none of the four were ever toggled — so the flip starts all four cleanly on
    the next deploy. Still **verify all four read RUNNING afterward** (RW UI, or the run store) and
    start any laggard — mandatory, not a formality, because a later manual toggle would silently win.
  - **Queue-wedge note**: a daemon crash mid-run can leave a run `STARTED`, holding the single
    `max_concurrent_runs` slot, so subsequent scheduled runs QUEUE behind it. **`run_monitoring`
    does NOT reap such a run on its own here** (verified 2026-08-24, dagster source: the
    `DefaultRunLauncher` has no `check_run_worker_health`, so the MonitoringDaemon logs
    "Checking run ..." every 2 min and does nothing); the only automatic backstop is
    `run_monitoring.max_runtime_seconds` (dagster.yaml), which fails any run older than that and
    frees the slot. Until it fires, an operator terminates the orphan in the RW UI
    (through a tunnel; run -> Terminate). Concrete case: a
    deploy dispatched at 20:41 UTC while the 20:30 UTC `lightning_refresh` was mid-COPY
    recreated `dagster-code`, killed the run worker, and the run stayed `STARTED` - wedging
    routstr / nutzaps / zapstore / marketplaces behind it for the night. **Rule: check the
    read-only run list (the UI, or GraphQL `runsOrError(filter:{statuses:[STARTED,STARTING]})`)
    before ANY deploy, and never deploy inside the nightly schedule block, from the first
    schedule's fire time to about half an hour after the last one's. The schedules are in
    `definitions.py`, so a change to one moves the block, and the deployment's runbook, which
    states it, changes with it.** Since 2026-09-14 the deployment's Dagster script also refuses on its own, right before migrate and again before the recreate, while the run store holds a run not started, queued, starting, started or canceling, or a backfill requested or canceling; the manual check stays, because the deploy builds for minutes first. The refusal names each run and since when; a run stuck in NOT_STARTED, STARTING or MANAGED never clears by itself, so it blocks every deploy until an operator terminates it in the RW UI (as above). The
    dial-down `finally` + the deployment's monitoring still backstop a stuck dial. Concurrency itself is safe: a QUEUED run has NOT entered `AuroraDial.yield_for_execution`,
    so it holds no dial — two surfaces never dial Aurora at once even when one overruns its 30-min slot.
- Stage 3 — **native-ization (Dagster-only, "burn the boats"): replace the run.sh/aws-cli shell-out
  with in-process boto3/psycopg2 in a new `dagster_code/pipeline/` package (dependency-injected so
  each module unit-tests with mock clients, no boto3/AWS locally). Phased, each reviewed +
  deployed:**
  - **Phase 1 (DONE 2026-06-30)** — the Aurora dial. `AuroraDial` resource dials via boto3
    (`pipeline/aurora.py`: `_modify_scaling`/`DIAL_UP`/`DIAL_DOWN` port), not `run.sh dial-up/down`.
  - **Phase 2 (DONE 2026-06-30)** — the load + **MV-as-asset**. `pipeline/load.py:run_load` resolves
    endpoint+secret, applies the idempotent schema (also the resume gate, connect_timeout=90), S3
    preflight+fetch (boto3, paginated), runs the loader as a plain Python subprocess
    (`--truncate --min-rows <floor> --no-refresh-mv`). The loader's `--no-refresh-mv` suppresses the
    in-loader MV refresh (kind-10002 (relay-list-metadata) + kind-3 (follow-lists)); the 3 `/relays` MVs are now their **own asset** `relays_mv`
    (downstream of `relays_table`, inside the dial bracket) via a reusable `_mv_refresh_asset`
    factory. **This is the pattern other surfaces adopt as serving moves onto MVs** (e.g. `/lightning`
    chart data — a new MV in `schema/init.sql` + a `<surface>_mv` asset with `deps=[its tables]`;
    `apply_schema` in `run_load` creates the MV, the asset refreshes it CONCURRENTLY). New modules:
    `kinds`, `pg`, `s3util`, `proc`, `load`. Every never-wipe guard preserved (empty-prefix fail-loud,
    per-kind MIN_ROWS, loader exit-2 abort). ⚠️ flock cross-path exclusion is gone — until the run.sh
    refresh-* verbs are deleted, an operator must pause the schedules before a break-glass refresh.
  - **Phase 3 (DONE 2026-06-30)** — the extract. `pipeline/extract.py:run_extract` runs the extract
    sequence in-process: `flush.flush_kind` (urllib POST to `/admin/flush/{kind}`, 200 ok / 504
    retry / else fatal — port of `_flush_kind`) → `glue_catalog.register_partitions` (the CRITICAL
    port of `cmd_register_partitions` + `partition-diff.sh`: S3-leaf discovery, catalog diff via
    Python **set membership** — no `comm`/collation hazard, empty-catalog bootstrap returns ALL of
    S3 — `batch_create_partition` 100/call AlreadyExists-tolerant, then the **completeness gate**
    `registered+tolerated==new` AND the **positive read-back gate** re-reading the catalog to prove
    it covers every discovered leaf) → `sql.render` (template by `kinds.kind_to_class`, the zapstore
    a-tag `__EXTRA_WHERE__`) → `athena.run_unload` (start + poll, terminal-state + timeout-cancel,
    rides out a transient `get_query_execution` failure) → `s3util.count_gz_parts`. The `_extract`
    asset calls it instead of `run.sh refresh-extract-only`. New modules: `flush`, `sql`, `athena`,
    `glue_catalog`, `extract`; `kinds.kind_to_class` + `s3util.{iter_object_keys,count_gz_parts,
    bucket_reachable}` added. fd7/fd9 flock + `--since-dt` dropped (the QueuedRunCoordinator
    serializes runs; each Dagster extract registers its own kind, full). 35 new unit tests (mock
    s3/glue/athena/POST clients); reviewed (no blocker; folded in transient-describe
    tolerance, a generous 300s flush timeout, and a docstring noting the diff-safe `Expression`
    scoping of the main catalog fetch).
  - **Phase 4 (DONE 2026-06-30)** — the surface post-steps, in `pipeline/postwork.py`.
    `rebuild_nutzap_mau` runs `schema/nutzap_active_users.sql` via psycopg2 (`pg.execute_sql_file`,
    generalized out of `apply_schema`) + counts the MAU table; `load_signers` applies `init.sql` then
    runs the loader's `--load-lightning-signers` mode as a Python subprocess (no compose/socket) +
    counts. The `nutzaps_active_users` and `lightning_signers` assets call these instead of
    `run_simple(...)`. Post-step row counts are **best-effort** (`_count_best_effort` — a count blip
    after the work committed must not fail the asset, matching bash's `|| true; rows=0`). **With this,
    `run_sh.py` is fully unreferenced in production** (extract+load+post-steps all native). 6 new
    tests; reviewed (no blocker/major; the best-effort-count MINOR folded in).
  - **Phase 5 (DONE 2026-06-30)** — teardown. Deleted `dagster_code/run_sh.py` (+ its test),
    `scripts/aws/partition-diff.sh` (+ its test), `test-run-helpers.sh`, `test_dagster_seam.sh`; and
    from the deployment's `run.sh` removed every data verb (`refresh*`, `register-partitions`,
    `rebuild-nutzap-mau`, `load-signers`, `dial-up/down`) + their exclusive helpers (`_modify_scaling`,
    `DIAL_UP/DOWN`, `claim/release_dial`, `_flush_kind`, `render_sql`, `kind_to_*`, `min_rows_for`,
    `_emit_stats`, `snapshot_on_success`) + the Athena/Glue/dial config block — `run.sh` went 1550→327
    lines, keeping ONLY the operator's bootstrap verbs and the helpers they share. Dropped
    aws-cli (+ its `unzip`) from `Dockerfile.dagster` (in-container is all boto3 now; the
    deployment's own scripts run outside the image). Removed the now-vestigial
    `NOSTROLOGY_DIAL_HELD` env cover from `resources.py`. The standalone `sql/extract_*.sql` +
    `schema/*.sql` + `loader/` stay (the native code reads them). NOT touched (separate future
    cleanup): the legacy DuckDB-container subsystem (`bin/refresh.sh`, `bin/load.sh`, the compose
    `refresh` profile, `cmd_build`), retired on 2026-11-09 with `bridge/`. `bash -n` + a grep proving zero surviving reference to any
    deleted symbol gate the edit; 85 tests pass (the 6 run_sh-bridge tests went with the module).
- Stage 4: dbt; staging-table+swap (partly subsumed by MVs); controlled parallelism.

## The /marketplaces surface (added 2026-08-23)

The fifth surface job, `marketplaces_refresh` (`marketplaces_daily`, 22:00 UTC at the time; 02:15 UTC since 2026-09-08, RUNNING - third-last in the nightly order since `ngit_daily` (03:15) followed `routstr_daily` (02:45) on 2026-09-25; it was
last in the nightly order), diverges from the loader surfaces in one deliberate way: **no
per-kind raw serving tables**. Its three extract assets reuse the `_extract_asset` factory
(the extract half of `_kind_chain`, factored out) -

- `marketplaces_listings_extract` - kind-30402 (markets-product-listings), **all versions**
  (`sql/extract_addressable_all.sql`): the farm-title detector and `archive_events` span
  every revision, and the current-version selection needs the exact NIP-01 tie-break
  (`created_at DESC, id ASC`) that the `..._latest` template's window lacks, so selection
  happens in Python instead.
- `marketplaces_shipping_extract` - kind-30406 (markets-shipping-options), same template.
- `marketplaces_deletions_extract` - kind-5 (event-deletion-requests) via
  `sql/extract_markets_deletions.sql`, an **aggregation**: only the DISTINCT self-authored
  `(author, coord)` pairs naming `30402:...` coordinates leave Athena (NIP-09 scoping in the
  SQL, asserted again in the compute), never the tens of millions of raw deletion events.
  This chain flushes the WAL of kind-5 (event-deletion-requests) daily like any other extract - one extra chunk-set per
  day on a kind the 12h roll already splits twice, judged negligible.

The full surface design - sources through the web serving contract - is
[docs/design/marketplaces-design.md](marketplaces-design.md); this section covers only the
orchestration fit. Downstream, ONE compute asset (`marketplaces_snapshot`) fetches the three part sets and runs
`pipeline/marketplaces.py` - the exact port of `crosscheck/build_markets_snapshot.py`
(current-version selection, six precedence-ordered exclusions, geo resolution via the
vendored `markets_geolib.py` + pinned `reverse_geocoder`/`pycountry`, every aggregate) - then
writes, in ONE transaction behind a `MIN_ACTIVE_LISTINGS` anti-shrink gate:

- `marketplaces_listings` - one row per current coordinate with classification + geo (the
  queryable asset for future MCP exposure), TRUNCATE+INSERT;
- `marketplaces_snapshot` - the page's full dataset as an APPENDED JSONB row;
  nostrology-web serves the newest row (`/marketplaces` `+page.server.ts`).

Failure ordering is parse -> build -> invariants (`check_invariants`, also a recorded asset
check) -> anti-shrink -> write, so a broken refresh can never blank the page: the web keeps
serving the previous snapshot row. It is one document rather than MVs until a real query-performance
need shows up; the **agreement contract** - the cross-check generator re-derives the snapshot independently over
raw S3 and `crosscheck/compare_markets_snapshots.py` must show ZERO diffs - is the standing
cross-check for any methodology change (change it in BOTH places or the gate fails).

Manual materialization is audited like a deploy: the deployment's re-run action runs
`dagster job launch` inside dagster-code - submit-only, serialized by the
QueuedRunCoordinator; watch the run via `last-run-marketplaces_refresh.json` or the RO webserver.

## The /routstr surface (added 2026-08-24)

The sixth surface job, `routstr_refresh` (`routstr_daily`, 22:30 UTC at the time; 02:45 UTC since 2026-09-08, RUNNING - last
in the nightly order until `ngit_daily` joined after it on 2026-09-25), follows the marketplaces shape exactly - four `_extract_asset`
extracts, no loader, ONE compute asset (`routstr_snapshot`, `pipeline/routstr.py`)
writing its substrate tables (`routstr_providers` + `routstr_provider_days`) and the
appended `routstr_snapshot` JSONB row in one transaction behind two anti-shrink gates
(`MIN_LEDGER_ROWS` / `MIN_PROVIDERS`), with the same parse -> build -> invariants ->
anti-shrink -> write failure ordering and the same independent-generator agreement
contract (`crosscheck/build_routstr_snapshot.py` +
`crosscheck/compare_routstr_snapshots.py`, ZERO diffs).

Two orchestration-relevant novelties:

- **A size-reducing extract that is not an aggregation**: kind-38422
  (routstr-analytics-snapshots) events are ~120 KB and republished every ~15 min, so
  `sql/extract_routstr_analytics_daily.sql` keeps only the LAST snapshot per
  (pubkey, UTC day) plus the `routstr.analytics.snapshot.v1` schema-tag filter -
  ~703 rows leave Athena instead of ~21,800 events, provably lossless for the
  downstream ledger merge. The three small kinds - kind-38421 (routstr-provider-announcements), kind-38423
  (routstr-model-catalog), kind-38425 (routstr-provider-reviews) - reuse
  `extract_addressable_all.sql` via `_INTO_TEMPLATE` entries.
- **The compute reconstructs history** rather than selecting current versions: the
  snapshots' trailing windows carry UTC-grid daily/weekly activity buckets, merged
  under full-coverage-only + newest-wins rules
  ([docs/design/routstr-design.md](routstr-design.md) for the full surface design,
  `docs/dataset-reference/routstr-dataset-reference.md` for metric semantics).

Manual materialization, as for marketplaces (and nutzaps): the deployment's re-run action
runs `dagster job launch` inside dagster-code; watch via
`last-run-routstr_refresh.json` or the RO webserver.

## The /git surface (added 2026-09-25 as /ngit; the page moved to /git on 2026-09-29)

The seventh surface job, `ngit_refresh` (`ngit_daily`, 03:15 UTC, RUNNING - now last in the
nightly order, so the block ends at about 03:45 UTC), follows the routstr shape: seven
`_extract_asset` extracts, no loader, ONE compute asset (`ngit_snapshot`,
`pipeline/ngit.py`) writing its substrate tables - `ngit_repos` (one row per
kind-30617 (repo-announcements) coordinate, kept or excluded, with the noise-ledger family
that matched) and `ngit_pushes` (one row per kind-30618 (repo-states) version of a kept
repository, with its `changed` / `push` / `counted` flags) - and the appended
`ngit_snapshot` JSONB row in one transaction, with the same parse -> build -> invariants ->
anti-shrink -> write failure ordering. The full surface design is
[docs/design/ngit-design.md](ngit-design.md); what every number means is
`docs/dataset-reference/ngit-dataset-reference.md`, which wins over both.

Orchestration-relevant novelties:

- **Seven extracts, all `into="ngit"`** (`_INTO_TEMPLATE` entries): kind-30617
  (repo-announcements) and kind-30618 (repo-states) reuse `extract_addressable_all.sql`;
  kind-10317 (grasp-server-lists) gets `extract_replaceable_all.sql` (the replaceable
  template minus its latest-per-pubkey stage); kind-1111 (comments), kind-7 (reactions)
  and kind-9735 (zap-receipts) carry the git-subset filter IN their templates
  (`extract_ngit_comments.sql` / `_reactions.sql` / `_zaps.sql`), so only the git subset
  leaves Athena; kind-5 (event-deletion-requests) is reduced to its DISTINCT self-authored
  `(author, coord, created_at)` triples (`extract_ngit_deletions.sql`, modelled on the
  marketplaces one; the compute applies the horizon per triple). The `k9735-ngit` and `k5-ngit` prefixes never collide
  with the lightning, zapstore or marketplaces extracts of the same kinds.
- **`allow_empty` on `_extract_asset`**: reactions, zaps and deletions are unmeasured
  subsets that may be empty, so their `extract_nonempty` check is non-blocking and passes
  with a warning on zero parts. Every other extract, on every surface, keeps the blocking
  check (the parameter defaults to False).
- **A cross-surface read**: display names for at most 2,000 kept maintainers come from
  `events_kind_0_profiles` (kind-0 (profile-metadata), loaded by `lightning_daily` at
  00:45), resolved before the pure build. Names are presentation only: any failure (the
  table missing on a fresh cluster, a timeout) degrades to no names with a logged warning,
  never to a failed run.
- **Relative anti-shrink**: `write_pg` reads the newest served row's summary first and
  refuses, before any write, when `announcements_kept`, `pushes_total` or any of the seven
  `summary.archive_events.*` input counts falls below 90% of it (the input gates are what
  protect the three allow-empty extracts); absolute floors (3,500 kept coordinates, 12,000 pushes) apply only to the first
  run. A deliberate methodology change that shrinks a figure sets `SHRINK_OVERRIDE_UNTIL`
  (an ISO date in `pipeline/ngit.py`) in the same commit; the run logs the bypass.
- **Two asset checks on `ngit_snapshot`**: `snapshot_invariants` (blocking; recorded on
  success, and a failure raises before any write) and `roster_growth_sane` (non-blocking,
  WARN severity: kept coordinates first seen on `as_of_day` against max(20, 5 x the median
  daily count over the 30 days before it) - a coordinate flood lands as repositories and
  never trips a version-based rule).
- **Verification is not a two-implementation agreement gate**: unit tests + invariants, a
  real-data dry run (2026-09-25, the profile's DuckDB copy), and the row-level cross-check
  `crosscheck/ngit_check.py`, written from the dataset reference alone and diffed
  against `ngit_repos` / `ngit_pushes` after the first production materialization.

Manual materialization, as for routstr: the deployment's re-run action runs
`dagster job launch` inside dagster-code. Watch via `last-run-ngit_refresh.json` or the RO
webserver. First deploy, in order: apply the schema (the three tables), deploy the Dagster stack
(it loads the job and schedule), materialize the job ONCE so a snapshot row exists, then deploy
nostrology-web (`getNgitSnapshot` throws on an empty table by design).

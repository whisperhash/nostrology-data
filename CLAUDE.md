# nostrology-data

The **data half** of the nostrology webapp: it extracts Nostr events from **nozzl's archive in
S3** (Parquet, written by the `nozzl` producer) via **Athena**, and loads them into the
serving tables of a **managed Aurora Serverless v2 Postgres** cluster. The
SvelteKit UI that reads those tables is a **separate repo, `nostrology-web`** (deploys on its own;
reaches the same endpoint as a read-only role). This repo owns the schema + the load pipeline that
WRITES the serving DB, and two services: the public **MCP server** (`nostrology_mcp/`, which reads
that DB as its own read-only role; served at `https://nostrolo.gy/mcp` through nostrology-web's
Caddy) and its **Cashu redemption sidecar** (`cashu_redeemer/`, which has no database access: the
only process that talks to a Cashu mint or keeps proof secrets; no host port). Each service's
README is its reference.

## The load pipeline is native Dagster

The loads migrated from a monolithic bash `run.sh` to Dagster via a **strangler-fig**, completed in
**Stage 3 (native-ization, 2026-06-30)**: the assets no longer shell out - the **entire data
pipeline is in-process Python** (boto3 + psycopg2) under `dagster_code/pipeline/`.

- **Orchestration (`dagster_code/`)** - the **sole** refresh path. One job per surface
  (`relays_refresh`, `nutzaps_refresh`, `lightning_refresh`, `zapstore_refresh`,
  `marketplaces_refresh`, `routstr_refresh`, `ngit_refresh`), each a daily
  `default_status=RUNNING` schedule in run order `relays_daily` 00:15 / `lightning_daily` 00:45 /
  `zapstore_daily` 01:15 / `nutzaps_daily` 01:45 / `marketplaces_daily` 02:15 /
  `routstr_daily` 02:45 / `ngit_daily` 03:15 UTC (staggered 30 min; the block ends at about
  03:45 UTC). The block runs JUST PAST the midnight-UTC data
  boundary (moved there 2026-09-08 from 20:00-22:30) because everything these jobs compute
  is "as of midnight UTC" - running before the boundary published against a day that had
  not closed. They serialize through the `QueuedRunCoordinator` (`max_concurrent_runs: 1`), so
  they never hold the Aurora dial concurrently. Recreating `dagster-code` kills a run mid-load, so a
  deploy of the Dagster stack belongs outside the block, and the deployment's Dagster deploy refuses
  while any run or backfill is queued or in progress.
- **`dagster_code/pipeline/`** - the native pipeline modules, dependency-injected so each unit-tests
  with mock clients (no AWS/dagster locally):
  - the per-kind chain: `aurora` (dial + endpoint/secret), `flush` (urllib WAL flush),
    `glue_catalog` (the partition diff/register + completeness & read-back gates), `athena`
    (UNLOAD start+poll), `sql` (template render), `extract` (flush->register->UNLOAD), `load`
    (fetch->loader subprocess->count);
  - `postwork` (the surface post-steps: mint classification, MAU rebuild, signer load);
  - `marketplaces` + `markets_geolib` + `markets_medialib` (the /marketplaces metrics compute -
    snapshot JSONB + listings table written in-asset, no loader; kept in FULL agreement with the
    cross-check generator via crosscheck/compare_markets_snapshots.py, except `markets_geolib`,
    which that generator imports, so only its own tests hold it). `markets_medialib` is the odd
    one out and says so in its own docstring: it is a transcription of one MARKETPLACE's rule -
    Conduit Market's `normalizePublicMediaUrl` plus the rule deciding which images it even reads -
    for the `market_image` column, which feeds no metric and exists so nostrology-web's Recent
    listings section can link a row to a marketplace that will actually render it. The decision
    needs the event's CONTENT, which the serving table does not carry and the web never sees, so
    it cannot live over there; nostrology-web#25 (Re-read each marketplace client's
    listing-visibility rules periodically, at the versions they are serving) is its drift check;
  - `routstr` (the /routstr metrics compute - the analytics-window ledger reconstruction, same
    in-asset snapshot shape + agreement gate via crosscheck/compare_routstr_snapshots.py;
    docs/design/routstr-design.md). With the snapshot it writes the five substrate tables the
    MCP's Routstr domain reads (`routstr_providers`, `routstr_provider_days`,
    `routstr_model_days`, `routstr_catalog_versions`, `routstr_reviews`), invariant-tied to the
    document - docs/design/routstr-mcp-design.md;
  - `ngit` (the /git metrics compute - the noise ledger, reciprocal-maintainer repositories and
    the push ledger of changed kind-30618 (repo-states) versions and, since 2026-10-12,
    hashtree's pushes read from its kind-30617 (repo-announcements) republishes, writing
    `ngit_repos` / `ngit_pushes` / `ngit_repo_stats` (one row per repository; the page lists
    Stars and Zaps from it since 2026-10-18) + one `ngit_snapshot` row behind RELATIVE
    anti-shrink gates; the rule text is docs/dataset-reference/ngit-dataset-reference.md,
    verified by a row-level cross-check written from it, not a second implementation;
    docs/design/ngit-design.md);
  - `nectar` (the `nectar_refresh` job's dataset publish; its schedule ships STOPPED);
  - the alerting: `lineage` (below), `alerts` (the Discord failure and recovery posts) and
    `freshness` (the watchdog's verdicts over the last-run files);
  - helpers: `aws` (the one boto3 client factory), `pg`, `s3util`, `proc`, `kinds`, `config`.

  The loader (`loader/load_jsonl_exports_into_postgres.py`) still runs as an in-container
  **python subprocess** (no docker socket).
- **Break-glass** for a failed run or a missed tick: the deployment's re-run action, for
  marketplaces, routstr, ngit, nutzaps or zapstore (a `dagster job launch` in the running
  `dagster-code`, so the run queues like a scheduled one, on the code the last Dagster deploy
  shipped), or, for any job, the Dagster UI: Materialize all on the job, or Re-execute > All
  steps on the failed run, as the failure post says. Not Re-execute from failure: that is a new
  run, each load and compute reads the extract prefix named by its own run id, and one whose
  extract step is skipped finds no parts and refuses. Either path only queues a run, so a run
  stuck holding the single slot must be terminated in the Dagster UI first (the design doc's
  queue-wedge note), and a dead daemon launches nothing. Nothing refreshes data outside Dagster.

Success/freshness is read from the sensors' per-surface **`last-run-<job>.json`**, beside the file
`DAGSTER_LAST_RUN_JSON` names. The dead-daemon dead-man is `daemon-heartbeat.json`, from the
systemd timer in `deploy/dagster/heartbeat/`, which nothing installs: a deployment must (what it
provides, the install included: docs/design/dagster-orchestration-design.md, "What a deployment
provides"). Without it a dead daemon
raises no alert: it shows only to whoever looks, as the `last-run-<job>.json` files going stale
and in `daemonHealth` on
Dagster's GraphQL, while the `freshness_sensor` (daemon-hosted, so blind to that one case) covers
the rest.

## Canonical docs - read before non-trivial work

- **[docs/design/dagster-orchestration-design.md](docs/design/dagster-orchestration-design.md)** - the
  **living design doc for the whole Dagster orchestration**. The surface-as-job
  model, the `_kind_chain` factory, the run-scoped Aurora dial resource, the native `pipeline/`
  modules, the observability split (a read-write UI for the operator, a read-only one for
  observers), and the staged roadmap.
- **Per-surface design docs** - `docs/design/nutzaps-design.md` (what a nutzap payment is and how
  its value is read: the proofs in `proof` tags only; which mints are excluded as test or
  private, from the reviewed `data/nutzaps/mint_registry.yaml`), `docs/design/routstr-design.md`,
  `docs/design/marketplaces-design.md`, `docs/design/ngit-design.md`. /relays, /lightning and
  /zapstore have none; Zapstore's is nostrology-data#37 (Write a short
  docs/design/zapstore-design.md).
- **MCP design docs** - `docs/design/zapstore-leaderboard-mcp-design.md`,
  `docs/design/zapstore-reference-records-mcp-design.md`, `docs/design/routstr-mcp-design.md`,
  `docs/design/nutzaps-mcp-design.md`.
- **The rule text of three surfaces and its cross-checks** - in `docs/dataset-reference/`,
  `ngit-dataset-reference.md`, `routstr-dataset-reference.md` and `markets-dataset-reference.md`: what
  every number counts. `crosscheck/` (its README): the independent re-derivations of those numbers, run by
  hand and in no image. `ngit_check.py` there must not be changed by anyone who has read
  `pipeline/ngit.py`.
- **Where a new doc goes.** A design doc that is kept current goes in `docs/design/`, undated, and a
  surface's rule text in `docs/dataset-reference/`.
- **The `dt=` partition is INGEST date, not `created_at`** - a recurring source of wrong pruning
  assumptions. It is the UTC day of the newest ingest time the chunk's writer had seen when it
  flushed (`last_ingested_at` in nozzl's `crates/nozzl-archive/src/chunk_writer.rs`), so
  never earlier than the ingest day of any event in the chunk: a backfill puts years of events
  under one recent `dt`, and pruning on `dt` cannot select a `created_at` range. A lower bound is
  the exception: an event is ingested after it is created, so for a trailing "last N days" window
  `dt >= today - (N + 1)` (UTC dates; the extra day covers an author clock up to a day fast)
  keeps every event created in it.

## The architecture, in one breath

A surface refresh is a Dagster job whose assets run native Python: the **Aurora dial** resource
raises the cluster's capacity floor for the run and returns it to rest after; per kind, `extract`
flushes the producer WAL, registers the new S3 leaves into the Glue catalog (diff + completeness +
read-back gates), and runs an **Athena** UNLOAD to S3; then
`load` fetches the gz-NDJSON parts and runs the Python loader (`loader/load_jsonl_exports_into_postgres.py`)
as an in-container subprocess, streaming the `.gz` directly into Postgres (TRUNCATE+COPY,
drop/rebuild indexes), applying the **blocklist** (bot and spam pubkeys, a gitignored data
artifact: `blocklists/README.md`; bind-mounted read-only at `/app/blocklists`; a missing or empty
one loads unfiltered and the run stays green, so read the load's `Blocklist: N pubkeys loaded`
line); a per-kind **MIN_ROWS** floor (`pipeline/kinds.py`) is the anti-shrink gate; MVs +
post-steps (the /nutzaps mint classification and MAU rebuild, the /lightning signer load) are
downstream assets. The /marketplaces,
/routstr and /git surfaces skip the loader entirely: their compute assets consume the UNLOAD parts directly
and write the snapshot JSONB + substrate tables themselves (their own anti-shrink + invariant
gates: MIN_ACTIVE_LISTINGS for marketplaces, MIN_LEDGER_ROWS/MIN_PROVIDERS for routstr, 90% of
the newest served row for ngit). The serving DB is **Aurora**, not a container - endpoint + owner
password resolved at runtime from the cluster id via the instance role. The deployment's scripts
run no part of the pipeline: they apply the schema and the roles,
build and deploy the MCP server, the redeemer and the Dagster stack from this tree, and launch one
surface's Dagster job on request. `schema/init.sql` is idempotent and reaches
Aurora two ways: the deployment applies a checkout's copy, and the pipeline applies the Dagster
image's before every load, every compute and the signer load; `schema/roles.sql` reaches it only
through the deployment, which provisions the read-only roles from it. `data/` (the mint
registry, the signer YAML) is baked into the Dagster image, so a change there needs a deploy of
the Dagster stack before a run sees it.

## Dependencies: locked where a lock exists, exact specifiers everywhere

- **cashu-redeemer (Node)**: `package-lock.json` decides every install; both image
  stages run `npm ci`, which stops without the lock. Specifiers are exact and
  `cashu_redeemer/.npmrc` (`save-exact=true`) keeps `npm install <pkg>` writing them
  that way. An upgrade is deliberate: bump, then commit lock + manifest together; the
  test gate runs before any deploy of it. Judge an image's dependencies from the build
  log, never from the
  Dockerfile text: the sibling nostrology-web ran unpinned for 2.5 months behind a
  Dockerfile that said `--frozen-lockfile`, nostrology-web#2 (build: Dockerfile runtime
  stage installs prod deps UNPINNED (pnpm-lock.yaml not copied)).
- **The two Python images, nostrology-mcp and the Dagster image,** are hash-locked.
  Each installs a GENERATED lock -
  `nostrology_mcp/requirements.txt`, `dagster_code/requirements.txt` -
  with `pip install --require-hashes --only-binary :all:`
  followed by `pip check`: every package, transitive ones included, as `==` plus sha256.
  The direct pins, and why each version, live in the hand-edited `.in` beside each lock;
  never edit a lock by hand. reverse_geocoder (PyPI has only its sdist) installs from the
  committed reproducible wheel in `deploy/dagster/wheels/` (README there). Every deploy
  that can build runs `scripts/check_python_locks.py` first and refuses a Dockerfile, lock or
  compose file in this tree that breaks this (the allowed base image, builds and images are
  constants in that script). It is stdlib-only and must stay runnable on Python 3.9,
  so after changing it or a Dockerfile, run
  `python3 scripts/check_python_locks_variants.py`, which runs it under `/usr/bin/python3` when
  there is one: every mutation variant must behave as expected. The test venv `.venv-mcp`
  installs the MCP image's lock plus `nostrology_mcp/requirements-dev.txt` on Python 3.12 (the
  README, under Development): the lock pins cp312 wheels, so the minor version must match the
  image's and the patch need not.
  `dagster_code/requirements-test.txt` is pytest constrained to the Dagster lock, which the gate
  installs beside it, as the README's `.venv` does.
  `requirements-pipeline.txt` is no image's any more: the test gate takes its DuckDB from it.
  `crosscheck/requirements.txt` is the cross-checks' `.venv-crosscheck`, recipe in its `.in`.
  To change a Python dependency (locally, from the repo root; `.venv-uv` bootstrap in
  `scripts/requirements-uv.in`):
  1. direct: edit the `.in` pin, then `scripts/python-lock.sh compile <target>`;
     transitive (a security fix, say): `scripts/python-lock.sh upgrade <target> <package>`;
  2. `scripts/python-lock.sh check` (re-derives every lock and rebuilds the wheel) and
     `python3 scripts/check_python_locks.py`;
  3. review the lock diff: only the intended packages may move (a consistent hand edit
     passes both checks, so the diff is the only place it shows);
  4. build the image locally and test inside it, rebuild `.venv-mcp` or `.venv` for a change to
     its lock (the README, under Development), deploy, and read the build log: the FROM
     digest and the pip `Successfully installed` line.
  Still floating: the images' apt packages and the Dagster run store's `postgres:16` (a local
  tag).

## Writing conventions

These hold everywhere: comments, docs, log strings, commit messages, issues.

- **Name every Nostr kind as `kind-NNNNN (slug)`**, for example `kind-9735 (zap-receipts)`. The
  slug is the `slug =` field of that kind's entry in nozzl's `config/kinds.toml`; a kind that
  file does not list keeps the slug this tree already gives it, such as `kind-1 (posts)`. For a
  kind with neither, say what it is in a few words: `kind-24133 (NIP-46 remote-signing)`.
  nozzl's `scripts/check-kind-slugs.py <file>` checks the kinds that file lists. A line carrying
  `kind-slug-ok` is exempt, for a fixture or an assertion on runtime output.
- **Name every issue as `#NN (title)`**, with the issue's own title, cut at most to its leading
  clause, on every mention.
- **Name every HTTP status as `NNN (reason phrase)`**, for example `502 (bad gateway)`. Consumers
  branch on exactly which one they get, so a bare number invites a wrong guess.
- **Write times in UTC.**
- **Use plain ASCII punctuation in new text**: `-` rather than an em-dash, `->` rather than an
  arrow, `...` rather than an ellipsis. Non-ASCII punctuation escapes in JSON and breaks `grep`.

## Conventions that bite

- **Never use `from __future__ import annotations` in a module defining a Dagster
  `@asset`/`@op`/`@sensor`/`ConfigurableResource`** - PEP-563 stringifies the annotations and
  Dagster's context-type-hint validation rejects it (`DagsterInvalidDefinitionError`). It IS fine
  in plain modules Dagster never introspects (e.g. `loader/load_jsonl_exports_into_postgres.py`).
- **OSS Dagster sensors AND schedules default to STOPPED** - so each one that must run says
  `default_status=RUNNING` in code: the five observability sensors (last-run ok/fail writers, the Discord failure and recovery sensors, the freshness watchdog) and the seven surface
  schedules (`nectar_daily` is STOPPED). A state stored by a UI toggle overrides the default.
- **Who reads what is declared, not inferred from job names** (`dagster_code/pipeline/lineage.py`):
  each asset's serving tables, and each consumer (page, landing panel, MCP domain) with the
  tables it reads. definitions.py makes the consumers non-materializable assets in the
  `consumers` group, and the Discord failure post walks that graph to say who a failed run leaves
  stale. A new page, panel, MCP table or serving table is a lineage.py edit; `python3
  scripts/check_lineage.py` (also run by the tests when nostrology-web sits beside this repo,
  which it does not in the gate, so run it locally after such an edit) fails on any drift
  between the declarations and the code. `python -m dagster_code.alert_selftest` in the
  dagster-code container posts marked TEST samples; with `--probe` it posts nothing and says
  whether Discord accepts the webhook from there: the last check the deployment's Dagster deploy
  runs (it warns, never fails the deploy). Discord's edge
  answers urllib's default User-Agent with 403 (forbidden), error code 1010, so every request to
  it sends `sensors.DISCORD_HEADERS`.
- Tests live beside the code: `dagster_code/tests/`, `loader/test_*.py`, `nostrology_mcp/tests/`
  and `cashu_redeemer/test/` (`npm test`; `npm run test:mint` is live against a public test mint,
  and nothing runs it automatically). The `test` job in `.github/workflows/gate.yml` runs all
  four, and the deploy tooling's own checks, with the contract tests against a scratch Postgres. A
  deploy that changes what runs dispatches it on main and ships only the commit it passed; its
  header says which deploys do not wait for it.
  Nothing runs it on a push. Each pytest suite has a ceiling on its skips and a floor on the tests
  that run
  (`scripts/check_skip_ceiling.py`; the redeemer's `npm test` has neither), so a deliberate new
  skip or a large deletion moves the number in `gate.yml` and `test_gate_workflow.py` in the same
  change.
  `nostrology_mcp/verification/` is not a suite: its batteries ask the deployed endpoint and read
  production, are run by hand at a launch or release, and go live even with `--reuse` or
  `--self-test` when there is no cache for today (its README).
  The local checks are the README's, under Development: `.venv`, built from the Dagster lock and
  its pytest lock, runs `dagster_code/tests` and `loader`, and `.venv-mcp` runs
  `nostrology_mcp/tests`. What needs DuckDB or the gate's scratch Postgres
  (`NOSTROLOGY_CONTRACT_DSN`) skips there and runs in the gate; the MCP's live-database tests run
  only by hand, against the database `NOSTROLOGY_TEST_DSN` names (`nostrology_mcp/README.md`);
  `bash -n` and `py_compile` cover the rest. To run `dagster_code/tests` inside the Dagster
  image itself (the DuckDB tests, the lineage drift check and the synced-blocklist test skip
  there), add pytest from
  `dagster_code/requirements-test.txt`, the gate's own pytest lock, since the image has none. From
  the repo root, with `T` a scratch directory, on a throwaway builder so nothing touches the shared
  Docker build cache, for linux/arm64, the platform the Dagster lock is compiled for:

  ```sh
  mkdir -p "$T/ctx" && git ls-files -co --exclude-standard -z | tar --null -T - -cf - | tar -xf - -C "$T/ctx"
  docker buildx create --name lockprobe --driver docker-container
  docker buildx build --builder lockprobe --platform linux/arm64 --load -t lockprobe-dagster \
    -f "$T/ctx/deploy/dagster/Dockerfile.dagster" "$T/ctx"
  docker run --rm -i lockprobe-dagster sh -c 'cat > /tmp/pt.txt && pip install --no-cache-dir \
    --require-hashes --only-binary :all: -q -r /tmp/pt.txt && python -m pytest dagster_code/tests -q -p no:cacheprovider' \
    < dagster_code/requirements-test.txt
  docker rmi lockprobe-dagster && docker buildx rm lockprobe
  ```

  The pytest lock is constrained to the Dagster lock, so the tests run against exactly the locked
  packages plus pytest, pluggy and iniconfig. `buildx create` refuses a name already taken, so a run
  that stopped before its last line leaves the builder behind for the next one to remove. Remove the
  `moby/buildkit` image afterwards if it was not present before.
- Commit/push only when explicitly asked; no AI co-authorship trailer. Logs are shipped to a log
  store that others read - never log secrets (DSN-with-password, PGPASSWORD, the Discord webhook).
  Nor record a client's IP address or a visitor's npub, in a log line, a metric label, a report or
  a table. Reading one from the live request is fine, as the MCP server does
  to feed the redeemer's per-IP rate limit, held in memory only.

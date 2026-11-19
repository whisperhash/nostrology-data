# The /routstr surface - design

**Living design doc** for the Routstr data surface end to end: from the raw Nostr
events in the nozzl S3 archive, through the nightly Dagster compute, to the serving
contract the webapp reads. Built 2026-08-24.

Companions, each owning a different layer:

- [dagster-orchestration-design.md](dagster-orchestration-design.md) ("The /routstr surface") -
  how this job fits the whole Dagster orchestration.
- `docs/dataset-reference/routstr-dataset-reference.md` - the METRIC SEMANTICS: what every
  number counts, what it excludes, what it must not be read as - and (section 8) the
  reconciliation against routstr.com/stats. This doc does not restate them.

## 1. Shape of the whole thing

```
  relays (nos.lol / damus / primal / nostr.mom ...)
     |
     v  nozzl producer (continuous acquisition since 2026-06-03/04)
  s3://$NOZZL_ARCHIVE_BUCKET  (Parquet, c=addressable/k=3842x/p={shard}/dt={ingest-date})
     |
     |  nightly 02:45 UTC (routstr_daily) or the audited manual materialize
     v
  routstr_refresh (Dagster, one run)
     |-- routstr_announcements_extract  k=38421, ALL versions   \
     |-- routstr_catalog_extract        k=38423, ALL versions    \  Athena UNLOAD
     |-- routstr_reviews_extract        k=38425, ALL versions    /  (gz NDJSON)
     |-- routstr_analytics_extract      k=38422 -> LAST snapshot/
     |         per (pubkey, UTC day), schema-tag-filtered
     |         each: flush WAL -> Glue diff-register -> UNLOAD -> parts check
     v
  routstr_snapshot (compute asset, in-container Python, pipeline/routstr.py)
     |   fetch parts -> parse -> horizon filter -> build_snapshot()
     |     (registry + LEDGER RECONSTRUCTION + catalog + reviews)
     |   -> invariants -> anti-shrink floors -> ONE transaction:
     |        TRUNCATE+INSERT routstr_providers      (per-provider registry, ~51 rows)
     |        TRUNCATE+INSERT routstr_provider_days  (the ledger, ~1,100 rows)
     |        INSERT          routstr_snapshot       (ONE JSONB document, append-only)
     v
  Aurora (serving DB)    <-----------------------------------------+
     |                                                             |
     |  SELECT data ... ORDER BY generated_utc DESC, id DESC       |  independent
     |  LIMIT 1   (role nostrology_web)                            |  re-derivation +
     v                                                             |  zero-diff compare
  nostrology-web SvelteKit server (+page.server.ts)                |  (the agreement
     |  SSR: snapshot embedded once in the HTML (~134 KB doc)      |   gate, section 5)
     v                                                             |
  browser (4 pills, each its own address, render client-side)      |
                                                                   |
  a workstation: crosscheck/build_routstr_snapshot.py (DuckDB over raw S3)
```

Same two defended properties as /marketplaces: (1) a failed refresh can never blank,
shrink or tear the page - the page reads only the newest COMPLETE snapshot row, rows
are appended never updated, every gate runs before the single write transaction;
(2) the served numbers are independently re-derivable, field-for-field.

## 2. Sources

| Kind | Role in the dataset |
|---|---|
| kind-38421 (routstr-provider-announcements) | Provider registry: names, endpoints, mints, versions, first/last-seen. ALL versions (~5,951 events / 50 pubkeys). Squatted kind - the u+d discriminator and the exclusion ledger live in the compute. |
| kind-38422 (routstr-analytics-snapshots) | The usage ledger. ~120 KB/event, ~15-min republish cadence, so the extract reduces IN Athena to the last snapshot per (pubkey, UTC day) with the `routstr.analytics.snapshot.v1` schema tag (~703 rows) - lossless for the ledger up to one trailing-edge intraday-revision case. |
| kind-38423 (routstr-model-catalog) | The curated allowlist + its size history. ALL versions (23 events; 2 non-curated coordinates ignored by the compute). |
| kind-38425 (routstr-provider-reviews) | Curation labels + timeline. ALL versions (30 events, 2 reviewer keys; only the curated key sets status). |

Freshness: each extract flushes its kind's WAL first, so a run sees the archive as of
its own start.

## 3. The pipeline (nostrology-data)

One Dagster job, `routstr_refresh`, group `routstr`, on the shared
`in_process_executor` + run-scoped `AuroraDial` pattern every surface uses. Schedule
`routstr_daily` 02:45 UTC (sixth in the nightly order, before `ngit_daily` at 03:15; the `QueuedRunCoordinator`
with `max_concurrent_runs: 1` serializes it against everything else).

**Extract assets** (four instances of `_extract_asset`): template selection by
`(kind, into)` override in `pipeline/sql.py` - the three small kinds reuse
`extract_addressable_all.sql` unchanged; kind-38422 (routstr-analytics-snapshots) has
its own `sql/extract_routstr_analytics_daily.sql` carrying BOTH semantic filters
(schema tag + last-per-(pubkey, day) with the created_at DESC, id ASC tie-break) so
they are shared verbatim with the reference generator's SQL. Blocking
`extract_nonempty` check on each.

**Compute asset** (`routstr_snapshot`), fail-loud in this order - every step before
the write leaves the previous serving data live:

1. Fetch the four part sets to `NOSTROLOGY_EXTRACT_TMP`, parse gz NDJSON.
2. `pipeline/routstr.build_snapshot()` - pure (rows in, doc out; no AWS/DB/clock):
   the `created_at <= now+2d` horizon on all four kinds, the provider registry +
   endpoint/test classification, the LEDGER RECONSTRUCTION (full-coverage-only +
   newest-wins bucket merge on the UTC grid; integer msats; weekly pre-daily era -
   the rules are stated in the dataset reference and pinned by
   `tests/test_pipeline_routstr.py`), reviews, catalog, and every aggregation the
   page renders.
3. `check_invariants()` - cumulative monotone + endpoint agreement (both at sats and
   msats grain), daily-series sorted/deduped/contiguous/not-future, the model rows
   re-derived by an independent walk of the ledger (the wire-side named+others ==
   totals identity is a THIRD-PARTY property and is deliberately NOT an invariant),
   provider partition against the uncapped registry, display-id uniqueness,
   catalog-history tail, success-rate range - and, since 2026-08-29, the substrate
   checks (model days re-sum to the model rows and the chart series, catalog versions
   re-derive the history, reviews carry the timeline). Failure raises before any
   write; success is recorded as the `snapshot_invariants` asset check.
4. `write_pg()` - `MIN_LEDGER_ROWS` (850) + `MIN_PROVIDERS` (39) anti-shrink gates
   (~80% of the 2026-08-24 measured 1,088 ledger rows / 49 announced providers; the
   ledger only grows, so a shrink signals an extract/merge regression), then ONE
   transaction: TRUNCATE+INSERT the five substrate tables (`routstr_providers`,
   `routstr_provider_days`, `routstr_model_days`, `routstr_catalog_versions`,
   `routstr_reviews`), INSERT one `routstr_snapshot` row. `strip_nul` on every string
   reaching Postgres.

**Manual materialization** is audited like a deploy: the deployment's re-run action runs
`dagster job launch` in the running dagster-code container. Submit-only; the run queues like a
scheduled one.

## 4. The serving contract - how the webapp gets the data (the "API")

Identical shape to /marketplaces: no HTTP API; the API is
a database contract - one table, one query, one document, two roles.

- **Writer** (role `nostrology`): `routstr_snapshot(id IDENTITY, generated_utc, run_id,
  data JSONB)`, append-only, written in the same transaction as the two substrate
  tables so they can never disagree. `schema/init.sql` is applied in-asset, so a fresh
  database self-provisions; roles.sql DEFAULT PRIVILEGES auto-grant the read roles.
- **Substrate** (not read by the page; the MCP tools' source since 2026-08-29 -
  [routstr-mcp-design.md](routstr-mcp-design.md)):
  `routstr_providers` (one row per provider - registry + classification + review +
  30d and all-time aggregates), `routstr_provider_days` (the reconstructed ledger:
  (pubkey, grain 'd'|'w', day) -> msats/requests/tokens + per-model JSONB),
  `routstr_model_days` ((model, grain, day) over included providers),
  `routstr_catalog_versions` (every archived curated version with its added/removed
  delta) and `routstr_reviews` (the timeline, full keys). All written in the document's
  transaction and tied to it by `check_invariants` - so the tools and the page cannot
  diverge, and "why is this provider excluded?" is one SELECT (or one MCP call) away.
- **Reader** (role `nostrology_web`): `SQL_ROUTSTR_SNAPSHOT` = newest row by
  `(generated_utc DESC, id DESC)`, exported from `postgres.ts` so the integration
  test runs the production SQL verbatim. Zero rows throws - the page 500s loudly.
- **Document contract**: producer = whatever `build_snapshot()` returns (the
  agreement gate makes the two implementations one shape); consumer = the
  `RoutstrSnapshot` interface in nostrology-web `src/lib/shared/data-types.ts`.
  Revenue in series/tables is integer MILLISATS; only summary headline fields are
  pre-converted sats; the page's msats->sats division is the one permitted
  presentation-level derivation. Evolution rules as in the marketplaces doc
  (additive: both computes -> TS -> page; breaking: page first).
- **Freshness**: newest complete refresh, normally the same UTC day's 02:45 UTC run;
  `generated_utc` rendered in the page footer next to the self-reported caveat.

The page itself: `/routstr`, four pills (Overview / Providers / Models / Network)
over the one embedded document, each with an address of its own since 2026-10-19
(`/routstr/providers`, `/routstr/models`, `/routstr/network`),
noindexed with the page and out of the sitemap - deliberately NOT in
the top-level nav or the landing domain cards; linked from `/wip` while incubating
(guardrail tests pin both the nav absence and the /wip presence). Since 2026-10-18
(the operator's call while the page is still hidden) the
exclusion ledger's section is the site's standard "What is counted": last on Overview,
collapsed, with an address of its own, `/routstr/methodology` (`/routstr#methodology`
until 2026-10-19), and the hover icon that copies it. Until then it was Overview's
first card, always open, headed "What is counted - and who is not".

## 5. The verification harness (the agreement gate)

Two full implementations of the metrics exist ON PURPOSE:

- **Production**: `dagster_code/pipeline/routstr.py`, fed by Athena extracts,
  writing Aurora.
- **Reference**: `crosscheck/build_routstr_snapshot.py`, DuckDB straight over
  the raw S3 parquet, run on a workstation - deliberately structured differently
  (SQL-side current-version selection, candidate-set bucket merge vs the
  production's ordered overwrite) so agreement is evidence, not tautology.

`crosscheck/compare_routstr_snapshots.py` diffs the two snapshot documents field-by-field;
the contract is ZERO diffs (procedure in its header: read the newest DB row, re-run
the reference pinned to its `generated_utc` - as `--as-of` AND, since 2026-08-30, as
`--ingested-before`, which drops archive rows ingested after that instant so the gate
reproduces a past run even after the 12h chunk roll has landed newer events; an
`archive_events` mismatch still means the inputs differ - re-run rather than chase).
First passed 2026-08-24 over the full year-deep archive (5,951 + 703 + 23 + 30 input
events, 133,944-byte documents, byte-identical numbers). The Routstr MCP battery
(`nostrology_mcp/verification/routstr_run.py`) runs this gate on every battery run and
uses the reference document as the independent side for every document-backed metric.

Any methodology change must land in BOTH implementations in lockstep or the next
check fails.

## 6. Forks and ambiguities decided (2026-08-24)

The decisions below were open when the surface was built; each was settled from primary
sources (the Routstr repos, the live network, our archive) and is recorded here so a
future reader can tell a deliberate choice from an accident. Reversing any of them is a
methodology change: it lands in BOTH implementations (section 5) and in the dataset
reference.

| # | Fork | Decision | Why |
|---|---|---|---|
| 1 | Snapshot-of-the-latest vs reconstructed history | Reconstruct a daily ledger from the trailing windows of EVERY archived kind-38422 (routstr-analytics-snapshots) event. | The archive is the only place the old windows survive; a latest-only view would be a trailing-30d approximation with no history. Probed live: buckets are UTC-grid aligned, trailing-edge buckets get truncated, late revisions go upward - the merge rules follow from those three facts. |
| 2 | Verify vs relay the self-reported numbers | Serve as reported, label "self-reported" everywhere, no outlier capping. | Payments are RIP-01 HTTP-header Cashu, invisible on Nostr: nothing CAN verify them. Capping would substitute our guess for their claim. |
| 3 | Which reporters count | Exclude test/staging-marked (word-boundary on name/host labels, TLD exempt) and private-endpoint-only nodes from headline aggregates; keep them visible with the reason. | A staging node's "revenue" is not network revenue; a loopback-only node serves no client. The exclusion ledger keeps the choice auditable. `.dev`/`.test` TLDs are NOT markers - `routstr.rewolf.dev` is a production node. |
| 4 | Provider identity | The Nostr pubkey; `provider_id` inside the analytics content is informational only. | Announcements and snapshots are signed by the key; the content field is free text a node can set to anything. |
| 5 | What is a "genuine" kind-38421 (routstr-provider-announcements) event | A valued `u` tag AND a valued `d` tag (the consumers' own discriminator). | The kind is squatted by lnproxy fee quotes, which carry `d` but no `u`. |
| 6 | Model identifiers | Verbatim, no normalization; long tail folded into "(other)" past 100 rows. | `openai/gpt-x` vs `gpt-x` is how the providers report; any folding rule would be ours, and the official stats page does not fold either. |
| 7 | Curation semantics | Status = newest label from the curated key ONLY; positive = {trusted, verified, lgtm}; anything else disables. Other reviewer keys show in the timeline, set no status. | Follows routstr-sdk's own allowlist logic (the enable set is copied from it); the live `bad` label is outside the SDK vocabulary and therefore disables. |
| 8 | Athena reduction | Last snapshot per (pubkey, UTC day), schema-tag filtered. | ~703 rows instead of ~21,800 ~120 KB events; lossless for the ledger except one trailing-edge intraday-revision case. |
| 9 | Weekly (1y) buckets | Used only for a provider's PRE-daily era. | Grains never overlap, so all-time sums are direct; daily wins wherever both exist. |
| 10 | Timestamp trust | `generated_at` only within 1 day of `created_at`, else `created_at`. Buckets ending after OUR clock are dropped. | A node's clock is third-party input; the clamp blocks future-day injection. |
| 11 | Units | Integer msats in the document; sats only in headline summary fields and in the page's display division. | msats is what the providers publish; rounding once, at display, avoids drift between the two implementations. |
| 12 | Anti-flood caps | 100 model rows, 200 reviews, 250 provider rows (+ `providers_total`), 20 URLs / 50 mints per provider, 20 provider chart series. | Open kinds are floodable; every list in a served document is bounded. |
| 13 | Provider chart Top-N | Ship the top 20 rank-ordered plus a residual; the page's Top 5/10/20 selector folds ranks N+1.. client-side. | One document serves every selector state; the `sum(series) == daily` invariant makes the fold exact. |
| 14 | Schedule slot | 02:45 UTC, sixth in the nightly order (`ngit_daily` follows at 03:15 since 2026-09-25). **Retimed 2026-09-08** from 22:30 UTC when the whole block moved past the midnight-UTC data boundary, so a refresh reads a COMPLETE closed day. NOTE the original rationale also cited a quiet kind-38422 (routstr-analytics-snapshots) chunk window at 22:30; that claim was never re-derived for 02:45 and should be treated as unverified. | Serialized behind the other five surfaces by the run coordinator. The original slot also cited a quiet kind-38422 (routstr-analytics-snapshots) chunk window at 22:30 UTC; that claim was NOT re-derived for 02:45 and is no longer relied on - the slot is now chosen by the midnight-UTC data boundary instead. |
| 15 | Placement | `/routstr`, linked from `/wip` only; not in the nav or the landing cards. | Incubating; graduation is a separate, deliberate change (guardrail tests pin both states). |
| 16 | "30d" window bound | The last 30 COMPLETE UTC days, day-aligned. | A bound at `generated_at - 30x86400 s` lost the 30th day on any run after midnight; found by reconciling against routstr.com/stats (dataset reference section 8). |
| 17 | A day a newer snapshot omits | Keep the observed day (floor semantics); never zero it. | Buckets are sparse, so absence is not evidence; a node that reset its own history does not un-happen what we archived. Pinned by test; the doc used to say the opposite. |
| 18 | Deploy order | Data repo first (schema, job, one materialize), then web. | `getRoutstrSnapshot` throws on an empty table by design; the page must never 500 on first deploy. |
| 19 | The MCP surface (2026-08-29) | The tools select from the document and the substrate; nothing in the MCP server computes a Routstr number. New substrate tables are pipeline-written and invariant-tied. | [routstr-mcp-design.md](routstr-mcp-design.md) - the page and the tools read the same refresh by construction. |

## 7. Operations

| Question | Answer |
|---|---|
| Did last night's refresh run? | `last-run-routstr_refresh.json`, beside the file `DAGSTER_LAST_RUN_JSON` names (`overall`, `finished`), or the read-only Dagster UI's GraphQL, or the Dagit run list |
| Refresh failed - is the page ok? | Yes by construction: it serves the previous snapshot row; fix the pipeline at leisure |
| Force a refresh now | The deployment's re-run action for `routstr_refresh` (queued, serialized) |
| The run stays QUEUED | Something holds the single run slot - look for a `STARTED` run in the read-only Dagster UI. An orphan (worker killed by a deploy of the Dagster stack mid-run) is only reaped by `run_monitoring.max_runtime_seconds`; an operator can terminate it in the RW UI. Never deploy the Dagster stack while a run is in flight (`dagster-orchestration-design.md`, queue-wedge note). |
| First-ever deploy ordering | data repo first (the schema, then the Dagster stack for the job), then ONE materialize so a snapshot row exists, THEN the web deploy - `getRoutstrSnapshot` throws on an empty table by design, and /wip links the page the moment the web deploy lands |
| Failure alerting | `discord_failure_sensor` -> the CI channel, like every surface |
| The numbers look wrong | Run the agreement procedure (section 5); zero diffs means the dispute is with the methodology (dataset reference), not the pipeline |
| Anti-shrink tripped | If legitimate (a provider mass-vanished from the archive - should be impossible; the archive keeps every version), LOWER the floor in `pipeline/routstr.py` deliberately, redeploy, re-materialize |
| A provider looks mis-excluded | `SELECT * FROM routstr_providers WHERE pubkey LIKE '<prefix>%'` (any read role) - the classification inputs are all in the row |
| Where is the raw ledger? | `routstr_provider_days` on Aurora (any read role), or `nostrology_query_metric` with `metric=provider_activity, domain=routstr` on the public MCP endpoint |
| Do the tools and the page agree? | By construction (same refresh, invariant-tied substrate); to check anyway: the `Routstr - *` questions of `python3 -m nostrology_mcp.verification.run`, or the live gates in `nostrology_mcp/tests/test_routstr.py` |

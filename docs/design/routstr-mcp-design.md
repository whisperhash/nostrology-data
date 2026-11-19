# The Routstr MCP surface - design (2026-08-29)

The Routstr AI-inference marketplace (dev.nostrolo.gy/routstr) becomes the second
domain of the public MCP endpoint (`https://dev.nostrolo.gy/mcp`, domain
`routstr`): ten metrics and five reference records, listed on `/data` after
Zapstore and usable by any MCP client. This doc records the
decision forks, the architecture, and the test tiers; the serving-side design of
the page itself is [routstr-design.md](routstr-design.md) and the metric semantics
are `docs/dataset-reference/routstr-dataset-reference.md`.

## 1. The requirement that shaped everything

The page and the tools must be powered by the SAME underlying calculated metrics -
no divergence. The /routstr page renders exactly one thing: the newest
`routstr_snapshot.data` JSONB document, computed once nightly by
`dagster_code/pipeline/routstr.py` and proven against an independent DuckDB
re-derivation (the agreement gate, `crosscheck/compare_routstr_snapshots.py`). Anything the tools compute themselves
is, by construction, a second implementation of the methodology that the
agreement gate does not cover. The whole design follows from refusing that.

## 2. Forks decided

| # | Fork | Decision | Why |
|---|---|---|---|
| 1 | Who aggregates - the pipeline, or MCP SQL per call? | **The pipeline.** A tool may filter, window and select pre-computed rows; it never computes a number the page publishes. Where a drilldown shape the page lacks is wanted, the pipeline writes it as another substrate table from the same in-memory state, in the same transaction, tied to the document by `check_invariants`. | Substrate SQL that re-implements the exclusion join, the weekly-grain rule, the day-aligned 30d bound and the floor semantics is a third implementation of the ledger rules, in a third language - the 2026-08-24 29-day bug's shape. The document-only alternative could not serve per-provider or per-model series beyond the chart's top-N fold. |
| 2 | Tool topology - extend the two tools, or dedicated Routstr tools? | **Extend `nostrology_query_metric` and `nostrology_query_reference`** with `domain="routstr"`. | `domain` was the axis the registry was built on; Routstr's shape IS metrics and records. The README's own rule for a new tool is a structural difference (records vs measures), not a new domain. Keeps the free tool count at two and `/data` registry-driven. Cost: the entity filters had to be generalized (`provider_pubkey`, `model`) and the tool descriptions grow. |
| 3 | Catalog shape | Multi-measure rows (msats + requests + tokens in one row) where the document row is multi-measure; fixed 30d/all-time columns served from the document; caller windows only on the series and the substrate-summed `@total` grains. | One call answers an Overview tab; a fixed window is honest page parity; a caller window over pre-aggregated buckets is the only SQL arithmetic, and it is gated (section 5). |
| 4 | "How has the catalog changed?" | A new `routstr_catalog_versions` substrate table with per-version `added`/`removed`, computed in the pipeline. | The document keeps only counts per version; the delta is a calculated metric and so belongs in the pipeline (fork 1). |
| 5 | Model per-day drilldown | A new `routstr_model_days` table `(model, grain, day)`, exclusions already applied. | Per-model per-day for every model is a pipeline aggregation over the ledger's JSONB, not a selection. It has a `grain` column like the provider ledger so an unbounded SUM equals the document's all-time model figures exactly (the weekly pre-daily era is in both). |
| 6 | Reviews | A new `routstr_reviews` table (newest 5,000 events, full node key, reviewer key, event id). | The document carries 200 rows with 12-hex node ids; a per-provider timeline needs the full key. |
| 7 | Windows on `@total` grains | Inline `from_day`/`to_day` binds applied BEFORE aggregation (`window_mode="day_inline"`), keyed by bucket START day; a bound window sums DAILY buckets only, an unbounded call sums both grains. | The nutzaps convention ("a window includes exactly the buckets STARTING inside it"), already in the registry. Parity by construction: the page's windowed figures are daily-only (`grain == "w"` never enters the 30d accumulators), so the SQL carries `((from IS NULL AND to IS NULL) OR grain = 'd')` rather than relying on the geometric fact that a 30-complete-day window rarely contains a weekly bucket start (a second review, 2026-08-29, flagged the geometry-only version); the pg tier asserts it with a window that deliberately spans a weekly bucket, and the live gate asserts the 30d/all-time equality. **The window is anchored to the REFRESH day** (`win30_lo` derives from the run instant): "from = refresh day - 30, to = refresh day - 1", where `network_summary.refresh_generated_at` names the day - NOT the caller's today (a review caught the prose saying today; every test already used the refresh day). |
| 7b | Row caps on the substrate metrics | `provider_activity@day`, `model_activity@day` and `provider_leaderboard@current` carry `row_cap=2000` (`METRIC_ROW_CAP`): the server wraps the spec SQL with `COUNT(*) OVER () AS __total_rows ... LIMIT`, strips the count and emits a `truncated:` line (the reference records' contract). | These populations are third-party-growable (one row per provider-bucket / model-bucket) on a public unauthenticated endpoint; an unfiltered `model_activity@day` measured ~1 MB on a realistic table. The Zapstore metrics stay uncapped (bounded by nature). |
| 8 | Domain default | `domain` keeps its `zapstore` default; a metric or record name unique to one other domain resolves there (`resolve_domain`), and the ask is recorded (`params.domain`, `params.domain_resolved`). The tool descriptions say so plainly: the NAME wins over `domain`, even an explicit wrong one. | A model that omits `domain` on a Routstr name would otherwise take the unknown-metric path; metric names are unique across domains (pinned). The body cannot tell a defaulted `domain` from an explicit one, so "resolve only the default" is not implementable; making the rule explicit is the honest alternative. |
| 9 | Read consistency | A document spec reads only `routstr_snapshot` via the page's own newest-row selection; a substrate spec reads only substrate tables. Never both in one statement. | The 02:45 UTC write replaces both; one statement per side cannot straddle it. |
| 10 | Freshness | One probe for the family: `MAX(generated_utc)` of `routstr_snapshot`, declared via `MetricSpec.as_of_table` / `RecordSpec.as_of_table`. | The substrate is written in the snapshot row's transaction, so that instant IS its freshness; declaring it keeps the "probe reads this metric's own table" invariant checkable. |
| 11 | /data ordering | `DOMAIN_ORDER = ['zapstore', 'routstr']`, rest alphabetical. | Presentation order is a product decision; the paid nutzaps domain would otherwise sort ahead of the second free domain. |

## 3. Architecture

```
  routstr_refresh (nightly 02:45 UTC, pipeline/routstr.py)
     build_snapshot() -> (state, data)
     check_invariants(state, data)   <- document self-checks + SUBSTRATE checks (new)
     write_pg(): ONE transaction
        TRUNCATE+INSERT routstr_providers        (+ requests_alltime, tokens_alltime)
        TRUNCATE+INSERT routstr_provider_days    (unchanged)
        TRUNCATE+INSERT routstr_model_days       (new: model, grain, day, msats, requests, tokens, providers)
        TRUNCATE+INSERT routstr_catalog_versions (new: event_id, version_utc, model_count, models, added, removed)
        TRUNCATE+INSERT routstr_reviews          (new: event_id, node, label, reviewed_utc, reviewer_pubkey, curated)
        INSERT          routstr_snapshot         (the page's document, unchanged shape)
                |                                          |
     nostrology-web /routstr                     nostrology_mcp (domain routstr)
     reads the document                          document specs -> routstr_snapshot (page's row selection)
                                                 substrate specs -> the tables above (select/filter/window)
                                                          |
                                     /data (registry-driven)
```

**Metrics** (`metric.py`, `ROUTSTR_METRIC_SPECS`): `network_activity@day` (dense),
`cumulative_revenue@day`, `network_summary@current` (one row), `provider_leaderboard@current`
(substrate, filter `provider_pubkey`), `provider_activity@day|@total` (substrate ledger;
unfiltered = included providers so the day sums tile `network_activity`; a filter reaches an
excluded provider too), `model_leaderboard@current` (document, filter `model`),
`model_activity@day|@total` (substrate `routstr_model_days`), `catalog_size@day`,
`success_rate@current`, `provider_facets@current`.

**Records** (`reference.py`, `ROUTSTR_RECORD_SPECS`): `provider_profile` (optional
`provider_pubkey`), `provider_reviews` (optional `provider_pubkey`, windowable),
`catalog_versions` (no scope, windowable), `model_catalog` (no scope), `exclusions` (no scope,
document).

**Generalizations that carried the second domain**: `ENTITY_FILTER_BINDS` maps a filter
name to its bind (`provider_pubkey` -> `filter_provider`, `model` -> `filter_model`); the
server validates acceptance per spec before shape, with cross-domain remedies naming the
metrics that DO take the filter; `RecordSpec.scope_param` (`publisher_pubkey` |
`provider_pubkey` | None) replaces the implicit publisher scope, with `scope_required` and
`scope_bind`; the registry publishes a generic `params.scope` block for records (the Zapstore
`publisher_pubkey` block is kept for the v2 web reader) and the existing `params.entity_filters`
map for metrics; the envelope's scope line echoes every applied filter; `_cell` renders NULL
as `\N` for every type (the summary carries genuinely absent values).

## 4. The invariants that make the substrate safe to serve

`check_invariants` (pipeline) now also proves, before any write: every named model row's six
measures and `providers_30d` re-sum from `routstr_model_days` (the `(other)` row = residual +
folded tail) - a GENUINE cross-check, since `model_days` is accumulated independently of the
document's `model_tot`/`model_day`; each top model's chart series IS its daily model rows and
`(others)` is the per-day remainder; every catalog version's `added`/`removed` delta re-derives
from its own and the previous version's lists (genuine), while the `catalog.history` and
`catalog.models` equalities are consistency PINS rather than independent derivations (both sides
come from the same parse loop - a builder bug there would pass); the persisted review timeline is
a superset prefix of the document's, in the document's own `(-created_at, node)` order with a
total tie-break (a same-second batch of reviews would otherwise order differently and fail the
refresh every night - found in review, pinned by test). The document's key set is pinned unchanged
(`test_document_shape_is_unchanged_by_the_substrate`), so the agreement gate (which
compares documents) is untouched by the substrate work.

## 5. Test tiers

| Tier | Where | Catches |
|---|---|---|
| Pipeline unit | `dagster_code/tests/test_pipeline_routstr.py` (d) | substrate arithmetic on the hand-checkable fixture, invariant corruption detection, folded-tail handling, `write_pg` writing all six tables in one transaction |
| Spec invariants | `nostrology_mcp/tests/test_metric.py`, `test_reference.py` (parameterized, both domains) + `test_routstr.py` (a) | binds vs placeholders, declared tables, no clock anchor, freshness declaration, prose conventions, one-side-only reads, column aliases in SQL |
| Wired tools, stubbed DB | `test_routstr.py` (b) | domain resolution, filter acceptance-before-shape and remedies, `day_inline` binds, envelope scope lines, reference scope rules, schema advertisement |
| **Pipeline-to-tool contract** | `test_routstr_contract_pg.py` (local Postgres, `NOSTROLOGY_CONTRACT_DSN`) | the REAL `write_pg` on the pipeline fixture, then every metric and record through the wired tools against the same build's document - THE drift detector, incl. the 30d/all-time parity of the `@total` grains |
| Live gates | `test_routstr.py` (bottom, `NOSTROLOGY_TEST_DSN`) | EXPLAIN every statement on Aurora; the windowed SUM equals the production document's per-provider 30d/all-time; `network_activity` is the production daily series |
| Deployed battery (shared) | `nostrology_mcp/verification/` (R01-R22, inside the Zapstore run) | the public endpoint vs the document read via psql, an independent SQL re-sum in `truth_sql`, and the live /routstr page's embedded figures |
| **Routstr battery** (2026-08-30) | `nostrology_mcp/verification/routstr_run.py` (+ `routstr_plan/questions/truth`) | ~220 checks organised per tool/metric/record + contract, against THREE independent sides: the cross-check's DuckDB re-derivation over the raw archive pinned to the served refresh (a second implementation), the raw substrate rows sliced in Python, and the page-embedded document via `node`. |
| Web | nostrology-web `catalog.loaders.test.ts`, `mcpRegistry.test.ts` | domain order, generic scope rendering |

## 6. Deploy order

1. nostrology-web FIRST (domain order, generic record scope on `/data`). The new record page reads `params.scope` with a `publisher_pubkey` fallback, so
   it renders both the old and the new registry payloads; the OLD page dereferences
   `params.publisher_pubkey.required` unconditionally and would 500 on every
   `/data/routstr/<record>` page from the moment the new MCP server is up (review finding).
2. nostrology-data: the schema (the two provider columns + three tables, all
   `IF NOT EXISTS` / `DEFAULT 0`), then the Dagster stack (the code-location image is BUILT
   from the repo, so the new `write_pg` needs it; never while a run is in flight), then a run
   of `routstr_refresh`, **verify** that `routstr_providers.requests_alltime` is non-zero and
   `routstr_model_days` is populated, and only THEN the MCP server. Between the schema apply
   and a successful materialize the pre-existing provider rows carry
   `requests_alltime = tokens_alltime = 0` and the three new tables are empty - so an early
   MCP deploy would serve zeros presented as real all-time figures, not just empty tables.

**As executed, 2026-08-29:** the web and the data schema at ~20:30 UTC; the nightly chain
was left to finish (`routstr_refresh` 22:30:08-22:31:26 UTC ok, with the OLD code - the new
columns stayed `DEFAULT 0`, the new tables empty, as predicted); the Dagster stack at 22:33
UTC; a manual run of `routstr_refresh` submits to the run coordinator and RETURNS AT SUBMIT
TIME - its "success" only means the launch was accepted, the run itself (22:35:45-22:37:17
UTC, `overall: ok`) is watched via `last-run-routstr_refresh.json`; verified in production:
52 providers (47 included) with `requests_alltime` = 560,244, `routstr_model_days` 3,084 rows
/ 250 models (392 weekly-era), 24 catalog versions, 30 reviews, `routstr_snapshot` newest row
from that run; then the MCP server (server `0.3.0` advertising `domain: [routstr, zapstore]`).
Verification: the live battery (217 checks, 0 FAIL), and `/data` listing the 15 Routstr
entries under `?domain=routstr` with every record page 200 (ok).

## 7. Not done, with triggers

- Distinct provider counts at `model_activity@total` (not additive over buckets) - trigger: a
  caller asks; the answer is a `jsonb_object_keys` walk over `routstr_provider_days.models`
  in the pipeline, not in SQL.
- The reference generator (`crosscheck/build_routstr_snapshot.py`) does not produce the substrate
  tables; they are covered by the pipeline invariants instead. Trigger: a substrate-only
  methodology change (none exists - every substrate value is a re-arrangement of ledger
  buckets the document already gates).

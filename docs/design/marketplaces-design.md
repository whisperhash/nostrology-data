# The /marketplaces surface - design

**Living design doc** for the marketplaces data surface end to end: from the raw Nostr
events in the nozzl S3 archive, through the nightly Dagster compute, to the serving
contract the webapp reads - the "API" section below. Live since 2026-08-23.

Companions, each owning a different layer:

- [dagster-orchestration-design.md](dagster-orchestration-design.md) ("The /marketplaces
  surface") - how this job fits the whole Dagster orchestration.
- `docs/dataset-reference/markets-dataset-reference.md` - the METRIC SEMANTICS: what every
  number counts, what it excludes, what it must not be read as. The definitions live
  there so they survive any serving surface; this doc does not restate them.

## 1. Shape of the whole thing

```
  relays (plebeian/conduit/nos.lol/damus/primal/nostr.mom)
     |
     v  nozzl producer (continuous acquisition)
  s3://$NOZZL_ARCHIVE_BUCKET  (Parquet, c={class}/k={kind}/p={shard}/dt={ingest-date})
     |
     |  nightly 02:15 UTC (marketplaces_daily) or the audited manual materialize
     v
  marketplaces_refresh (Dagster, one run, ~90-100 s wall)
     |-- marketplaces_listings_extract    k=30402, ALL versions      \
     |-- marketplaces_shipping_extract    k=30406, ALL versions       > Athena UNLOAD
     |-- marketplaces_deletions_extract   k=5 -> self-authored rows   /  (gz NDJSON)
     |         each: flush WAL -> Glue diff-register -> UNLOAD -> parts check
     |         deletions runs after listings: its template joins to their ids
     v
  marketplaces_snapshot (compute asset, in-container Python)
     |   fetch parts -> parse -> horizon filter -> build_snapshot() -> invariants
     |   -> anti-shrink floors -> ONE transaction:
     |        TRUNCATE+INSERT marketplaces_listings   (per-coordinate, ~12k rows)
     |        INSERT          marketplaces_snapshot   (ONE JSONB document, append-only)
     v
  Aurora (serving DB)    <-----------------------------------------+
     |                                                             |
     |  SELECT data ... ORDER BY generated_utc DESC, id DESC       |  independent
     |  LIMIT 1   (role nostrology_web, 0.017 ms)                  |  re-derivation +
     v                                                             |  zero-diff compare
  nostrology-web SvelteKit server (+page.server.ts)                |  (the agreement
     |                                                             |   gate, section 5)
     |  SSR: snapshot embedded once in the HTML (~99 KB page)      |
     v                                                             |
  browser (charts/maps render client-side from the embedded doc)   |
                                                                   |
  a workstation: crosscheck/build_markets_snapshot.py (DuckDB over raw S3)
```

Two properties fall out of this shape and everything else defends them:

1. **A failed refresh can never blank, shrink or tear the page.** The page reads only
   the newest COMPLETE snapshot row; rows are appended, never updated; every gate runs
   before the single write transaction.
2. **The served numbers are independently re-derivable.** A second implementation
   recomputes the whole snapshot from raw S3 on a workstation and must match
   field-for-field (section 5).

## 2. Sources

| Kind | Role in the dataset |
|---|---|
| kind-30402 (markets-product-listings) | The corpus. ALL versions extracted (~12k events -> ~11.8k current coordinates -> ~4.9k live listings after exclusions). |
| kind-30406 (markets-shipping-options) | Shipping reach (the `country` / `pickup-country` multi-value tags). Current version per coordinate. |
| kind-5 (event-deletion-requests) | NIP-09 deletions. Never extracted as events (tens of millions): Athena reduces them to the DISTINCT SELF-AUTHORED `(author, tag, value, created_at)` rows naming a listing, by coordinate (`a` tag) or by the id of one version (`e` tag, joined to the kind-30402 (markets-product-listings) ids to establish who wrote it). NIP-09 scoping is IN the SQL and asserted again in the compute (`parse_deletions`). A listing is deleted when a request covers its current version: `docs/dataset-reference/markets-dataset-reference.md` has the rule. |
| kind-30405 (markets-collections) | Captured by nozzl, feeds no page metric - deliberately not an input. |

Freshness: each extract flushes its kind's WAL first (`NOZZL_ADMIN_URL`, the producer's
admin endpoint over obs-net), so a run sees the archive as of its own start. The daily
kind-5 (event-deletion-requests) flush adds one chunk-set/day to a kind the 12h roll already splits twice -
accepted as negligible.

## 3. The pipeline (nostrology-data)

One Dagster job, `marketplaces_refresh`, group `marketplaces`, on the shared
`in_process_executor` + run-scoped `AuroraDial` pattern every surface uses. Schedule
`marketplaces_daily` 02:15 UTC (third-last in the nightly order since 2026-09-25, ahead of `routstr_daily` 02:45 and `ngit_daily` 03:15; the `QueuedRunCoordinator`
with `max_concurrent_runs: 1` serializes it against everything else).

**Extract assets** (three instances of `_extract_asset`, the factored extract half of
`_kind_chain` - AST-identical behavior to the loader surfaces' extracts):

- Templates are selected by `(kind, into)` override in `pipeline/sql.py`:
  `("30402","markets")` and `("30406","markets")` -> `sql/extract_addressable_all.sql`,
  `("5","markets-deletions")` -> `sql/extract_markets_deletions.sql`. Everything else
  falls through to the class-based defaults, untouched.
- `extract_addressable_all.sql` differs from the `..._latest` template on purpose: NO
  latest-per-coordinate window (the farm detector and `archive_events` span every
  revision) and `d_tag` IS emitted. Current-version selection happens in Python with the
  exact NIP-01 tie-break (`created_at DESC, id ASC`) the latest-template's window lacks.
- Costs per run (measured 2026-11-04): kind-5 (event-deletion-requests) scan 3.6 GiB (~1.8 cents, and the one to watch: the workgroup's enforced cutoff is 10 GiB per query), kind-30402 (markets-product-listings) 21 MiB,
  kind-30406 (markets-shipping-options) 0.7 MiB. UNLOAD parts land in `s3://$NOSTROLOGY_EXTRACTS_BUCKET/k=<kind>/
  <dagster_run_id>-k<kind>-<into>/` - the run-scoped prefix both halves re-derive.
- Blocking `extract_nonempty` check on each: a 0-part extract stops the run.

**Compute asset** (`marketplaces_snapshot`), fail-loud in this order - every step before
the write leaves the previous serving data live:

1. Fetch the three part sets to `NOSTROLOGY_EXTRACT_TMP` (real disk, never /tmp tmpfs),
   parse gz NDJSON.
2. `MIN_DELETION_PAIRS` (500, `a` tags) and `MIN_DELETION_EVENT_PAIRS` (600, `e` tags) input
   floors, per tag and in distinct `(author, value)` pairs: the anti-shrink gate below is
   one-directional, and an under-counted deletions extract would INFLATE the pool
   (deleted listings resurfacing) while passing every other check. Per tag because the `e`
   side rests on a join the `a` side does not need.
3. `pipeline/marketplaces.build_snapshot()` - the metrics compute (see the dataset
   reference for semantics). Engineering-relevant internals:
   - `created_at` horizon: events dated past now+2d are dropped before EVERYTHING
     (current-version selection, farm detection, `archive_events`). One hostile
     timestamp would otherwise hijack its coordinate forever, and year-10000 values
     crash the date formatters. Identical bound in the reference implementation.
   - Listing dates capped at now: `when()` (published_at if plausible, else
     created_at) never returns a time after the run, so a listing dated ahead inside
     the horizon counts on the run's day and in its month, and no series,
     `last_listing` or `effective_ts` runs past the run. Uncapped, a month-end run could
     draw a bar for a month not yet begun. Identical in the reference implementation.
   - One row per product: `fold_product_options` runs after `classify`, over what the six
     groups left live, and moves a family's other rows to `excl = 'option'`. A family is
     one key's `variation` rows naming one parent. It keeps the parent if that is live, the
     same key's and not itself a child, else its newest child, and that row takes the
     newest priced child's `price` only if it has none with a unit. Stated in the dataset
     reference; `check_invariants` asks the result for at most one live row per family.
     Identical in the reference implementation, and the agreement gate cannot be what says so: the
     archive holds few of the shapes where the two could part. `crosscheck/compare_markets_fold.py` replays generated shapes through both, as the gate's first step.
   - DuckDB-parity primitives, probe-pinned: `duck_trim` (Unicode Zs only, not
     tab/newline), `duck_try_cast_bigint` (ASCII-only; unsigned 0x/0b; float path
     rounds half-away; NBSP/unicode digits/signed hex -> None), `duck_lower` (SIMPLE
     per-codepoint fold - Python's full `.lower()` diverges on dotted capital I and
     final sigma, which would move farm/synthetic membership).
   - Geo: `markets_geolib.py` + `reverse_geocoder`/`pycountry` pinned in
     `requirements.txt` - the gazetteer and country tables are version-shaped DATA; an
     unpinned bump silently moves country attributions. Imported LAZILY inside the
     asset so a broken wheel cannot fail definitions loading for the other surfaces.
   - A `g` tag is NOT trusted on its syntax. Every digit 0-9 is a geohash base32
     character, so a number that was never a geohash still decodes: a Spanish postal
     code "12520" decodes 2.4 km from the South Pole, and the nearest-city lookup -
     planar in degrees, with no distance limit - then answers Chile, 20 listings deep,
     badged "exact location" because the badge gate is only `gh_len >= 5`.
     `implausible_geohash()` refuses a decode south of `ANTARCTIC_LAT` (-60.0, the
     Antarctic Treaty parallel; the gazetteer holds exactly one row below it). A refused
     decode is KEPT in `gh_lat`/`gh_lon`/`gh_len` - it is the record of why the listing is
     unplaced - but it names no country, draws no bubble, and the listing falls through to
     its `location` tag as if it carried no `g`; `cc_src` reads `geohash-implausible` when
     nothing else resolves. This is a STOPGAP for the visible case, not the remedy: a `g`
     tag still outranks an explicit `location` tag unconditionally, and a junk geohash that
     lands NEAR a city is still believed. nostrology-data#26 (/marketplaces: a junk `g` tag
     outranks the seller's own location tag) carries the rest, and #27 (/marketplaces: ten
     latent defects in the geo resolution) the short-geohash half.
4. `check_invariants()` - cross-checks between the snapshot's own numbers
   (exclusions+active == coordinates, monotone cumulative series, geo/currency sums,
   coordinate ranges, nothing dated after the run). Failure raises before any write;
   success is recorded as the `snapshot_invariants` asset check.
5. `write_pg()` - `MIN_ACTIVE_LISTINGS` (4000) anti-shrink gate, then ONE transaction:
   `TRUNCATE`+`INSERT` `marketplaces_listings`, `INSERT` one `marketplaces_snapshot`
   row. `strip_nul` sanitizes every string reaching Postgres (TEXT/JSONB reject NUL) -
   persistence-side only, so the comparator still sees a NUL-bearing event as a diff.

**Manual materialization** is audited like a deploy: the deployment's re-run action runs
`dagster job launch` in the running dagster-code container. Submit-only; the run then queues
like a scheduled one.

## 4. The serving contract - how the webapp gets the data (the "API")

There is **no HTTP API** between the backend and the webapp. The API is a **database
contract**: one table, one query, one document shape, two roles. Everything a page
render needs crosses the boundary as a single JSONB document.

### 4.1 Writer side (nostrology-data, role `nostrology`)

```sql
CREATE TABLE marketplaces_snapshot (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    generated_utc TIMESTAMPTZ NOT NULL,   -- the compute's pinned NOW
    run_id        TEXT        NOT NULL,   -- the Dagster run that wrote it
    data          JSONB       NOT NULL    -- the ENTIRE page dataset
);
CREATE INDEX idx_mks_generated ON marketplaces_snapshot (generated_utc DESC);
```

- **Append-only.** A refresh INSERTs a new row; nothing updates or deletes old ones.
  This is what makes the reader's consistency trivial: a row is either absent or
  complete, and the newest row is by definition the newest COMPLETE refresh. History
  accrues at ~26 KB compressed/day - years are trivial; retention is a non-problem
  until it is not.
- The write happens in the same transaction as the `marketplaces_listings` refresh, so
  the queryable per-coordinate table and the served document can never disagree with
  each other. The TRUNCATE's ACCESS EXCLUSIVE lock touches only `marketplaces_listings`,
  which the page never reads - the page's read path cannot be blocked by a refresh.
- The compute connects as the owner role `nostrology` and applies `schema/init.sql`
  first (idempotent), so a fresh database self-provisions. `schema/roles.sql`'s
  DEFAULT PRIVILEGES gave `nostrology_web` / `nostrology_ro` / `nostrology_mcp` SELECT
  on the new tables automatically (verified live).

`marketplaces_listings` (one row per current coordinate: classification flags, the
precedence bucket `excl`, resolved country, geohash decode, price/categories JSONB) is the
queryable substrate - the future MCP tool reads it, and any ad-hoc "why is this listing
excluded?" question is one SELECT.

**The page reads it too, since 2026-10-23**: /marketplaces' Recent listings section takes the
newest live rows straight from this table, beside the snapshot document, the way /git takes its
Stars and Zaps lists from `ngit_repo_stats` rather than growing the document. The
read is `SQL_MARKETPLACES_RECENT` over there: `excl IS NULL`, a per-seller `row_number()` cap
applied BEFORE the limit, unexpired only, newest first. On 2026-10-24 it gained six columns
that feed no metric at all - `event_id`, `title_tag`, `summary_len`, `content_warning`,
`expires_at`, `market_image` - each a condition one of the three marketplaces reads from the
listing, so the section can send a row to a marketplace that will actually render it and leave
out a row none of them can. They are not methodology (no snapshot figure reads them, so the
agreement gate neither sees nor needs them), except that since 2026-11-04 `event_id` breaks a tie
in the one-row-per-product fold and the generator mirrors it; `pipeline/marketplaces.py`'s docstring and
`pipeline/markets_medialib.py` carry the detail. A partial index landed with
them, `idx_mkl_live_seller_eff` on `(pubkey, effective_ts DESC, coalesce(d_tag,'')) WHERE excl
IS NULL`, shaped for the SORT inside that read - the per-seller window's input ordering, which
is the only sort an index here can remove. **It is retained, unused and deferred on purpose**
(the operator's call, 2026-11-04): the planner on this cluster does not choose it and will not
until a planner cost parameter changes, and this estate deliberately sets none. Measured on the
live cluster with the statement the web ships, 2026-11-04: the plan in force is a sequential
scan into a sort that now SPILLS TO DISK (`external merge` of about 3.9 MB), cost 2,797.35, 23.0 ms
at best. Lower `random_page_cost` - 4 here, the stock default - to 1.0 and the planner takes the
index unprompted with no sort node at all, cost 2,304.93 and 9.97 ms, about 2.4x; the crossover
is between 1.3 and 1.4. The near-term risk is not the curve but a PLAN FLIP: once the planner's
estimated rows times (width + 24) passes `work_mem`, it switches to a parallel plan that
measures SLOWER and far less predictable (26.5 to 56.0 ms over six runs), and at the 4 MB
default that tips once the ESTIMATE passes 10,082 rows, against 10,066 today. The lever is one
line in `schema/roles.sql`, which already carries three such settings for this very role:
`ALTER ROLE nostrology_web SET work_mem = '8MB'`. That removes the spill and pre-empts the flip
together, needs no nostrology-web change, and is bounded by the web pool's five connections; ask
for 8 MB rather than 5 MB, which leaves only about a fortnight of headroom. The index is a
second, separate step. `schema/init.sql` carries
the full measurement and the trap list beside the index. Note what this read is NOT: it is
gated behind the section's `unreleased` flag, so it runs for the operator's session alone and
not once per visit.

### 4.2 Reader side (nostrology-web, role `nostrology_web`)

The whole read path, top to bottom:

1. Browser requests `GET /marketplaces` (Caddy -> nostrology-web:3000).
2. SvelteKit runs `src/routes/(app)/marketplaces/+page.server.ts`:

   ```ts
   export const load: PageServerLoad = async () => {
       return { snapshot: await getMarketplacesSnapshot() };
   };
   ```

3. `getMarketplacesSnapshot()` (`src/lib/db/postgres.ts`) runs the serving query
   verbatim - exported as `SQL_MARKETPLACES_SNAPSHOT` so the integration tests execute
   the production SQL against a temp-table fixture:

   ```sql
   SELECT data
   FROM marketplaces_snapshot
   ORDER BY generated_utc DESC, id DESC
   LIMIT 1
   ```

   `id DESC` makes two rows in the same second deterministic. Measured plan: index
   scan on `idx_mks_generated` + incremental sort, **0.017 ms** execution; node-pg
   parses the JSONB to an object, no further transformation. Zero rows (a fresh DB
   before the first refresh) throws - the page 500s loudly rather than rendering an
   empty page that reads as "the marketplace died".
4. Connection posture: the shared `pg.Pool` (max 5) over the app's `DATABASE_URL` as
   role `nostrology_web` - read-only (`default_transaction_read_only = on`),
   `statement_timeout 30s`, `CONNECTION LIMIT 10` (schema/roles.sql; the pool-size
   comment in postgres.ts still says 8 - roles.sql is the authority), TLS required
   (the DSN's sslmode is stripped and TLS driven by the pool's `ssl` object -
   `rejectUnauthorized: false`, the "encrypt, don't pin the CA" posture the file
   documents).
5. SSR renders the page with the document and embeds it ONCE in the HTML (SvelteKit's
   data serialization) - the browser never fetches data separately; the charts, donut
   and Leaflet maps hydrate client-side from the embedded document. Whole page ~99 KB.

So "the API" to the *server* is one SQL statement against one table; "the API" to the
*browser* is the SSR'd page itself. One DB round-trip per page view, no caching layer,
no client-side data endpoint - deliberate, because 0.017 ms of DB time per view is
below any traffic multiple this site will see (what would change this is a real
query-performance need, not speculation).

### 4.3 The document contract

The shape of `data` is defined twice, on purpose, and pinned on both ends:

- **Producer**: whatever `pipeline/marketplaces.build_snapshot()` returns (and,
  identically, the reference generator - the agreement gate makes the two one shape).
- **Consumer**: the `MarketsSnapshot` interface in nostrology-web
  `src/lib/shared/data-types.ts` - `generated_utc`, `summary` (12 headline counts),
  `exclusions[]`, `cum_sellers[]`/`cum_listings[]` (daily), `new_monthly[]` +
  `partial_month` (the run's month),
  `categories[]` + coverage, `currency[]` + coverage, `countries[]`,
  `seller_countries[]` + coverage, `geo_nonattr`, `geo_attributable`, `map_points[]`,
  `shipping[]` + coverage, `clients[]`. The page renders NOTHING that is not in the
  document, and computes only presentation-level derivations (the bitcoin share, the
  place-able percentage).

This is a **compile-time contract with no runtime validation**: the writer's invariant
checks are the guarantee, and a hand-inserted malformed row would 500 the page. Accepted
deliberately - the only writer is the gated pipeline.

**Evolution rules** (the agreement gate shapes these):

- *Additive* field: add to BOTH compute implementations (or the zero-diff gate fails),
  then to the TS interface, then use it in the page. Old rows lacking the field keep
  serving until the next refresh; guard the page for one cycle or deploy web after the
  first refreshed row exists.
- *Breaking* change (rename/remove/retype): page first (tolerate both shapes), then
  both computes, then page cleanup. Never compute-first - the page would break at the
  next refresh, not at deploy time.
- `generated_utc` is the user-visible freshness signal (rendered in the page footer)
  and the pin for the verification procedure below - never repurpose it.

### 4.4 Freshness semantics

The page serves the newest complete refresh - normally the same UTC day's 02:15 UTC run, at
worst ~24h + one WAL-roll old. A new row becomes visible atomically at commit and the
very next page view serves it (observed live: the hardened run's row appeared on the
page with no web deploy). If a refresh fails for days, the page silently serves the
last good snapshot, its footer date honestly aging - the alerting for that is the
pipeline's (Discord failure sensor + `last-run-marketplaces_refresh.json` freshness),
not the page's.

## 5. The verification harness (the agreement gate)

Two full implementations of the metrics exist ON PURPOSE:

- **Production**: `dagster_code/pipeline/marketplaces.py` (+ geolib), fed by
  Athena extracts, writing Aurora.
- **Reference**: `crosscheck/build_markets_snapshot.py` (importing the same geolib, so a
  mistake there is outside what the gate can see),
  DuckDB straight over the raw S3 parquet, run on a workstation.

`crosscheck/compare_markets_snapshots.py` diffs the two snapshot documents field-by-field;
the contract is ZERO diffs (procedure in its header: read the newest DB row, re-scan
the kind-5 (event-deletion-requests) rows - the run flushed that WAL - then run the
reference pinned to the row's `generated_utc`; an `archive_events` mismatch means the
inputs differ, re-run rather than chase). Passed three times on 2026-08-23, twice
end-to-end against live production runs.

The gate is what turns the duplication from a liability into the audit mechanism: **any
methodology change must land in both implementations in lockstep** or the next check
fails. It is a manual procedure today; if the surface hardens further, a scheduled
comparison is the natural next step.

## 6. Operations

| Question | Answer |
|---|---|
| Did last night's refresh run? | `last-run-marketplaces_refresh.json`, beside the file `DAGSTER_LAST_RUN_JSON` names (`overall`, `finished`), or the read-only Dagster UI's GraphQL, or the Dagit run list |
| Refresh failed - is the page ok? | Yes by construction: it serves the previous snapshot row; fix the pipeline at leisure |
| Force a refresh now | The deployment's re-run action for `marketplaces_refresh` (queued, serialized) |
| Failure alerting | `discord_failure_sensor` -> the CI channel, like every surface |
| The numbers look wrong | Run the agreement procedure (section 5); a zero-diff result means both implementations agree and the dispute is with the methodology (dataset reference), not the pipeline |
| Anti-shrink tripped on a legitimate mass deletion | LOWER `MIN_ACTIVE_LISTINGS` in `pipeline/marketplaces.py` deliberately, redeploy, re-materialize |
| Where are the raw classified rows? | `marketplaces_listings` on Aurora (any read role) |

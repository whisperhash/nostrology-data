# nostrology_mcp - MCP server for Nostrology metrics

**Three tools** - two free, one paid. The paid one,
`nostrology_query_nutzaps`, serves the network-wide kind-9321 (nutzaps) active
users (`active_users` at grain day/week/month; MAU is the default) for **21 sats per
call, paid in Cashu ecash** via its `cashu_token` argument. Calling it without a token
is free and returns the payment terms (an `is_error` result whose TEXT carries
the terms and whose `structuredContent` carries a NUT-18 `creqA`); redemption
happens in the `cashu_redeemer/` sidecar (NUT-03 swap at an allowlisted mint,
persist-before-swap, NUT-19/NUT-09/NUT-07 recovery ladder). Kill-switch:
`NOSTROLOGY_MCP_NUTZAPS_ENABLED=0` and a recreated container (env is read at container
create - a restart keeps the old env) disables the paid tool; the free tools
keep serving.

**The two free tools, three domains.** `nostrology_query_metric` serves the
quantitative metrics as `csv` (default) or `markdown`, preceded by a plain-text
envelope carrying metric identity / window / scope / freshness / column contract /
methodology; `nostrology_query_reference` serves the qualitative **reference
records** (`reference.py`) - entity data with no measures, row-capped at 1,000 with
an explicit `truncated:` envelope line. Both take a `domain` parameter:

- **`zapstore`** (the default): nine metrics in three families (PUBLISHING, ZAPS,
  LEADERBOARDS; ten specs, since `app_zaps` registers `@day` and `@total`) and
  three records (`app_catalog`, `publisher_profile`, `app_releases`), scoped by
  `publisher_pubkey` (+ optional `app_id`); long-text columns (description /
  release notes / about, capped 8,192 chars) ship only at single-app scope. The
  three leaderboard metrics accept the same entity filters. Together they make the
  nostrolo.gy Zapstore overview, leaderboards, app and publisher pages
  independently reproducible.
- **`routstr`** (2026-08-29, [design](../docs/design/routstr-mcp-design.md)):
  the Routstr AI-inference marketplace behind nostrolo.gy/routstr - ten metrics
  (`network_summary`, `network_activity`, `cumulative_revenue`, `success_rate`,
  `provider_leaderboard`, `provider_activity@day|total`, `model_leaderboard`,
  `model_activity@day|total`, `catalog_size`, `provider_facets`) and five records
  (`provider_profile`, `provider_reviews`, `catalog_versions`, `model_catalog`,
  `exclusions`), scoped by `provider_pubkey` / `model` (never `publisher_pubkey`
  / `app_id`). The rule that makes them safe next to the page: **nothing in this
  server computes a Routstr number** - the nightly `routstr_refresh` job is the
  single computing authority, writing the page's document and five substrate
  tables in one invariant-tied transaction, and the specs select, filter and
  window those rows. A metric or record name omitted `domain` resolves to its
  own domain by name (`resolve_domain`), recorded in the call log.
- **`nutzaps`** (2026-09-30, [design](../docs/design/nutzaps-mcp-design.md);
  metrics only, no records): everything nostrolo.gy/nutzaps and its per-mint pages
  draw, generalized to any from/to window, any mint and full history -
  `payment_activity@day|total`, `mint_leaderboard@total`, `mint_activity@day`,
  `mint_active_users@day|week|month` (distinct senders per mint; month is the
  default), all scoped by `mint_url`
  (folded with the loader's own `normalize_mint_url`, which the MCP image ships).
  Live SQL over the event tables with the web's population predicates (payments
  only, test/private mints excluded), like Zapstore - not a pipeline substrate
  like Routstr. The one thing it never serves is a distinct-sender count
  deduplicated across mints: that is the paid `nostrology_query_nutzaps`, and
  asking the free tool for `active_users` returns a redirect to it. (Per-mint day
  and week counts summed over mints match the paid DAU on most days; they were
  made free anyway on 2026-09-30 - design record sec 4.) Pinned by `tests/test_nutzaps.py`
  (pure) and `tests/test_nutzaps_contract_pg.py` (real Postgres, independent
  Python recomputation, session at UTC+14 so a non-UTC day cut fails).

The original two families:

| `metric=` | counts | unit | live shape (2026-07-30) |
|---|---|---|---|
| `new_native_app_count` | distinct apps on the day of their **first-ever** kind-30063 (zapstore-app-releases) release, native partition | apps | 259 rows, 425 total, 2024-02-20 -> 2026-07-28 |
| `new_republished_app_count` | distinct apps on the day of their **first-ever** kind-30063 (zapstore-app-releases) release, republished partition | apps | 827 rows, 3,094 total, 2015-12-29 -> 2026-07-28 |
| `native_release_count` | **every** kind-30063 (zapstore-app-releases) release row self-published by a developer via zapstore-cli | releases | 358 rows, 3,322 total, 2024-02-20 -> 2026-07-29 |
| `republished_release_count` | **every** kind-30063 (zapstore-app-releases) release row the Zapstore curator republished from GitHub / F-Droid | releases | 939 rows, 12,883 total, 2015-12-29 -> 2026-07-29 |
| `zap_count` | kind-9735 (zap-receipts) zap receipts carrying a Zapstore app a-tag | zaps | 195 rows, 916 total, 2025-06-06 -> 2026-07-29 |
| `zapped_msats` | the amount those same receipts carry, in **milli**-satoshis | msats | 195 rows, 2,137,343,000 total (= 2,137,343 sats), 2025-06-06 -> 2026-07-29 |

**Family 1 - publishing (4 metrics)** is one 2x2: the native/republished partition pair counted in two
units. The `*_release_count` pair are the two bars of nostrology-web's "App
Releases" chart - exact complements over the same table, so they stack to the
chart's total. The `new_*_app_count` pair is the same partition pair deduped to
each app's first-ever release; each is bounded above by its releases counterpart
day for day (live-gated), and together they tile the set of all apps ever released
(live-gated). Cumulative-summing them reproduces nostrology-web's "Zapstore Apps"
chart exactly (native line, github line, total = their sum) and the "Nostr-native
Apps as % of Total" chart as `100 * native_cum / (native_cum + republished_cum)` -
the derivability gap that motivated the fourth metric.

**Family 2 - zaps (2 metrics)** is a different *row* population, not the
publishing partition sliced another way: kind-9735 (zap-receipts) receipts bearing a
top-level `["a","32267:<dev_pubkey>:<app_d_tag>"]` tag, which is what makes them
Zapstore-attributed. (The row populations are disjoint and their totals never
reconcile with the publishing metrics - but they cover largely the same apps: 174
of 175 zapped apps also appear in the releases table.) They are the count and the
sum of one set of rows, so neither bounds the other, and together they reproduce
nostrology-web's "Zapstore Zaps" chart exactly: keep receipts whose `created_at`
is within the last 365 days - a **timestamp** cutoff, not a calendar-day one, so
the chart's oldest day is partial - densify zeros, then plot `zap_count` as bars
against the running sum of `zapped_msats`, converted to sats once at the end. The
live gate runs the chart's statement in full (dense calendar, running sum, the
page's rounding) and rebuilds it from the two series: 0 mismatches on all 366
points.

Three things about the zaps pair that a consumer gets wrong by default, all carried
in the methodology: **the unit is msats, not sats** (a 1000x error available to
anyone who reads only the name - sum msats first, convert once); **the series'
earliest day is a fabricated receipt** (`778150ee...`, 2025-06-06, 100,000 sats,
embedded zap request id is the literal string `demo-zap-request`; the only row
before 2025-09-03, and several percent of the all-time sum) which must be excluded from
all-time figures; and **attribution is to the app, not to developer earnings**
(20.9% of all-time sats went to the Zapstore curator/indexer account). The first
three are pinned by live gates - whole-sat amounts, the left-edge receipt's shape,
and the curator share against a tolerance - so if the data moves the prose fails
loudly instead of lying. The one dated figure with **no** gate is the ~0.22%
acquisition loss, which came from a 2026-07-27 cross-relay audit that cannot be
re-run cheaply; it is labelled with its date for that reason.

These are also the first metrics on a **second source table**, which is exactly the
trigger "What the second metric taught" (below) predicted for the per-spec freshness
probe: a module-global `AS_OF_SQL` would now be reporting the *releases* table's
freshness in the zaps envelope, silently. `MetricSpec` gained a declared
`source_table` so that invariant is checkable rather than assumed; it is
deliberately not published in `registry.json` (physical schema is not part of the
public contract).

Back in family 1, `native_release_count` vs `new_native_app_count` is the easy
misread - same partition, same-looking name, different unit - which is why the
methodology string, not the metric name, carries the distinction, and why `metric`
is the one **required** parameter: silently defaulting an under-specified call
would answer a question the caller did not ask. Family 2 has its own version of
that trap in `zap_count` vs `zapped_msats`.

**Deployed (2026-07-30):**
public Streamable-HTTP endpoint at **`https://nostrolo.gy/mcp`** (no auth,
read-only), with the human/crawler registry + per-harness registration
instructions at [nostrolo.gy/data](https://nostrolo.gy/data). The
methodology prose is authored once in `metric.py` (`summary`/`partition`/`caveats`;
`methodology` is their derived join) and serves three surfaces: the MCP envelope,
`GET /registry.json` (SSR-consumed by nostrology-web's /data pages), and the
`nostrology://registry` MCP resource.

Deliberately absent (decided, not forgotten): DB views for the partition predicate,
`list_metrics`/`describe` tools, token budgets on metric responses (the reference
records ARE row-capped - 1,000 rows, loud `truncated:` line - because one live
scope, the Zapstore curator's, is 13k rows; the metrics' populations are bounded
by nature), structuredContent/outputSchema, result caching, OAuth, the official
MCP registry / JSON-LD / llms.txt. Each lands when its trigger does - see "What
the second metric taught" below. (from/to windowing
and entity filters, once on this list, landed with the leaderboard metrics and the
reference records respectively; calendar-month and ISO-week buckets landed with the
nutzaps domain's per-mint active users, 2026-09-30.)

## Layout

| file | role |
|---|---|
| `metric.py` | the registry-in-code: per metric, SQL + bind params + freshness probe + column contract + summary/partition/caveats (methodology = their join); `registry_entries()` feeds /registry.json |
| `reference.py` | the reference records (`RecordSpec`): scope rules (publisher required / app-filterable), scope-dependent column sets, the 1,000-row cap + `__total_rows` plumbing, 8,192-char long-text caps; `registry_reference_entries()` is the registry's v2 half |
| `window.py` | from/to resolution: inclusive ISO UTC days -> half-open epoch bounds + the day-grain SQL wrapper |
| `db.py` | RO Postgres access (password out-of-band, lazy psycopg2 - pg.py conventions); duck-types MetricSpec and RecordSpec |
| `render.py` | rows -> envelope + csv/markdown text block; the one sanitize pass for third-party text (NULL sentinel, escape-first ordering, per-column caps) |
| `calllog.py` | the log: v4 JSON records (`kind` = `tool` \| `rpc` \| `http`) to two sinks: `logs/calls-YYYY-MM-DD.jsonl` + stdout (opt-in, container-only) -> Loki |
| `middleware.py` | `ServerMiddleware` writing one `kind:"rpc"` record per inbound JSON-RPC message - the discovery + rejected-call surface the tool body cannot see |
| `asgilog.py` | raw-ASGI middleware writing one `kind:"http"` record per request on `/mcp` - the transport-level rejections (400 missing session, 409, 406) that never become a JSON-RPC message and so are invisible to `middleware.py` |
| `recovery.py` | raw-ASGI middleware attaching `error.data` to those same rejections - a stable `recovery` token plus prose naming the call that fixes it. Additive only: status, `code` and `message` are untouched |
| `inflight.py` | raw-ASGI middleware serializing a JSON-RPC id that is already in flight on the same session. The SDK keys its response streams on the client's id, so two concurrent POSTs sharing one race to cross-delivery / hang / 500; this holds the second until the first finishes, and 409s past a bounded wait |
| `inputschema.py` | the ADVERTISED shape of the tools' optional params: titled `anyOf` branches (an explicit "leave it out" branch + a value branch) instead of pydantic's bare `Optional`, so the one client that renders the schema as a form gets human labels and a submittable default state. Advertisement only - it validates nothing. |
| `cashu.py` | Cashu payment support: the NUT-18 `creqA` encoder (fixed-shape CBOR, pinned byte-for-byte against cashu-ts), the challenge text/structured builders, the receipt hash, and the HTTP client for the `cashu_redeemer` sidecar. This process never touches proof secrets or mints - the sidecar does everything monetary |
| `server.py` | MCP wiring: `nostrology_query_metric` + `nostrology_query_reference` + the paid `nostrology_query_nutzaps` (registered unless the kill-switch env disables it), stdio (local default) / streamable-http (container) transports, /health, /registry.json, the registry resource |

> **"The SDK"** in this README and in the Python sources always means the **Python MCP
> SDK** (`mcp` on PyPI), which serves this endpoint. Two others appear in the traffic
> analysis and are always named in full there: the **TypeScript MCP SDK**
> (`@modelcontextprotocol/sdk`) and the **ContextVM SDK** (`@contextvm/sdk`).

### What gets logged, and why there are three record kinds

The first 43 hours of the public endpoint showed the tool-body record alone was
blind in two directions, so `middleware.py` was added to close both:

* **Discovery was invisible.** `initialize` / `tools/list` / `resources/list` /
  `resources/read` wrote nothing, yet 18 of the first 38 sessions listed the tools
  and left without calling one, and the `nostrology://registry` resource was being
  read by clients we could not identify. The funnel could only be reconstructed by
  fingerprinting Caddy response *sizes*, which breaks whenever the catalog changes.
* **Rejected calls were invisible.** Params validation runs above the tool body, and
  a JSON-RPC error is still HTTP 200, so an ask the schema refused reached no log at
  all - and "what did a caller want that we don't offer" is the single most useful
  input to deciding what to expose next.

A third blind spot surfaced on 2026-08-03, one layer further out again:

* **Transport-level rejections were invisible.** The SDK's session manager answers
  some requests itself, ABOVE `middleware.py` - `400 Bad Request: Missing session
  ID`, `409` on a second SSE stream, `406`, `421` - so they never become a JSON-RPC
  message and never reached any seam we had. A live ContextVM gateway sent that 400
  every 1-3 hours for eight hours, failing 100%, visible only in Caddy's access log.
  `asgilog.py` (raw ASGI, wrapping the SDK's app from outside) closes it.

The same investigation showed the *reply* to those requests was a dead end: correct, and
useless to whoever is reading the client's logs. `recovery.py` sits at the same layer and
adds `error.data` to them - `{"recovery": "initialize", "detail": ..., "spec": ..., "docs": ...}` -
so a rejection names the call that fixes it. It is strictly additive (status, `code` and
`message` unchanged, and an existing `data` is never overwritten), keyed on
`(status, JSON-RPC code)` rather than on the SDK's English message, and it is a **separate
middleware from `asgilog.py` on purpose**: that one passes `send` through untouched, which
is what makes it safe across the long-lived SSE stream, while this one must buffer the
body it rewrites. One property each.

A fourth problem surfaced on 2026-08-04, and it was not an observability gap but a
correctness one:

* **A duplicate in-flight JSON-RPC id corrupted the transport.** The SDK keys its
  per-request response streams on the **client-chosen** id and overwrites silently, so
  two concurrent POSTs on one session sharing an id race to one of three outcomes: the
  second request receives the FIRST one's result at HTTP 200 (cross-delivery, silent), a
  hang, or a 500 that the SDK logs at `logger.debug` and is therefore invisible at INFO.
  ContextVM's announcement manager sends four `*/list` calls concurrently all stamped
  `id: "announcement"`, so it met this on every gateway restart. `inflight.py` holds the
  colliding request until the id frees rather than refusing it - the calls are
  independent, sequencing them costs milliseconds, and it makes a client that violates
  the id-uniqueness rule work anyway. Bounded (`MAX_WAIT_SECONDS`, `MAX_WAITERS_PER_ID`)
  because this endpoint has no auth and no rate limit; past the bound it answers `409`
  with `recovery: "unique_request_id"`. Only OVERLAPPING ids are held - sequential reuse
  is common, harmless, and accepted.

The three raw-ASGI seams are ordered, and every relationship is load-bearing. Starlette
builds the stack inside-out (the LAST `add_middleware` is the OUTERMOST), so a request
meets them in this order:

```
HttpCallLogMiddleware        (asgilog.py)   observes  - records what the client SAW
DuplicateRequestIdMiddleware (inflight.py)  gates     - before the session manager takes the id
RecoveryHintMiddleware       (recovery.py)  rewrites  - decorates the SDK's own rejections
<SDK StreamableHTTPSessionManager>
```

The call log must be outermost or it records a status the recovery seam has not finished
rewriting; duplicate-id must sit above recovery because it authors a complete body
including `error.data`, which the rewriter would only have to decline.

| kind | written by | one per | carries |
|---|---|---|---|
| `http` | `asgilog.py` | inbound HTTP request on `/mcp` | method, path, status, `had_session_header`, duration, client identity (session id, UA, traffic_type). HTTP transport only - stdio writes none |
| `rpc` | `middleware.py` | inbound JSON-RPC message | method, **params as they reached the middleware**, request id (null = notification), outcome, client identity |
| `tool` | the tool body | tool call that reached it | effective (post-default) params, resolved `sql_key`, rows/bytes/est_tokens, per-phase timings |

Two traps in the `http` record, both deliberate:

* **`had_session_header` is not derivable from `client.session_id`.** That field falls
  back to the id the *server* minted so `initialize` stays joinable; the flag is what
  says whether the CLIENT sent one, which is the never-initialized-transport signature.
* **A rejected request carries no `session_id` at all.** The SDK mints a throwaway
  transport for a session-less POST and stamps its id on the 400 too; recording it
  would give every request in a failure storm a distinct session id no client ever
  held. `status >= 400` therefore never takes the fallback.

`http` records for a long-lived `GET` land when the stream CLOSES, so the day-file is
in completion order and their `ts` is the completion time.

The middleware is inserted **outermost** (`middleware.insert(0, ...)`, not the
`MCPServer(middleware=[...])` kwarg, which appends *after* the SDK's built-ins). From
third position the SDK's `RequestStateBoundary` would reject some `tools/call` /
`resources/read` messages before we ever saw them - the exact class of rejection this
exists to catch. Params are therefore the wire values in practice, but the honest
statement is "as they reached this middleware": a built-in sitting above us could in
principle rewrite them.

Over HTTP a `tools/call` writes **all three**; over stdio, two. Join them on `ts` +
`client.session_id`. Switch on `kind` - never on position. Two consequences worth
knowing:

* the `rpc` record answers *"did the caller pass `grain`, or inherit the default?"*,
  which the `tool` record structurally cannot (it logs values after defaults apply);
* `rpc.outcome` distinguishes three layers: `ok`, `error` (the message itself
  raised), and `tool_error` (**the SDK does not raise for a failed tool call** - it
  returns an ordinary result with `isError` set, so without that check every
  rejection logged as a success).

Response *sizes* are deliberately absent from `rpc` records: the `tool` record has
exact bytes for the one expensive path, and Caddy's access log has the HTTP size for
every message, joinable on the session id both logs carry.

Everything written is bounded, because the endpoint is unauthenticated and public:

* `params` are redacted by key name and capped at `MAX_PARAMS_CHARS` (the routing
  keys - `name`, `uri`, `method` - survive truncation, since "someone sent 2 MB at
  `nostrology_query_metric`" is the analyzable part);
* `client` fields are clipped at `MAX_CLIENT_FIELD_CHARS`. This one is not
  cosmetic: `clientInfo` is attacker-controlled but stamped on **every** record for
  the life of the session, so an uncapped one turns a single 4 MB handshake into
  4 MB *per subsequent message*, on disk and into Loki. Capping `params` alone does
  not close it.

The redaction is forward insurance (no credential can legitimately appear today),
because this log is read by a sandboxed observer whose whole security model is that
it cannot reach secrets - see `nozzl/CLAUDE.md`, "never log secrets".

Nothing prunes the day-files. Growth is now driven by message count rather than by
DB-backed calls, so it is worth watching: see "Operational notes".

### Marking our own traffic: `?traffic_type=internal`

Register the endpoint with the marking on the URL, and every record from that
session carries `client.traffic_type`:

```
https://nostrolo.gy/mcp?traffic_type=internal
```

```sh
claude mcp add --transport http nostrology "https://nostrolo.gy/mcp?traffic_type=internal"
```

**The problem it solves.** All of the endpoint's first 35 calls were our own, and
23 of them carried the client name any user of that client sends, so first-party traffic
could not be separated from demand. Nothing else available works: the Caddy
operator-marker cookie cannot reach an MCP client, and `clientInfo.name` is hardcoded
by the client (so no naming convention can tag our own sessions).

**Why a query param.** In MCP the **client config is the persistence**: the URL sits
in the client's config file and the client replays it verbatim on every request for the life
of the registration - the role a cookie has to play on the web. Verified against the
real client SDK: it survives `initialize`, every subsequent message, and the
terminating `DELETE`. It needs no client feature beyond accepting a URL (unlike a
custom header), it is the same host/path/handler real consumers hit (nothing forks),
and it lands in Caddy's access log for free because the query string is part of
`request.uri`.

**Why this name.** `traffic_type=internal` is [GA4's own convention][ga4] for the
identical job - its internal-traffic filter keys off exactly this parameter and
value - and GA4 already runs on the web side of this stack, so it is vocabulary an
operator here has already met. Rejected: `nozzl_op` (the web's operator marker cookie,
`__Host-nozzl_op`, uses that name for something else - one name, two meanings), and
`origin`/`client`/`src`, which collide with the HTTP `Origin` header, the `client`
block in these very records, and vagueness respectively.

[ga4]: https://support.google.com/analytics/answer/10104470

**Three properties to keep in mind:**

* **It is a LABEL, not a credential.** Nothing is gated on it and anyone may send it.
  That is fine - this is analytics, not access control. Values are ALLOWLISTED
  (`ALLOWED_TRAFFIC_TYPES`) so an unauthenticated caller cannot inject free text into
  a log that ships to Loki.
* **Absence means "not marked", never "external"** - see the cutover table below
  before computing any first-party split that reaches back before 2026-08-02.
* **There is no backstop, by choice.** This is the single mechanism; the
  `clientInfo.name` convention was deliberately NOT kept as a second signal. So every
  first-party touch must use the marked URL, **including ad-hoc `curl` and
  verification scripts** - an unmarked one silently joins the real-consumer count and
  nothing detects the lapse. Cheap self-check: a session with no `traffic_type` asking
  for precisely the metric you were demoing that day is probably yours.

#### Cutting the numbers correctly across the cutover

`client.traffic_type` only becomes informative at **2026-08-02T08:23:16Z**. Every
record before that is unmarked because the mechanism did not exist or was not yet in
use - **not** because it was external. The endpoint's entire first three days were
first-party, so a naive `traffic_type != "internal"` filter reports ~100% external
usage for a period that had none. Read the log in four eras (all times UTC):

| era | from | to | records | how to classify |
|---|---|---|---|---|
| **A. v2, tool-calls only** | 2026-07-30T19:27:06Z | 2026-07-31T14:15:26Z | 35 (`v:2`) | **All first-party.** No `kind`, no marking; discovery was not logged at all in this era. |
| **B. v3 logging, no marking yet** | 2026-08-01T16:08:00Z | 2026-08-01T17:04:58Z | 11 | **All first-party** (verification runs). |
| **C. Marking live, our client not yet registered** | 2026-08-01T17:04:59Z | 2026-08-02T08:23:15Z | mixed | **Unmarked does NOT mean external here.** The server could mark, but our own MCP client's registration still used the bare URL. |
| **D. Marked** | 2026-08-02T08:23:16Z | onward | - | any recorded `traffic_type` -> first-party (the server records only allowlisted values); **absent -> external**. Only era where absence is informative. |

Eras A-C are first-party in full. That is asserted from evidence, not assumed: over
that whole window Caddy recorded exactly one external touch of `/mcp` - a
browser-shaped `GET` on 2026-07-30 that 400'd - plus a scanner probing
`/.cursor/mcp.json`. Neither completed a session, so neither reached this log.

```sh
# first-party across all time = everything before the cutover, plus marked records after
jq -s '[.[] | select(.ts < "2026-08-02T08:23:16Z" or .client.traffic_type != null)] | length' calls-*.jsonl
```

Two traps in era D:

* **Deliberately-unmarked test sessions exist.** The cutover verification ran a
  matched pair - `marked-check` and `unmarked-check` - the second of which is ours but
  carries no marking BY DESIGN, as the control. Same for the
  earlier `*-verify-*` clients and `qs-live-test`. Filter by `client.name` if a precise count
  matters; any `clientInfo.name` we authored by hand is ours.
* **A live demo registers the bare URL on purpose** - it
  demonstrates what a real visitor copies from `/data`. Demo traffic therefore counts
  as external, deliberately.

Last era-A caveat: those records are `v:2` - tool calls only, no `kind` field.
Discovery traffic before 2026-08-01T16:08Z was never recorded, so **session counts for
era A must come from Caddy's access log, not from this file.**

### Why `metric`/`domain`/`grain` are `str` with an advertised value list

The two ways to advertise legal values differ in exactly the way that matters:

| | value list in the advertised schema | off-list value reaches the tool body |
|---|---|---|
| `Literal[...]` | yes | **no** - pydantic rejects it above our code |
| `Annotated[str, Field(json_schema_extra=...)]` | yes | **yes** |

A `Literal` would make an unsupported ask vanish before anything could record it
(and a JSON-RPC error is HTTP 200, so the access log cannot see it either). These
three carry demand - "someone wanted an hourly series", "someone wanted a
`lightning` domain" - so they advertise their values for model guidance but are
rejected **in the tool body, after a record is written**, with an error listing every
legal combination. Those values are derived from `METRICS`, so the schema cannot
drift from the registry. `format` keeps its `Literal`: a rendering switch carries no
demand signal, so there is nothing to observe and the SDK's free rejection is
correct.

`metric` and `domain` spell the list as a plain `enum`. `grain` cannot: it is
optional, so pydantic renders it as `anyOf: [string, null]`, and an `enum` sibling
there forbids the parameter's own null default - which is not theoretical, it made
the tool unsubmittable from contextvm.org's form. Its values are therefore one titled
branch each, built from `GRAIN_NAMES` by `metric.grain_choices()`; the same drift
guard, a different spelling. `inputschema.py` holds the full reasoning.

Since 0.5.1 a grain the metric does not serve is refused with its legal grains AND,
where something answers the ask, the call that does (`metric.grain_redirect`):
"for one week, call grain=total with from/to spanning it" on a metric with a total
grain, "its day rows add up - sum them over from/to" on an additive day-only series,
another metric where that one answers it (`mint_activity` total -> `mint_leaderboard`),
and how to read a sparse series where summing would be wrong (a running total, a
level, the Routstr ledgers' weekly era). Before it, model callers asking for
`grain=total` on a day-only series (11 times, 2026-08-01 to 09-30, 10 of them from one
first-party client) got the grain list alone and summed the day rows themselves; nostrology-data#14
proposes serving those totals. The derived clauses retire on their own when a grain is
added; the hand-written overrides (`_GRAIN_REDIRECTS`) do not - a test fails on one
whose grain became served, but the rest of it stays until deleted.

The package is `nostrology_mcp`, **not** `mcp/` - a top-level dir named `mcp` would
shadow the SDK package once the repo root is on `sys.path` (pytest puts it there).

## What the second metric taught

v0 shipped one metric on purpose, to find out what a registry would actually buy
before building one. Adding `republished_release_count` is that experiment. Findings,
in order of how much they matter:

**1. It exposed a real latent bug - a global that was "obviously fine" at one entry.**
`AS_OF_SQL` was a module-level constant hardcoded to `events_kind_30063_app_releases`,
and `db.fetch_metric` ran it unconditionally. Both metrics happen to read that table,
so nothing broke. But the *next* metric on any other table would have had its
envelope state an unrelated table's freshness as its own - silently, in the line
consumers trust for staleness. Freshness is now per-spec (`as_of_sql` +
`as_of_source`, named in the preamble and pinned by
`test_as_of_probe_reads_this_metrics_own_source_table`). This is the strongest
argument for the registry idea: not that duplication is tedious, but that
single-entry code grows globals that are indistinguishable from correct.

**2. Most of the machinery absorbed metric #2 for free.** `db.py`, `calllog.py`, and
`server.py` needed no per-metric code; `render.py` needed one fix, replacing
positional `row[0]`/`row[1]` with contract-driven formatting off `spec.columns`. The
shared test invariants are parameterized over `METRICS.values()`, so metric #3
inherits them by existing rather than by someone remembering to copy a test. That is
the part of "registry" that already exists and is working - the `MetricSpec` dataclass.

**3. What genuinely duplicated - and a registry only fixes one of the three:**

| Duplicated | Would a registry fix it? |
|---|---|
| The native/republished **predicate pair**. The two constants are now declared in three places across two repos (`nostrology-web/postgres.ts:1217,1225`, `dagster_code/pipeline/nectar.py:30-31`, here); the predicate itself is written inline at each use - 13 references in `postgres.ts` alone, plus `sql/zapstore_native_releases_daily.sql`, plus both statements in this module | **No.** The duplication spans two repos and two languages, so a Python registry cannot reach it. What fixes it is a **DB view** exposing an `origin` column, which every consumer selects instead of restating the rule - a schema decision, not a registry decision. |
| The **day-bucket expression** `date_trunc('day', to_timestamp(X) AT TIME ZONE 'UTC')::date`, written verbatim in both SQL statements | **Yes** - a declarative `grain` would generate it. Deliberately not abstracted yet: the literal SQL stays greppable and paste-able into `psql`, and `test_sql_day_bucketing_is_utc_explicit` pins that both spell it identically. This is the win that arrives *with monthly grain*, not before. |
| Shared **methodology caveats** (sparseness, the partition definition, kind-30063 (zapstore-app-releases), not kind-32267 (zapstore-app-catalog)) restated per metric | **Not clearly.** A registry could compose methodology from fragments, but composed prose reads worse than written prose, and this text is read by a model making an interpretation decision. Hand-written is currently the better artifact. |

**4. Two drift surfaces a registry would close - one since CLOSED, one still held by
tests.** *(Updated 2026-08-01.)* The metric enum used to be a hand-typed
`Literal[...]` in `server.py`, kept honest only by `test_tool_metric_enum_matches_registry`.
That is no longer true: the enum is now advertised via
`Field(json_schema_extra={"enum": list(METRIC_NAMES)})`, **derived from `METRICS` at
import time**, so an enum entry with no metric behind it is impossible by construction
rather than by assertion. (The move was made for observability - see "Why
`metric`/`domain`/`grain` are `str`..." above - and closing this drift surface was a
free side effect of it.) `grain` gained the same property by the same route, via
`grain_choices()`, when its value list moved into branches. The remaining test,
`test_every_registry_metric_is_reachable_through_the_tool`, now covers the other
direction: a registered metric the tool cannot serve.
The tool **description** is still hand-written prose that enumerates both metrics;
since the description *is* the routing logic the model reads, good prose is worth
more than generated prose, but it can drift from the registry silently and the test
only checks that both names appear.

**Verdict at six metrics: still not worth a registry - but trigger (a) has now
fired.** The zaps pair IS a metric family on a different source table, and the
per-spec freshness probe (finding 1) is what kept the envelope honest; the response
was one declared `source_table` field plus generalized invariants, not a registry.
What the second table actually cost was **test generality**, not code: five
"every metric" invariants had silently hardcoded one table and one bind-parameter
set, and a d_tag substring check turned out to be a false positive against the new
table's name. Those are now stated over the spec rather than over constants, so a
seventh metric on a third table inherits them. Remaining triggers: (b) **monthly
grain**, which turns the day-bucket expression into a real parameter (it came on
2026-09-30: nutzaps.mint_active_users builds one statement per grain from a
`date_trunc` unit - day, week, month), or (c) the
metric enum outgrowing what one tool description can route on (~15-20 entries) -
the description is now visibly doing three-family routing. (The 2026-08-02
reference records took the OTHER release valve for (c): a shape that is not a
metric got a second tool with its own small schema, rather than stretching one
description - so the per-tool routing load stays bounded by family, not by the
whole catalog.)

## Run it (the public endpoint)

The deployment runs the server as the `nostrology-mcp` container
(`deploy/Dockerfile.mcp`), public at `https://nostrolo.gy/mcp` behind Caddy. Register from
any harness:

```sh
claude mcp add --transport http nostrology https://nostrolo.gy/mcp
# stdio-only harnesses:  npx mcp-remote https://nostrolo.gy/mcp
```

All harness syntaxes: [nostrolo.gy/data](https://nostrolo.gy/data).
Calls answer in a few seconds; the ~15-30s first-call stall is GONE since
2026-08-03 (Aurora took a 0.5 ACU warm floor and no longer pauses). DB access is
the dedicated read-only `nostrology_mcp` role (CONNECTION LIMIT 32,
statement_timeout 10s).

A deploy provisions the role and binds its password, builds at HEAD while the old
container still serves, then recreates it; the steps are the deployment's runbook. Call log:
JSONL day-files in `NOSTROLOGY_MCP_LOG_DIR` + stdout -> Loki `{container="nostrology-mcp"}`
(Grafana Explore live-tail = the demo surface).

## Run it locally (stdio)

```sh
# 1. the MCP environment, as under Development in the repository's README: CPython 3.12,
#    since the lock pins cp312 wheels, from a python3.12 whose default certificate store
#    is maintained.

# 2. reach the serving PostgreSQL on a local port, for example over an SSH tunnel through a host
#    that can reach it. Only one process may hold the port.
ssh -f -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 \
  -L 127.0.0.1:15432:<postgres host>:5432 <a host that can reach it>

# 3. the read-only role's password, in a LOCAL ~/.pgpass keyed to the near end of the tunnel
#    (below). Typed, never echoed, never in argv:
umask 077; read -rs PW
printf 'localhost:15432:nostrology:nostrology_ro:%s\n' "$PW" >> ~/.pgpass
printf '127.0.0.1:15432:nostrology:nostrology_ro:%s\n' "$PW" >> ~/.pgpass
chmod 600 ~/.pgpass; unset PW

# 4. register it with your MCP client; with Claude Code, in local scope (the paths are yours).
#    PYTHONPATH makes the spawn cwd irrelevant - Claude Code does not guarantee the server's
#    working directory, so `-m nostrology_mcp.server` must not depend on it.
claude mcp add --env PYTHONPATH=/path/to/nostrology-data \
  --transport stdio nostrology -- \
  /path/to/nostrology-data/.venv-mcp/bin/python -m nostrology_mcp.server

# 5. inspect the wire (JSON-RPC both directions, manual tool invocation)
npx @modelcontextprotocol/inspector -- .venv-mcp/bin/python -m nostrology_mcp.server
```

Env knobs (all defaulted for the tunnel): `NOSTROLOGY_PGHOST` (localhost),
`NOSTROLOGY_PGPORT` (15432), `NOSTROLOGY_PGDATABASE` (nostrology),
`NOSTROLOGY_PGUSER` (nostrology_ro), `NOSTROLOGY_PGPASSWORD` (else ~/.pgpass),
`NOSTROLOGY_MCP_LOG_DIR` (else `nostrology_mcp/logs/`). Container-mode knobs (set
by the compose, not needed locally): `NOSTROLOGY_MCP_TRANSPORT`
(stdio|streamable-http, default stdio), `NOSTROLOGY_MCP_HOST`/`_PORT` (127.0.0.1 /
8080), `NOSTROLOGY_MCP_LOG_STDOUT` (mirror records to stdout - container-only;
under stdio, stdout is the protocol channel), `NOSTROLOGY_MCP_GIT_SHA` (baked
provenance where git is absent).

Note: there is no installed package in v0, so `python -m nostrology_mcp.server`
needs the repo root importable. The `--env PYTHONPATH=...` in the registration
guarantees that regardless of spawn cwd; the Inspector command above works because
it's run from the repo root.

### Why the local pgpass line names the tunnel

libpq matches a `~/.pgpass` line by **literal string comparison against the host and
port you connect to** - it does not resolve names or follow tunnels. A host that connects
straight to the database keys its line to the endpoint:

```
<postgres host>:5432:nostrology:nostrology_ro:<pw>
```

Through a tunnel you connect to its **near end**, so the key must be the local address. Same
password, different key - a copy of the direct line silently does nothing:

```
localhost:15432:nostrology:nostrology_ro:<pw>
127.0.0.1:15432:nostrology:nostrology_ro:<pw>
```

Both spellings are written because the match is literal: `NOSTROLOGY_PGHOST`
defaults to `localhost`, but a `127.0.0.1` override would otherwise miss.
`chmod 600` is mandatory - libpq silently ignores a group/world-readable pgpass.
`NOSTROLOGY_PGPASSWORD` is the alternative if you prefer env over file.

## Tests

```sh
.venv-mcp/bin/python -m pytest nostrology_mcp/tests -q
```

1,050 tests on 2026-09-14: without a database 962 pass and 88 skip (most of the skips are
the live gates below). **97% statement coverage** of the package from that run, not counting
`verification/` (the verification battery, run by hand against the live endpoint):

```sh
.venv-mcp/bin/python -m coverage run --source=nostrology_mcp --omit='*/tests/*,*/verification/*' \
  -m pytest nostrology_mcp/tests -q && .venv-mcp/bin/python -m coverage report -m
```

Pure by default (SQL contract, render snapshots, log records, in-process tool calls
with a stubbed DB). Invariants that must hold for *every* metric are parameterized
over `METRICS.values()`, so a new metric inherits them without anyone copying a test.

Set `NOSTROLOGY_TEST_DSN` (with the tunnel up) to also run the live gates:

```sh
NOSTROLOGY_TEST_DSN="$(.venv-mcp/bin/python -c 'from nostrology_mcp.db import dsn_from_env; print(dsn_from_env())')" \
  .venv-mcp/bin/python -m pytest nostrology_mcp/tests -q
```

* `EXPLAIN` every metric SQL + its as_of probe against the real schema - catches
  column/table drift, moves no data.
* **Partitions tile the table**: `native + republished == total` and no NULL
  `app_d_tag`. This is the invariant no pure test can prove, and it is what would
  catch a NULL leaking out of *both* predicates (`NULL = ANY(...)` and
  `NULL <> ALL(...)` are both NULL, so such a row would vanish from each side).
  The apps-unit half is gated too: the two `new_*_app_count` series sum to the
  count of all distinct apps - what makes their cumsums a true catalog total.
* **Daily series accounts for its whole partition**: the per-day counts sum to the
  partition total, are strictly ascending, and carry no zero rows.
* **Apps never exceed releases**: on each partition, the deduped apps series is
  bounded above by its releases series on every day.
* **The backdated tail is really there**: both republished series reach back to 2015
  while the native series starts in 2024. If that contrast ever disappears, the
  created_at-provenance caveat in the methodology has gone stale.

## Operational notes

* **Connections**: one per call, closed after - deliberately NO pool (on-box connects take
  tens of ms; a held-open connection also blocked Aurora's auto-pause, until the cluster
  stopped pausing on 2026-08-03). The deployed container uses
  the dedicated `nostrology_mcp` role (CONNECTION LIMIT 32, statement_timeout 10s);
  local stdio dev shares `nostrology_ro` with live troubleshooting.
* **Aurora cold start: RETIRED 2026-08-03.** A cold first call used to block
  ~15-30 s in connect while the cluster resumed from zero. It now holds a 0.5 ACU
  warm floor and never pauses, so there is no resume to wait on. The 90 s
  `connect_timeout` is KEPT deliberately - it costs nothing warm and still covers
  a restart or failover; only its stated reason changed.
* **Claude Code output caps**: warns at 10 k tokens of tool output, truncates at
  25 k by default. Measured 2026-07-30: `new_native_app_count` is 259 rows / ~7 KB,
  `new_republished_app_count` 827 rows / ~13 KB, `republished_release_count` 939
  rows / 20 KB / ~5 k tokens - the largest fixed component is the ~1.4 KB
  methodology preamble, constant per call. All inside the warn threshold, but the
  trend is visible; the time-window parameter is the planned fix when one outgrows
  it.
* **Verified against production**: `republished_release_count` reproduces the
  `github_count` arm of `SQL_ZAPSTORE_APP_RELEASES_DAILY`
  (`nostrology-web/src/lib/db/postgres.ts`) exactly - 364 of 365 days match
  per-day. The one difference is the chart's left-boundary day, which is partial
  because its window starts at `NOW() - 365 days` (a mid-day timestamp) while this
  series is unwindowed.
* **Freshness is data-derived** (`MAX(created_at)` on the source table): it lags
  ingest and cannot distinguish a quiet period from a stalled pipeline. That is
  a fact about OUR pipeline, so the envelope deliberately does NOT say it - the
  caller has no pipeline and no lever, and it reads as an admission that the
  number may be junk. The envelope carries a caller-actionable caveat instead
  ("the most recent day or two may be absent rather than empty"); staleness
  belongs to the alerting stack, on our side of the boundary. A Dagster load
  watermark is the planned replacement for the derivation itself.
* **Call-log day-files are never pruned.** Nothing rotates the `calls-*.jsonl`
  day-files in `NOSTROLOGY_MCP_LOG_DIR`, and since 2026-08-01 growth
  is driven by inbound MESSAGE count, not by DB-backed calls (roughly 7-10x more
  records: ~5 per session rather than ~1-2). At observed volumes that is ~100 KB
  per day against a dedicated volume, so this is a watch item, not a problem -
  but it is unbounded in the long run. Per-record size
  IS bounded (`MAX_PARAMS_CHARS`, `MAX_CLIENT_FIELD_CHARS`), so no single caller
  can accelerate it; only sustained traffic can. Revisit with a retention sweep if
  the endpoint gets real usage.

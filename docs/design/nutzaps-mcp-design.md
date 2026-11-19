# Nutzaps over MCP - the free `nutzaps` domain (2026-09-30)

Status: implemented 2026-09-30; per-mint active users extended from month to day
and week the same evening, by the operator's decision (section 4). 0.5.1
(2026-10-02): a refused grain names the call that answers it where one does, and
`payment_activity@total` no longer claims to be additive (section 3, fork 17).
Third domain of `nostrology_query_metric`, after Zapstore (2026-07-30) and Routstr
(2026-08-29).
The PAID tool `nostrology_query_nutzaps` is unchanged: it still serves
`nutzaps.active_users@{day,week,month}` - network-wide DAU/WAU/MAU, deduplicated
across mints - for 21 sats per call, and nothing below serves that count.

## 1. What the surface plots, and which metric carries each

Everything `/nutzaps` and its descendants draw, and the metric that reproduces it:

| Page | Figure | Metric (free unless marked) |
|---|---|---|
| `/nutzaps` Overview | Nutzap Sats (daily + cumulative, trailing 365 d) | `payment_activity@day`, `sats` (cumulative = running sum from the window start) |
| `/nutzaps` Overview | Nutzap Payments (daily + cumulative) | `payment_activity@day`, `payments` |
| `/nutzaps` Overview | Monthly active users (trailing year) | PAID `nostrology_query_nutzaps` `active_users@month` |
| `/nutzaps/mints` | Mint leaderboard, periods All / 7d / 30d / 1y: Configured By, Payments, Total Amount | `mint_leaderboard@total` (no window = All; a from/to window = the periods) |
| `/nutzaps/mints` | Cumulative sats by mint, top 5 / 10 / 20 (trailing 365 d) | rank with `mint_leaderboard@total` over the window, then `mint_activity@day` running sums |
| `/nutzaps/mints/<mint>` | Headline: all-time payments + sats, Configured By | `mint_leaderboard@total` with `mint_url`, no window |
| `/nutzaps/mints/<mint>` | Headline: past-year payments + sats | `mint_leaderboard@total` with `mint_url` + the year's window (or `payment_activity@total`) |
| `/nutzaps/mints/<mint>` | Daily + cumulative sats and payments | `payment_activity@day` with `mint_url` |
| `/nutzaps/mints/<mint>` | Monthly active users of that mint | `mint_active_users@month` with `mint_url` |

The generalizations, the same way the Zapstore metrics are more general than their
charts: any from/to window (inclusive UTC days) instead of the page's fixed ones,
any mint instead of the page's one, full history instead of the trailing year, and
one-row totals (`payment_activity@total`) that no page draws, and distinct senders
per mint by UTC day and ISO week as well as by month (no page draws those).
Deliberately NOT generalized: a distinct-sender count across mints (section 4).

## 2. The population (every free nutzaps metric)

Rows of `events_kind_9321_nutzaps` - kind-9321 (nutzaps) - that are PAYMENTS
(`value_source = 'proof_tags'`: at least one Cashu proof in a `proof` tag,
nostrology-data#8) on a mint that `nutzap_mint_classes` does NOT mark `excluded`
(test and private mints, nostrology-data#9; a mint missing from that table
counts). The sender is the event `pubkey`; the day is the UTC calendar day of
`created_at`. This is exactly the web's `NUTZAP_PAYMENT_FILTER` +
`NUTZAP_REAL_MINT_FILTER` (nostrology-web `src/lib/db/postgres.ts`), and the same
population the paid `active_users` rebuild counts (`schema/nutzap_active_users.sql`).

An excluded mint stays excluded under a `mint_url` filter: it returns no rows, as
its page 404s (`payment_activity@total`, which always answers with exactly one row,
returns its row of zeros with NULL first/last day). The filter narrows within the
counted population; it never reaches past it.

## 3. The metrics

All on `nostrology_query_metric` with `domain="nutzaps"` (a bare metric name
resolves to it - names are unique across domains). Optional entity filter
`mint_url` on every one of them; no other entity filter applies.

| Metric @ grain | Rows | Columns | Window |
|---|---|---|---|
| `payment_activity@day` (default grain) | one per UTC day with >= 1 payment (sparse) | `utc_day`, `payments`, `sats` | from/to filter `utc_day` |
| `payment_activity@total` | exactly one | `payments`, `sats`, `mints`, `first_day`, `last_day` | epoch bounds on `created_at` |
| `mint_leaderboard@total` | one per LISTED mint | `mint_url`, `payments`, `sats`, `configured_by` | epoch bounds; see Configured By; row cap 2,000 |
| `mint_activity@day` | one per (UTC day, mint) with >= 1 payment | `utc_day`, `mint_url`, `payments`, `sats` | from/to filter `utc_day`; row cap 2,000 |
| `mint_active_users@day`, `@week`, `@month` (the default) | one per (bucket, mint) with >= 1 sender | `period_start`, `mint_url`, `active_users` | buckets STARTING inside from/to; row cap 2,000 |

- `sats` are sats (not msats): `amount_sats` is the sum of the proofs, never the
  `amount` tag.
- `mints` in `payment_activity@total` = distinct mints with a payment in the window;
  `first_day` / `last_day` = the first and last UTC day with a payment (NULL when
  there is none - the row still comes back, with zeros). Across separate windows
  only `payments` and `sats` add up - `mints` is a distinct count and the days
  combine as a min and a max - so the grain says `additive_over_time: false`, like
  `mint_leaderboard@total` (whose `configured_by` does not add either). It said
  `true` until 0.5.1; the flag speaks for the whole row.
- `additive_over_time` is therefore true on exactly two nutzaps grains,
  `payment_activity@day` and `mint_activity@day`, and the battery checks it on every
  successful call.
- An explicit grain a metric does not serve is refused with its legal grains and,
  since 0.5.1, the call that answers the ask (`metric.grain_redirect`, all three
  domains): `payment_activity` week or month -> `grain=total` with from/to spanning
  it; `mint_activity` total, week or month -> `mint_leaderboard` over that window (or
  the day rows summed per mint); `mint_leaderboard` day -> `mint_activity`;
  `mint_active_users` total -> why no sum of buckets replaces it.
- `active_users` = DISTINCT sender pubkeys per mint per UTC bucket: the calendar
  day, the ISO week from Monday 00:00 UTC (keyed by the Monday - `date_trunc('week')`,
  the paid table's rule) or the calendar month (keyed by the 1st). The three grains
  are one statement but for the `date_trunc` unit. Not additive over time and not
  additive across mints (one sender on two mints counts at both). The network-wide
  distinct count is the PAID `active_users`; the free tool never serves a count
  deduplicated across mints, so the unknown-metric error for `active_users` on the
  free tool names the paid tool.
- An omitted grain is `month` for `mint_active_users` (`MetricSpec.is_default_grain`),
  where the house rule (current, else day, else the metric's only grain) would give
  day; `payment_activity` and `mint_activity` default to day and `mint_leaderboard` to
  total. Month because it is the chart each per-mint page draws, a distinct count is
  the kind a caller must not sum and a month default invites that least, and month
  was the metric's only grain until the evening of 2026-09-30, so a bare call still
  answers as it did.
- Grains are named day, week, month, total, current in the schema's choices, in each
  registry entry's `params.grain.values` and in the invalid-grain error - plain
  sorting read "day | month | week". The registry's entries themselves stay sorted
  by key, and /data lists a metric's default grain first.
- A filtered call that matches nothing (an excluded mint, a URL not stored under
  that spelling, or no payment in the window) says so on its window line - "nothing
  matches the filter - ..." - instead of the generic "empty" line, which read as
  "this mint is idle". `payment_activity@total` counts as matching nothing when its
  one row has zero payments.
- `mint_url` is the normalized mint URL (lowercase scheme and host, no default
  port, no trailing slash, PATH CASE KEPT - `https://mint.minibits.cash/Bitcoin`).
  It is third-party text (a sender's `u` tag), so it is escaped and flagged
  untrusted like every other authored column.

### Configured By and the listing rule (`mint_leaderboard@total`)

Identical to the `/nutzaps/mints` leaderboard, generalized from its fixed periods:

- `configured_by` = distinct pubkeys whose LATEST kind-10019 (nutzap-configs) event
  lists the mint (current state: announcements, not usage). A window keeps only the
  pubkeys whose latest config was published inside it - the page's 7d / 30d / 1y
  reading ("configured by users who (re)published their config recently").
- `payments` and `sats` count payments whose `created_at` is inside the window.
- A mint is listed when it has at least one payment in the window, or
  `configured_by` >= 10 with no window (the page's All floor against single-typo
  mints) / >= 1 with a window. Excluded mints are never listed.
- Ordered by sats, payments, configured_by (all descending), then mint_url.

Parity with the page: exact at All (no window) - as the page shows it with its
"Include 0-sat mints" toggle on; by default the page hides the mints with no sats,
which the tool lists. The page's 7d / 30d / 1y are ROLLING
(`now - N x 86400 s`); the tool's windows are whole UTC days, so the same numbers
come back only for a window whose edges fall on midnight UTC.

### The `mint_url` filter

`mint_url` takes the URL as `mint_leaderboard` lists it. The server folds it with the
loader's own normalizer (`loader/nutzap_mints.normalize_mint_url`, shipped in the MCP
image so there is one spelling rule, not two): scheme and host case, a default port
and trailing slashes. A value with no scheme is read as https (the web's per-mint
route does the same). A value that does not normalize (not http(s), a bad port,
more than 1,024 bytes - the loader's cap) is rejected with the remedy: call
`mint_leaderboard` for the exact URLs. The scope line echoes the NORMALIZED value.

## 4. What stays paid, and where the line moved on the evening of 2026-09-30

The paid tool sells the distinct-sender count deduplicated ACROSS mints -
network-wide DAU / WAU / MAU - and still does, untouched: same tool, same schema,
same price, same payment path. The free metrics count payments and sats, and
distinct senders PER MINT.

The first cut went further - per-mint `mint_active_users` at day and week, and a
windowed `senders` column on `mint_leaderboard` - and the fresh-context review
measured what that gave away (live data, trailing 365 days): on 147 of the 194 days
with payments a single mint carried every payment, so that mint's per-mint day row
WAS the paid DAU; summing per-mint day rows equalled the paid DAU exactly on 169 of
194 days (87%), and summing week rows the paid WAU on 20 of 47 weeks. A one-day
window on the leaderboard's `senders` gave the same per-mint day counts. Both were
withdrawn before the first deploy, leaving month, which is different in kind:
summing per-mint months matched the paid MAU on only 2 of 12 months, and the
network-wide monthly chart is public on `/nutzaps` anyway.

The same evening the operator decided to serve per-mint day and week FREE all the
same, with those numbers in hand: `mint_active_users@day` and `@week` are live since
2026-09-30 22:17 UTC (nostrology-mcp 0.5.0). What that means for the paid
tool, stated plainly: a caller
can now rebuild its DAU from free rows on most days (the sum over mints is exact
wherever no sender used two mints that day) and its WAU on about four weeks in ten.
The paid tool stays the only count deduplicated across mints, and the only exact one
on the days a sender used two mints; whether it keeps its price is fork 2.

The leaderboard's `senders` column stays withdrawn: it was not part of the decision,
and `mint_active_users` over a one-day window now answers the same question. The
battery measures the paid line on every run (its report's "The paid line, measured"
paragraph, over the trailing year and all history) and checks that the free
per-mint counts bracket the paid table in every bucket - no mint above it, the sum
over mints never below it.

## 5. Chart-parity traps (the battery checks these)

- The web's 365-day series cut on a TIMESTAMP (`created_at >= NOW() - 365 days`)
  while their calendar starts at `date_trunc('day', NOW() - 365 days)`, so the
  page's OLDEST day (and the per-mint MAU's oldest month) is partial. Compare the
  days after it; a cumulative comparison starts after it too.
- The web buckets with `date_trunc('day', to_timestamp(created_at))` in the session
  time zone (UTC on Aurora); the tool pins `AT TIME ZONE 'UTC'`.
- The top-N chart ranks by trailing-365 d sats with the same timestamp cutoff.

## 6. Decision forks (taken, and worth revisiting)

1. **Extend the free metric tool vs a dedicated free nutzaps tool.** Taken: a third
   `domain` on `nostrology_query_metric`, the Routstr precedent (one schema to learn,
   name-based domain resolution). Revisit if the tool description outgrows what a
   model reads well - it now carries three domains.
2. **Where the paid line sits.** First taken: per-mint distinct senders free at MONTH
   only - per-mint day/week and a windowed `senders` column were withdrawn on review
   because they reproduced the paid DAU on 87% of days (section 4). Revised the same
   evening by the operator: per-mint day and week are free as well; only the count
   deduplicated across mints stays paid. Open: whether the paid tool keeps its 21
   sats now that most of its daily series can be rebuilt from free rows - reprice,
   add what the free side lacks (recipients, per-mint breakdowns of the network
   count), or retire it.
3. **Live SQL vs a pipeline substrate.** Taken: live SQL on the event tables with the
   web's predicates (the Zapstore pattern). Routstr's "pipeline aggregates, MCP
   selects" rule would need a nightly substrate for a population the web already
   computes live; revisit if the web moves to a snapshot document.
4. **Configured By under a window.** Taken: the page's reading (latest config
   published inside the window). A true "configured as of `to`" snapshot (each
   pubkey's latest config at or before `to`) answers the historical question
   properly and needs one more CTE; it would diverge from the page.
5. **Calendar-day windows vs the page's rolling 7d / 30d / 1y.** Taken: whole UTC days
   (the house contract). A `rolling=` option could reproduce those periods exactly.
6. **Excluded mints under a filter.** Taken: still excluded (the page 404s). Routstr's
   provider filter deliberately reaches excluded providers; a caller asking about
   testnut by name gets an empty table here, with the rule stated in the methodology.
7. **Normalizer reuse.** Taken: ship the loader's `normalize_mint_url` in the MCP image
   (one rule). Alternatives: exact match only (brittle for callers), or a copy (drift).
8. **No reference records yet.** A `mint_directory` record (mint class, review
   evidence, first/last seen, the NUT-06 `/v1/info` facts the mint pages fetch
   client-side) and a `mint_configs` record were left out.
9. **Recipients.** Not counted anywhere (the page does not draw them); a per-mint
   `recipients` column and self-addressed-share metrics are natural next steps.
10. **Value tiers.** Only the served tier (proofs in `proof` tags) is exposed; the
    receivable / DLEQ-verified tiers are not.
11. **Why an empty filter is empty.** The note names the three possible causes
    rather than the actual one. A lookup in `nutzap_mint_classes` (excluded, and
    why) plus "URL not in the archive" would be exact, at the cost of a second
    query on the empty path.
12. **Row-cap direction.** Capped day series sort oldest first, so a truncated
    answer drops the NEWEST rows (861 of 2,000 today). Newest-first, or a cap that
    keeps the tail, suits "recent" questions better.
13. **The tool description outgrew Claude Code.** Claude Code shows about 2,000
    characters of a tool description; the full text is ~6,000. A domain index now
    leads it, but the per-domain detail reaches such clients only through the
    server INSTRUCTIONS and each response's methodology. A discovery tool or
    per-domain tools would fix it properly.
14. **Tie order is the database collation.** The final `mint_url` tie-breaker sorts
    in Aurora's `en_US.UTF-8`, which ignores punctuation (`mint1.example.com` before
    `mint.21linz.at`); 10 of 52 tie groups differ from code-point order. The battery
    reads the database's order rather than assuming one. `COLLATE "C"` would make
    the order portable and predictable; the web's own leaderboard has no
    tie-breaker at all.
15. **`mint_active_users` defaults to month.** Taken: `is_default_grain` on the month
    spec (section 3), against the house rule that an event metric defaults to day.
    Revisit if callers mostly ask daily questions; the envelope's `grain:` line
    names the served grain either way.
16. **`mint_leaderboard` with `mint_url`.** The filter keeps one mint's row but not
    its rank, and a no-window call still applies the 10-config listing floor to the
    mint the caller named (a mint with 8 configs and no payment answers "nothing
    matches" with no window, and its row with `from=2020-01-01`). Proposal - a
    `rank` and a `listed_mints` column computed before the filter, and no floor for
    a named mint - in nostrology-data#13.
17. **No week or month on the payment series.** Taken (2026-10-02): `payment_activity`
    and `mint_activity` keep day (and `payment_activity` total). Coarser buckets exist
    only where a period cannot be rebuilt from days - distinct senders - and payments
    and sats add up: a week or a month is its day rows summed, or one `grain=total`
    call with that window. No caller had asked for week or month on any day-only
    metric (1,005 tool calls, 2026-08-01 to 09-30), the largest `mint_activity`
    answer (all mints, all time) was 867 rows on 2026-10-02, under the 2,000 cap, and
    Zapstore and Routstr serve no week or month either. What model callers do ask for
    is `grain=total` on day-only series - 11 refusals, all in Zapstore and Routstr (our
    batteries' deliberate probes aside) - so since 0.5.1 a refusal names the remedy
    where there is one, and nostrology-data#14 proposes serving those totals.
    Revisit if callers ask for week-aligned payments next to `mint_active_users@week`
    (payments per sender per week): a model then has to place each day in its ISO
    week itself.

## 7. Verification

Live battery against the deployed server, run on both builds of the day with the
same result - nostrology-mcp 0.4.0 as first deployed on 2026-09-30
and again after the second review's fixes that day -
204 checks, 201 PASS, 3 BOUNDED (the page's rolling 7d / 30d periods and the
per-mint chart's partial oldest month, each bracketed by the tool's whole-day
windows and exact against the page's own rolling rule), 0 FAIL. The deployed paid tool was exercised end to end the same day
with a free testnut token: paid call answered,
receipt line hides the mint, replay refused as already spent.

Per-mint day and week (nostrology-mcp 0.5.0) extended the battery: day and week
buckets for every mint and for one, a window from a Wednesday (the week that began
that Monday is left out whole), Monday bucket starts, every (day, mint) nested inside
its week and month, the day buckets matching `mint_activity`'s (day, mint) keys, and
a new section holding the free per-mint counts against the PAID table
(`active_user_counts_kind_9321_nutzaps`): the paid table counts the senders the raw
rows hold, and in every bucket no mint's count exceeds it and the sum over mints
never falls below it. Before the deploy, a run against a local server built from the
uncommitted working tree (reading Aurora read-only - its report was
not kept, and its `server:` line names the HEAD commit, which the dirty tree was
ahead of): 216 checks, 213 PASS, 3 BOUNDED, 0 FAIL. Against the deployed server
(0.5.0, with nostrology-web as of 2026-09-30):
226 checks, 223 PASS, 3 BOUNDED (the same three page
brackets), 0 FAIL; its 10 more checks were end-to-end ones that a local run skips. Its measured paid line reproduces the review's numbers
exactly: the per-mint sum equals the network count on 169 of 194 days (87%), 20 of
47 weeks and 2 of 12 months in the trailing year. The contract tier (`tests/test_nutzaps_contract_pg.py`) runs the
paid table's own rebuild SQL on its fixture for the same bracket, and was
mutation-checked: a bucket cut in the session zone, Sunday-start weeks, a window
keeping overlapping buckets, the exclusion predicate dropped, the month default
dropped and a row total counted before the window (which printed a false
`truncated:` line on windowed calls) each fail tests. The fresh-context review of this
change found no must-fix; its five should-fixes - a case-sensitive paid-line pin, paid
counts printed by a failing battery check, a fixture where DISTINCT equalled COUNT at
day grain, the deploy order, and that false `truncated:` line - are fixed or followed.

0.5.1 (2026-10-02 - the refusal redirects and the `payment_activity@total` flag)
tightened three battery questions and added three: the refusals NZ81-83 must now name
the call that answers them, NZ89 must say why no sum of buckets replaces a total,
every successful call's `additive_over_time` must be true on exactly
`payment_activity@day` and `mint_activity@day`, and NZ07's envelope must say only
payments and sats add up. Replayed against a 0.5.0-shaped server, exactly those six
fail (`test_battery_fails_against_the_0_5_0_rejections_and_flag`). Fourteen
mutations of `grain_redirect`, the server's use of it and the flag each fail the
tests aimed at them: an override dropped or trimmed to one grain, a sum suggested on
a non-additive series, a dead override, an unknown metric or an unserved grain
named, `grain=day` suggested for a week, quarter and year left without a redirect,
the weekly-era and running-total overrides dropped, the flag back to true, and the
server dropping the clause or computing it from the domain passed instead of the one
the metric resolves to. Against a local server built from the working tree (Aurora
read-only, so without the 10 end-to-end checks the deployed run adds): 219 checks, 216
PASS, 3 BOUNDED, 0 FAIL. The Zapstore
battery, whose M48 now checks the redirect too, ran against the same local server:
217 checks, 214 PASS, 2 BOUNDED, 1 FAIL - unrelated to 0.5.1: its
`routstr.model_catalog` question compares the tool's rows, which SQL sorts by the
database collation (`ORDER BY model`, punctuation ignored), with the pipeline
document's list, sorted by code point, and the curated list now holds ids where the
two disagree (`gpt-6.1-sol` beside `gpt-6-astra`).

Against the deployed server (0.5.1, live 2026-10-02 15:15 UTC, with
nostrology-web as of the same day):
229 checks, 226 PASS, 3 BOUNDED (the same three page
brackets), 0 FAIL. Its paid line, re-measured:
the per-mint sum equals the network count on 170 of 196 days (87%), 20 of 47 weeks
and 3 of 12 months in the trailing year. The Zapstore battery against the deployed
server: the same 217 / 214 / 2 / 1 as locally, its M48 passing on the live redirect.
The Routstr battery against the deployed server: 200 checks, 197 PASS,
3 FAIL - all three that same `model_catalog` sort order, seen from the catalog
versions, the reference document and the sorted-list check - with both RS56 checks,
the redirect included, passing.

The fresh-context review of 0.5.1 found no must-fix; every redirect clause was true
as written. Its should-fixes, all taken: the derived clauses trust a whole-row flag
that `routstr.network_activity@day` gets wrong (its override is now pinned grain by
grain, and per-column additivity is proposed in nostrology-data#14); the sparse
Routstr levels (`cumulative_revenue`, `catalog_size`) were told to "read the row for
the day you need", which reads a quiet month as empty, so they now say the value on a
day is the latest row on or before it; and the permanent records counted 11 refusals
where the log holds 20 - the other 9 are our batteries' own probes. Of its nits, the
Routstr ledgers' weekly era got its own redirect (a bound window on their totals sums
daily buckets only, so a 2025 month read as empty), the field comments now state the
whole-row rule, and the overrides are documented as not retiring on their own.

# The Routstr dataset - metric semantics reference

What every number on the `/routstr` page (nostrolo.gy/routstr) counts, what it
excludes, and what it must not be read as. The definitions live HERE, so they survive
any serving surface - the serving design is
`docs/design/routstr-design.md`, and the two computes that implement these
semantics are `dagster_code/pipeline/routstr.py` (production) and
[crosscheck/build_routstr_snapshot.py](../../crosscheck/build_routstr_snapshot.py) (the independent
cross-check; `crosscheck/compare_routstr_snapshots.py` enforces zero-diff agreement).

## 0. The one caveat that frames everything: self-reported

Every usage number (revenue, requests, tokens, success rate, model mix) comes from
kind-38422 (routstr-analytics-snapshots) events that **each provider's own node
publishes about itself**. Nothing verifies them: a node can overstate, understate, or
stay silent, and a node that goes offline takes its unreported history with it. The
page says "self-reported" wherever these numbers appear; so must any downstream use.
The announcement, catalog and review layers are also just signed
statements - by providers and by the Routstr curation key respectively - but they ARE
the marketplace's own ground truth (discovery and curation work off exactly these
events), so for those "what the network says" and "what the network is" coincide.

## 1. Sources

| Kind | Role |
|---|---|
| kind-38421 (routstr-provider-announcements) | The provider registry. ALL versions extracted; ~5,951 events, 50 pubkeys (2026-08-24). |
| kind-38422 (routstr-analytics-snapshots) | The usage ledger. Reduced in the extract to the LAST snapshot per (pubkey, UTC day) carrying the `routstr.analytics.snapshot.v1` schema tag; ~703 provider-days from 34 pubkeys. |
| kind-38423 (routstr-model-catalog) | The curated "routstr-21-models" allowlist. ALL versions; only the curated key's `d=routstr-21-models` coordinate counts (an impostor pubkey and a `:usage:checkpoint:` experiment also live on this kind and are ignored). |
| kind-38425 (routstr-provider-reviews) | Curation labels. ALL versions; the current status comes from the curated key only. |

The curated key (`4ad6fa2d...eacc8`) is the default the official routstr-sdk trusts
for both kind-38423 (routstr-model-catalog) and kind-38425 (routstr-provider-reviews).
Review events from other keys appear in the timeline,
labeled, but set no provider status.

## 2. Providers

- **Provider identity = pubkey.** A node that announced under several `d` tags
  (observed live) is ONE provider; its current announcement is the newest current
  version among its coordinates (NIP-01 tie-break: created_at DESC, id ASC).
- **A genuine announcement** carries at least one valued `u` (endpoint) tag AND a
  valued `d` tag - the discriminator every Routstr consumer uses, because
  **kind-38421 (routstr-provider-announcements) is squatted**: lnproxy publishes fee
  quotes on the same kind (no `u`).
  Squatter pubkeys are counted in the exclusion ledger, not as providers.
- **providers_announced** - distinct pubkeys with a genuine announcement (49).
- **providers_reporting** - included (see 4) pubkeys with at least one ledger row.
- **providers_active_30d** - included pubkeys with nonzero daily-ledger activity in
  the 30-day window. **Every ledger-derived `*_30d` figure uses the same window: the
  last 30 COMPLETE UTC days before the snapshot's generation instant** (the one
  exception is `success_rate_30d`, which aggregates the providers' OWN trailing-30d
  summaries - section 5) (day-aligned, i.e. days
  `today-30 .. today-1`; today is never a ledger day). Anchored to the generation
  instant, not to the last date in the data - a stale snapshot honestly shrinks the
  window's content rather than silently shifting it. (Until 2026-08-24 the bound was
  the raw instant minus 30x86400 s, which made a 17:46 run serve 29 days as "30d".)
- **first_seen** - earliest genuine announcement OR earliest ledger activity,
  whichever is older (the 1y analytics windows reach months before our archive's
  first capture of a provider's announcement).
- **Anti-flood caps.** Every list a third party can grow is bounded (identically in
  both implementations): model identifiers clip at 60 chars (clip-collisions merge),
  the doc lists cap at 100 named model rows (tail folded into "(other)", sums
  preserved), 200 newest reviews, 250 provider rows (summary counts stay uncapped;
  `providers_total` marks truncation), 20 URLs / 50 mints per provider at 200 chars
  each. Display ids are 12-hex pubkey prefixes, extended to the full pubkey for any
  colliding set. Non-finite JSON numbers (NaN/Infinity) in any numeric field read as
  absent or fail that bucket - never the refresh.
- **Endpoint class** per announced URL: `clearnet`; `onion` (`.onion` host);
  `private` (loopback / RFC-1918 / CGNAT 100.64/10 incl. Tailscale / `localhost` /
  dotless container names / `.local`-style suffixes); `invalid` (not http(s), bad
  IP). Provider class: `both` > `clearnet` > `onion` > `private-only` > `none` - a
  private mirror never demotes a provider that also has a public endpoint.

## 3. The usage ledger - reconstructing history from trailing windows

Each analytics snapshot embeds trailing windows (`24h`/`7d`/`30d`/`3m`/`1y`) whose
`model_usage_mix.metrics` arrays carry **UTC-grid-aligned activity buckets**: daily
buckets (1440 min) in the `30d`/`3m` windows, weekly buckets (10080 min, epoch-aligned
= Thursday-start) in `1y`. Buckets are sparse - a provider omits buckets with no
activity, so absence in a window says nothing on its own; the ledger only ever writes
buckets that are PRESENT. A day observed in an older snapshot is therefore kept even
when a newer, fully-covering snapshot omits it (seen live: "An OpenRouter Node"
dropped its own pre-2026-08-02 history from later snapshots; we still serve the six
days we archived). Observed history is a floor - see rule 3 and section 8. The ledger
merges buckets across all snapshots ever archived:

1. **Full-coverage only.** A bucket is trusted only when its whole `[start,
   start+len)` span lies inside `[generated_at - hours_back*3600, generated_at]`.
   Probed live (2026-08-24): at the window's trailing edge the node truncates the
   bucket to the window (a 297-request day re-reported as 3 once the 3m window slid
   past its start), and at the leading edge the current day is still accumulating.
2. **Newest wins.** Among fully-covering observations of the same (provider, grain,
   bucket): the snapshot with the highest (created_at, id) wins - probed live, late
   accounting revises a closed day upward across consecutive snapshots. Within one
   snapshot the `30d` window beats `3m` (they agree whenever both fully cover).
3. **Weekly only pre-daily.** Weekly (1y-window) rows are kept only for weeks that
   end at or before the provider's earliest daily-covered date, so the two grains
   never overlap and all-time sums add them directly. A mid-history gap longer than
   the daily windows' reach therefore stays a gap - observed history is a floor.
4. **Units.** Bucket revenue is msats (float on the wire, integral in practice);
   every value is rounded half away from zero to integer msats at ingest and all
   arithmetic is integer msats. Sats shown anywhere = round(msats/1000).
5. **generated_at** (the aggregation instant, used for window math) is trusted only
   within a day of the event's created_at; otherwise created_at substitutes.
6. Malformed buckets/snapshots (unparseable timestamp, off-grid timestamp, negative
   values, non-JSON content) are counted in `anomalies`, never guessed at. Live
   archive: zero of either.

Because the last snapshot of a day fully covers every previous day it reports on, the
extract's last-per-(pubkey, day) reduction is lossless up to one edge: a late
intraday revision of a TRAILING-edge bucket can be visible only to an intraday
snapshot that the reduction discards (the day's final snapshot no longer covers
that bucket once the window slides). Both implementations share the reduction, so
the agreement gate is unaffected; the ledger then serves the newest surviving
fully-covering observation.

The daily series starts at the earliest daily-covered date network-wide
(2025-12-02-ish in principle; 2026-03-06 in the current archive), weekly history
reaches to 2025-08-13. "Today" never appears - the newest possible ledger day is
yesterday (UTC), by rule 1.

**Per-model figures** come from each bucket's per-model maps, which the node
truncates to its top models with an `others_*` residual - so `(other)` is part of
the data model. Identity `named + others == totals` held exactly on 1,810 live
buckets for all three measures. Model identifiers are reported verbatim, per
provider - the same model appears both as `openai/gpt-...` and `gpt-...` in the corpus;
no normalization is applied (the split is real information about reporting hygiene,
and any folding rule would be ours, not the network's).

## 4. The exclusion ledger (headline aggregates only)

Precedence-ordered, one bucket per provider; excluded providers stay in the
providers table with their reported numbers visible - excluded means "not in the
network totals", never hidden:

1. **Test / staging nodes** - the announcement name or an endpoint host LABEL
   (TLD exempt: `routstr.rewolf.dev` is a real node on the `.dev` TLD) matches
   test/testing/staging as a word.
2. **Private-endpoint-only nodes** - every announced endpoint is private/invalid;
   no marketplace client could ever reach them.

(Kind squatters are a registry-side ledger entry - they were never providers.)
A provider that reports usage but never genuinely announced IS included (classified
from its snapshots' own `endpoint_urls`, named by pubkey prefix): announcements
expire from relays' latest-only retention faster than our capture began.

## 5. Success rate

`success_rate_30d` = 1 - sum(failed)/sum(total) over the trailing-30d summary of
each included provider whose newest snapshot is at most 7 days old. It is
self-reported twice over (the provider computes the window; we only aggregate),
counts only providers still talking - both biases point up - and total_requests
includes non-chat endpoints. Read it as "what the still-reporting network says
about itself", nothing stronger.

## 6. Catalog and reviews

- **catalog** - the curated key's `routstr-21-models` list: current members, the
  date of the newest version, and a size-over-time history (one point per archived
  version; 22 versions since 2026-06-01). The official SDK 403s models outside it.
- **reviews** - every valued review event, newest first, with reviewer provenance
  (`curated` vs `other`). A provider's status badge = the newest curated label for
  its pubkey. Positive labels (`lgtm`/`trusted`/`verified`) enable a node in the
  SDK; anything else - including labels outside the SDK's own vocabulary, like the
  live `bad` - disables it. The timeline is the point: nodes get demoted (lgtm ->
  avoid) and re-promoted, and that history is otherwise invisible (addressable
  events; relays keep only the latest).

## 7. What these numbers are NOT

- Not network totals: only what providers chose to report, over the windows their
  snapshots still covered when we archived them. A floor, never an overstatement -
  except insofar as a provider inflates its own numbers, which nothing prevents.
- Not payment-verified: RIP-01 payments are HTTP-header Cashu, invisible on Nostr.
- Not per-user anything: the protocol publishes no user-side events.
- The announced-network facets (transports, versions, mints) describe CURRENT
  announcements including stale ones - an old `version` here means a stale
  announcement, not necessarily a running old binary.

## 7b. The same numbers over MCP (2026-08-29)

Everything above is served by the public MCP endpoint (`https://nostrolo.gy/mcp`,
registry at nostrolo.gy/data) under `domain="routstr"` of the two Nostrology
tools. The rule that keeps the tools and the
page identical: **no MCP code computes a Routstr number**. The nightly pipeline is the
single computing authority; it writes the page's document and five substrate tables
in one transaction, and the tools select, filter and window those rows. Where a
drilldown the page does not render was wanted (per-model per-day activity, the catalog
versions' added/removed deltas, the full review timeline), the PIPELINE gained a table
for it, tied to the document by invariants. The one SQL arithmetic the tools do - a SUM
over pre-aggregated buckets for a caller-chosen window on `provider_activity` /
`model_activity` at `grain=total` - is proven equal to the page's 30d and all-time
figures at the windows the page publishes (no window; and from = the refresh's
generation day minus 30, to = that day minus 1 - anchored to the refresh day named by
`network_summary.refresh_generated_at`, NOT the caller's date), by a Postgres-backed
contract test and a live gate. Design record:
`docs/design/routstr-mcp-design.md`.

Consumer-facing conventions worth restating here because they follow from sections
0-4: every Routstr revenue column is integer **msats** except `network_summary`'s two
`*_sats` headline columns; unfiltered per-provider rows cover INCLUDED providers only,
and a `provider_pubkey` filter reaches an excluded provider (flagged); a `from/to` window
on the `@total` grains includes exactly the DAILY buckets STARTING inside it and no
weekly bucket (the page's own windowed rule, section 4), while an unbounded call sums
both grains and so equals the all-time figure.

## 8. Cross-check against routstr.com/stats (2026-08-24)

The official network page, `https://routstr.com/stats`, computes its headline numbers
in the browser from the same kind-38422 (routstr-analytics-snapshots) events. It is
early-stage software and not ground truth, but a disagreement is a signal worth
explaining - so it was reproduced FROM SOURCE (Routstr/landingpage `2202e64`,
`app/stats/page.tsx`; the line numbers below are that file's), the reproduction was
validated against the live page rendered in headless Chrome (all eight figures
matched to display precision), and every delta was reconciled to the request.

**Their method.** Read `{kinds:[38422], limit:20000}` from damus, relay.nostr.band,
nos.lol, nostr.mutinywallet.com and relay.routstr.com (L119-121; nostr.band times out
and mutinywallet does not resolve, and relay.primal.net - one of OUR sources - is not
read; strfry clamps the limit to 500 silently). Accept schemas usage.v1/usage.v2/
snapshot.v1, drop `:checkpoint:` d-tags (L1172-1173, L835), identify providers by
`content.provider_id` rather than pubkey (L1175-1179). Per provider take the single
LATEST `:stats`/`:usage` event, with no freshness cutoff (L1222-1228), and sum its
`windows["30d"].summary` key-by-key across providers (L488-491): `total_requests`
(INCLUDES failed requests, L610-614), `total_tokens`, float `revenue_sats` (gross of
refunds, L633-634). "Active providers" = every provider with any accepted snapshot,
ever (L1692). "Active models" = distinct model ids with `model_counts > 0` across
those latest windows, `unknown` excluded (L645-659). No exclusions of any kind.

So theirs is a sum of per-provider trailing-720-hour windows anchored at each
provider's OWN generation instant (from 2026-04-11 to now), provider-computed; ours
is a merged daily ledger over every archived snapshot, complete UTC days in one
common window, successful requests only, integer msats, with the section-4
exclusions, keyed by pubkey.

**The numbers** (theirs reproduced from the relays at 2026-08-24 19:59Z, live page in
brackets at 20:25Z; ours = the 17:46Z snapshot):

| Metric | routstr.com/stats | Nostrology | Why they differ |
|---|---|---|---|
| Active providers | 34 [34] | 8 active in 30d (20 ever reporting) | Theirs counts any provider ever seen: 15 of the 34 last reported more than 30 days ago (oldest 2026-04-11), 4 are test/private nodes. |
| 30d requests | 66,284 [66.4K] | 65,713 | Theirs: +2,577 failed requests, +2,422 from 9 providers whose latest window ended 30-97 days ago, +556 test/private. Ours: +6,452 retained from "An OpenRouter Node"'s pre-08-02 days (section 3, floor rule), +2,467 from a provider only relay.primal.net still carries. Reconciles exactly. |
| 30d tokens | 1.10 B [1.1B] | 1.02 B | Same drivers; one stale provider's July window alone is 82.6 M of theirs. Reconciles exactly. |
| 30d revenue | 2,466,274 sats [2.5M] | 2,459,591 sats | Both GROSS of refunds (their net would be 1,360,655 - refunds are 44.8% of gross). Reconciles to 0.45 sat (float bucket sums). |
| Active models | 71 [71] | 50 | Theirs spans all 34 providers' latest windows incl. stale and excluded; restricted to our providers and window it is 44; ours adds the retained days' models. |
| Avg tokens/request | 16.6K | 15.5K (derived) | Ratio of the above; their denominator includes 0-token failed requests. |
| Top provider share | 50.1% | 44.5% (thefux, not a headline for us) | Theirs includes today's partial day, where thefux is the busiest node. |

**What it changed on our side** (both implementations, same day):

1. The `*_30d` window bound is now day-aligned - the last 30 COMPLETE UTC days. The
   old `generated_at - 30*86400` bound dropped the 30th day at any run after
   midnight, so "30d" served 29 days.
2. Section 3's "absent means zero activity" sentence contradicted the code, which
   keeps an observed day when a newer snapshot omits it. The code's behaviour
   (observed history is a floor) is the deliberate one - a node that resets its own
   history does not un-happen the activity we archived - and is now stated and
   pinned by a test.
3. The page's footer says that ledger days end yesterday and that a snapshot arriving
   after the archive's 12-hour Parquet roll shows up a day later (two providers had
   08-24 snapshots on the relays that the 17:46Z run could not yet see).

**What it says about theirs** (reported here, not our bug): no freshness filter
("30d" includes windows that ended in May-July: 3.7% of requests, 8.8% of tokens);
requests include failures while their own per-model chart is successful-only; a
provider whose current snapshots reach only relay.primal.net is shown at its stale
July value; thefux's 235 KB snapshot exceeds nos.lol's and relay.routstr.com's
`max_message_length` and survives on damus alone; the staging host and a CGNAT-only
node are counted.

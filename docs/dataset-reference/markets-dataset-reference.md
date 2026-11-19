# Marketplaces dataset - metric definitions and provenance

**Living reference.** The authoritative definition of every metric the
marketplaces dataset exposes: what it counts, what it excludes, and what it
must not be read as.

Since **2026-08-23** the metrics are computed in production by the Dagster
`marketplaces_refresh` job
(`dagster_code/pipeline/marketplaces.py`, daily 02:15 UTC), and
[`crosscheck/build_markets_snapshot.py`](../../crosscheck/build_markets_snapshot.py) is the
**independent re-derivation** of the same methodology straight over the raw S3
archive. The two are held in FULL agreement by
[`crosscheck/compare_markets_snapshots.py`](../../crosscheck/compare_markets_snapshots.py)
(zero diffs; procedure in its header). Keep this file current with BOTH
implementations - a methodology change lands in both or the agreement gate
fails, except in `dagster_code/pipeline/markets_geolib.py`, the geo resolution
and text folding that both import, where the gate cannot catch a mistake; if
this file disagrees with the code, the code is the truth and this file is the
bug.

This exists so the semantics survive the surface they are served through.
Today the numbers reach one static page; the intent is to serve them the way
the other surfaces are served, and eventually through the MCP endpoint. **A
metric served without its definition is a metric that will be misread** - the
counts here are heavily conditioned, and none of them means the obvious thing.

---

## 1. Provenance

| | |
|---|---|
| Store | `s3://$NOZZL_ARCHIVE_BUCKET`, Parquet, Hive-partitioned `c={class}/k={kind}/p={shard}/dt={ingest-date}` |
| Kinds | kind-30402 (markets-product-listings), kind-30406 (markets-shipping-options), kind-5 (event-deletion-requests) |
| Also captured, unused | kind-30405 (markets-collections) |
| Source relays | relay.plebeian.market, relay.conduit.market, nostr.mom, nos.lol, relay.damus.io, relay.primal.net |
| Access | read-only: an AWS profile that can read the archive (`AWS_PROFILE`) |
| Freshness | daily (`marketplaces_daily`, 02:15 UTC); each extract flushes its kind's WAL itself. A manual refresh is the deployment's audited re-run action; only an ad-hoc DuckDB read needs the manual flush (`POST /admin/flush/{kind}` on nozzl's admin listener) or the tail of the data is missing |

**`dt=` is the INGEST date, not `created_at`.** Do not use it to answer "when
was this published".

**Completeness is inherited, not asserted here.** The six-relay configuration
was measured at 100% of an 11,568-coordinate union on 2026-08-20, and that
measurement calls itself a lower bound. Nothing in this dataset re-verifies it.

---

## 2. The unit: what one row is

A marketplace listing is an **addressable** event, so the archive holds every
revision of it. Events dated more than 2 days past the compute instant are dropped
before everything (both implementations, since 2026-08-23): a hostile `created_at`
would otherwise permanently win its coordinate's current-version selection, and a
year-10000-scale value crashes the date handling. The archive held zero such events
when the bound landed. An event inside the 2 days is kept with its `created_at`;
only the date a listing is counted on is capped, at the run (see Dates, below).
The unit of every count below is the **current version of a
`(pubkey, d_tag)` coordinate**: newest `created_at`, lowest event id breaking
ties, per NIP-01.

Consequences that bite:

- **Archived events > coordinates.** A refresh can add 254 events but only 59
  coordinates; the rest were new versions of listings already held.
- **Distinct coordinates < row counts in the raw archive.** The archive keeps
  history the relays have already discarded.
- **A `d_tag` can be NULL** (41 rows). Build coordinate keys with
  `coalesce(d_tag, '')` or joins silently never match.

---

## 3. Exclusions

Six groups are removed from the current coordinates to get `active_listings`, and a
product's options are folded into the product, which is the seventh row below.
**Each coordinate lands in at most one group and the order is the precedence**,
so every count except the first UNDERSTATES its group. Where the unconditioned
count matters it is published separately (`withdrawn_any`, `deleted_any`).

| # | Group | Rule | Identified by |
|---|---|---|---|
| 1 | Non-retail bulk publishers | Three accounts that publish at volume but are not offering their own goods | pubkey prefix |
| 2 | Carrier / staging clients | Apps using kind-30402 (markets-product-listings) as transport for something else, and one staging host | `client` tag, lowercased |
| 3 | Unrenderable | No title, no `content`, no `summary`, no `image` - nothing a buyer could see | content |
| 4 | Synthetic / test | Test-worded title, OR identical title from >= 4 pubkeys at <= 1 distinct price | title + price |
| 5 | Deleted (NIP-09) | A kind-5 (event-deletion-requests) from the coordinate's OWN author covers its current version | kind-5 (event-deletion-requests) `e` tag naming that version, or `a` tag dated at or after it |
| 6 | Withdrawn | `status` in sold/taken/deleted/inactive/completed/cancelled/hidden/closed/reserved, or `visibility` hidden, or `stock` <= 0 | tags |
| 7 | Product options | A size, colour or other option a seller published as its own listing: see below | `type` and `a` tags |

**Invariant, checked every run: the seven counts plus `active_listings` equal
`current_coordinates` exactly.** If they do not, a coordinate is double-counted
or silently dropped.

### One row per product

The GammaMarkets spec lets a seller publish a product as a parent (`type` variable)
and one child per option (`type` variation, with an `a` tag naming the parent), each
its own event and coordinate. A T-shirt with 33 sizes and colours is then 34 coordinates
and one product. Conduit Market reads it the same way: a child's link opens its parent's
page with the selector on it.

A **family** is one key's children naming one parent, and it keeps one row:

- the parent, if it is live, the same key's, and not itself a child;
- otherwise the family's newest child (newest `created_at`, lowest event id breaking a
  tie, as NIP-01 picks a current version).

The other children go to this group. It is last in the precedence, so a child excluded
for another reason stays where it was. `type` is compared exactly, and the parent is the
first `a` tag naming a kind-30402 (markets-product-listings) coordinate; a child with no
such tag is no family's and stays.

- **The family is per key.** Naming another seller's listing as a parent folds nothing of
  theirs, only the namer's own rows into one row of their own. So no seller leaves the
  pool by this rule, and `sellers` does not move.
- **A parent that is itself a child stands for nothing.** Otherwise a child naming itself,
  or two naming each other, would fold away with no row left.
- **A parent that is not live still leaves a row.** One key gives its parents `stock: 0`
  and its children `stock: 100`, so the parents read as Withdrawn and the children as live.
- **The row kept takes a price only if it has none with a unit:** the newest priced
  child's. Otherwise those products would drop out of the currency figures. An amount
  that is no number still counts as a price, as it does in those figures.

A one-sided change to this rule would mostly pass the agreement gate, because the archive holds
few of the shapes where the two implementations could part. So the gate starts with
[`crosscheck/compare_markets_fold.py`](../../crosscheck/compare_markets_fold.py), which replays generated
shapes through both, row by row.

### The named accounts

Rules 1 and 2 name specific publishers, which is a maintenance liability worth
seeing plainly.

| Identifier | What it is | Why excluded |
|---|---|---|
| `7d33ba57d8a6` | Stock-photo catalogue | One $0.99 listing per photo; its `g` geohash is where the PHOTO was taken |
| `1f92b7f28bff` | Mempool telemetry | One listing per 5-minute bucket at 1 sat; the kind is a carrier for a time series |
| `b8e363fd8fbc` | Directory of bitcoin-accepting businesses | Titles are other people's company names; **0 of 249 carry a price** |
| `client = ulendo` | Malawian ride-hailing app | Driver/rider profiles; 45 of 52 untitled. Applied per LISTING, so the 3 of 31 pubkeys with genuine listings keep them |
| `client = account.staging.synvya.com` | Staging host | 273 listings, 4 pubkeys, one 7-day burst; none of the 4 has any listing outside this client |

**These are name-based and will silently stop matching if a client renames
itself or a publisher rotates keys.** Any consumer of this dataset should treat
a sudden jump in `active_listings` as a suspected rule failure before treating
it as growth.

### Why the synthetic rule needs a price condition

Shared titles alone are not evidence of a test farm.

| Title | pubkeys | distinct prices | verdict |
|---|---:|---:|---|
| `neo-brutalist vintage chair` | 130 | 0 | farm |
| `test bike - click-through` | 96 | 1 | farm |
| `buying btc for ngn` | 6 | 9 | **real** - Nigerian P2P, six different rates |
| `beef` | 5 | 2 | **real** |

`demo` and `sample` are deliberately absent from the test-word list:
`[rare] 2010 specialized demo 8` is a mountain bike.

### Deletions: NIP-09 scoping is load-bearing

A delete request is valid **only for its own author's events**. Of 981 (author,
coordinate) pairs in the archive, **99 are cross-author from 2 authors**, the
worst being the stock-photo bot, which issued 129 kind-5 (event-deletion-requests)
naming **98 other pubkeys' coordinates**. Honouring them removed 33 live
listings their owners never withdrew.

**What a request has to say.** NIP-09 gives two forms, clients use both, and they cover different
things. An `e` tag names one version's id and covers exactly that event. An `a` tag names the
coordinate and covers every version up to the request's `created_at`, inclusive, so a listing
republished after its request is live again. Only the CURRENT version is asked about: a request that
names an older version by id leaves the listing live. A request dated past the now + 2 days horizon
is not part of the dataset, row by row, so it cannot mask an earlier valid one. Before 2026-11-04
this read `a` tags alone and compared no dates, which counted 434 listings their authors had deleted
as live and excluded 74 that had been republished since.

The delete scan pre-filters kind-5 (event-deletion-requests) to pubkeys that authored
a kind-30402 (markets-product-listings). **That is lossless only for
coordinates present in the archive**, and only while both scans read the same
listing snapshot. Run the delete scan against a stale or narrower listing set
and the loss becomes real.

---

## 4. Metric definitions

### Counts

| Metric | Definition | Do not read as |
|---|---|---|
| `archive_events` | All kind-30402 (markets-product-listings) events, every version, DISTINCT event ids (an archive-level duplicate row is one event, matching the production extract's id-dedup) | Number of listings |
| `current_coordinates` | Distinct `(pubkey, d_tag)`, current version only | Live listings |
| `active_listings` | `current_coordinates` minus the seven groups above | Listings a buyer could actually purchase - NIP-99 has no expiry, see below |
| `sellers` | Distinct pubkeys with >= 1 listing in the pool | Distinct people; one person may hold several keys |
| `active_90d` / `sellers_90d` | Pool listings/sellers whose effective date is within 90 days of run time | A cohort - the window slides, so this changes without any new data |
| `countries` | Countries with >= 1 placed listing | Countries with a market; 18 have exactly 1 listing |
| `withdrawn_any`, `deleted_any` | Unconditioned counts across ALL current coordinates | The exclusion-table rows, which are precedence-conditioned and smaller |

**There is no expiry in NIP-99.** A listing with no `status` tag counts as
active forever, and only 313 of 11,727 coordinates are ever marked withdrawn -
2.7% across three years, which no real marketplace sustains. **The 90-day
figures carry more signal than the totals.**

### Dates

The NIP-99 `published_at` tag where present and plausible
(`1500000000 <= t <= now`), else the event's `created_at`. About 68% of
coordinates carry `published_at`.

Never after the run. A listing dated by a `created_at` ahead of the run (section
2's 2-day allowance) is dated at the run: it counts on the run's day and in its
month, the buckets still filling, and takes its own date on the first run after
it. So no series, `last_listing` or `effective_ts` runs past `generated_utc`, and
`check_invariants` fails the run if one does. Both implementations, since
2026-10-08; before it, a listing dated past the
month's end could give the next month a bar. Of the 20,607 listing events received
2026-08-20 to 2026-10-08, 24 were dated ahead of receipt, by at most 33 s, and none
of the 49 snapshots so far dates anything past its run. A run reads its clock only
after flushing the listings, so a listing has to be dated further ahead than that
gap.

**Both growth series are floors, and both are survivorship series.**

- *Floor*: relays keep only the current version, so a listing edited since
  publication and carrying no `published_at` is dated by its edit. The
  ecosystem can only be older than the curve shows.
- *Survivorship*: a listing published in 2024 and since deleted or withdrawn
  appears at NO point on the cumulative curve, because it is not in the pool at
  all. These are not records of activity as it happened.

| Series | Definition |
|---|---|
| `cum_sellers` | Daily cumulative count of pool sellers, each counted on the date of their earliest pool listing |
| `cum_listings` | Daily cumulative count of pool listings by effective date |
| `new_monthly[].v` | Pool listings whose effective date falls in that month |
| `new_monthly[].s` | Sellers whose FIRST pool listing falls in that month. Each seller appears in exactly one month; the series sums to `sellers`. This is first-time sellers, not sellers active that month |
| `partial_month` | The month the run falls in (generated_utc's): the bucket still filling, and the last `new_monthly` row, because no listing is dated after the run (see Dates). With nothing dated in it yet - the 1st, before the day's first listing - `new_monthly` has no row for it. Until 2026-10-08 it was the newest listing's month, which on such a 1st named the month just completed, and a listing dated past the month's end could give the next month a row after it |

### Geography

Resolution ladder: geohash (>= 2 chars, decoded, checked for plausibility,
reverse-geocoded offline) -> free-text `location` (alias / country-name /
US-state / whole-string scan / city gazetteer) -> seller context (a seller's
unambiguous listings resolve their ambiguous ones, ties broken on country code).

Three deliberate guards:

- A bare city name is accepted **only where the gazetteer maps it to exactly
  one country**. `Zaragoza` and `Barcelona` stay ambiguous.
- Country names that double as English nouns are held out - `turkey`, `china`,
  `chad`, `jersey`, `georgia`, `mali`, `niger`, `guinea`, `grenada`,
  `dominica`, `malta`, `panama`, `cuba`, `guam` - **before** the exact-name
  lookup, not only in the substring scan.
- A geohash that decodes south of 60S is refused (2026-10-23). Every digit 0-9
  is a geohash base32 character, so a number that was never a geohash still
  decodes: one seller's `g` tag ladder `1/12/125/1252/12520` is the postal code
  of Nules, Castellon, and decodes 2.4 km from the South Pole, where the
  nearest-city lookup - planar in degrees, unbounded - answered Chile for 20
  listings. The decode is kept in the listings table; it just names no country
  and draws no bubble. **This guard is narrow**: of the 100,000 five-digit
  strings, 40,000 decode south of the cut and the rest land elsewhere on Earth,
  so a junk geohash near a coast or a city is still believed.

**Nothing is inferred from an IP address, from which relay carried the event,
or from any signal outside the event itself.** About 17% of listings place to a
country (1,531 of 9,232 on 2026-10-23); the rest is published as five explicit buckets (`NOTAG`, `NONGEO`,
`UNRES`, `REGION`, `AMBIG`) rather than dropped.

`location` is the declared location **of the goods**, not the seller's
residence. Country shares describe the placed subset only, and sellers who fill
in a location are plausibly more serious than those who do not, so the shares
likely understate the casual tail.

**Rank countries by SELLERS, not listings**, wherever the question is "where is
there a market". Listing counts let one bulk catalogue outweigh a whole
community, which is exactly what happened before rule 1 existed.

Map bubbles sit on a named town or a geohash of >= 5 characters (~2.4 km) where
available, else the country centroid; only the former is badged exact. A 3-4
character geohash is a 39-156 km cell.

### Categories and currency

- **Categories** are the NIP-12 `t` tag, lowercased. Only tags used by **>= 5
  distinct sellers** are surfaced; without that floor the ranking is topped by
  single-shop vanity tags. There are ~1,790 distinct tags in total.
- **Currency** is the third element of the `price` tag. `btc`, `sats`, `sat`,
  `msat`, `msats`, `satoshi`, `satoshis`, `bitcoin`, `xbt`, `sat(s)` fold into
  one Bitcoin group in every capitalisation. **The bitcoin percentage is a
  share of PRICED listings (~79% of the pool), not of all listings.** Always
  serve the denominator with it.

### Shipping

From kind-30406 (markets-shipping-options). **The `country` tag is ONE tag
holding MANY values** - an EU-wide option carries all 27 member codes, the
largest observed holds 31. Reading only the first value discards ~1,100
destinations and, because `AUT` sorts first among EU ISO-3 codes, invents a
phantom Austrian cluster. Values arrive as ISO-2, ISO-3 and full names in the
same corpus; anything resolving to no country is dropped rather than guessed.

Only ~84 sellers publish a shipping option with a resolvable country, so this
describes a small, self-selecting slice.

---

## 5. Known limitations

- A bare `china` resolves to the seller's modal country (the risky-name holdout
  returns unresolved and seller context then fills it). One listing today.
- 40 listings take their country from a 3-4 character geohash - a 39-156 km
  cell, which is a country centroid in disguise and is not badged as precise.
- A `g` tag outranks an explicit `location` tag unconditionally: the text is
  consulted only when no geohash decodes. One listing today names Monterrey,
  Nuevo Leon, Mexico in its `location` and is published as US, 7 km from
  Prichard, Alabama, badged "exact location". nostrology-data#26
  (/marketplaces: a junk `g` tag outranks the seller's own location tag).
- `NOW` is evaluated at run time, so `active_90d` and `sellers_90d` drift
  between runs with no new data. The snapshot is not byte-reproducible.
- The `client`-based carrier rules are name-based (see above).
- kind-30405 (markets-collections) is captured but unused.

---

## 6. Serving

The full serving design - pipeline, tables, and the DB contract the webapp
reads - is `docs/design/marketplaces-design.md`; the short version:
since 2026-08-23 the daily `marketplaces_refresh` job writes two Aurora
tables (schema/init.sql) -

- **`marketplaces_snapshot`** - the page's full dataset as one JSONB document
  per refresh, append-only; `/marketplaces` serves the newest row (a
  single-row read per request; no finer-grained tables or MVs
  until a real query-performance need shows up).
- **`marketplaces_listings`** - one row per current coordinate with
  classification + geo, TRUNCATE+INSERT each refresh. **This is the queryable
  substrate for the eventual MCP tool** - every exclusion flag, the
  precedence bucket, and the resolved country travel with each row, so a
  future serving surface can re-derive any conditioned count without
  re-reading S3.

The committed-static-JSON stopgap (a manual refresh writing
`nostrology-web/src/lib/data/markets.json`) is retired. The remaining manual
lever is the audited materialize path: the deployment's re-run action for
`marketplaces_refresh`.

Whatever the surface, these travel with the numbers or the numbers are wrong:

1. **The denominator.** Every percentage here has a conditioned base.
2. **The exclusion list**, including that it is precedence-ordered.
3. **`published_at` vs `created_at`**, and that the series are floors.
4. **Survivorship** - the cumulative series is not history.
5. **That geography is declared, optional, and ~76% absent.**
6. **Run time**, because the 90-day windows slide.

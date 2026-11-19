"""The /marketplaces metrics compute - one snapshot per run from the marketplace kinds.

PRODUCTION HOME of the metric methodology that crosscheck/build_markets_snapshot.py
pioneered (that script is now the independent cross-check: crosscheck/compare_markets_snapshots.py
re-derives the snapshot from the raw S3 archive via DuckDB on a workstation and diffs it
against ours field-by-field; the two must FULLY agree). Every rule here is therefore an exact
port of that generator's DuckDB SQL + Python -- down to DuckDB's trim() and try_cast()
semantics (`_duck_trim` / `duck_try_cast_bigint`, pinned by probe on 2026-08-23 and by the
unit tests). Change methodology IN BOTH PLACES or the agreement gate fails. The exception is
markets_geolib, which the generator imports: the gate cannot catch a mistake there, and that
module's own tests hold its rules.

INPUTS (parsed NDJSON rows from the three Athena UNLOAD extracts - see sql/):
  * listing_events  - kind-30402 (markets-product-listings), ALL versions, id-deduped,
                      each {id, pubkey, created_at, content, tags, d_tag}
  * shipping_events - kind-30406 (markets-shipping-options), same shape
  * deletion_rows   - SELF-AUTHORED kind-5 (event-deletion-requests) requests naming a listing,
                      {author, tag, value, created_at}: tag 'a' names a coordinate, tag 'e' one
                      version's id (NIP-09: a delete request is valid only for its own author's
                      events; the SQL enforces it, parse_deletions asserts it)
  kind-30405 (markets-collections) is captured by nozzl but feeds no metric on the page,
  so it is deliberately not an input.

WHAT COUNTS AS A LISTING - the current version of each (pubkey, d_tag) coordinate (newest
created_at, lowest event id breaking ties, per NIP-01), minus six precedence-ordered groups
that are not someone selling something: bulk publishers (by pubkey), carrier/staging clients
(by `client` tag), unrenderable, synthetic/test (incl. >=4-pubkey same-title farms at <=1
distinct price), NIP-09-deleted (self-authored, and covering the current version - see
parse_deletions), withdrawn (status/visibility/stock). Then one row per product: a size, colour or
other option a seller published as its own coordinate counts with its product, not beside it - see
fold_product_options.

OUTPUTS: `build_snapshot` returns (recs, data) - the per-coordinate records and the page's
snapshot dict; `write_pg` persists both (marketplaces_listings TRUNCATE+INSERT + one
marketplaces_snapshot row) in ONE transaction, behind the MIN_ACTIVE_LISTINGS anti-shrink
gate, so a failed run leaves the previous serving data untouched.

THE SIX COLUMNS THAT FEED NO METRIC (added 2026-10-24, nostrology-data#29 (/marketplaces: the
listings table drops the title as published and four other per-listing facts the web needs)).
`event_id`, `title_tag`, `summary_len`, `content_warning`, `expires_at` and `market_image` are
written for ONE reader: nostrology-web's Recent listings section, which links each row to a
marketplace that can actually render it and drops a row no marketplace can. Each is a
condition some marketplace reads, and each was parsed here already and then discarded; what
they cost is one more value per row in an INSERT that rewrites every row anyway.

Five of them are NOT part of the methodology, and that is the point: no snapshot figure reads
them, no exclusion depends on them, and nothing on the page counts with them - so
`crosscheck/compare_markets_snapshots.py`'s ZERO-diff agreement gate neither sees them nor needs
to, and the reference generator (crosscheck/build_markets_snapshot.py) does not have to mirror
them. The standing rule that methodology lands in BOTH implementations is untouched, because
this is not methodology. `event_id` was the sixth until 2026-11-04: it now also breaks a tie in
fold_product_options, as NIP-01 breaks one between versions, so a same-second family moves a
figure by it and the generator mirrors it. Read in nostrology-web's $lib/markets/links.ts (what each condition
is). `market_image` is the one with logic and it lives in markets_medialib.py,
which says why one marketplace's own rule is computed here rather than there.
"""
import collections
import datetime as dt
import gzip
import json
import math
import re
import typing
import unicodedata

from . import markets_geolib as G
from . import markets_medialib as M

# --- exclusion 1: non-retail bulk publishers, by pubkey prefix -------------
# Prefixes are 12 hex chars and verified unique across the archive.
BULK = {
    "7d33ba57d8a6": "stock-photo catalogue, one $0.99 listing per photo",
    "1f92b7f28bff": "mempool telemetry, one listing per 5-minute bucket",
    "b8e363fd8fbc": "directory of bitcoin-accepting businesses, not its own goods",
}
# --- exclusion 2: carrier / staging clients, by `client` tag ---------------
# Keys are lowercased; the lookup lowercases too, so a `Ulendo` cannot slip
# past. This rule is NAME-based and will silently stop working if a client
# renames itself -- re-check it whenever the client mix shifts.
CARRIER = {
    "ulendo": "ride-hailing profiles carried on"
              " kind-30402 (markets-product-listings), 45 of 52 untitled",
    "account.staging.synvya.com": "restaurant menus published from a staging host",
}
# `demo` and `sample` are deliberately NOT in TESTRE: "2010 Specialized Demo 8" is a
# mountain bike and "sample pack" is a product.
TESTRE = r"(^|[^a-z])(test|testing|prueba|asdf|foobar|dummy)([^a-z]|$)"
FARM_MIN_PUBKEYS = 4
WITHDRAWN = {"sold", "taken", "deleted", "inactive", "completed",
             "cancelled", "hidden", "closed", "reserved"}
CAT_MIN_SELLERS = 5
# nostrology-web's src/lib/markets/currency.ts holds this set and this fold too, to draw a bitcoin
# price orange on Recent listings, and its page lists these units in a sentence: change them
# together, no test reaches across the repos.
BTC_UNITS = {"btc", "sats", "sat", "msat", "msats", "satoshi", "satoshis",
             "bitcoin", "xbt", "sat(s)"}
STABLES = {"usdt", "usdc", "dai", "usdd", "busd"}
SPECIAL = ("NONGEO", "REGION", "AMBIG")

# Anti-shrink floor for the serving write - ~80% of the 2026-08-22 pool (5,008), the same
# posture as pipeline/kinds.py MIN_ROWS: NOT an integrity gate, a never-wipe guard. A pool
# below it aborts BEFORE the TRUNCATE, preserving the previous serving data.
MIN_ACTIVE_LISTINGS = 4000

# Input-sanity floors on the kind-5 (event-deletion-requests) extract, checked by the snapshot
# asset BEFORE compute: the anti-shrink gate above is one-directional, and a silently
# under-counting deletions aggregation INFLATES the pool (deleted listings resurface) while
# passing every other gate. ONE PER TAG, because they fail apart: the `e` side rests on a join
# the `a` side does not need, and a join that matches nothing would leave `a` above any shared
# floor and put 434 deleted listings back. Counted in distinct (author, value) pairs, so a burst
# of repeat requests cannot prop a floor up. MIN_DELETION_PAIRS is ~40% of the 2026-08-23 count
# (1,203; 1,889 by 2026-11-04, so about 26% now); MIN_DELETION_EVENT_PAIRS ~40% of the 2026-11-04
# count (1,492).
MIN_DELETION_PAIRS = 500
MIN_DELETION_EVENT_PAIRS = 600

_BIGINT_MIN, _BIGINT_MAX = -(2 ** 63), 2 ** 63 - 1

# DuckDB's trim() strips Unicode SPACE SEPARATORS (category Zs: ' ', NBSP, ideographic
# space, ...) but NOT \t \n \r - narrower than Python's str.strip(). Pinned by probe
# (2026-08-23, duckdb 1.5.x) and by the unit tests; the static generator computes
# `unrenderable` with DuckDB trim(), so we must match it exactly.
_ZS_CHARS = "".join(chr(c) for c in range(0x10000) if unicodedata.category(chr(c)) == "Zs")


def _duck_trim(s: str) -> str:
    return (s or "").strip(_ZS_CHARS)


# DuckDB's cast strips ASCII whitespace only; Python's default strip()/int() also accept
# Unicode spaces and Unicode digits, which DuckDB rejects (probed: NBSP-padded and
# Arabic/fullwidth digits -> NULL). Everything non-ASCII is therefore rejected up front.
_ASCII_WS = " \t\n\r\v\f"


def duck_try_cast_bigint(v):
    """DuckDB try_cast(VARCHAR AS BIGINT), pinned by probe (2026-08-23, duckdb 1.5.x):
    ASCII-whitespace padding ok (tab/newline included; NBSP is NOT); plain signed integers
    (incl. '+7'); UNSIGNED '0x10' hex and '0b101' binary (signed hex/binary -> NULL, octal
    '0o17' -> NULL); non-ASCII digits -> NULL; decimals/scientific go through DOUBLE and
    round HALF AWAY FROM ZERO ('12.5'->13, '-12.5'->-13, '1e3'->1000);
    ''/'inf'/'nan'/garbage/overflow -> None."""
    if v is None:
        return None
    s = str(v).strip(_ASCII_WS)
    if not s or not s.isascii():
        return None
    try:
        if re.fullmatch(r"0[xX][0-9a-fA-F]+", s):
            n = int(s, 16)
        elif re.fullmatch(r"0[bB][01]+", s):
            n = int(s, 2)
        elif re.fullmatch(r"[+-]?[0-9]+", s):
            n = int(s)
        else:
            raise ValueError
        return n if _BIGINT_MIN <= n <= _BIGINT_MAX else None
    except ValueError:
        pass
    try:
        f = float(s)
    except ValueError:
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    n = int(math.floor(f + 0.5)) if f >= 0 else int(math.ceil(f - 0.5))
    return n if _BIGINT_MIN <= n <= _BIGINT_MAX else None


def duck_lower(s: str) -> str:
    """DuckDB lower() does SIMPLE per-codepoint case folding; Python str.lower() does the
    full Unicode algorithm. The two differ exactly where full casing is context-sensitive
    or expanding (probed 2026-08-23): 'TESTING' with a dotted capital I -> DuckDB folds the
    I-with-dot to a bare 'i' (Python emits 'i' + combining dot, length grows); a FINAL
    Greek sigma stays the ordinary lowercase sigma in DuckDB (Python picks the final
    form). Folding per CHARACTER removes Python's context sensitivity, and taking the
    first codepoint of a rare multi-codepoint expansion matches DuckDB's simple mapping.
    The reference generator lowercases titles IN DuckDB SQL, so farm/synthetic membership
    must use THIS fold, not str.lower()."""
    out = []
    for c in s:
        low = c.lower()
        out.append(low if len(low) == 1 else low[:1])
    return "".join(out)


# --- NDJSON input --------------------------------------------------------------------------

def parse_ndjson_gz(paths) -> list[dict]:
    """Parse Athena UNLOAD .gz JSON-lines parts into row dicts (one object per line)."""
    rows: list[dict] = []
    for p in sorted(paths):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


def tag_value(tags, name):
    """First tag named `name`, its second element - the port of DuckDB
    list_filter(tags, x -> x[1]=name)[1][2] (NULL when the tag is absent OR 1-element)."""
    for t in tags or []:
        if t and t[0] == name:
            return t[1] if len(t) >= 2 else None
    return None


def _tag_values_all(tags, name):
    """Every tag named `name` -> its second element (None for a 1-element tag) - the port of
    list_transform(list_filter(tags, x -> x[1]=name), x -> x[2])."""
    return [(t[1] if len(t) >= 2 else None) for t in (tags or []) if t and t[0] == name]


def _first_tag(tags, name):
    for t in tags or []:
        if t and t[0] == name:
            return t
    return None


def parent_coordinate(tags):
    """The first `a` tag naming a kind-30402 (markets-product-listings) coordinate, its value - the
    port of list_filter(tags, x -> x[1]='a' AND starts_with(x[2],'30402:'))[1][2]. A variation can
    carry other `a` tags (a collection), and the spec lets a product name a parent once."""
    for t in tags or []:
        if t and t[0] == "a" and len(t) >= 2 and t[1] and t[1].startswith("30402:"):
            return t[1]
    return None


def coordinate(pubkey: str, d_tag) -> str:
    """A listing's coordinate as a tag names it. coalesce: a NULL d_tag ends at the colon, and a
    row with no `d` tag must still be nameable, as the generator's coalesce(d_tag,'') has it."""
    return "30402:" + pubkey + ":" + (d_tag or "")


def has_price(price) -> bool:
    """Whether a `price` tag says what it is priced in - what the currency figures count."""
    return bool(price and len(price) >= 3 and (price[2] or "").strip())


# --- selection + per-record fields (the port of the generator's SQL layer) -----------------

def current_versions(events) -> list[dict]:
    """Current version per (pubkey, d_tag): newest created_at, lowest id breaking ties, per
    NIP-01 - the port of row_number() OVER (PARTITION BY pubkey, d_tag
    ORDER BY created_at DESC, id ASC). A NULL d_tag is its own coordinate group (SQL window
    partitioning groups NULLs together), distinct from d_tag=''."""
    groups: dict = {}
    for ev in events:
        key = (ev["pubkey"], ev.get("d_tag"))
        cur = groups.get(key)
        if cur is None or (-ev["created_at"], ev["id"]) < (-cur["created_at"], cur["id"]):
            groups[key] = ev
    return list(groups.values())


def farm_titles(events) -> set:
    """Titles published verbatim by >= FARM_MIN_PUBKEYS pubkeys at <= 1 distinct price,
    over ALL versions (client E2E farms). The price condition matters: six people posting
    "Buying BTC for NGN" at six different rates is a real market, not a farm - and
    "at most one" includes ZERO distinct prices (count(DISTINCT) ignores NULLs), so
    price-less farm titles qualify vacuously."""
    pks = collections.defaultdict(set)
    prices = collections.defaultdict(set)
    for ev in events:
        title = duck_lower(tag_value(ev.get("tags"), "title") or "")
        if title == "":
            continue
        pks[title].add(ev["pubkey"])
        pt = _first_tag(ev.get("tags"), "price")
        pv = pt[1] if pt and len(pt) >= 2 else None
        if pv is not None:
            prices[title].add(pv)
    return {t for t, s in pks.items()
            if len(s) >= FARM_MIN_PUBKEYS and len(prices.get(t, ())) <= 1}


class Deletions(typing.NamedTuple):
    """What the kind-5 (event-deletion-requests) extract says, reduced to what a listing asks of
    it. See parse_deletions."""
    coord_newest: dict  # coordinate -> the newest request naming it by `a` tag
    event_ids: frozenset  # ids of the listing versions named by `e` tag

    def covers(self, coord: str, event_id: str, created_at: int) -> bool:
        """Whether a coordinate's CURRENT version is deleted, per NIP-09: an `e` tag names that
        event exactly, and an `a` tag reaches every version up to its own created_at, inclusive.
        Asked only of the current version, after selection, so naming an older version by id
        deletes nothing - the listing has moved on, and a relay would still serve the new one."""
        if event_id in self.event_ids:
            return True
        newest = self.coord_newest.get(coord)
        return newest is not None and newest >= created_at


NO_DELETIONS = Deletions({}, frozenset())


def parse_deletions(deletion_rows, listing_events, horizon: int) -> Deletions:
    """Reduce the extract's rows to a Deletions, asserting what the SQL promised.

    The SQL keeps self-authored requests only; this checks rather than re-filters, so an extract
    bug fails loud instead of silently diverging from the reference generator. An `e` row can
    only be checked against a listing we hold: one naming an id absent here matches nothing, which
    is normal for a version past the horizon or one that landed after the listings extract.

    The horizon applies to each ROW, as it does to every listing event: a request dated past it
    is not part of the dataset. Rows rather than one newest date per pair so that such a request
    cannot mask an earlier valid one."""
    owner = {ev["id"]: ev["pubkey"] for ev in listing_events}
    coord_newest: dict = {}
    event_ids = set()
    for r in deletion_rows:
        tag, value, author, ts = r["tag"], r["value"], r["author"], int(r["created_at"])
        if tag == "a":
            parts = value.split(":")
            if len(parts) < 2 or parts[1] != author:
                raise ValueError(f"deletions extract returned a non-self-authored row: {r!r}")
            if ts <= horizon and ts > coord_newest.get(value, ts - 1):
                coord_newest[value] = ts
        elif tag == "e":
            if value in owner and owner[value] != author:
                raise ValueError(f"deletions extract returned a non-self-authored row: {r!r}")
            if ts <= horizon:
                event_ids.add(value)
        else:
            raise ValueError(f"deletions extract returned a row with an unknown tag: {r!r}")
    return Deletions(coord_newest, frozenset(event_ids))


def check_deletion_floors(deletion_rows) -> None:
    """The MIN_DELETION_* input floors, per tag: raises, naming each one missed, before any
    compute. A row with a tag other than the two counts toward neither: parse_deletions refuses
    it."""
    pairs: dict = {"a": set(), "e": set()}
    for r in deletion_rows:
        if r["tag"] in pairs:
            pairs[r["tag"]].add((r["author"], r["value"]))
    short = [
        f"{len(pairs[tag])} (author, value) pairs for tag '{tag}', below the {name} floor ({floor})"
        for tag, name, floor in (("a", "MIN_DELETION_PAIRS", MIN_DELETION_PAIRS),
                                 ("e", "MIN_DELETION_EVENT_PAIRS", MIN_DELETION_EVENT_PAIRS))
        if len(pairs[tag]) < floor
    ]
    if short:
        raise RuntimeError("deletions extract returned " + "; ".join(short)
                           + " - refusing to compute with a suspiciously shrunken deletion set")


def build_records(listing_events, deletions: Deletions) -> list[dict]:
    """One record per current kind-30402 (markets-product-listings) coordinate, carrying the
    same derived fields as the generator's SQL SELECT."""
    farm = farm_titles(listing_events)
    testre = re.compile(TESTRE)
    recs = []
    for ev in current_versions(listing_events):
        tags = ev.get("tags") or []
        title_raw = tag_value(tags, "title")
        title = duck_lower(title_raw or "")
        content = ev.get("content")
        d_tag = ev.get("d_tag")
        recs.append({
            "pubkey": ev["pubkey"],
            "d_tag": d_tag,
            "created_at": ev["created_at"],
            "published_at": duck_try_cast_bigint(tag_value(tags, "published_at")),
            "title": title,
            "status": duck_lower(tag_value(tags, "status") or ""),
            "vis": duck_lower(tag_value(tags, "visibility") or ""),
            "stock": duck_try_cast_bigint(tag_value(tags, "stock")),
            "client": tag_value(tags, "client") or "",
            "unrenderable": (
                _duck_trim(title) == ""
                and _duck_trim(content or "") == ""
                and _duck_trim(tag_value(tags, "summary") or "") == ""
                and _first_tag(tags, "image") is None
            ),
            "loc": tag_value(tags, "location"),
            "geohashes": _tag_values_all(tags, "g"),
            "cats": _tag_values_all(tags, "t"),
            "price": _first_tag(tags, "price"),
            "synthetic": bool(testre.search(title)) or title in farm,
            # coalesce: rows with a NULL d_tag must still form a coord that can match the
            # deletion set ('30402:<pk>:'), mirroring the generator's coalesce(d_tag,'').
            "nip09_deleted": deletions.covers(
                coordinate(ev["pubkey"], d_tag), ev["id"], ev["created_at"]),
            # Exact, as the spec writes it: all 861 variations in the archive on 2026-11-04 are
            # lowercase, one `type` tag each. Plebeian compares it raw too.
            "product_type": tag_value(tags, "type"),
            "parent": parent_coordinate(tags),
            # --- the per-listing facts nostrology-web's Recent listings reads (none of them
            # feeds a snapshot figure, except event_id through fold_product_options's tie-break;
            # see THE SIX COLUMNS in the module docstring) ---------
            "event_id": ev["id"],
            # The title AS PUBLISHED, read the way CONDUIT reads it - the first `title` tag
            # that has a value, skipping one that does not, which this file's own tag_value
            # does not do (markets_medialib.tag_value_as_conduit_reads_it says why it cannot
            # change). `title` above is lowercased because the farm and synthetic classifiers
            # compare it that way, and the page was drawing that fold ("arcanine for abra").
            # NULL here means the listing carries no title tag at all, which is a different
            # case from the empty one Conduit's schema throws on. One corner: write_pg's
            # strip_nul runs AFTER this, so a 201-character title containing a NUL is stored as
            # 200 and passes the web's bound. The archive holds no NULs at all, and this is the
            # only length-bearing text column, so it is noted rather than modelled.
            "title_tag": M.tag_value_as_conduit_reads_it(tags, "title"),
            "summary_len": M.summary_tag_len(tags),
            # RAW - not lowercased, not trimmed. Its only reader compares raw: Plebeian's
            # isNSFWProduct is `getProductContentWarning(event)?.[1] === 'nsfw'`, so `NSFW`
            # is NOT gated there and a folded column would have the web decline Plebeian for
            # a listing it renders in full. The one unavoidable exception is write_pg's
            # strip_nul, which Postgres forces (TEXT rejects the NUL codepoint): a value of
            # "n\0sfw" is stored as "nsfw" and so reads as gated, where Plebeian's raw
            # comparison would not match it. That costs that marketplace one row's traffic,
            # never a dead link, and no live listing has ever carried a NUL.
            "content_warning": tag_value(tags, "content-warning"),
            # The NIP-40 `expiration` tag, which relays act on by deleting the event - so an
            # expired listing can be opened nowhere. Parsed with the permissive DuckDB cast
            # on purpose: reading a weird form as a timestamp can only drop a row from the
            # table, while refusing to read it leaves a dead link in it.
            "expires_at": duck_try_cast_bigint(tag_value(tags, "expiration")),
            "market_image": M.market_image(content, tags),
        })
    return recs


def classify(recs) -> None:
    """Attach bulk/carrier/withdrawn + the single precedence-ordered exclusion bucket."""
    for r in recs:
        r["bulk"] = next((v for k, v in BULK.items() if r["pubkey"].startswith(k)), None)
        r["carrier"] = CARRIER.get((r["client"] or "").strip().lower())
        r["withdrawn"] = (r["status"] in WITHDRAWN or r["vis"] in ("hidden", "<hidden")
                          or (r["stock"] is not None and int(r["stock"]) <= 0))
        r["excl"] = ("bulk" if r["bulk"] else
                     "carrier" if r["carrier"] else
                     "unrenderable" if r["unrenderable"] else
                     "synthetic" if r["synthetic"] else
                     "deleted" if r["nip09_deleted"] else
                     "withdrawn" if r["withdrawn"] else None)


def fold_product_options(recs) -> None:
    """One row per product, over the rows classify left live.

    The GammaMarkets spec lets a seller publish a product as a parent (`type` variable) and one
    child per option (`type` variation, with an `a` tag naming the parent), each its own event and
    coordinate. A T-shirt with 33 sizes and colours is then 34 coordinates and one product, and on
    the seller's own site it is one page with pickers. Counted as they stand, one shop's re-publish
    moved a month by hundreds.

    A FAMILY is one key's children naming one parent, and it keeps ONE row: the parent if it can
    stand for the family, else the family's newest child (created_at DESC, id ASC, as NIP-01 picks a
    current version). The other children go to the `option` group. It is last in the precedence:
    a child excluded for another reason stays where it was and is no option of anything.

    A parent stands for a family only if it is live, the SAME KEY's, and not itself a child:
      - The key scope is the defence against a stranger's tag: a family is per key, so naming
        another seller's listing as a parent folds nothing of theirs, and only the namer's own
        rows, into one row of their own. No seller can leave the pool by this rule.
      - "Not itself a child" is what keeps a row for every family. Otherwise a child naming itself,
        or two naming each other, would each fold into the other and the product would vanish.

    A parent that is not live is common for one reason: key e9c9ccd1bc06 gives its parents
    `stock: 0` and each child `stock: 100`, so the parents read as Withdrawn while the product is on
    sale. Hence a family without a standing parent still counts, once.

    The row kept takes a price only if it has none of its own: 6 parents carry none, and without
    this the product would drop out of the currency figures. It takes the newest priced child's,
    which is also what Recent listings then draws. No other field needs it: no live parent lacks
    categories or a location that a child has (2026-11-04).
    """
    def newest_first(r):
        return (-r["created_at"], r["event_id"])

    # A NULL d_tag and an empty one share a coordinate string (see coordinate), so the choice
    # between two such rows has to be stated or it would follow input order.
    standing: dict = {}
    for r in recs:
        if r["excl"] is None and r["product_type"] != "variation":
            c = coordinate(r["pubkey"], r["d_tag"])
            if c not in standing or newest_first(r) < newest_first(standing[c]):
                standing[c] = r
    families = collections.defaultdict(list)
    for r in recs:
        if r["excl"] is None and r["product_type"] == "variation" and r["parent"]:
            families[r["pubkey"], r["parent"]].append(r)
    for (pubkey, parent), kids in families.items():
        own = parent.startswith(coordinate(pubkey, ""))
        kept = (standing.get(parent) if own else None) or min(kids, key=newest_first)
        for k in kids:
            if k is not kept:
                k["excl"] = "option"
        if not has_price(kept["price"]):
            priced = [k for k in kids if has_price(k["price"])]
            if priced:
                kept["price"] = min(priced, key=newest_first)["price"]


def resolve_geo(recs, pool) -> dict:
    """Geohash decode + free-text country resolution + the seller-context modal pass.
    Returns the per-seller modal country map. Identical to the generator."""
    import reverse_geocoder as rg
    for r in recs:
        best, blen = None, 0
        for gh in (r["geohashes"] or []):
            pt = G.geohash_decode(gh)
            if pt and len(gh.strip()) >= 2 and len(gh.strip()) > blen:
                best, blen = pt, len(gh.strip())
        r["gh_xy"], r["gh_len"] = best, blen
        # An implausible decode is KEPT in gh_xy/gh_len - the stored gh_lat/gh_lon/gh_len are
        # the record of why the listing ends up unplaced - but it names no country and draws
        # no bubble, and the listing falls through to the text tag as if it carried no `g`.
        # cc_src reads geohash-implausible when nothing else resolves; the general signal for a
        # refused decode is a gh_lat with a cc that did not come from it (the seller-context
        # pass below can overwrite cc_src, so the marker alone is not a census).
        r["gh_implausible"] = G.implausible_geohash(best)
        if r["gh_implausible"]:
            cc, src = G.text_to_cc(r["loc"])
            r["cc"], r["cc_src"] = cc, (src or "geohash-implausible")
        else:
            r["cc"], r["cc_src"] = (None, "geohash") if best else G.text_to_cc(r["loc"])
    gh = [r for r in recs if r["gh_xy"] and not r["gh_implausible"]]
    if gh:
        for r, h in zip(gh, rg.search([r["gh_xy"] for r in gh], mode=1)):
            r["cc"] = h["cc"]

    # A seller ships from one place, so let their unambiguous listings resolve
    # their ambiguous ones. Ties break on the country name, never on dict order.
    acc = collections.defaultdict(collections.Counter)
    for r in pool:
        if r["cc"] and r["cc"] not in SPECIAL:
            acc[r["pubkey"]][r["cc"]] += 1
    modal = {pk: sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))[0][0] for pk, c in acc.items()}
    for r in recs:
        if r["cc"] and r["cc"] not in SPECIAL:
            continue
        m = modal.get(r["pubkey"])
        if not m:
            continue
        if r["cc"] == "AMBIG" and m in G.candidates_for(r["loc"]):
            r["cc"], r["cc_src"] = m, "seller-context"
        elif r["cc"] is None and (r["loc"] or "").strip():
            r["cc"], r["cc_src"] = m, "seller-context"
    return modal


# --- the snapshot --------------------------------------------------------------------------

def build_snapshot(listing_events, shipping_events, deletion_rows, now_ts: int):
    """The full metrics compute. Pure: rows in, (recs, data) out - no AWS, no DB, no clock
    (now_ts is the caller's). `data` is exactly the page's snapshot JSON."""
    NOW = int(now_ts)
    # Events dated beyond NOW + 2 days are not part of the dataset AT ALL (dropped before
    # current-version selection, farm detection and archive_events - the reference
    # generator applies the identical bound in its raw views). Without this, one
    # garbage/hostile created_at (a) permanently wins its coordinate's current-version
    # selection, and (b) at year-10000 scale crashes every date formatter, wedging the
    # refresh forever. The 2-day allowance covers clock skew; the archive held ZERO such
    # events when the bound landed (probed 2026-08-23), so it changed nothing on real data.
    horizon = NOW + 2 * 86400
    listing_events = [e for e in listing_events if e["created_at"] <= horizon]
    shipping_events = [e for e in shipping_events if e["created_at"] <= horizon]

    recs = build_records(listing_events, parse_deletions(deletion_rows, listing_events, horizon))
    classify(recs)
    fold_product_options(recs)
    pool = [r for r in recs if r["excl"] is None]
    if not pool:
        raise ValueError("empty listing pool - refusing to build a snapshot from nothing")
    archive_events = len(listing_events)

    modal = resolve_geo(recs, pool)

    # A listing's date: published_at when plausible, else its current version's created_at -
    # and never after NOW. The horizon admits created_at up to two days past the run, so
    # uncapped, a listing dated ahead could draw a bar for a month not yet begun and run both
    # cumulative series (and last_listing) past the run's day. Capped, it counts on the run's
    # day and in its month, the buckets still filling, and takes its own date on the first run
    # after it. Of the 20,607 listing events received 2026-08-20..10-08, 24 were dated ahead of
    # receipt, by 33 s at most; none of the 49 snapshots so far dates anything past its run.
    def when(r):
        p = r["published_at"]
        return min(p if (p and 1500000000 <= p <= NOW) else r["created_at"], NOW)

    def day(ts):
        return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d")

    def mon(ts):
        return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m")

    def cumulative(times):
        per = collections.Counter(day(t) for t in times)
        days = sorted(per)
        d, end, run, out = dt.date.fromisoformat(days[0]), dt.date.fromisoformat(days[-1]), 0, []
        while d <= end:
            run += per.get(d.isoformat(), 0)
            out.append({"d": d.isoformat(), "v": run})
            d += dt.timedelta(days=1)
        return out

    for r in recs:
        r["when_ts"] = when(r)

    first_seen = {}
    for r in pool:
        t = when(r)
        if r["pubkey"] not in first_seen or t < first_seen[r["pubkey"]]:
            first_seen[r["pubkey"]] = t

    per_m = collections.Counter(mon(when(r)) for r in pool)
    sel_m = collections.Counter(mon(t) for t in first_seen.values())
    ms = sorted(per_m)
    m, end = dt.date.fromisoformat(ms[0] + "-01"), dt.date.fromisoformat(ms[-1] + "-01")
    new_monthly = []
    while m <= end:
        k = m.strftime("%Y-%m")
        new_monthly.append({"m": k, "v": per_m.get(k, 0), "s": sel_m.get(k, 0)})
        m = (m.replace(day=28) + dt.timedelta(days=4)).replace(day=1)

    # ---- categories
    STOP = {"", "shopstr", "nostr", "product", "products", "listing", "listings",
            "item", "items", "forsale", "for-sale", "sale", "buy", "sell", "store", "shop"}
    cl, cs = collections.Counter(), collections.defaultdict(set)
    for r in pool:
        for c in {G.norm(x) for x in (r["cats"] or [])}:
            if c in STOP or not 2 <= len(c) <= 28:
                continue
            cl[c] += 1
            cs[c].add(r["pubkey"])
    categories = sorted(
        ({"cat": k, "listings": v, "sellers": len(cs[k])}
         for k, v in cl.items() if len(cs[k]) >= CAT_MIN_SELLERS),
        key=lambda x: (-x["listings"], x["cat"]))[:20]

    # ---- currency
    ul, us_ = collections.Counter(), collections.defaultdict(set)
    priced = 0
    for r in pool:
        p = r["price"]
        if not has_price(p):
            continue
        priced += 1
        u = G.norm(p[2])
        grp = ("Bitcoin (BTC / sats)" if u in BTC_UNITS else
               "Stablecoin (USDT / USDC)" if u in STABLES else
               "USD" if u in ("usd", "us$", "$", "dollar", "dollars") else
               "EUR" if u in ("eur", "euro", "euros", "€") else
               "GBP" if u in ("gbp", "£") else
               u.upper() if len(u) == 3 and u.isalpha() else "Other")
        ul[grp] += 1
        us_[grp].add(r["pubkey"])
    head = [k for k, _ in sorted(ul.items(), key=lambda kv: (-kv[1], kv[0]))[:7]]
    tail = [k for k in ul if k not in head]
    currency = [{"cur": k, "listings": ul[k], "sellers": len(us_[k])} for k in head]
    if tail:
        currency.append({"cur": "Other", "listings": sum(ul[k] for k in tail),
                         "sellers": len(set().union(*(us_[k] for k in tail)))})
    currency.sort(key=lambda x: (-x["listings"], x["cur"]))

    # ---- geography
    gl, gs = collections.Counter(), collections.defaultdict(set)
    for r in pool:
        b = ("NOTAG" if r["cc"] is None and not (r["loc"] or "").strip()
             else "UNRES" if r["cc"] is None else r["cc"])
        gl[b] += 1
        gs[b].add(r["pubkey"])
    countries = sorted([{"cc": k, "name": G.cc_name(k), "listings": v, "sellers": len(gs[k])}
                        for k, v in gl.items() if k not in ("NOTAG", "UNRES") + SPECIAL],
                       key=lambda x: (-x["listings"], x["cc"]))
    nonattr = {k: {"listings": gl.get(k, 0), "sellers": len(gs.get(k, set()))}
               for k in ("NOTAG", "NONGEO", "UNRES", "REGION", "AMBIG")}

    # A bubble is "precise" only when it sits on a named town or a geohash of
    # >= 5 chars (~2.4 km). A 3-4 char geohash is a 39-156 km cell -- that is a
    # country centroid wearing a disguise, so it is not badged as exact.
    pts = collections.defaultdict(lambda: {"listings": 0, "sellers": set()})
    for r in pool:
        cc = r["cc"]
        if not cc or cc in SPECIAL:
            continue
        xy = None
        if r["gh_xy"] and r["gh_len"] >= 5 and not r["gh_implausible"]:
            xy = (round(r["gh_xy"][0], 2), round(r["gh_xy"][1], 2), "geohash")
        else:
            hit = G.text_to_xy(r["loc"] or "")
            if hit and hit[2] == cc:
                xy = (round(hit[0], 2), round(hit[1], 2), "city")
        if xy is None:
            c = G.CENTROID.get(cc)
            if not c:
                continue
            xy = (round(c[0], 2), round(c[1], 2), "country")
        k = (xy[0], xy[1], cc, xy[2])
        pts[k]["listings"] += 1
        pts[k]["sellers"].add(r["pubkey"])
    # prec is in the key: without it, two points equal on the other four fields tie, and
    # a stable sort then exposes INPUT ROW ORDER (Athena part order vs DuckDB scan order)
    # as a spurious cross-method ordering diff.
    map_points = sorted([{"lat": k[0], "lon": k[1], "cc": k[2], "name": G.cc_name(k[2]),
                          "prec": k[3], "listings": v["listings"], "sellers": len(v["sellers"])}
                         for k, v in pts.items()],
                        key=lambda p: (-p["listings"], p["cc"], p["lat"], p["lon"], p["prec"]))

    pool_pks = {r["pubkey"] for r in pool}
    seller_cc = {pk: cc for pk, cc in modal.items() if pk in pool_pks}
    sd = collections.Counter(seller_cc.values())
    seller_countries = sorted([{"cc": k, "name": G.cc_name(k), "sellers": v} for k, v in sd.items()],
                              key=lambda x: (-x["sellers"], x["cc"]))

    # ---- shipping, kind-30406 (markets-shipping-options)
    sh_cur = current_versions(shipping_events)
    sh_total = len(sh_cur)
    # `country` is ONE tag holding MANY values -- an EU shipping option carries all 27 member
    # codes in a single tag. Keep every element from the second on (the x[2:] port); taking
    # only the first silently discards ~1,100 destinations.
    sh_rows = []
    for ev in sh_cur:
        tags = ev.get("tags") or []
        vals = []
        for t in tags:
            if t and t[0] == "country":
                vals.extend(t[1:])
        for t in tags:
            if t and t[0] == "pickup-country":
                vals.extend(t[1:])
        sh_rows.append((ev["pubkey"], vals))
    excluded_pks = {r["pubkey"] for r in recs if r["excl"] in ("bulk", "carrier", "synthetic")} - pool_pks
    ship_o, ship_s, with_country = collections.Counter(), collections.defaultdict(set), 0
    for pk, vals in sh_rows:
        if pk in excluded_pks:
            continue
        got = {cc for cc in (G.any_country_to_cc(v) for v in (vals or [])) if cc}
        if not got:
            continue
        with_country += 1
        for cc in got:
            ship_o[cc] += 1
            ship_s[cc].add(pk)
    shipping = sorted([{"cc": k, "name": G.cc_name(k), "options": v, "sellers": len(ship_s[k]),
                        "lat": round(G.CENTROID[k][0], 2), "lon": round(G.CENTROID[k][1], 2)}
                       for k, v in ship_o.items() if k in G.CENTROID],
                      key=lambda x: (-x["sellers"], -x["options"], x["cc"]))

    cl2, cs2 = collections.Counter(), collections.defaultdict(set)
    for r in pool:
        c = (r["client"] or "").strip() or "(untagged)"
        cl2[c] += 1
        cs2[c].add(r["pubkey"])

    data = {
        "generated_utc": dt.datetime.fromtimestamp(NOW, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"),
        "summary": {
            "archive_events": archive_events,
            "current_coordinates": len(recs),
            "active_listings": len(pool),
            "sellers": len(pool_pks),
            # Anchored to NOW, like the 90-day windows. Reading it off the
            # cumulative series instead would anchor it to the last date IN
            # THE DATA, so a stale snapshot would silently shift the window
            # while the label still said "the last 30 days".
            "added_30d": sum(1 for r in pool if when(r) >= NOW - 30 * 86400),
            "active_90d": sum(1 for r in pool if when(r) >= NOW - 90 * 86400),
            "sellers_90d": len({r["pubkey"] for r in pool if when(r) >= NOW - 90 * 86400}),
            "countries": len(countries),
            "first_listing": day(min(when(r) for r in pool)),
            "last_listing": day(max(when(r) for r in pool)),
            # Unconditioned counts across ALL current coordinates -- the
            # exclusion table below is precedence-ordered and therefore
            # UNDERSTATES each group after the first.
            "withdrawn_any": sum(1 for r in recs if r["withdrawn"]),
            "deleted_any": sum(1 for r in recs if r["nip09_deleted"]),
        },
        "exclusions": [
            {"k": "Non-retail bulk publishers", "v": sum(1 for r in recs if r["excl"] == "bulk"),
             "why": "three single accounts: a stock-photo catalogue, a mempool telemetry feed, "
                    "and a directory of bitcoin-accepting businesses"},
            {"k": "Carrier / staging clients", "v": sum(1 for r in recs if r["excl"] == "carrier"),
             "why": "apps using kind-30402 (markets-product-listings) as transport"
                    " for something else, and one staging host"},
            {"k": "Unrenderable", "v": sum(1 for r in recs if r["excl"] == "unrenderable"),
             "why": "no title, no body text, no summary and no image -- nothing a buyer could see"},
            {"k": "Synthetic / test listings", "v": sum(1 for r in recs if r["excl"] == "synthetic"),
             "why": "test-worded titles, and identical titles at one price from 4+ pubkeys (E2E farms)"},
            {"k": "Deleted (NIP-09)", "v": sum(1 for r in recs if r["excl"] == "deleted"),
             "why": "the listing's own author published a kind-5 (event-deletion-requests)"
                    " event covering its newest version"},
            {"k": "Withdrawn", "v": sum(1 for r in recs if r["excl"] == "withdrawn"),
             "why": "status sold/taken/inactive, visibility hidden, or stock 0"},
            {"k": "Product options", "v": sum(1 for r in recs if r["excl"] == "option"),
             "why": "a size, colour or other option a seller published as its own listing,"
                    " so each product counts once"},
        ],
        "cum_sellers": cumulative(list(first_seen.values())),
        "cum_listings": cumulative([when(r) for r in pool]),
        "new_monthly": new_monthly,
        # The month the run falls in: the bucket still filling. Not new_monthly[-1], the newest
        # LISTING's month - on the 1st, before any listing is dated in the new month, that is the
        # month just completed, which the page then drew as partial. With nothing dated in it
        # yet, the run's month has no new_monthly row, and the page marks nothing. Otherwise it
        # is the last row: when() dates nothing after NOW.
        "partial_month": mon(NOW),
        "categories": categories,
        "categories_coverage": {"with_tag": sum(1 for r in pool if r["cats"]),
                                "total": len(pool), "distinct_tags": len(cl),
                                "min_sellers": CAT_MIN_SELLERS},
        "currency": currency,
        "currency_coverage": {"priced": priced, "total": len(pool)},
        "countries": countries,
        "seller_countries": seller_countries,
        "seller_geo_coverage": {"placed": len(seller_cc), "total": len(pool_pks)},
        "geo_nonattr": nonattr,
        "geo_attributable": sum(c["listings"] for c in countries),
        "map_points": map_points,
        "shipping": shipping,
        "shipping_coverage": {"options_total": sh_total, "with_country": with_country,
                              "sellers": len(set().union(*ship_s.values())) if ship_s else 0},
        "clients": [{"client": k, "listings": v, "sellers": len(cs2[k])}
                    for k, v in sorted(cl2.items(), key=lambda kv: (-kv[1], kv[0]))[:10]],
    }
    return recs, data


# --- integrity ------------------------------------------------------------------------------

def check_invariants(recs, data) -> list[str]:
    """Cross-checks between the snapshot's own numbers. Every failure is a wrong-metric bug -
    the caller turns a non-empty list into a blocking asset-check failure."""
    problems: list[str] = []
    s = data["summary"]

    excl_sum = sum(e["v"] for e in data["exclusions"])
    if excl_sum + s["active_listings"] != s["current_coordinates"]:
        problems.append(
            f"exclusions ({excl_sum}) + active ({s['active_listings']}) != "
            f"current coordinates ({s['current_coordinates']})")
    if len(recs) != s["current_coordinates"]:
        problems.append(f"len(recs) {len(recs)} != current_coordinates {s['current_coordinates']}")

    # One row per product, asked of the result rather than of the code that produced it: at most
    # one live row per family, and none at all beside a parent that stands for it.
    live_products = {coordinate(r["pubkey"], r["d_tag"]) for r in recs
                     if r["excl"] is None and r["product_type"] != "variation"}
    families = collections.Counter(
        (r["pubkey"], r["parent"]) for r in recs
        if r["excl"] is None and r["product_type"] == "variation" and r["parent"])
    for (pubkey, parent), n in families.items():
        if n > 1:
            problems.append(f"{n} live rows for one product: {parent} under {pubkey}")
        elif parent in live_products and parent.startswith(coordinate(pubkey, "")):
            problems.append(f"a live child beside its live parent: {parent}")

    for name in ("cum_sellers", "cum_listings"):
        series = data[name]
        if any(b["v"] < a["v"] for a, b in zip(series, series[1:])):
            problems.append(f"{name} is not monotone")
    if data["cum_listings"] and data["cum_listings"][-1]["v"] != s["active_listings"]:
        problems.append("cum_listings does not end at active_listings")
    if data["cum_sellers"] and data["cum_sellers"][-1]["v"] != s["sellers"]:
        problems.append("cum_sellers does not end at sellers")
    if sum(mrow["v"] for mrow in data["new_monthly"]) != s["active_listings"]:
        problems.append("new_monthly listing counts do not sum to active_listings")
    if sum(mrow["s"] for mrow in data["new_monthly"]) != s["sellers"]:
        problems.append("new_monthly first-time-seller counts do not sum to sellers")

    if sum(c["listings"] for c in data["currency"]) != data["currency_coverage"]["priced"]:
        problems.append("currency listings do not sum to priced")
    if data["currency_coverage"]["priced"] > data["currency_coverage"]["total"]:
        problems.append("priced exceeds the pool")

    nonattr_sum = sum(v["listings"] for v in data["geo_nonattr"].values())
    if data["geo_attributable"] + nonattr_sum != s["active_listings"]:
        problems.append(
            f"geo attributable ({data['geo_attributable']}) + non-attributable ({nonattr_sum}) "
            f"!= active ({s['active_listings']})")
    if sum(p["listings"] for p in data["map_points"]) != data["geo_attributable"]:
        problems.append("map_points listings do not sum to geo_attributable")
    if len(data["countries"]) != s["countries"]:
        problems.append("countries list length != summary countries")

    for coll, latk, lonk in (("map_points", "lat", "lon"), ("shipping", "lat", "lon")):
        for p in data[coll]:
            if not (-90 <= p[latk] <= 90 and -180 <= p[lonk] <= 180):
                problems.append(f"{coll} carries an out-of-range point: {p}")
                break
    # The range check above can never fire on a geohash (a decode is in range by construction),
    # so the implausibility rule needs its own invariant or only the unit tests enforce it.
    for p in data["map_points"]:
        if p["prec"] == "geohash" and p["lat"] < G.ANTARCTIC_LAT:
            problems.append(f"map_points carries an implausible geohash placement: {p}")
            break
    for c in data["categories"]:
        if c["sellers"] < CAT_MIN_SELLERS:
            problems.append(f"category below the {CAT_MIN_SELLERS}-seller floor: {c}")
            break
    if data["seller_geo_coverage"]["placed"] > data["seller_geo_coverage"]["total"]:
        problems.append("placed sellers exceed total sellers")
    if s["first_listing"] > s["last_listing"]:
        problems.append("first_listing after last_listing")

    # Nothing is dated after the run. build_snapshot caps every listing's date at NOW, so this
    # holds on any data: a failure is that cap regressing, and the page would draw a month (or
    # days) not yet begun. Checked against generated_utc, not partial_month, which is checked
    # here too, so the two cannot regress together.
    run_day = data["generated_utc"][:10]
    if data["partial_month"] != run_day[:7]:
        problems.append(f"partial_month {data['partial_month']} is not the run's month {run_day[:7]}")
    if data["new_monthly"] and data["new_monthly"][-1]["m"] > run_day[:7]:
        problems.append(f"new_monthly runs past the run's month, to {data['new_monthly'][-1]['m']}")
    for name in ("cum_sellers", "cum_listings"):
        if data[name] and data[name][-1]["d"] > run_day:
            problems.append(f"{name} runs past the run's day, to {data[name][-1]['d']}")
    if s["last_listing"] > run_day:
        problems.append(f"last_listing {s['last_listing']} is after the run's day {run_day}")
    return problems


# --- persistence ----------------------------------------------------------------------------

def strip_nul(v):
    """Strip NUL bytes from strings (recursing into containers) - Postgres TEXT/JSONB
    reject the NUL codepoint, and one NUL-bearing tag would otherwise fail the INSERT and
    freeze the refresh. Persistence-side ONLY (write_pg): the snapshot dict handed to the
    agreement comparator is untouched, so if a NUL-bearing event ever lands, the
    comparator surfaces it as a diff and the methodology gets a deliberate both-sides
    rule then (the archive held zero NULs when this guard landed, probed 2026-08-23)."""
    if isinstance(v, str):
        return v.replace(chr(0), "")
    if isinstance(v, list):
        return [strip_nul(x) for x in v]
    if isinstance(v, dict):
        return {strip_nul(k): strip_nul(x) for k, x in v.items()}
    return v


def write_pg(conn, recs, data, run_id: str, *, min_active: int = MIN_ACTIVE_LISTINGS) -> dict:
    """Persist one refresh in ONE transaction: TRUNCATE+INSERT marketplaces_listings (the
    per-coordinate classified table - the queryable asset behind the snapshot) and append the
    marketplaces_snapshot row the page serves. The anti-shrink gate runs BEFORE the TRUNCATE,
    so a shrunken/broken compute leaves the previous serving data untouched (the same
    never-wipe posture as the loader's MIN_ROWS)."""
    active = sum(1 for r in recs if r["excl"] is None)
    if active < min_active:
        raise RuntimeError(
            f"anti-shrink: active listings {active} < floor {min_active} - refusing to "
            "TRUNCATE the serving tables (LOWER the MIN_ACTIVE_LISTINGS floor deliberately "
            "if the pool has legitimately shrunk)")

    # Lazy, and AFTER the gate: the repo's pg-touching code imports psycopg2 only when it
    # actually writes (pipeline/pg.py's posture), so the gate is testable without it.
    from psycopg2.extras import Json, execute_values

    rows = [(
        r["pubkey"], strip_nul(r["d_tag"]), r["created_at"], r["published_at"], r["when_ts"],
        strip_nul(r["title"]), strip_nul(r["status"]), strip_nul(r["vis"]), r["stock"],
        strip_nul(r["client"]), strip_nul(r["loc"]),
        Json(strip_nul(r["geohashes"])), Json(strip_nul(r["cats"])), Json(strip_nul(r["price"])),
        r["unrenderable"], r["synthetic"], r["nip09_deleted"], r["withdrawn"], r["excl"],
        r["cc"], r["cc_src"],
        (r["gh_xy"][0] if r["gh_xy"] else None), (r["gh_xy"][1] if r["gh_xy"] else None),
        r["gh_len"] or None,
        r["event_id"], strip_nul(r["title_tag"]), r["summary_len"],
        strip_nul(r["content_warning"]), r["expires_at"], r["market_image"],
    ) for r in recs]

    generated = dt.datetime.strptime(data["generated_utc"], "%Y-%m-%d %H:%M:%SZ").replace(
        tzinfo=dt.timezone.utc)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE marketplaces_listings")
        execute_values(cur, """
            INSERT INTO marketplaces_listings (
              pubkey, d_tag, created_at, published_at, effective_ts,
              title, status, visibility, stock, client, location,
              geohashes, categories, price,
              unrenderable, synthetic, nip09_deleted, withdrawn, excl,
              cc, cc_src, gh_lat, gh_lon, gh_len,
              event_id, title_tag, summary_len, content_warning, expires_at, market_image
            ) VALUES %s""", rows, page_size=1000)
        cur.execute(
            "INSERT INTO marketplaces_snapshot (generated_utc, run_id, data) VALUES (%s, %s, %s)",
            (generated, run_id, Json(strip_nul(data))))
    conn.commit()
    return {"listings_rows": len(rows), "active_listings": active}

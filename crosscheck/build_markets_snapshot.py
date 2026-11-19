#!/usr/bin/env python3
"""
Build the /marketplaces dataset from the Nozzl S3 archive - the INDEPENDENT
re-derivation of the metrics that nostrology-data's marketplaces_refresh
Dagster job computes in production (dagster_code/pipeline/marketplaces.py,
daily; the page serves the newest marketplaces_snapshot row). This script is
no longer how the page gets its data: since 2026-08-23 its job is the
agreement gate - re-derive the snapshot here, over the raw archive, and diff
it against the production row with crosscheck/compare_markets_snapshots.py (ZERO
diffs required; full procedure in that script's header). A methodology change
lands in BOTH implementations or the gate fails. The exception is
dagster_code/pipeline/markets_geolib.py, which both import: a change to the geo resolution or to
norm(), which also folds categories and price units, reaches both by itself, so this gate cannot
catch a mistake there. That module's own tests hold its rules.

    (the venv: crosscheck/requirements.in, on a Python 3.14 with a maintained certificate store; its pins follow production's.
    AWS_PROFILE names a profile that can read the archive, NOZZL_ARCHIVE_BUCKET the archive's bucket.)
    eval "$(aws configure export-credentials --profile "$AWS_PROFILE" --format env)"
    .venv-crosscheck/bin/python -I crosscheck/build_markets_snapshot.py \
        --deletions <dir>/deleted_30402_selfauthored.csv \
        --as-of <the production row's generated_utc, as YYYY-MM-DDTHH:MM:SS> \
        --out static.json
    .venv-crosscheck/bin/python -I crosscheck/build_markets_snapshot.py --self-test   # no AWS

SOURCES
    kind-30402 (markets-product-listings)  s3://$NOZZL_ARCHIVE_BUCKET/c=addressable/k=30402/
    kind-30406 (markets-shipping-options)  s3://$NOZZL_ARCHIVE_BUCKET/c=addressable/k=30406/
    kind-5 (event-deletion-requests)       scanned separately -- see below

PRODUCING --deletions
    kind-5 (event-deletion-requests) is ~12 GB, so the delete scan is a separate step (~50-65 min)
    rather than part of this script. Keep the machine awake for it: a request signed before a sleep
    and sent after one fails the whole scan with 403 (forbidden) RequestTimeTooSkewed. It must emit
    (author, tag, value, created_at) rows, and the caller must keep only the SELF-AUTHORED ones:
    NIP-09 says a delete request covers only its own author's events. Two publishers here have
    issued cross-author requests
    -- 129 from one and 1 from another -- naming 95 coordinates across 45 OTHER pubkeys.
    Honouring them would delete 32 currently-live listings that their owners never withdrew.
    Re-derive these figures when you re-scan; they drift.

    A request names a listing two ways, and clients use both: an `a` tag names the coordinate,
    which carries its author, and an `e` tag names one version's id, which does not, so only a
    join to the listing shows who wrote it. All dates are kept, not the newest per pair, because
    the horizon below applies to each row.

        duckdb -c "
        SET lambda_syntax='ENABLE_SINGLE_ARROW';
        INSTALL httpfs; LOAD httpfs;
        SET ca_cert_file = '<the interpreter''s default store>';  -- httpfs reads the OS's static bundle otherwise
        CREATE SECRET s (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');
        CREATE TABLE versions AS SELECT DISTINCT id, pubkey
          FROM read_parquet('s3://$NOZZL_ARCHIVE_BUCKET/c=addressable/k=30402/**/*.parquet');
        CREATE TABLE reqs AS
          SELECT author, created_at, tg[1] AS tag, tg[2] AS value
          FROM (SELECT pubkey AS author, created_at, unnest(tags) AS tg
                FROM read_parquet('s3://$NOZZL_ARCHIVE_BUCKET/c=regular/k=5/**/*.parquet')
                WHERE pubkey IN (SELECT pubkey FROM versions))
          WHERE tg[1] IN ('a', 'e');
        COPY (
          SELECT DISTINCT author, tag, value, created_at FROM reqs
          WHERE tag = 'a' AND value LIKE '30402:%'
            AND author = split_part(value, ':', 2)      -- SELF-AUTHORED ONLY
          UNION ALL
          SELECT DISTINCT r.author, r.tag, r.value, r.created_at FROM reqs r
          JOIN versions v ON v.id = r.value AND v.pubkey = r.author  -- SELF-AUTHORED ONLY
          WHERE r.tag = 'e'
        ) TO 'deleted_30402_selfauthored.csv' (HEADER);"

    The pre-filter to seller pubkeys is lossless for self-authored deletes: if
    the author IS the coordinate's pubkey, that pubkey authored the listing.

WHAT COUNTS AS A LISTING
    Addressable events are versioned, so the archive holds every revision. We
    take the CURRENT version of each (pubkey, d_tag) coordinate -- newest
    `created_at`, lowest event id breaking ties, per NIP-01 -- and then drop
    six groups that are not a person selling a thing, and fold a product's options
    into the product. Each coordinate lands in at most one group; the order below
    is the precedence.

      1. non-retail bulk publishers -- single accounts pushing thousands of
         events that are not retail offers (a stock-photo catalogue, a mempool
         telemetry feed, a directory of bitcoin-accepting businesses). They are
         identified by pubkey, because nothing in the event says "I am a bot".
      2. carrier / staging clients -- apps that use kind-30402
         (markets-product-listings) as transport for
         something else (ride profiles), or that publish from a staging env.
         Identified by the `client` tag.
      3. unrenderable -- no title, no `content`, no `summary` and no `image`:
         nothing a buyer could see.
      4. synthetic -- test-worded titles, plus titles published verbatim by
         >= 4 distinct pubkeys carrying AT MOST ONE distinct price (client E2E
         farms). The price condition matters: six people posting "Buying BTC
         for NGN" at six different rates is a real market, not a farm. Note
         that "at most one" includes ZERO: 8 of the 13 farm titles carry no
         price tag at all and qualify vacuously, so this guard is weaker than
         "at the same price" would suggest.
      5. deleted -- a kind-5 (event-deletion-requests) event from the coordinate's
         OWN author covers its current version: an `e` tag naming that version's id,
         or an `a` tag naming the coordinate and dated at or after that version
         (NIP-09: "up to the created_at"), so a listing republished after its request
         is live again. NIP-09 is explicit that a delete request is only valid for its
         author's own events, so cross-author requests are ignored; honouring them let
         one publisher delete other people's listings out of this dataset.
      6. withdrawn -- `status` sold/taken/..., `visibility` hidden, `stock` 0.
      7. product options -- one row per product, over what the six left live. The
         GammaMarkets spec lets a seller publish a product as a parent (`type`
         variable) and one child per size or colour (`type` variation, with an `a`
         tag naming the parent). A family is one key's children naming one parent, and
         it keeps one row: the parent if it is live, the same key's and not itself a
         child, else the family's newest child. See fold_product_options.

DATES
    The NIP-99 `published_at` tag where present and plausible, else the event's
    `created_at` - and never after the run (`--as-of`): a listing dated ahead is dated
    at the run. Relays keep only the current version of an addressable event,
    so an edited listing with no `published_at` is dated by its edit: the growth
    curves are a floor on the ecosystem's age, never an overstatement.
"""
import argparse, collections, csv, datetime as dt, json, os, re, ssl, sys, tempfile

import duckdb
# The geo resolution and text folding are production's own module, so the two implementations cannot
# differ in them. NOSTROLOGY_DATA points at a scratch copy of the repository, for a run of a scratch
# copy of this script.
sys.path.insert(0, os.environ.get("NOSTROLOGY_DATA", os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dagster_code.pipeline import markets_geolib as G  # noqa: E402

# DuckDB's httpfs reads none of Python's TLS settings: it verifies against the first existing path
# in its own short list of well-known bundles, which on macOS is only the OS's static bundle (still
# holding two roots compromised in 2025), and it ignores SSL_CERT_FILE. On macOS, point it at the venv
# interpreter's own default store instead, and refuse an interpreter whose default is missing or is
# that same static file: a bare python3 from PATH may be such a build. Elsewhere the list finds the
# distribution's maintained bundle, and an empty ca_cert_file keeps that default.
CA_FILE = ssl.get_default_verify_paths().openssl_cafile if sys.platform == "darwin" else ""
APPLE_STATIC = os.path.realpath(CA_FILE).endswith("/etc/ssl/cert.pem") if CA_FILE else False  # tls-posture-ok: the refusal names the file it refuses
if CA_FILE and (not os.path.isfile(CA_FILE) or APPLE_STATIC):
    sys.exit(f"{sys.executable}: its default CA bundle {CA_FILE!r} is missing or is the OS's static one; "
             "run under .venv-crosscheck (crosscheck/requirements.in)")

# The archive's root: None reads s3://$NOZZL_ARCHIVE_BUCKET when first needed (archive_root());
# a self-test or a caller points it at a local tree instead.
BUCKET = None


def archive_root():
    """The nozzl archive's root: BUCKET when set, else s3://$NOZZL_ARCHIVE_BUCKET."""
    if BUCKET:
        return BUCKET
    bucket = os.environ.get("NOZZL_ARCHIVE_BUCKET", "").strip()
    if not bucket:
        sys.exit("set NOZZL_ARCHIVE_BUCKET to the nozzl archive's S3 bucket (the pipeline reads it too)")
    return "s3://" + bucket

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
# --- exclusion 3: unrenderable ---------------------------------------------
# No title, no content, no summary, no image: nothing a buyer could see.
# `demo` and `sample` are deliberately NOT here: "2010 Specialized Demo 8" is a
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


def connect():
    con = duckdb.connect()
    con.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW'")
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute(f"SET ca_cert_file = '{CA_FILE}'")
    con.execute("CREATE SECRET s (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1')")
    return con


def load_listings(con, deletions_csv, now_ts):
    # Events dated beyond now + 2 days are not part of the dataset AT ALL (identical
    # bound in the production compute, marketplaces.build_snapshot): one hostile
    # created_at would otherwise permanently win its coordinate's current-version
    # selection, and a year-10000-scale value crashes the date formatters.
    con.execute(f"""
      CREATE VIEW raw AS SELECT * FROM read_parquet('{archive_root()}/c=addressable/k=30402/**/*.parquet')
        WHERE created_at <= {int(now_ts) + 2 * 86400};
      CREATE VIEW cur AS SELECT * EXCLUDE rn FROM (
        SELECT *, row_number() OVER (PARTITION BY pubkey, d_tag
                                     ORDER BY created_at DESC, id ASC) AS rn
        FROM raw) WHERE rn = 1;
    """)
    # A farm title is the SAME title from >= 4 pubkeys at <= 1 distinct price.
    con.execute(f"""
      CREATE VIEW farm AS
        SELECT lower(coalesce(list_filter(tags, x -> x[1]='title')[1][2],'')) AS title
        FROM raw
        GROUP BY 1
        HAVING title <> ''
           AND count(DISTINCT pubkey) >= {FARM_MIN_PUBKEYS}
           AND count(DISTINCT list_filter(tags, x -> x[1]='price')[1][2]) <= 1;
    """)
    # Column types are stated because a sniffer that guesses `value` from a sample has nothing to
    # guarantee it. Then what production asserts in parse_deletions, asserted here too and BEFORE
    # the horizon, so a broken scan fails loud instead of honouring what production refuses.
    con.execute(f"""
      CREATE TABLE del_rows AS
        SELECT * FROM read_csv('{deletions_csv}', header = true, columns = {{
          'author': 'VARCHAR', 'tag': 'VARCHAR', 'value': 'VARCHAR', 'created_at': 'BIGINT'}})
    """)
    bad = con.execute("""
      SELECT count(*) FROM del_rows d
      WHERE d.tag NOT IN ('a', 'e')
         OR (d.tag = 'a' AND split_part(d.value, ':', 2) <> d.author)
         OR (d.tag = 'e' AND d.value IN (SELECT id FROM raw r WHERE r.pubkey <> d.author))
    """).fetchone()[0]
    if bad:
        raise ValueError(f"{deletions_csv}: {bad} row(s) with an unknown tag or not self-authored")
    # The same bound on the requests as on the listings: a request dated past the horizon is not
    # part of the dataset, row by row, so it cannot mask an earlier valid one.
    con.execute(f"""
      CREATE TABLE del AS SELECT * FROM del_rows WHERE created_at <= {int(now_ts) + 2 * 86400};
      CREATE TABLE del_a AS
        SELECT value AS coord, max(created_at) AS deleted_at FROM del WHERE tag = 'a' GROUP BY 1;
      CREATE TABLE del_e AS SELECT DISTINCT value AS id FROM del WHERE tag = 'e';
    """)
    return con.execute(f"""
      SELECT
        cur.id AS event_id, pubkey, d_tag, created_at,
        list_filter(tags, x -> x[1]='type')[1][2]                           AS product_type,
        list_filter(tags, x -> x[1]='a' AND starts_with(x[2], '30402:'))[1][2] AS parent,
        try_cast(list_filter(tags, x -> x[1]='published_at')[1][2] AS BIGINT) AS published_at,
        lower(coalesce(list_filter(tags, x -> x[1]='title')[1][2],''))      AS title,
        lower(coalesce(list_filter(tags, x -> x[1]='status')[1][2],''))     AS status,
        lower(coalesce(list_filter(tags, x -> x[1]='visibility')[1][2],'')) AS vis,
        try_cast(list_filter(tags, x -> x[1]='stock')[1][2] AS BIGINT)      AS stock,
        coalesce(list_filter(tags, x -> x[1]='client')[1][2],'')            AS client,
        (trim(lower(coalesce(list_filter(tags, x -> x[1]='title')[1][2],''))) = ''
           AND trim(coalesce(content,'')) = ''
           AND trim(coalesce(list_filter(tags, x -> x[1]='summary')[1][2],'')) = ''
           AND len(list_filter(tags, x -> x[1]='image')) = 0)                AS unrenderable,
        list_filter(tags, x -> x[1]='location')[1][2]                       AS loc,
        list_transform(list_filter(tags, x -> x[1]='g'), x -> x[2])         AS geohashes,
        list_transform(list_filter(tags, x -> x[1]='t'), x -> x[2])         AS cats,
        list_filter(tags, x -> x[1]='price')[1]                             AS price,
        (regexp_matches(lower(coalesce(list_filter(tags, x -> x[1]='title')[1][2],'')), '{TESTRE}')
           OR lower(coalesce(list_filter(tags, x -> x[1]='title')[1][2],'')) IN (SELECT title FROM farm)) AS synthetic,
        -- The current version only: an `e` tag on an older version deletes nothing, and an `a`
        -- tag dated before this version predates its republishing.
        (cur.id IN (SELECT id FROM del_e) OR coalesce(del_a.deleted_at >= cur.created_at, false)) AS nip09_deleted
      FROM cur
      -- coalesce: 41 rows have a NULL d_tag, and NULL || ':' is NULL, so a
      -- bare concat silently never matches the deletion set.
      LEFT JOIN del_a ON del_a.coord = '30402:' || cur.pubkey || ':' || coalesce(cur.d_tag,'')
    """).fetchall(), [d[0] for d in con.description]


def classify(recs):
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


def coordinate(pubkey, d_tag):
    return "30402:" + pubkey + ":" + (d_tag or "")


def has_price(price):
    return bool(price and len(price) >= 3 and (price[2] or "").strip())


def fold_product_options(recs):
    """One row per product, over the rows classify left live: the rule in this file's header. The
    same rule as nostrology-data's pipeline, whose docstring has the reasons. A family is per key,
    so a key can only fold its own rows, and a parent that is itself a child stands for nothing, so
    a child naming itself or two naming each other keep a row. The row kept takes the newest priced
    child's price only if it has none of its own."""
    def newest_first(r):
        return (-r["created_at"], r["event_id"])

    standing = {}
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


def resolve_geo(recs, pool):
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


def product_self_test():
    """One row per product, case by case, through this file's own SQL reader and fold: the same cases
    as nostrology-data's test_pipeline_marketplaces.py ("one row per product"), written separately.
    A tie is written both ways round, because one alone also passes on a scan order that merely
    happens to agree with it. compare_markets_fold.py is what holds the two implementations together.
    Returns the failure lines and the case count."""
    global BUCKET
    NOW = 1_800_000_000
    P, Q = "c" * 64, "d" * 64
    VAR, PAR = ["type", "variation", "physical"], ["type", "variable", "physical"]

    def of(d, owner=P):
        return ["a", f"30402:{owner}:{d or ''}"]

    def price(amount, unit="USD"):
        return ["price", amount, unit]

    # name, [(d tag, tags, optional overrides)], [kind-5 (event-deletion-requests) rows],
    # {d tag: excl}, {d tag: the price the row ends with}
    CASES = [
        ("a live parent stands for its children",
         [("tee", [PAR]), ("s", [VAR, of("tee")]), ("m", [VAR, of("tee")])], [],
         {"tee": None, "s": "option", "m": "option"}, {}),
        ("a parent withdrawn by stock 0: the newest child stays",
         [("tee", [PAR, ["stock", "0"]]), ("s", [VAR, of("tee")], {"at": 100}), ("m", [VAR, of("tee")], {"at": 200})], [],
         {"tee": "withdrawn", "s": "option", "m": None}, {}),
        ("a hidden parent: the newest child stays",
         [("tee", [PAR, ["visibility", "hidden"]]), ("s", [VAR, of("tee")], {"at": 200}), ("m", [VAR, of("tee")], {"at": 100})], [],
         {"tee": "withdrawn", "s": None, "m": "option"}, {}),
        ("a deleted parent: the newest child stays",
         [("tee", [PAR]), ("s", [VAR, of("tee")], {"at": 200}), ("m", [VAR, of("tee")], {"at": 100})],
         [(P, "a", f"30402:{P}:tee", 500)], {"tee": "deleted", "s": None, "m": "option"}, {}),
        ("a parent not in the archive: the newest child stays",
         [("s", [VAR, of("gone")], {"at": 100}), ("m", [VAR, of("gone")], {"at": 200})], [],
         {"s": "option", "m": None}, {}),
        ("the same second: the lowest id stays, as NIP-01 picks a version",
         [("s", [VAR, of("gone")], {"at": 100, "id": "b"}), ("m", [VAR, of("gone")], {"at": 100, "id": "a"})], [],
         {"s": "option", "m": None}, {}),
        ("the same second, the other way round",
         [("s", [VAR, of("gone")], {"at": 100, "id": "a"}), ("m", [VAR, of("gone")], {"at": 100, "id": "b"})], [],
         {"s": None, "m": "option"}, {}),
        ("another key's children fold only into their own row",
         [("tee", [PAR]), ("s", [VAR, of("tee")]),
          ("x", [VAR, of("tee")], {"pk": Q, "at": 100}), ("y", [VAR, of("tee")], {"pk": Q, "at": 200})], [],
         {"tee": None, "s": "option", "x": "option", "y": None}, {}),
        ("one child under another key's parent stays",
         [("tee", [PAR]), ("x", [VAR, of("tee")], {"pk": Q})], [], {"tee": None, "x": None}, {}),
        ("a child naming itself stays",
         [("self", [VAR, of("self")])], [], {"self": None}, {}),
        ("a child naming itself and a sibling naming it: the newest stays",
         [("self", [VAR, of("self")], {"at": 100}), ("m", [VAR, of("self")], {"at": 200})], [],
         {"self": "option", "m": None}, {}),
        ("two children naming each other both stay",
         [("a", [VAR, of("b")]), ("b", [VAR, of("a")])], [], {"a": None, "b": None}, {}),
        ("a child of a child: only the one under the real parent folds",
         [("tee", [PAR]), ("mid", [VAR, of("tee")]), ("low", [VAR, of("mid")])], [],
         {"tee": None, "mid": "option", "low": None}, {}),
        ("a child with no parent tag stays", [("s", [VAR]), ("m", [VAR])], [], {"s": None, "m": None}, {}),
        ("an `a` tag of another kind names no parent",
         [("s", [VAR, ["a", f"30405:{P}:shop"]]), ("m", [VAR, ["a", f"30405:{P}:shop"]])], [],
         {"s": None, "m": None}, {}),
        ("an `a` tag with no value names no parent, and the next one does",
         [("tee", [PAR]), ("s", [VAR, ["a"], of("tee")]), ("m", [VAR, ["a"]])], [],
         {"tee": None, "s": "option", "m": None}, {}),
        ("`type` is read exactly", [("tee", [PAR]), ("s", [["type", "Variation"], of("tee")]),
                                     ("m", [["type"], of("tee")])], [],
         {"tee": None, "s": None, "m": None}, {}),
        ("only the first `type` tag counts",
         [("tee", [PAR]), ("s", [["type", "simple"], VAR, of("tee")])], [], {"tee": None, "s": None}, {}),
        ("`type` with a stray space is no child",
         [("tee", [PAR]), ("sp", [["type", " variation"], of("tee")]), ("tr", [["type", "variation "], of("tee")])], [],
         {"tee": None, "sp": None, "tr": None}, {}),
        ("a coordinate is matched exactly: capitals or a stray space name nobody",
         [("tee", [PAR]), ("real", [VAR, of("tee")]), ("caps", [VAR, of("tee", P.upper())]), ("caps_d", [VAR, of("TEE")]),
          ("after", [VAR, ["a", f"30402:{P}:tee "]]), ("before", [VAR, ["a", f" 30402:{P}:tee"]])], [],
         {"tee": None, "real": "option", "caps": None, "caps_d": None, "after": None, "before": None}, {}),
        ("a kind merely starting with the number names no parent",
         [("s", [VAR, ["a", f"304021:{P}:shop"]]), ("m", [VAR, ["a", f"304021:{P}:shop"]])], [],
         {"s": None, "m": None}, {}),
        ("only the first product `a` tag names the parent",
         [("tee", [PAR]), ("first", [VAR, of("tee"), of("gone")]), ("second", [VAR, of("gone"), of("tee")])], [],
         {"tee": None, "first": "option", "second": None}, {}),
        ("a parent of any type stands, if it is no child",
         [("tee", [["type", "simple"]]), ("s", [VAR, of("tee")]), ("bare", []), ("m", [VAR, of("bare")])], [],
         {"tee": None, "s": "option", "bare": None, "m": "option"}, {}),
        ("a child excluded for another reason stays in its group",
         [("tee", [PAR]), ("sold", [VAR, of("tee"), ["status", "sold"]]), ("gone", [VAR, of("tee")]),
          ("testing", [VAR, of("tee")]), ("s", [VAR, of("tee")])],
         [(P, "a", f"30402:{P}:gone", 500)],
         {"tee": None, "sold": "withdrawn", "gone": "deleted", "testing": "synthetic", "s": "option"}, {}),
        ("a parent with no d tag is still nameable",
         [(None, [PAR]), ("s", [VAR, of(None)])], [], {None: None, "s": "option"}, {}),
        ("a d tag with colons in it",
         [("a:b:c", [PAR]), ("s", [VAR, of("a:b:c")])], [], {"a:b:c": None, "s": "option"}, {}),
        ("no d tag and an empty one share a coordinate: the newer stands",
         [(None, [PAR], {"at": 100}), ("", [PAR, price("5")], {"at": 200}), ("s", [VAR, of(""), price("9")])], [],
         {None: None, "": None, "s": "option"}, {"": ["price", "5", "USD"], None: None}),
        ("no d tag newer than an empty one: the newer takes the price",
         [(None, [PAR], {"at": 200}), ("", [PAR], {"at": 100}), ("s", [VAR, of(""), price("9")])], [],
         {None: None, "": None, "s": "option"}, {None: ["price", "9", "USD"], "": None}),
        ("an empty d tag newer than no d tag: the newer takes the price",
         [(None, [PAR], {"at": 100}), ("", [PAR], {"at": 200}), ("s", [VAR, of(""), price("9")])], [],
         {None: None, "": None, "s": "option"}, {"": ["price", "9", "USD"], None: None}),
        ("a parent with no price takes the newest priced child's",
         [("tee", [PAR]), ("old", [VAR, of("tee"), price("1")], {"at": 100}),
          ("new", [VAR, of("tee"), price("2")], {"at": 200}), ("none", [VAR, of("tee")], {"at": 300})], [],
         {"tee": None, "old": "option", "new": "option", "none": "option"}, {"tee": ["price", "2", "USD"]}),
        ("the newest priced child's price, the newer one written first",
         [("tee", [PAR]), ("new", [VAR, of("tee"), price("2")], {"at": 200}),
          ("old", [VAR, of("tee"), price("1")], {"at": 100}), ("none", [VAR, of("tee")], {"at": 300})], [],
         {"tee": None, "new": "option", "old": "option", "none": "option"}, {"tee": ["price", "2", "USD"]}),
        ("priced children of the same second: the lowest id's price",
         [("tee", [PAR]), ("x", [VAR, of("tee"), price("1")], {"id": "b"}), ("y", [VAR, of("tee"), price("2")], {"id": "a"})], [],
         {"tee": None, "x": "option", "y": "option"}, {"tee": ["price", "2", "USD"]}),
        ("priced children of the same second, the other way round",
         [("tee", [PAR]), ("x", [VAR, of("tee"), price("1")], {"id": "a"}), ("y", [VAR, of("tee"), price("2")], {"id": "b"})], [],
         {"tee": None, "x": "option", "y": "option"}, {"tee": ["price", "1", "USD"]}),
        ("a parent with a price keeps it",
         [("tee", [PAR, price("9")]), ("s", [VAR, of("tee"), price("1")])], [],
         {"tee": None, "s": "option"}, {"tee": ["price", "9", "USD"]}),
        ("a price with no unit is no price, and a child's replaces it",
         [("tee", [PAR, ["price", "9"]]), ("s", [VAR, of("tee"), price("1")])], [],
         {"tee": None, "s": "option"}, {"tee": ["price", "1", "USD"]}),
        ("an empty unit is no price",
         [("tee", [PAR, ["price", "9", ""]]), ("s", [VAR, of("tee"), price("1")])], [],
         {"tee": None, "s": "option"}, {"tee": ["price", "1", "USD"]}),
        ("a blank unit is no price",
         [("tee", [PAR, ["price", "9", " "]]), ("s", [VAR, of("tee"), price("1")])], [],
         {"tee": None, "s": "option"}, {"tee": ["price", "1", "USD"]}),
        ("a blank unit on a child gives no price",
         [("tee", [PAR]), ("s", [VAR, of("tee"), ["price", "1", " "]])], [],
         {"tee": None, "s": "option"}, {"tee": None}),
        ("an amount that is no number is still a price",
         [("tee", [PAR, price("Infinity")]), ("s", [VAR, of("tee"), price("35")])], [],
         {"tee": None, "s": "option"}, {"tee": ["price", "Infinity", "USD"]}),
        ("an empty amount with a unit is still a price",
         [("tee", [PAR, price("")]), ("s", [VAR, of("tee"), price("35")])], [],
         {"tee": None, "s": "option"}, {"tee": ["price", "", "USD"]}),
        ("no amount at all with a unit is still a price",
         [("tee", [PAR, price(None)]), ("s", [VAR, of("tee"), price("35")])], [],
         {"tee": None, "s": "option"}, {"tee": ["price", None, "USD"]}),
        ("a withdrawn child's price is not taken",
         [("tee", [PAR]), ("s", [VAR, of("tee"), price("1"), ["status", "sold"]])], [],
         {"tee": None, "s": "withdrawn"}, {"tee": None}),
        ("a kept child with no price takes a sibling's",
         [("s", [VAR, of("gone")], {"at": 200}), ("m", [VAR, of("gone"), price("3")], {"at": 100})], [],
         {"s": None, "m": "option"}, {"s": ["price", "3", "USD"]}),
    ]
    failed = []
    for name, events, requests, want, want_price in CASES:
        with tempfile.TemporaryDirectory() as td:
            BUCKET = td
            tree = os.path.join(td, "c=addressable", "k=30402")
            os.makedirs(tree)
            src = duckdb.connect()
            src.execute("CREATE TABLE t (id VARCHAR, pubkey VARCHAR, created_at BIGINT, content VARCHAR, "
                        "tags VARCHAR[][], d_tag VARCHAR)")
            for i, (d, tags, *over) in enumerate(events):
                o = over[0] if over else {}
                eid = o.get("id", f"id-{i}")
                title = "test" if d == "testing" else "x"
                src.execute("INSERT INTO t VALUES (?, ?, ?, 'body', ?, ?)",
                            [eid, o.get("pk", P), o.get("at", 100), [["title", title]] + tags, d])
            src.execute(f"COPY t TO '{os.path.join(tree, 'part.parquet')}' (FORMAT parquet)")
            path = os.path.join(td, "del.csv")
            with open(path, "w", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                w.writerow(("author", "tag", "value", "created_at"))
                w.writerows(requests)
            con = duckdb.connect()
            con.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW'")
            rows, cols = load_listings(con, path, NOW)
        recs = [dict(zip(cols, r)) for r in rows]
        classify(recs)
        fold_product_options(recs)
        got = {r["d_tag"]: r["excl"] for r in recs}
        if got != want:
            failed.append(f"{name}: want {want}, got {got}")
        prices = {r["d_tag"]: r["price"] for r in recs}
        for d, p in want_price.items():
            if prices.get(d) != p:
                failed.append(f"{name}: {d!r} want price {p}, got {prices.get(d)}")
    return failed, len(CASES)


def self_test():
    """The deletion rule, case by case, over a local Parquet tree: no AWS and no network. It holds
    the same rule as nostrology-data's pipeline tests (test_pipeline_marketplaces.py, "the NIP-09
    rule"), written separately, because the gate only exercises what the archive happens to
    contain: no real request sits on the horizon, say. Then one row per product, the same way
    (product_self_test). Exit 0 = every case held."""
    NOW = 1_800_000_000
    HORIZON = NOW + 2 * 86400
    # d tag, [(version id, created_at)], [(tag, value or None for the coordinate, request date)],
    # deleted? Ids are per case; the seller and the rest of the event are fixed.
    CASES = [
        ("a-after", [("v1", 100)], [("a", None, 101)], True),
        ("a-same-second", [("v1", 100)], [("a", None, 100)], True),
        ("a-before", [("v1", 100)], [("a", None, 99)], False),
        ("republished", [("v1", 50), ("v2", 100)], [("a", None, 75)], False),
        ("taken-again", [("v1", 50), ("v2", 100)], [("a", None, 75), ("a", None, 120)], True),
        ("e-current", [("v1", 100)], [("e", "v1", 200)], True),
        ("e-current-dated-before-it", [("v1", 100)], [("e", "v1", 10)], True),
        ("e-older-version", [("v1", 50), ("v2", 100)], [("e", "v1", 200)], False),
        ("stale-a-and-e", [("v1", 50), ("v2", 100)], [("a", None, 75), ("e", "v2", 200)], True),
        ("e-unknown-id", [("v1", 100)], [("e", "nothing-we-hold", 200)], False),
        ("a-on-the-horizon", [("v1", 100)], [("a", None, HORIZON)], True),
        ("a-past-the-horizon", [("v1", 100)], [("a", None, HORIZON + 1)], False),
        ("e-on-the-horizon", [("v1", 100)], [("e", "v1", HORIZON)], True),
        ("e-past-the-horizon", [("v1", 100)], [("e", "v1", HORIZON + 1)], False),
        ("future-cannot-mask", [("v1", 100)], [("a", None, 110), ("a", None, HORIZON + 1)], True),
        ("future-cannot-rescue-stale", [("v1", 100)], [("a", None, 90), ("a", None, HORIZON + 1)], False),
        # CSV has to carry these intact, and the coordinate has to match afterwards.
        ('commas, "quotes" and a\nline', [("v1", 100)], [("a", None, 100)], True),
        # NULL d_tag: its coordinate ends at the colon, and the date is still compared.
        (None, [("v1", 100)], [("a", None, 100)], True),
    ]
    pk = "c" * 64

    def run(cases, author=pk):
        """load_listings over these cases, as {d_tag: nip09_deleted}. `author` signs the requests."""
        global BUCKET
        with tempfile.TemporaryDirectory() as td:
            BUCKET = td
            tree = os.path.join(td, "c=addressable", "k=30402")
            os.makedirs(tree)
            src = duckdb.connect()
            src.execute("CREATE TABLE t (id VARCHAR, pubkey VARCHAR, created_at BIGINT, content VARCHAR, "
                        "tags VARCHAR[][], d_tag VARCHAR)")
            path = os.path.join(td, "del.csv")
            with open(path, "w", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                w.writerow(("author", "tag", "value", "created_at"))
                for i, (d, versions, requests, *_) in enumerate(cases):
                    ids = {name: f"{i}-{name}" for name, _ in versions}
                    for name, at in versions:
                        src.execute("INSERT INTO t VALUES (?, ?, ?, 'body', [['title', 'x']], ?)",
                                    [ids[name], pk, at, d])
                    for tag, value, at in requests:
                        named = f"30402:{pk}:{d or ''}" if value is None else ids.get(value, value)
                        w.writerow((author, tag, named, at))
            src.execute(f"COPY t TO '{os.path.join(tree, 'part.parquet')}' (FORMAT parquet)")
            con = duckdb.connect()
            con.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW'")
            rows, cols = load_listings(con, path, NOW)
        return {r[cols.index("d_tag")]: r[cols.index("nip09_deleted")] for r in rows}

    got = run(CASES)
    failed = [f"{d!r}: want nip09_deleted={want}, got {got.get(d)}"
              for d, _, _, want in CASES if got.get(d) is not want]
    # What production refuses in parse_deletions, this refuses too, past the horizon as well.
    REFUSED = [
        ("another key's coordinate", [("x", [("v1", 100)], [("a", None, 100)])], "d" * 64),
        ("another key's version", [("x", [("v1", 100)], [("e", "v1", 100)])], "d" * 64),
        ("another key's, past the horizon", [("x", [("v1", 100)], [("e", "v1", HORIZON + 1)])], "d" * 64),
        ("an unknown tag", [("x", [("v1", 100)], [("p", "v1", 100)])], pk),
    ]
    for why, cases, author in REFUSED:
        try:
            run(cases, author)
            failed.append(f"{why}: accepted")
        except ValueError:
            pass
    for line in failed:
        print("FAILED", line)
    total = len(CASES) + len(REFUSED)
    print(f"{total - len(failed)} of {total} deletion cases held")
    product_failed, product_total = product_self_test()
    for line in product_failed:
        print("FAILED", line)
    print(f"{product_total - len({f.split(':')[0] for f in product_failed})} of {product_total} product cases held")
    return 1 if failed or product_failed else 0


def main():
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    ap = argparse.ArgumentParser()
    ap.add_argument("--deletions", required=True,
                    help="CSV of SELF-AUTHORED kind-5 (event-deletion-requests) rows naming a"
                         " listing: author, tag (a or e), value (a coordinate or an id), created_at")
    ap.add_argument("--out", required=True)
    ap.add_argument("--as-of", default=None,
                    help="ISO-8601 UTC instant for the 'last 90 days' windows "
                         "(default: now). Pin it to reproduce a snapshot exactly.")
    args = ap.parse_args()

    NOW = int((dt.datetime.fromisoformat(args.as_of).replace(tzinfo=dt.timezone.utc)
               if args.as_of else dt.datetime.now(dt.timezone.utc)).timestamp())
    con = connect()
    rows, cols = load_listings(con, args.deletions, NOW)
    recs = [dict(zip(cols, r)) for r in rows]
    classify(recs)
    fold_product_options(recs)
    pool = [r for r in recs if r["excl"] is None]
    # DISTINCT ids over the bounded raw view: the production extract id-dedups (duplicate
    # archive rows for one event are expected there), so a raw count(*) here would diverge
    # by construction the day a duplicate lands. Equal today (12,182 = 12,182 verified).
    archive_events = con.execute("SELECT count(DISTINCT id) FROM raw").fetchone()[0]

    modal = resolve_geo(recs, pool)

    # Never after NOW, as in the production compute (marketplaces.build_snapshot): the bound in
    # load_listings admits created_at up to two days past the run, and uncapped, a listing dated
    # ahead could draw a bar for a month not yet begun and run the cumulative series past the
    # run's day. Capped, it counts on the run's day, and takes its own date on the first run
    # after it.
    def when(r):
        p = r["published_at"]
        return min(p if (p and 1500000000 <= p <= NOW) else r["created_at"], NOW)

    def day(ts): return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d")
    def mon(ts): return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m")

    def cumulative(times):
        per = collections.Counter(day(t) for t in times)
        days = sorted(per)
        d, end, run, out = dt.date.fromisoformat(days[0]), dt.date.fromisoformat(days[-1]), 0, []
        while d <= end:
            run += per.get(d.isoformat(), 0)
            out.append({"d": d.isoformat(), "v": run})
            d += dt.timedelta(days=1)
        return out

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
    map_points = sorted([{"lat": k[0], "lon": k[1], "cc": k[2], "name": G.cc_name(k[2]),
                          "prec": k[3], "listings": v["listings"], "sellers": len(v["sellers"])}
                         for k, v in pts.items()],
                        # prec in the key: a tie on the other four fields must never fall
                        # back to input row order (a spurious cross-method ordering diff)
                        key=lambda p: (-p["listings"], p["cc"], p["lat"], p["lon"], p["prec"]))

    pool_pks = {r["pubkey"] for r in pool}
    seller_cc = {pk: cc for pk, cc in modal.items() if pk in pool_pks}
    sd = collections.Counter(seller_cc.values())
    seller_countries = sorted([{"cc": k, "name": G.cc_name(k), "sellers": v} for k, v in sd.items()],
                              key=lambda x: (-x["sellers"], x["cc"]))

    # ---- shipping, kind-30406 (markets-shipping-options)
    con.execute(f"""
      CREATE VIEW sh_raw AS SELECT * FROM read_parquet('{archive_root()}/c=addressable/k=30406/**/*.parquet')
        WHERE created_at <= {NOW + 2 * 86400};
      CREATE VIEW sh AS SELECT * EXCLUDE rn FROM (
        SELECT *, row_number() OVER (PARTITION BY pubkey, d_tag
                                     ORDER BY created_at DESC, id ASC) AS rn
        FROM sh_raw) WHERE rn = 1;
    """)
    # `country` is ONE tag holding MANY values -- an EU shipping option carries
    # all 27 member codes in a single tag. Taking tg[2] keeps only the first and
    # silently discards ~1,100 destinations, which alphabetical ordering turns
    # into a phantom Austrian cluster. Keep every element from index 1 on.
    sh_rows = con.execute("""
      SELECT pubkey,
             list_concat(
               coalesce(flatten(list_transform(list_filter(tags, x -> x[1]='country'),
                                               x -> x[2:])), []),
               coalesce(flatten(list_transform(list_filter(tags, x -> x[1]='pickup-country'),
                                               x -> x[2:])), [])) AS vals
      FROM sh
    """).fetchall()
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
    sh_total = con.execute("SELECT count(*) FROM sh").fetchone()[0]

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
    json.dump(data, open(args.out, "w"), separators=(",", ":"))
    print(f"wrote {args.out}")
    for k in ("archive_events", "current_coordinates", "active_listings", "sellers",
              "active_90d", "sellers_90d", "countries", "withdrawn_any", "deleted_any"):
        print(f"  {k:22} {data['summary'][k]}")
    for e in data["exclusions"]:
        print(f"  - {e['k']:30} {e['v']}")


if __name__ == "__main__":
    sys.exit(main())

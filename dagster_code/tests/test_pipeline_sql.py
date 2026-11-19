"""Unit tests for pipeline/sql — template selection by class, the zapstore EXTRA_WHERE guard, and
placeholder substitution (render against fake templates in a tmp sql dir)."""

import re

import pytest

from dagster_code.pipeline import sql

_TEMPLATE = (
    "UNLOAD (SELECT * FROM __GLUE_DB__.__GLUE_TABLE__ "
    "WHERE c = 'X' AND k = __KIND__ __EXTRA_WHERE__) "
    "TO 's3://__BUCKET__/k=__KIND__/__RUN_ID__/' WITH (format = 'JSON')\n"
)


def _make_sql_dir(tmp_path):
    for name in (
        "extract_replaceable_latest.sql",
        "extract_addressable_latest.sql",
        "extract_regular_id_dedup.sql",
    ):
        (tmp_path / name).write_text(_TEMPLATE)
    return str(tmp_path)


def test_template_for_kind_picks_by_class():
    assert sql.template_for_kind(10002) == "extract_replaceable_latest.sql"
    assert sql.template_for_kind(32267) == "extract_addressable_latest.sql"
    assert sql.template_for_kind(9735) == "extract_regular_id_dedup.sql"


def test_template_for_kind_rejects_ephemeral():
    with pytest.raises(RuntimeError, match="no extract template"):
        sql.template_for_kind(25000)


def test_extra_where_empty_by_default():
    assert sql.extra_where(10002) == ""
    assert sql.extra_where(9735) == ""


def test_extra_where_zapstore_filter_for_9735():
    w = sql.extra_where(9735, "zapstore")
    assert "element_at(t, 1) = 'a'" in w
    assert "element_at(t, 2) LIKE '32267:%'" in w


def test_extra_where_zapstore_requires_9735():
    with pytest.raises(RuntimeError, match=re.escape("requires kind-9735 (zap-receipts) (got kind=9321")):
        sql.extra_where(9321, "zapstore")
    with pytest.raises(RuntimeError, match="INTO=zapstore requires"):
        sql.extra_where(10002, "zapstore")


def test_render_substitutes_all_placeholders(tmp_path):
    sql_dir = _make_sql_dir(tmp_path)
    out = sql.render(sql_dir, glue_db="example_db", glue_table="events", kind=10002, bucket="example_extracts", run_id="dag-k10002")
    assert "__KIND__" not in out and "__BUCKET__" not in out
    assert "__RUN_ID__" not in out and "__EXTRA_WHERE__" not in out
    assert "k = 10002" in out
    assert "s3://example_extracts/k=10002/dag-k10002/" in out
    assert "FROM example_db.events " in out
    assert "__EXTRA_WHERE__ " not in out  # the empty where leaves a clean clause


def test_render_zapstore_injects_the_filter(tmp_path):
    sql_dir = _make_sql_dir(tmp_path)
    out = sql.render(sql_dir, glue_db="example_db", glue_table="events", kind=9735, bucket="b", run_id="dag-k9735-zapstore", into="zapstore")
    assert "element_at(t, 2) LIKE '32267:%'" in out
    assert "s3://b/k=9735/dag-k9735-zapstore/" in out


# --- marketplaces INTO overrides -------------------------------------------------------------
# These three extracts swap the TEMPLATE (all-versions / the kind-5 (event-deletion-requests)
# pair aggregation) rather than adding an EXTRA_WHERE, and must never disturb the zapstore
# subset's behavior.

def test_template_for_kind_marketplaces_overrides():
    assert sql.template_for_kind("30402", "markets") == "extract_addressable_all.sql"
    assert sql.template_for_kind("30406", "markets") == "extract_addressable_all.sql"
    assert sql.template_for_kind("5", "markets-deletions") == "extract_markets_deletions.sql"
    # Without the INTO the same kinds keep their class defaults.
    assert sql.template_for_kind("30402") == "extract_addressable_latest.sql"
    assert sql.template_for_kind("5") == "extract_regular_id_dedup.sql"


def test_extra_where_empty_for_marketplaces_intos():
    assert sql.extra_where("30402", "markets") == ""
    assert sql.extra_where("5", "markets-deletions") == ""


def test_unknown_into_still_rejected():
    with pytest.raises(RuntimeError, match="INTO=zapstore requires"):
        sql.extra_where("9735", "markets")  # not a marketplaces pair, not the zapstore pair


# --- routstr INTO overrides ------------------------------------------------------------------
# The three small kinds reuse the marketplaces all-versions template; the fat
# kind-38422 (routstr-analytics-snapshots) swaps in its own last-per-(pubkey, day) template.

def test_template_for_kind_routstr_overrides():
    assert sql.template_for_kind("38421", "routstr") == "extract_addressable_all.sql"
    assert sql.template_for_kind("38423", "routstr") == "extract_addressable_all.sql"
    assert sql.template_for_kind("38425", "routstr") == "extract_addressable_all.sql"
    assert sql.template_for_kind("38422", "routstr") == "extract_routstr_analytics_daily.sql"
    # Without the INTO the same kinds keep their class default.
    assert sql.template_for_kind("38422") == "extract_addressable_latest.sql"


def test_extra_where_empty_for_routstr_intos():
    for k in ("38421", "38422", "38423", "38425"):
        assert sql.extra_where(k, "routstr") == ""


def test_render_routstr_analytics_template_against_real_sql_dir():
    """Render the REAL kind-38422 (routstr-analytics-snapshots) template, pinning the properties the compute depends on:
    the schema-tag filter, the last-per-(pubkey, UTC day) window with the NIP-01-style
    tie-break, and the id-level dedup stage."""
    import os
    real_sql_dir = os.path.join(os.path.dirname(__file__), "..", "..", "sql")

    q = sql.render(real_sql_dir, glue_db="example_db", glue_table="events", kind="38422", bucket="B", run_id="RID-k38422-routstr", into="routstr")
    assert "__" not in q
    assert "routstr.analytics.snapshot.v1" in q
    assert "element_at(t, 1) = 'schema'" in q
    assert "PARTITION BY pubkey, date_format(from_unixtime(created_at), '%Y-%m-%d')" in q
    assert "ORDER BY created_at DESC, id ASC" in q
    assert "PARTITION BY id ORDER BY ingested_at" in q
    assert "c = 'addressable' AND k = 38422" in q
    assert "TO 's3://B/k=38422/RID-k38422-routstr/'" in q


def test_render_marketplaces_templates_against_real_sql_dir():
    """Render the REAL templates (repo sql/), pinning the properties the compute depends on:
    the all-versions template emits d_tag and has NO latest-per-coordinate stage; the
    deletions template carries the NIP-09 self-authored filter and the 30402 coordinate scope."""
    import os
    real_sql_dir = os.path.join(os.path.dirname(__file__), "..", "..", "sql")

    q = sql.render(real_sql_dir, glue_db="example_db", glue_table="events", kind="30402", bucket="B", run_id="RID-k30402-markets", into="markets")
    assert "__" not in q
    assert "d_tag" in q and "rn_coord" not in q
    assert "c = 'addressable' AND k = 30402" in q
    assert "TO 's3://B/k=30402/RID-k30402-markets/'" in q

    d = sql.render(real_sql_dir, glue_db="example_db", glue_table="events", kind="5", bucket="B", run_id="RID-k5-markets-deletions", into="markets-deletions")
    assert "__" not in d
    assert "LIKE '30402:%'" in d
    assert "r.author = split_part(r.value, ':', 2)" in d  # NIP-09 self-authored only
    assert "c = 'regular' AND k = 5" in d
    assert "TO 's3://B/k=5/RID-k5-markets-deletions/'" in d
    # ONE pass over kind-5 (event-deletion-requests): Athena expands a WITH per reference, and two
    # passes of a 3.6 GiB scan come close to the workgroup's enforced 10 GiB per-query cutoff.
    assert d.count("c = 'regular'") == 1
    assert "UNION" not in d.upper().split("UNLOAD", 1)[1]


# --- what the deletions template actually selects ---------------------------------------------
# The rule that was wrong for ten weeks lived in this SQL, and Athena cannot run in a test. So the
# REAL template's SELECT runs in DuckDB over fixture rows: the only edit is element_at(tg, N),
# which DuckDB reserves for maps, to tg[N], which means the same on an array. DuckDB is not in the
# Dagster image, so this skips there and runs under the local test venv.

_SELLER = "c" * 64
_OTHER = "d" * 64
_LISTING = "1" * 64
_OTHERS_LISTING = "2" * 64
_A_NOTE = "9" * 64
_AN_ARTICLE = "8" * 64
_A_SHIPPING_OPTION = "7" * 64


def _deletions_select(requests, listings, others=()):
    """(author, tag, value, created_at) rows the template selects from these fixture events:
    `requests` as (pubkey, created_at, tags), `listings` as (id, pubkey), and `others` as
    (c, k, id, pubkey) for events of any other class or kind. Sorted, as DISTINCT is not."""
    import os
    import re

    duckdb = pytest.importorskip("duckdb", reason="duckdb is not in the Dagster image")
    real_sql_dir = os.path.join(os.path.dirname(__file__), "..", "..", "sql")
    q = sql.render(real_sql_dir, glue_db="example_db", glue_table="events", kind="5", bucket="B", run_id="R", into="markets-deletions")
    select = re.search(r"^UNLOAD \(\n(.*)\n\)\nTO 's3://", q, re.S | re.M).group(1)
    select = re.sub(r"element_at\(tg, (\d)\)", r"tg[\1]", select)
    con = duckdb.connect()
    con.execute("CREATE SCHEMA example_db")
    con.execute("CREATE TABLE example_db.events (c VARCHAR, k INTEGER, id VARCHAR, "
                "pubkey VARCHAR, created_at BIGINT, tags VARCHAR[][])")
    for pubkey, created_at, tags in requests:
        con.execute("INSERT INTO example_db.events VALUES ('regular', 5, 'r', ?, ?, ?)",
                    [pubkey, created_at, tags])
    # Each listing carries an `e` tag naming itself, so a template that read the listings as
    # requests too would select them.
    for eid, pubkey in listings:
        con.execute("INSERT INTO example_db.events VALUES ('addressable', 30402, ?, ?, 1, ?)",
                    [eid, pubkey, [["e", eid]]])
    for c, k, eid, pubkey in others:
        con.execute("INSERT INTO example_db.events VALUES (?, ?, ?, ?, 1, [])", [c, k, eid, pubkey])
    return sorted(con.execute(select).fetchall())


_LISTINGS = [(_LISTING, _SELLER), (_OTHERS_LISTING, _OTHER)]


def test_deletions_select_keeps_both_forms_of_a_self_authored_request():
    coord = f"30402:{_SELLER}:shoes"
    got = _deletions_select(
        [(_SELLER, 100, [["e", _LISTING], ["a", coord], ["k", "30402"]])], _LISTINGS)
    assert got == [(_SELLER, "a", coord, 100), (_SELLER, "e", _LISTING, 100)]


def test_deletions_select_refuses_another_authors_listing_in_either_form():
    # The join exists for the second row: an `e` tag carries no author of its own, so without it
    # one key could delete another's listing by id, which NIP-09 says a client must not honour.
    got = _deletions_select(
        [(_SELLER, 100, [["a", f"30402:{_OTHER}:shoes"], ["e", _OTHERS_LISTING]])], _LISTINGS)
    assert got == []


def test_deletions_select_drops_what_names_no_listing():
    got = _deletions_select(
        [(_SELLER, 100, [["e", _A_NOTE],                  # a post the seller deleted
                         ["a", f"30617:{_SELLER}:repo"],  # another kind's coordinate
                         ["e"], ["a"],                    # one-element tags name nothing
                         []])], _LISTINGS)
    assert got == []


def test_deletions_select_joins_to_listings_and_nothing_else():
    # The seller's own events of another kind or class, named by id: a deleted post, an article, a
    # shipping option. Dropping the join's own filter would let them leave Athena and prop up the
    # floor that exists to catch a bad join, and would scan the whole archive past the cutoff.
    got = _deletions_select(
        [(_SELLER, 100, [["e", _A_NOTE], ["e", _AN_ARTICLE], ["e", _A_SHIPPING_OPTION]])], _LISTINGS,
        others=[("regular", 1, _A_NOTE, _SELLER),
                ("addressable", 30023, _AN_ARTICLE, _SELLER),
                ("addressable", 30406, _A_SHIPPING_OPTION, _SELLER)])
    assert got == []


def test_deletions_select_keeps_every_date_and_collapses_only_exact_repeats():
    # Rows, not the newest per pair: the compute applies its horizon to each. Two publishes of
    # the same request (or two relays' copies) are one row.
    coord = f"30402:{_SELLER}:shoes"
    got = _deletions_select(
        [(_SELLER, 100, [["a", coord]]), (_SELLER, 200, [["a", coord]]), (_SELLER, 200, [["a", coord]])],
        _LISTINGS)
    assert got == [(_SELLER, "a", coord, 100), (_SELLER, "a", coord, 200)]


def test_deletions_select_reads_nothing_but_kind_5():
    # A kind-30402 (markets-product-listings) event that carries an `e` tag is not a request.
    assert _deletions_select([], _LISTINGS) == []


# --- ngit INTO overrides ---------------------------------------------------------------------
# Eight extracts, all into=ngit: the roster kinds read ALL versions, the four regular kinds
# carry the git-subset filter inside their templates, and kind-5 (event-deletion-requests)
# is an aggregation. None of them may disturb the zapstore subset or the class defaults.

_NGIT = {
    "30617": "extract_addressable_all.sql",
    "30618": "extract_addressable_all.sql",
    "10317": "extract_replaceable_all.sql",
    "1111": "extract_ngit_comments.sql",
    "7": "extract_ngit_reactions.sql",
    "17": "extract_ngit_external_reactions.sql",
    "9735": "extract_ngit_zaps.sql",
    "5": "extract_ngit_deletions.sql",
}


def test_template_for_kind_ngit_overrides():
    for kind, tmpl in _NGIT.items():
        assert sql.template_for_kind(kind, "ngit") == tmpl
        assert sql.extra_where(kind, "ngit") == ""
    # Without the INTO the same kinds keep their class defaults, and the zapstore subset
    # of kind-9735 (zap-receipts) is untouched.
    assert sql.template_for_kind("10317") == "extract_replaceable_latest.sql"
    assert sql.template_for_kind("1111") == "extract_regular_id_dedup.sql"
    assert sql.template_for_kind("17") == "extract_regular_id_dedup.sql"
    assert sql.template_for_kind("9735", "zapstore") == "extract_regular_id_dedup.sql"
    assert "32267:%" in sql.extra_where("9735", "zapstore")


def test_render_ngit_templates_against_real_sql_dir():
    """Render the REAL ngit templates, pinning what the compute depends on: every
    placeholder substituted, the archive class, the id-level dedup, and each subset filter."""
    import os
    real_sql_dir = os.path.join(os.path.dirname(__file__), "..", "..", "sql")
    q = {k: sql.render(real_sql_dir, glue_db="example_db", glue_table="events", kind=k, bucket="B", run_id=f"RID-k{k}-ngit", into="ngit")
         for k in _NGIT}
    for k, text in q.items():
        assert "__" not in text, k
        assert f"TO 's3://B/k={k}/RID-k{k}-ngit/'" in text, k
    assert "c = 'addressable' AND k = 30617" in q["30617"] and "d_tag" in q["30617"]
    assert "rn_coord" not in q["30618"]
    # all versions of the replaceable kind: id dedup only, no latest-per-pubkey stage
    assert "c = 'replaceable' AND k = 10317" in q["10317"]
    assert "PARTITION BY id ORDER BY ingested_at" in q["10317"] and "rn_coord" not in q["10317"]
    comments = q["1111"]
    assert "c = 'regular' AND k = 1111" in comments
    assert "IN ('1617', '1618', '1619', '1621', '30617', '30618')" in comments
    assert "element_at(t, 1) IN ('A', 'a')" in comments and "LIKE '30617:%'" in comments
    assert "PARTITION BY id ORDER BY ingested_at" in comments
    reactions = q["7"]
    assert "element_at(t, 1) = 'k'" in reactions and "element_at(t, 2) = '30617'" in reactions
    assert "element_at(t, 1) = 'a'" in reactions and "LIKE '30617:%'" in reactions
    # kind-17 (external-content-reactions): Iris Git's stars only - BOTH a k=git-repo and a
    # 30617 i tag (ref 6.2), where the kind-7 (reactions) subset takes either of its two tags
    ext = q["17"]
    assert "c = 'regular' AND k = 17" in ext
    assert "element_at(t, 1) = 'k'" in ext and "element_at(t, 2) = 'git-repo'" in ext
    assert "element_at(t, 1) = 'i'" in ext and "LIKE '30617:%'" in ext
    assert "OR cardinality" not in ext and ext.count("AND cardinality(filter(tags") == 2
    assert "PARTITION BY id ORDER BY ingested_at" in ext
    zaps = q["9735"]
    assert "c = 'regular' AND k = 9735" in zaps and "LIKE '30617:%'" in zaps
    assert "32267" not in zaps
    dels = q["5"]
    # DISTINCT triples, not the newest per pair: the compute applies the horizon per triple
    assert "SELECT DISTINCT pubkey AS author, element_at(tg, 2) AS coord, created_at" in dels
    assert "max(" not in dels and "GROUP BY" not in dels
    assert "pubkey = split_part(element_at(tg, 2), ':', 2)" in dels  # NIP-09 self-authored only
    assert "LIKE '30617:%'" in dels and "c = 'regular' AND k = 5" in dels


def test_no_extract_template_names_a_database():
    # The archive's Glue table is the deployment's own (NOSTROLOGY_GLUE_DB, NOSTROLOGY_GLUE_TABLE): every
    # extract template reads it through the two placeholders, so none names a database.
    import glob
    import os
    real = os.path.join(os.path.dirname(__file__), "..", "..", "sql")
    paths = sorted(glob.glob(os.path.join(real, "extract_*.sql")))
    assert len(paths) >= 12
    for p in paths:
        refs = re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_]\w*\.[A-Za-z_]\w*)", open(p).read(), re.I)
        assert refs and set(refs) == {"__GLUE_DB__.__GLUE_TABLE__"}, (os.path.basename(p), refs)


def test_render_refuses_a_placeholder_it_does_not_know(tmp_path):
    sql_dir = _make_sql_dir(tmp_path)
    import os
    for name in os.listdir(sql_dir):
        with open(os.path.join(sql_dir, name), "w") as fh:
            fh.write("SELECT 1 FROM __GLUE_DB__.__GLUE_TABLE__ WHERE x = __NOT_A_PLACEHOLDER__")
    with pytest.raises(RuntimeError, match="__NOT_A_PLACEHOLDER__"):
        sql.render(sql_dir, glue_db="d", glue_table="t", kind=10002, bucket="b", run_id="r")

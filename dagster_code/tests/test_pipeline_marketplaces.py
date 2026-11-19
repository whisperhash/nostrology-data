"""Unit tests for pipeline/marketplaces - the /marketplaces metrics compute.

Two kinds of pin here:
  * DuckDB-parity primitives (_duck_trim, duck_try_cast_bigint) are pinned to the probed
    DuckDB 1.5.x behavior (2026-08-23) - these are what keep this compute in exact agreement
    with the cross-check generator (crosscheck/build_markets_snapshot.py).
  * The methodology itself (current-version tie-break, farm detection, exclusion precedence,
    NIP-09 author scoping, the plausibility window on published_at, dates capped at the run,
    the aggregate sums) via synthetic fixtures small enough to hand-verify.

Needs pycountry + reverse_geocoder installed (the geolib loads its gazetteer at import) -
same deps the image pins; run inside the refresh venv locally.
"""
import ast
import collections
import copy
import datetime as dt
import gzip
import hashlib
import json
import os
import random
import re
import tempfile

import pytest

from dagster_code.pipeline import marketplaces as mk

NOW = int(dt.datetime(2026, 8, 23, 12, 0, 0, tzinfo=dt.timezone.utc).timestamp())


def _ev(pubkey="a" * 64, d_tag="d1", created_at=NOW - 86400, tags=None, content="body",
        eid="e" * 64, **extra_tags):
    """A minimal kind-30402 (markets-product-listings) event row as the extract parses it."""
    t = list(tags or [])
    for name, val in extra_tags.items():
        t.append([name.replace("_", "-") if name == "pickup_country" else name, val])
    return {"id": eid, "pubkey": pubkey, "created_at": created_at, "content": content,
            "tags": t, "d_tag": d_tag}


def _del(tag, value, author, created_at):
    """One row of the kind-5 (event-deletion-requests) extract, as it parses."""
    return {"author": author, "tag": tag, "value": value, "created_at": created_at}


def _pk(ch):
    return ch * 64


# --- DuckDB-parity primitives ---------------------------------------------------------------

def test_duck_try_cast_bigint_pinned_probe_values():
    # Pinned against DuckDB 1.5.x try_cast(VARCHAR AS BIGINT), probed 2026-08-23.
    assert mk.duck_try_cast_bigint("7") == 7
    assert mk.duck_try_cast_bigint(" 7 ") == 7
    assert mk.duck_try_cast_bigint("+7") == 7
    assert mk.duck_try_cast_bigint("12.5") == 13     # via DOUBLE, half AWAY from zero
    assert mk.duck_try_cast_bigint("-12.5") == -13
    assert mk.duck_try_cast_bigint("13.5") == 14
    assert mk.duck_try_cast_bigint("12.4") == 12
    assert mk.duck_try_cast_bigint(".5") == 1
    assert mk.duck_try_cast_bigint("1e3") == 1000
    assert mk.duck_try_cast_bigint("0x10") == 16
    assert mk.duck_try_cast_bigint("12.0") == 12
    assert mk.duck_try_cast_bigint("") is None
    assert mk.duck_try_cast_bigint("inf") is None
    assert mk.duck_try_cast_bigint("nan") is None
    assert mk.duck_try_cast_bigint("abc") is None
    assert mk.duck_try_cast_bigint(None) is None
    assert mk.duck_try_cast_bigint(str(2 ** 70)) is None  # BIGINT overflow -> NULL


def test_duck_trim_strips_zs_but_not_control_whitespace():
    # DuckDB trim() strips Unicode space separators (Zs) only - NOT \t \n \r.
    assert mk._duck_trim(" a ") == "a"
    assert mk._duck_trim("\u00a0a\u00a0") == "a"  # NBSP stripped
    assert mk._duck_trim("\u3000a") == "a"  # ideographic space stripped
    assert mk._duck_trim("\ta\t") == "\ta\t"          # tab NOT stripped
    assert mk._duck_trim("\na") == "\na"              # newline NOT stripped
    assert mk._duck_trim("") == ""
    assert mk._duck_trim(None) == ""


def test_tag_value_first_tag_and_arity():
    tags = [["title"], ["title", "second"], ["price", "10", "USD"]]
    # DuckDB [1][2]: FIRST matching tag; a 1-element tag yields NULL, not the next tag.
    assert mk.tag_value(tags, "title") is None
    assert mk.tag_value(tags, "price") == "10"
    assert mk.tag_value(tags, "missing") is None
    assert mk._tag_values_all(tags, "title") == [None, "second"]


# --- selection ------------------------------------------------------------------------------

def test_current_versions_newest_created_lowest_id_tiebreak():
    evs = [
        _ev(eid="b" * 64, created_at=100),
        _ev(eid="a" * 64, created_at=200),
        _ev(eid="c" * 64, created_at=200),  # same created_at as 'a...': lower id wins
    ]
    cur = mk.current_versions(evs)
    assert len(cur) == 1 and cur[0]["id"] == "a" * 64


def test_current_versions_null_d_tag_is_its_own_coordinate():
    evs = [_ev(d_tag=None, eid="a" * 64), _ev(d_tag="", eid="b" * 64)]
    assert len(mk.current_versions(evs)) == 2  # NULL and '' do NOT collapse


# --- farm detection -------------------------------------------------------------------------

def test_farm_needs_four_pubkeys_and_at_most_one_distinct_price():
    farm_evs = [_ev(pubkey=_pk(c), d_tag=f"d{c}", eid=c * 64,
                    tags=[["title", "Same Thing"], ["price", "1", "USD"]])
                for c in "1234"]
    assert mk.farm_titles(farm_evs) == {"same thing"}

    # Four pubkeys at FOUR different prices: a real market, not a farm.
    market_evs = [_ev(pubkey=_pk(c), d_tag=f"d{c}", eid=c * 64,
                      tags=[["title", "Buying BTC"], ["price", str(i), "NGN"]])
                  for i, c in enumerate("1234")]
    assert mk.farm_titles(market_evs) == set()

    # Price-less titles qualify VACUOUSLY (count(DISTINCT) over zero non-null prices is 0).
    bare_evs = [_ev(pubkey=_pk(c), d_tag=f"d{c}", eid=c * 64, tags=[["title", "Bare"]])
                for c in "5678"]
    assert mk.farm_titles(bare_evs) == {"bare"}

    # Three pubkeys is below the floor.
    few = [_ev(pubkey=_pk(c), d_tag=f"d{c}", eid=c * 64, tags=[["title", "Trio"]])
           for c in "123"]
    assert mk.farm_titles(few) == set()


def test_farm_counts_span_all_versions_not_just_current():
    # The same coordinate revised by 4 pubkeys' events... farms are detected across the RAW
    # set, so 4 distinct pubkeys each publishing ONE (superseded or not) version still trip it.
    evs = []
    for i, c in enumerate("1234"):
        evs.append(_ev(pubkey=_pk(c), d_tag="d", eid=c * 64, created_at=100 + i,
                       tags=[["title", "Farmed"]]))
        evs.append(_ev(pubkey=_pk(c), d_tag="d", eid=("f%s" % c) + "0" * 62,
                       created_at=200 + i, tags=[["title", "Renamed"]]))
    assert "farmed" in mk.farm_titles(evs)


# --- per-record fields + classification -----------------------------------------------------

def test_unrenderable_requires_all_four_signals_absent():
    base = dict(tags=[], content="")
    assert mk.build_records([_ev(**base)], mk.NO_DELETIONS)[0]["unrenderable"] is True
    assert mk.build_records([_ev(tags=[["title", "x"]], content="")], mk.NO_DELETIONS)[0]["unrenderable"] is False
    assert mk.build_records([_ev(tags=[], content="words")], mk.NO_DELETIONS)[0]["unrenderable"] is False
    assert mk.build_records([_ev(tags=[["summary", "s"]], content="")], mk.NO_DELETIONS)[0]["unrenderable"] is False
    # ANY image tag counts, even a bare 1-element one.
    assert mk.build_records([_ev(tags=[["image"]], content="")], mk.NO_DELETIONS)[0]["unrenderable"] is False
    # Tab-only content is NOT trimmed by DuckDB trim() -> renderable (parity with the generator).
    assert mk.build_records([_ev(tags=[], content="\t")], mk.NO_DELETIONS)[0]["unrenderable"] is False


def test_synthetic_test_words_and_boundaries():
    def rec(title):
        return mk.build_records([_ev(tags=[["title", title]])], mk.NO_DELETIONS)[0]

    assert rec("test listing")["synthetic"] is True
    assert rec("TEST")["synthetic"] is True
    assert rec("my test")["synthetic"] is True
    assert rec("prueba de venta")["synthetic"] is True
    # Word-boundary: 'testing' matches; 'attest'/'contest' must NOT (regex requires a
    # non-letter or edge on both sides of the word).
    assert rec("attest")["synthetic"] is False
    assert rec("contest prize")["synthetic"] is False
    # 'demo'/'sample' are deliberately not test words.
    assert rec("2010 Specialized Demo 8")["synthetic"] is False
    assert rec("sample pack")["synthetic"] is False


def test_nip09_deleted_matches_coalesced_null_d_tag():
    ev = _ev(d_tag=None, tags=[["title", "x"]])
    coord = "30402:" + ev["pubkey"] + ":"
    deletions = mk.Deletions({coord: ev["created_at"]}, frozenset())
    assert mk.build_records([ev], deletions)[0]["nip09_deleted"] is True
    assert mk.build_records([ev], mk.NO_DELETIONS)[0]["nip09_deleted"] is False


def test_classify_precedence_order():
    bulk_pk = "7d33ba57d8a6" + "0" * 52
    r = mk.build_records([_ev(pubkey=bulk_pk, tags=[["title", "test"], ["status", "sold"],
                                                    ["client", "Ulendo"]])], mk.NO_DELETIONS)
    mk.classify(r)
    assert r[0]["excl"] == "bulk"  # bulk outranks carrier/synthetic/withdrawn

    r = mk.build_records([_ev(tags=[["title", "test"], ["client", "Ulendo"]])], mk.NO_DELETIONS)
    mk.classify(r)
    assert r[0]["excl"] == "carrier"  # carrier outranks synthetic (lookup lowercases)

    r = mk.build_records([_ev(tags=[["title", "test"], ["status", "sold"]])], mk.NO_DELETIONS)
    mk.classify(r)
    assert r[0]["excl"] == "synthetic"  # synthetic outranks withdrawn

    ev = _ev(tags=[["title", "ok"], ["status", "sold"]])
    coord = "30402:" + ev["pubkey"] + ":" + ev["d_tag"]
    r = mk.build_records([ev], mk.Deletions({coord: ev["created_at"]}, frozenset()))
    mk.classify(r)
    assert r[0]["excl"] == "deleted"  # deleted outranks withdrawn


def test_withdrawn_semantics():
    def excl(tags):
        r = mk.build_records([_ev(tags=[["title", "ok"]] + tags)], mk.NO_DELETIONS)
        mk.classify(r)
        return r[0]["excl"]

    for status in sorted(mk.WITHDRAWN):
        assert excl([["status", status]]) == "withdrawn"
    assert excl([["status", "active"]]) is None
    assert excl([["visibility", "hidden"]]) == "withdrawn"
    assert excl([["visibility", "<hidden"]]) == "withdrawn"
    assert excl([["stock", "0"]]) == "withdrawn"
    assert excl([["stock", "-1"]]) == "withdrawn"
    assert excl([["stock", "3"]]) is None
    assert excl([["stock", "unknown"]]) is None  # unparsable stock is NOT withdrawn


# --- build_snapshot end-to-end --------------------------------------------------------------

def _fixture_events():
    """A hand-verifiable pool: 3 live listings (2 sellers), 1 withdrawn, 1 deleted."""
    day = 86400
    return [
        _ev(pubkey=_pk("a"), d_tag="l1", eid="1" * 64, created_at=NOW - 400 * day,
            tags=[["title", "Alpha"], ["price", "100", "USD"], ["t", "Books"],
                  ["location", "Lisbon"]]),
        _ev(pubkey=_pk("a"), d_tag="l2", eid="2" * 64, created_at=NOW - 10 * day,
            tags=[["title", "Beta"], ["price", "21", "sats"], ["t", "books"]]),
        _ev(pubkey=_pk("b"), d_tag="l3", eid="3" * 64, created_at=NOW - 100 * day,
            tags=[["title", "Gamma"], ["published_at", str(NOW - 200 * day)],
                  ["g", "eycs0"], ["price", "5", "EUR"]]),
        _ev(pubkey=_pk("b"), d_tag="l4", eid="4" * 64, created_at=NOW - 50 * day,
            tags=[["title", "Sold thing"], ["status", "sold"]]),
        _ev(pubkey=_pk("c"), d_tag="l5", eid="5" * 64, created_at=NOW - 5 * day,
            tags=[["title", "Deleted thing"]]),
    ]


def _fixture_shipping():
    return [
        {"id": "s" * 64, "pubkey": _pk("a"), "created_at": NOW - 86400, "content": "",
         "tags": [["country", "PT", "ES"], ["pickup-country", "PT"]], "d_tag": "sh1"},
        # A shipping option from a pubkey outside the pool with no exclusion history -
        # counted (only bulk/carrier/synthetic publishers' options are dropped).
        {"id": "t" * 64, "pubkey": _pk("z"), "created_at": NOW - 86400, "content": "",
         "tags": [["country", "USA"]], "d_tag": "sh2"},
    ]


def _fixture_deletions():
    return [_del("a", "30402:" + _pk("c") + ":l5", _pk("c"), NOW - 5 * 86400)]


def test_build_snapshot_summary_and_invariants():
    recs, data = mk.build_snapshot(_fixture_events(), _fixture_shipping(), _fixture_deletions(), NOW)
    s = data["summary"]
    assert s["archive_events"] == 5
    assert s["current_coordinates"] == 5
    assert s["active_listings"] == 3
    assert s["sellers"] == 2
    assert s["withdrawn_any"] == 1 and s["deleted_any"] == 1
    assert {e["k"]: e["v"] for e in data["exclusions"]}["Withdrawn"] == 1
    assert {e["k"]: e["v"] for e in data["exclusions"]}["Deleted (NIP-09)"] == 1
    # The page renders each reason verbatim: the kind carries its slug from nozzl's kinds.toml.
    why = {e["k"]: e["why"] for e in data["exclusions"]}["Deleted (NIP-09)"]
    assert "kind-5 (event-deletion-requests)" in why
    assert "newest version" in why
    # published_at (plausible) dates Gamma at NOW-200d, not its created_at (NOW-100d) -
    # so of the pool only Beta (10d) falls inside the 90-day window.
    assert s["active_90d"] == 1
    assert s["sellers_90d"] == 1
    assert s["added_30d"] == 1
    assert mk.check_invariants(recs, data) == []


def test_build_snapshot_currency_grouping_and_coverage():
    # With the deletion applied the pool is exactly the three priced listings.
    _, data = mk.build_snapshot(_fixture_events(), [], _fixture_deletions(), NOW)
    by = {c["cur"]: c for c in data["currency"]}
    assert by["Bitcoin (BTC / sats)"]["listings"] == 1
    assert by["USD"]["listings"] == 1
    assert by["EUR"]["listings"] == 1
    assert data["currency_coverage"] == {"priced": 3, "total": 3}


def test_build_snapshot_geo_and_shipping():
    recs, data = mk.build_snapshot(_fixture_events(), _fixture_shipping(), _fixture_deletions(), NOW)
    by_cc = {c["cc"]: c for c in data["countries"]}
    assert "PT" in by_cc          # Lisbon resolves via the gazetteer
    assert data["geo_attributable"] >= 1
    ship = {p["cc"]: p for p in data["shipping"]}
    assert ship["PT"]["sellers"] == 1 and ship["ES"]["options"] == 1
    assert ship["US"]["options"] == 1          # 'USA' alias normalized
    assert data["shipping_coverage"]["options_total"] == 2
    assert data["shipping_coverage"]["with_country"] == 2


def test_build_snapshot_shipping_drops_bulk_publisher_options():
    bulk_pk = "7d33ba57d8a6" + "0" * 52
    listing = _ev(pubkey=bulk_pk, tags=[["title", "photo"]])
    ship = [{"id": "u" * 64, "pubkey": bulk_pk, "created_at": NOW, "content": "",
             "tags": [["country", "DE"]], "d_tag": "s"}]
    _, data = mk.build_snapshot(_fixture_events() + [listing], _fixture_shipping() + ship, [], NOW)
    assert "DE" not in {p["cc"] for p in data["shipping"]}


def test_build_snapshot_implausible_published_at_falls_back_to_created():
    day = 86400
    evs = [
        _ev(tags=[["title", "Future"], ["published_at", str(NOW + 999 * day)]],
            created_at=NOW - 2 * day, d_tag="f", eid="a" * 64),
        _ev(tags=[["title", "Ancient"], ["published_at", "42"]],
            created_at=NOW - 3 * day, d_tag="g", eid="b" * 64, pubkey=_pk("d")),
    ]
    _, data = mk.build_snapshot(evs, [], [], NOW)
    assert data["summary"]["added_30d"] == 2  # both dated by created_at, not the bogus tags


def test_build_snapshot_monthly_walk_fills_gap_months():
    day = 86400
    evs = [
        _ev(d_tag="a", eid="a" * 64, created_at=NOW - 200 * day, tags=[["title", "Old"]]),
        _ev(d_tag="b", eid="b" * 64, created_at=NOW, tags=[["title", "New"]]),
    ]
    _, data = mk.build_snapshot(evs, [], [], NOW)
    months = [m["m"] for m in data["new_monthly"]]
    assert len(months) >= 7 and months == sorted(months)
    assert any(m["v"] == 0 for m in data["new_monthly"])  # the gap months are present as zeros
    assert data["partial_month"] == "2026-08"  # the run's month, which here holds a listing
    assert months[-1] == "2026-08"


def test_build_snapshot_partial_month_is_the_runs_month():
    """partial_month names the month the run falls in, not the newest listing's month. At
    02:16 UTC on the 1st, before anything is dated in the new month, the month just completed
    is whole (it used to come back as partial_month and be drawn as partial), and the run's
    month has no new_monthly row yet."""
    first = int(dt.datetime(2026, 9, 1, 2, 16, 30, tzinfo=dt.timezone.utc).timestamp())
    day = 86400
    evs = [
        _ev(d_tag="a", eid="a" * 64, created_at=first - 40 * day, tags=[["title", "July"]]),
        _ev(d_tag="b", eid="b" * 64, created_at=first - 3 * 3600, tags=[["title", "Aug 31"]]),
    ]
    recs, data = mk.build_snapshot(evs, [], [], first)
    assert [m["m"] for m in data["new_monthly"]] == ["2026-07", "2026-08"]
    assert data["partial_month"] == "2026-09"
    assert data["generated_utc"] == "2026-09-01 02:16:30Z"
    assert mk.check_invariants(recs, data) == []


def test_build_snapshot_dates_nothing_after_the_run():
    """A listing dated after the run (the horizon admits created_at up to two days ahead) is
    dated at the run: it counts on the run's day and in its month, the buckets still filling,
    so no month, day or last_listing in the document is after generated_utc. Uncapped, this Aug
    31 run gave September a full bar after the partial August. A later run takes its own date."""
    last = int(dt.datetime(2026, 8, 31, 2, 16, 30, tzinfo=dt.timezone.utc).timestamp())
    ahead = last + 22 * 3600 + 44 * 60  # Sep 1 01:00:30 UTC
    day = 86400
    evs = [
        _ev(d_tag="a", eid="a" * 64, created_at=last - 20 * day, tags=[["title", "Aug 11"]]),
        # from a seller with no other listing, so the first-time sellers line is capped too
        _ev(pubkey=_pk("b"), d_tag="b", eid="b" * 64, created_at=ahead, tags=[["title", "Sep 1"]]),
    ]
    recs, data = mk.build_snapshot(evs, [], [], last)
    assert data["new_monthly"] == [{"m": "2026-08", "v": 2, "s": 2}]
    assert data["partial_month"] == "2026-08"
    assert data["cum_listings"][-1] == {"d": "2026-08-31", "v": 2}
    assert data["cum_sellers"][-1] == {"d": "2026-08-31", "v": 2}
    assert data["summary"]["last_listing"] == "2026-08-31"
    assert {r["d_tag"]: r["when_ts"] for r in recs}["b"] == last  # marketplaces_listings.effective_ts
    assert mk.check_invariants(recs, data) == []

    # Two days on, the run is after it: it takes its own date, and August, now whole, keeps
    # only its own listing.
    recs, data = mk.build_snapshot(evs, [], [], last + 2 * day)
    assert data["new_monthly"] == [{"m": "2026-08", "v": 1, "s": 1}, {"m": "2026-09", "v": 1, "s": 1}]
    assert data["partial_month"] == "2026-09"
    assert data["summary"]["last_listing"] == "2026-09-01"
    assert {r["d_tag"]: r["when_ts"] for r in recs}["b"] == ahead
    assert mk.check_invariants(recs, data) == []


def test_build_snapshot_cap_and_published_at():
    """The path 19 of the 24 real ahead-dated listings took: published_at is ahead too (the
    same clock), fails the plausibility test, and the created_at it falls back to is capped.
    The other direction: an edit dated ahead of a listing whose published_at is plausible keeps
    published_at - the cap never touches that branch."""
    last = int(dt.datetime(2026, 8, 31, 2, 16, 30, tzinfo=dt.timezone.utc).timestamp())
    ahead = last + 22 * 3600 + 44 * 60  # Sep 1 01:00:30 UTC
    long_ago = last - 100 * 86400
    evs = [
        _ev(pubkey=_pk("c"), d_tag="c", eid="c" * 64, created_at=ahead,
            tags=[["title", "Sep 1"], ["published_at", str(ahead)]]),
        _ev(pubkey=_pk("d"), d_tag="d", eid="d" * 64, created_at=ahead,
            tags=[["title", "Edited"], ["published_at", str(long_ago)]]),
    ]
    recs, data = mk.build_snapshot(evs, [], [], last)
    assert {r["d_tag"]: r["when_ts"] for r in recs} == {"c": last, "d": long_ago}
    assert mk.check_invariants(recs, data) == []
    recs, _ = mk.build_snapshot(evs, [], [], last + 2 * 86400)
    assert {r["d_tag"]: r["when_ts"] for r in recs} == {"c": ahead, "d": long_ago}


def test_build_snapshot_rejects_cross_author_deletion_pair():
    with pytest.raises(ValueError, match="non-self-authored"):
        mk.build_snapshot(_fixture_events(), [], [_del("a", "30402:" + _pk("c") + ":l5", _pk("a"), NOW)], NOW)


# --- the NIP-09 rule: what a request has to say for a listing to count as deleted -----------
# Behind each case, the census of 2026-11-04:
# reading `a` tags alone left 434 deleted listings live, and comparing no dates
# excluded 74 republished after their request. The reference generator states the same rule in
# SQL, so a change here without one there fails the agreement gate.

_SELLER = "c" * 64
_V1, _V2, _V3 = "1" * 64, "2" * 64, "3" * 64
_COORD = "30402:" + _SELLER + ":shoes"
_HORIZON = NOW + 2 * 86400


def _versions(*dated):
    """One coordinate's versions, each (id, created_at), with the seller and `d` tag fixed."""
    return [_ev(pubkey=_SELLER, d_tag="shoes", eid=eid, created_at=at, tags=[["title", "shoes"]])
            for eid, at in dated]


def _one(events, rows):
    """The single record the coordinate reduces to, through the real parse and classify."""
    recs = mk.build_records(events, mk.parse_deletions(rows, events, _HORIZON))
    mk.classify(recs)
    assert len(recs) == 1
    return recs[0]


@pytest.mark.parametrize("request_at, deleted", [
    (NOW - 99, True),   # after the version: covered
    (NOW - 100, True),  # the same second: NIP-09 says "up to", and /git's rule N6 is >= too
    (NOW - 101, False),  # before it: the seller republished after asking, so it is live
])
def test_a_tag_reaches_each_version_up_to_its_own_date(request_at, deleted):
    rec = _one(_versions((_V1, NOW - 100)), [_del("a", _COORD, _SELLER, request_at)])
    assert rec["nip09_deleted"] is deleted
    assert rec["excl"] == ("deleted" if deleted else None)


def test_a_listing_republished_after_its_request_is_live_and_a_later_request_takes_it_again():
    # The shape of the 74: deleted, then published again under the same `d`.
    events = _versions((_V1, NOW - 300), (_V2, NOW - 100))
    first = _del("a", _COORD, _SELLER, NOW - 200)
    assert _one(events, [first])["excl"] is None
    second = _del("a", _COORD, _SELLER, NOW - 50)
    assert _one(events, [first, second])["excl"] == "deleted"
    # The newest request decides whichever order the extract's parts arrive in.
    assert _one(events, [second, first])["excl"] == "deleted"


def test_e_tag_on_the_current_version_deletes_the_listing_whatever_its_date():
    # The shape of the 434. An id names that one event exactly, so the request's own date is not
    # consulted, unlike an `a` tag's: a request dated before the event still names it.
    events = _versions((_V1, NOW - 100))
    assert _one(events, [_del("e", _V1, _SELLER, NOW - 50)])["excl"] == "deleted"
    assert _one(events, [_del("e", _V1, _SELLER, NOW - 500)])["excl"] == "deleted"


def test_e_tag_on_an_older_version_leaves_the_listing_live():
    # The 43 rows whose request names only a superseded version. A newer version is not what the
    # author asked to remove, and comparing the request's date would wrongly take it: this
    # request is dated AFTER the current version and still must not delete it.
    events = _versions((_V1, NOW - 300), (_V2, NOW - 100))
    rec = _one(events, [_del("e", _V1, _SELLER, NOW - 50)])
    assert rec["event_id"] == _V2
    assert rec["nip09_deleted"] is False and rec["excl"] is None


def test_either_form_is_enough():
    # A stale `a` tag cannot excuse an `e` tag on the current version (2 of the 74 stay out so).
    events = _versions((_V1, NOW - 300), (_V2, NOW - 100))
    rows = [_del("a", _COORD, _SELLER, NOW - 200), _del("e", _V2, _SELLER, NOW - 50)]
    assert _one(events, rows)["excl"] == "deleted"


def test_e_tag_naming_an_id_we_do_not_hold_is_dropped_not_refused():
    # A version that landed after the listings extract, or one past the horizon: normal.
    assert _one(_versions((_V1, NOW - 100)), [_del("e", _V3, "d" * 64, NOW - 50)])["excl"] is None


def test_a_null_d_tag_coordinate_is_compared_by_date_too():
    ev = _ev(pubkey=_SELLER, d_tag=None, eid=_V1, created_at=NOW - 100, tags=[["title", "x"]])
    coord = "30402:" + _SELLER + ":"
    assert _one([ev], [_del("a", coord, _SELLER, NOW - 100)])["excl"] == "deleted"
    assert _one([ev], [_del("a", coord, _SELLER, NOW - 101)])["excl"] is None


@pytest.mark.parametrize("request_at, deleted", [
    (_HORIZON, True),       # the horizon itself is inside, as it is for a listing event
    (_HORIZON + 1, False),  # past it the request is not part of the dataset
])
def test_each_form_stops_at_the_horizon(request_at, deleted):
    events = _versions((_V1, NOW - 100))
    assert _one(events, [_del("a", _COORD, _SELLER, request_at)])["nip09_deleted"] is deleted
    assert _one(events, [_del("e", _V1, _SELLER, request_at)])["nip09_deleted"] is deleted


def test_a_future_dated_request_cannot_mask_an_earlier_valid_one():
    # Why the extract returns rows and not one newest date per pair. Against a coordinate
    # republished between them, a masked earlier request would change the answer the other way
    # too, so both directions are here.
    events = _versions((_V1, NOW - 100))
    rows = [_del("a", _COORD, _SELLER, NOW - 50), _del("a", _COORD, _SELLER, _HORIZON + 1)]
    assert _one(events, rows)["excl"] == "deleted"
    assert _one(events, list(reversed(rows)))["excl"] == "deleted"
    only_stale = [_del("a", _COORD, _SELLER, NOW - 200), _del("a", _COORD, _SELLER, _HORIZON + 1)]
    assert _one(events, only_stale)["excl"] is None


@pytest.mark.parametrize("row", [
    _del("a", "30402:" + "d" * 64 + ":shoes", _SELLER, NOW),  # another key's coordinate
    _del("a", "30402", _SELLER, NOW),                         # no pubkey in it to check
    _del("e", _V1, "d" * 64, NOW),                            # another key's version
    # Refused even past the horizon: a broken extract is broken whatever the date.
    _del("a", "30402:" + "d" * 64 + ":shoes", _SELLER, _HORIZON + 1),
    _del("e", _V1, "d" * 64, _HORIZON + 1),
])
def test_a_row_that_is_not_self_authored_fails_loud(row):
    with pytest.raises(ValueError, match="non-self-authored"):
        mk.parse_deletions([row], _versions((_V1, NOW - 100)), _HORIZON)


def test_an_unknown_tag_or_a_missing_date_fails_loud():
    with pytest.raises(ValueError, match="unknown tag"):
        mk.parse_deletions([_del("p", _SELLER, _SELLER, NOW)], [], _HORIZON)
    with pytest.raises(KeyError):
        mk.parse_deletions([{"author": _SELLER, "tag": "a", "value": _COORD}], [], _HORIZON)


def test_precedence_holds_for_a_listing_deleted_by_id():
    # Withdrawn AND deleted reads deleted, as for an `a` tag.
    sold = [_ev(pubkey=_SELLER, d_tag="shoes", eid=_V1, created_at=NOW - 100,
                tags=[["title", "shoes"], ["status", "sold"]])]
    assert _one(sold, [_del("e", _V1, _SELLER, NOW - 50)])["excl"] == "deleted"
    # Republished after its request and marked sold: not deleted, and still out (11 of the 74).
    assert _one(sold, [_del("a", _COORD, _SELLER, NOW - 200)])["excl"] == "withdrawn"
    # An earlier group keeps it, and the unconditioned count still sees the deletion.
    test_word = [_ev(pubkey=_SELLER, d_tag="shoes", eid=_V1, created_at=NOW - 100,
                     tags=[["title", "test"]])]
    rec = _one(test_word, [_del("e", _V1, _SELLER, NOW - 50)])
    assert rec["excl"] == "synthetic" and rec["nip09_deleted"] is True


def test_build_snapshot_counts_both_forms_and_the_stale_request():
    day = 86400
    events = _fixture_events() + [
        # Deleted by id: leaves the pool, and takes its seller with it.
        _ev(pubkey=_pk("d"), d_tag="m1", eid="6" * 64, created_at=NOW - 4 * day,
            tags=[["title", "Deleted by id"]]),
        # Named by an older request and republished since: stays in the pool.
        _ev(pubkey=_pk("e"), d_tag="m2", eid="7" * 64, created_at=NOW - 3 * day,
            tags=[["title", "Back again"]]),
    ]
    rows = _fixture_deletions() + [
        _del("e", "6" * 64, _pk("d"), NOW - 2 * day),
        _del("a", "30402:" + _pk("e") + ":m2", _pk("e"), NOW - 9 * day),
    ]
    recs, data = mk.build_snapshot(events, [], rows, NOW)
    s = data["summary"]
    assert {r["d_tag"]: r["excl"] for r in recs if r["pubkey"] in (_pk("d"), _pk("e"))} == {
        "m1": "deleted", "m2": None}
    assert s["active_listings"] == 4 and s["sellers"] == 3
    assert s["deleted_any"] == 2
    assert {e["k"]: e["v"] for e in data["exclusions"]}["Deleted (NIP-09)"] == 2
    assert mk.check_invariants(recs, data) == []


def test_build_snapshot_applies_the_horizon_and_the_ownership_check_to_requests():
    # Through the compute itself and not parse_deletions alone: no real request sits near the
    # horizon, so nothing else would notice build_snapshot passing the wrong bound or the wrong
    # events, the agreement gate included.
    coord = "30402:" + _pk("c") + ":l5"
    excl = lambda rows: next(r["excl"] for r in mk.build_snapshot(_fixture_events(), [], rows, NOW)[0]
                             if r["d_tag"] == "l5")
    horizon = NOW + 2 * 86400
    assert excl([_del("a", coord, _pk("c"), horizon)]) == "deleted"
    assert excl([_del("a", coord, _pk("c"), horizon + 1)]) is None
    assert excl([_del("e", "5" * 64, _pk("c"), horizon)]) == "deleted"
    assert excl([_del("e", "5" * 64, _pk("c"), horizon + 1)]) is None
    with pytest.raises(ValueError, match="non-self-authored"):
        mk.build_snapshot(_fixture_events(), [], [_del("e", "5" * 64, _pk("a"), NOW)], NOW)


def _floor_rows(a_pairs, e_pairs):
    return ([_del("a", f"30402:{_SELLER}:{i}", _SELLER, NOW) for i in range(a_pairs)]
            + [_del("e", f"{i:064x}", _SELLER, NOW) for i in range(e_pairs)])


def _floor_message(rows):
    with pytest.raises(RuntimeError, match="refusing to compute") as caught:
        mk.check_deletion_floors(rows)
    return str(caught.value)


def test_deletion_floors_hold_per_tag_so_a_dead_join_cannot_hide_behind_the_other_form():
    full_a, full_e = mk.MIN_DELETION_PAIRS, mk.MIN_DELETION_EVENT_PAIRS
    mk.check_deletion_floors(_floor_rows(full_a, full_e))  # both exactly at the floor: no raise
    # The `e` side alone gone: the case one shared floor would wave through.
    short_e = _floor_message(_floor_rows(full_a + full_e, full_e - 1))
    assert "MIN_DELETION_EVENT_PAIRS" in short_e and "MIN_DELETION_PAIRS" not in short_e
    short_a = _floor_message(_floor_rows(full_a - 1, full_e + full_a))
    assert "MIN_DELETION_PAIRS" in short_a and "MIN_DELETION_EVENT_PAIRS" not in short_a
    both = _floor_message([])
    assert "MIN_DELETION_PAIRS" in both and "MIN_DELETION_EVENT_PAIRS" in both


def test_deletion_floors_count_pairs_so_repeat_requests_cannot_prop_one_up():
    repeats = [_del("e", _V1, _SELLER, NOW - i) for i in range(mk.MIN_DELETION_EVENT_PAIRS)]
    short = _floor_message(_floor_rows(mk.MIN_DELETION_PAIRS, 0) + repeats)
    assert "1 (author, value) pairs for tag 'e'" in short


# The two lines of assets.py this rule rests on, checked with no dagster so that they run in the
# quick local gate: test_marketplaces_wiring.py proves the job honours the edge, but it needs the
# image and this repo has no CI.

def _assets_module():
    with open(os.path.join(os.path.dirname(__file__), "..", "assets.py"), encoding="utf-8") as fh:
        return ast.parse(fh.read())


def test_the_deletions_extract_is_declared_after_the_listings_extract():
    call = next(n.value for n in _assets_module().body if isinstance(n, ast.Assign)
                and getattr(n.targets[0], "id", "") == "marketplaces_deletions_extract")
    after = next(k.value for k in call.keywords if k.arg == "after")
    assert [e.id for e in after.elts] == ["marketplaces_listings_extract"]


def test_the_snapshot_asset_checks_the_floors_before_it_computes():
    fn = next(n for n in _assets_module().body
              if isinstance(n, ast.FunctionDef) and n.name == "marketplaces_snapshot")
    called = {c.func.attr: c.lineno for c in ast.walk(fn)
              if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)}
    assert called["check_deletion_floors"] < called["build_snapshot"]


def test_build_snapshot_category_floor_and_stopwords():
    day = 86400
    evs = [_ev(pubkey=_pk(c), d_tag=f"d{c}", eid=c * 64, created_at=NOW - day,
               tags=[["title", f"Item {c}"], ["t", "Vinyl"], ["t", "shopstr"]])
           for c in "12345"]
    _, data = mk.build_snapshot(evs, [], [], NOW)
    cats = {c["cat"] for c in data["categories"]}
    assert "vinyl" in cats            # 5 sellers >= CAT_MIN_SELLERS
    assert "shopstr" not in cats      # stopword
    evs4 = evs[:4]
    _, data4 = mk.build_snapshot(evs4, [], [], NOW)
    assert {c["cat"] for c in data4["categories"]} == set()  # below the 5-seller floor


def test_check_invariants_detects_seeded_corruption():
    recs, data = mk.build_snapshot(_fixture_events(), _fixture_shipping(), _fixture_deletions(), NOW)
    assert mk.check_invariants(recs, data) == []
    data["summary"]["active_listings"] += 1
    assert mk.check_invariants(recs, data) != []


def test_check_invariants_flags_each_date_after_the_run():
    """The guard behind when()'s cap. build_snapshot cannot produce these, so each is seeded
    into a valid document, shaped to leave every other invariant holding: each must be reported
    alone, under its own name."""
    recs, base = mk.build_snapshot(_fixture_events(), _fixture_shipping(), _fixture_deletions(), NOW)
    assert mk.check_invariants(recs, base) == []
    run_day = dt.date.fromisoformat(base["generated_utc"][:10])
    tomorrow = (run_day + dt.timedelta(days=1)).isoformat()
    next_month = (run_day.replace(day=28) + dt.timedelta(days=4)).strftime("%Y-%m")
    seeds = {
        "new_monthly": lambda d: d["new_monthly"].append({"m": next_month, "v": 0, "s": 0}),
        "cum_listings": lambda d: d["cum_listings"].append({"d": tomorrow, "v": d["cum_listings"][-1]["v"]}),
        "cum_sellers": lambda d: d["cum_sellers"].append({"d": tomorrow, "v": d["cum_sellers"][-1]["v"]}),
        "last_listing": lambda d: d["summary"].update(last_listing=tomorrow),
        "partial_month": lambda d: d.update(partial_month=next_month),
    }
    for name, seed in seeds.items():
        data = copy.deepcopy(base)
        seed(data)
        problems = mk.check_invariants(recs, data)
        assert len(problems) == 1 and problems[0].startswith(name), (name, problems)


# --- one row per product: a seller's sizes and colours count with the product ----------------
# The census of 2026-11-04 is behind this. The reference generator's --self-test holds the same cases through its own
# SQL reader, by hand. The agreement gate cannot hold them in step: it compares what the archive
# holds, and the archive holds almost none of these shapes, so most one-sided changes pass it.
# crosscheck/compare_markets_fold.py replays generated events through both, and catches that.

_OTHER = "d" * 64
_VAR = ["type", "variation", "physical"]
_PAR = ["type", "variable", "physical"]


def _of(d, owner=_SELLER):
    return ["a", mk.coordinate(owner, d)]


def _price(amount, unit="USD"):
    return ["price", amount, unit]


def _row(d, *tags, pubkey=_SELLER, at=NOW - 1000, eid=None, title="x"):
    """One listing of a family. The id follows the coordinate and date unless a case needs to set it."""
    return _ev(pubkey=pubkey, d_tag=d, created_at=at, tags=[["title", title], *tags],
               eid=eid or hashlib.sha256(f"{pubkey}:{d}:{at}".encode()).hexdigest())


def _fold(events, rows=()):
    recs = mk.build_records(events, mk.parse_deletions(list(rows), events, _HORIZON))
    mk.classify(recs)
    mk.fold_product_options(recs)
    return recs


def _groups(events, rows=()):
    return {r["d_tag"]: r["excl"] for r in _fold(events, rows)}


def test_a_live_parent_stands_for_its_children():
    assert _groups([_row("tee", _PAR), _row("s", _VAR, _of("tee")), _row("m", _VAR, _of("tee"))]) == {
        "tee": None, "s": "option", "m": "option"}


@pytest.mark.parametrize("parent_tags, group", [
    ([["stock", "0"]], "withdrawn"),  # key e9c9ccd1bc06's shape: parents at stock 0, children at stock 100
    ([["visibility", "hidden"]], "withdrawn"),
    ([["status", "sold"]], "withdrawn"),
])
def test_a_family_whose_parent_is_withdrawn_keeps_its_newest_child(parent_tags, group):
    assert _groups([_row("tee", _PAR, *parent_tags), _row("s", _VAR, _of("tee"), at=NOW - 200),
                    _row("m", _VAR, _of("tee"), at=NOW - 100), _row("l", _VAR, _of("tee"), at=NOW - 300)]) == {
        "tee": group, "s": "option", "m": None, "l": "option"}


def test_a_family_whose_parent_is_deleted_keeps_its_newest_child():
    rows = [_del("a", mk.coordinate(_SELLER, "tee"), _SELLER, NOW)]
    assert _groups([_row("tee", _PAR), _row("s", _VAR, _of("tee"), at=NOW - 100),
                    _row("m", _VAR, _of("tee"), at=NOW - 200)], rows) == {
        "tee": "deleted", "s": None, "m": "option"}


def test_a_family_whose_parent_is_not_in_the_archive_keeps_its_newest_child():
    assert _groups([_row("s", _VAR, _of("gone"), at=NOW - 200), _row("m", _VAR, _of("gone"), at=NOW - 100)]) == {
        "s": "option", "m": None}


@pytest.mark.parametrize("s_id, m_id, want", [
    ("b", "a", {"s": "option", "m": None}),
    ("a", "b", {"s": None, "m": "option"}),
])
def test_the_same_second_goes_to_the_lowest_id_as_nip01_picks_a_version(s_id, m_id, want):
    """Both ways round: one alone would also pass on an order that merely happens to agree with it."""
    assert _groups([_row("s", _VAR, _of("gone"), eid=s_id * 64), _row("m", _VAR, _of("gone"), eid=m_id * 64)]) == want


def test_another_keys_children_fold_only_into_a_row_of_their_own():
    """A stranger's tag: naming someone's listing as a parent folds nothing of theirs and, more to
    the point, cannot make the namer's rows vanish into a product that is not theirs."""
    recs = _fold([_row("tee", _PAR), _row("s", _VAR, _of("tee")),
                  _row("x", _VAR, _of("tee"), pubkey=_OTHER, at=NOW - 200),
                  _row("y", _VAR, _of("tee"), pubkey=_OTHER, at=NOW - 100)])
    assert {r["d_tag"]: r["excl"] for r in recs} == {"tee": None, "s": "option", "x": "option", "y": None}


def test_one_child_under_another_keys_parent_stays():
    assert _groups([_row("tee", _PAR), _row("x", _VAR, _of("tee"), pubkey=_OTHER)]) == {"tee": None, "x": None}


def test_a_child_naming_itself_stays():
    assert _groups([_row("self", _VAR, _of("self"))]) == {"self": None}


def test_a_child_naming_itself_and_a_sibling_naming_it_keep_the_newest():
    assert _groups([_row("self", _VAR, _of("self"), at=NOW - 200), _row("m", _VAR, _of("self"), at=NOW - 100)]) == {
        "self": "option", "m": None}


def test_two_children_naming_each_other_both_stay():
    """Neither stands for the other. If a live child could stand, each would fold into the other and
    the product would leave the pool altogether."""
    assert _groups([_row("a", _VAR, _of("b")), _row("b", _VAR, _of("a"))]) == {"a": None, "b": None}


def test_a_child_of_a_child_stays_and_the_one_under_the_real_parent_folds():
    assert _groups([_row("tee", _PAR), _row("mid", _VAR, _of("tee")), _row("low", _VAR, _of("mid"))]) == {
        "tee": None, "mid": "option", "low": None}


def test_children_with_no_parent_tag_are_no_family():
    assert _groups([_row("s", _VAR), _row("m", _VAR)]) == {"s": None, "m": None}


def test_an_a_tag_of_another_kind_names_no_parent():
    """The spec uses `a` for collections too. Two children in one collection are not one product."""
    collection = ["a", "30405:" + _SELLER + ":shop"]
    assert _groups([_row("s", _VAR, collection), _row("m", _VAR, collection)]) == {"s": None, "m": None}


def test_an_a_tag_with_no_value_names_no_parent_and_the_next_one_does():
    assert _groups([_row("tee", _PAR), _row("s", _VAR, ["a"], _of("tee")), _row("m", _VAR, ["a"])]) == {
        "tee": None, "s": "option", "m": None}


def test_the_collection_tag_ahead_of_the_parent_tag_does_not_hide_it():
    assert _groups([_row("tee", _PAR), _row("s", _VAR, ["a", "30405:" + _SELLER + ":shop"], _of("tee"))]) == {
        "tee": None, "s": "option"}


def test_type_is_read_exactly_and_only_from_the_first_tag():
    assert _groups([_row("tee", _PAR), _row("s", ["type", "Variation"], _of("tee")), _row("m", ["type"], _of("tee")),
                    _row("l", ["type", "simple"], _VAR, _of("tee")), _row("sp", ["type", " variation"], _of("tee")),
                    _row("tr", ["type", "variation "], _of("tee"))]) == {
        "tee": None, "s": None, "m": None, "l": None, "sp": None, "tr": None}


def test_a_coordinate_is_matched_exactly():
    """Capitals or a stray space name a coordinate nobody holds, so the child is under no parent: it
    stays, and the real parent's row is not disturbed."""
    coordinate = mk.coordinate(_SELLER, "tee")
    assert _groups([_row("tee", _PAR), _row("real", _VAR, _of("tee")),
                    _row("caps", _VAR, _of("tee", _SELLER.upper())), _row("caps_d", _VAR, _of("TEE")),
                    _row("after", _VAR, ["a", coordinate + " "]),
                    _row("before", _VAR, ["a", " " + coordinate])]) == {
        "tee": None, "real": "option", "caps": None, "caps_d": None, "after": None, "before": None}


def test_a_kind_merely_starting_with_the_number_names_no_parent():
    other_kind = ["a", "304021:" + _SELLER + ":shop"]
    assert _groups([_row("s", _VAR, other_kind), _row("m", _VAR, other_kind)]) == {"s": None, "m": None}


def test_only_the_first_product_a_tag_names_the_parent():
    assert _groups([_row("tee", _PAR), _row("first", _VAR, _of("tee"), _of("gone")),
                    _row("second", _VAR, _of("gone"), _of("tee"))]) == {"tee": None, "first": "option", "second": None}


def test_a_parent_of_any_type_stands_if_it_is_no_child():
    assert _groups([_row("tee", ["type", "simple"]), _row("s", _VAR, _of("tee")),
                    _row("bare"), _row("m", _VAR, _of("bare"))]) == {
        "tee": None, "s": "option", "bare": None, "m": "option"}


def test_a_child_excluded_for_another_reason_stays_in_its_group():
    rows = [_del("a", mk.coordinate(_SELLER, "gone"), _SELLER, NOW)]
    recs = _fold([_row("tee", _PAR), _row("sold", _VAR, _of("tee"), ["status", "sold"]), _row("gone", _VAR, _of("tee")),
                  _row("testing", _VAR, _of("tee"), title="test"), _row("s", _VAR, _of("tee"))], rows)
    assert {r["d_tag"]: r["excl"] for r in recs} == {
        "tee": None, "sold": "withdrawn", "gone": "deleted", "testing": "synthetic", "s": "option"}


def test_a_parent_with_no_d_tag_is_still_nameable():
    assert _groups([_row(None, _PAR), _row("s", _VAR, _of(None))]) == {None: None, "s": "option"}


def test_a_d_tag_with_colons_in_it():
    assert _groups([_row("a:b:c", _PAR), _row("s", _VAR, _of("a:b:c"))]) == {"a:b:c": None, "s": "option"}


def test_no_d_tag_and_an_empty_one_share_a_coordinate_and_the_newer_stands():
    """Both are live rows of one coordinate string. Which takes the child's price has to be stated,
    or it would follow input order, and Athena's part order is not DuckDB's scan order."""
    events = [_row(None, _PAR, at=NOW - 200), _row("", _PAR, at=NOW - 100),
              _row("s", _VAR, _of(""), _price("9"))]
    for order in (events, events[::-1]):
        recs = {r["d_tag"]: r for r in _fold(order)}
        assert {d: r["excl"] for d, r in recs.items()} == {None: None, "": None, "s": "option"}
        assert recs[""]["price"] == ["price", "9", "USD"] and recs[None]["price"] is None


@pytest.mark.parametrize("order", [1, -1])
def test_a_parent_with_no_price_takes_the_newest_priced_childs(order):
    recs = {r["d_tag"]: r for r in _fold([
        _row("tee", _PAR), *[_row("old", _VAR, _of("tee"), _price("1"), at=NOW - 300),
                             _row("new", _VAR, _of("tee"), _price("2"), at=NOW - 200)][::order],
        _row("none", _VAR, _of("tee"), at=NOW - 100)])}
    assert recs["tee"]["price"] == ["price", "2", "USD"]


@pytest.mark.parametrize("x_id, y_id, amount", [("b", "a", "2"), ("a", "b", "1")])
def test_priced_children_of_the_same_second_give_the_lowest_ids_price(x_id, y_id, amount):
    recs = {r["d_tag"]: r for r in _fold([_row("tee", _PAR), _row("x", _VAR, _of("tee"), _price("1"), eid=x_id * 64),
                                          _row("y", _VAR, _of("tee"), _price("2"), eid=y_id * 64)])}
    assert recs["tee"]["price"] == ["price", amount, "USD"]


def test_a_parent_with_a_price_keeps_it():
    recs = {r["d_tag"]: r for r in _fold([_row("tee", _PAR, _price("9")), _row("s", _VAR, _of("tee"), _price("1"))])}
    assert recs["tee"]["price"] == ["price", "9", "USD"]


@pytest.mark.parametrize("own", [["price", "9"], ["price", "9", ""], ["price", "9", " "]])
def test_a_price_with_no_unit_is_no_price_and_a_childs_replaces_it(own):
    recs = {r["d_tag"]: r for r in _fold([_row("tee", _PAR, own), _row("s", _VAR, _of("tee"), _price("1"))])}
    assert recs["tee"]["price"] == ["price", "1", "USD"]


def test_a_blank_unit_on_a_child_gives_no_price():
    recs = {r["d_tag"]: r for r in _fold([_row("tee", _PAR), _row("s", _VAR, _of("tee"), ["price", "1", " "])])}
    assert recs["tee"]["price"] is None


@pytest.mark.parametrize("amount", ["Infinity", "", None])
def test_an_amount_that_is_no_number_is_still_a_price(amount):
    """The same test the currency figures use: a unit, whatever the amount says. One real parent
    quotes Infinity USD and 165 live rows quote something other than a plain decimal, and two
    meanings of "priced" would be a worse defect than that row."""
    recs = {r["d_tag"]: r for r in _fold([_row("tee", _PAR, _price(amount)), _row("s", _VAR, _of("tee"), _price("35"))])}
    assert recs["tee"]["price"] == ["price", amount, "USD"]


def test_a_withdrawn_childs_price_is_not_taken():
    recs = {r["d_tag"]: r for r in _fold([_row("tee", _PAR), _row("s", _VAR, _of("tee"), _price("1"), ["status", "sold"])])}
    assert recs["tee"]["price"] is None


def test_a_kept_child_with_no_price_takes_a_siblings():
    recs = {r["d_tag"]: r for r in _fold([_row("s", _VAR, _of("gone"), at=NOW - 100),
                                          _row("m", _VAR, _of("gone"), _price("3"), at=NOW - 200)])}
    assert recs["s"]["excl"] is None and recs["s"]["price"] == ["price", "3", "USD"]


def test_every_figure_counts_the_product_once():
    """Through the compute itself: the rule is only worth anything if it reaches each figure."""
    day = 86400
    events = [
        _row("tee", _PAR, ["t", "clothing"], ["location", "Lisbon, Portugal"], at=NOW - 5 * day),
        _row("s", _VAR, _of("tee"), _price("35"), ["t", "clothing"], ["location", "Lisbon, Portugal"], at=NOW - 5 * day),
        _row("m", _VAR, _of("tee"), _price("35"), ["t", "clothing"], ["location", "Lisbon, Portugal"], at=NOW - 5 * day),
        _row("plain", _price("21", "sats"), pubkey=_OTHER, at=NOW - 200 * day),
    ]
    recs, data = mk.build_snapshot(events, [], [], NOW)
    s = data["summary"]
    assert s["current_coordinates"] == 4 and s["active_listings"] == 2 and s["sellers"] == 2
    assert s["added_30d"] == 1 and s["active_90d"] == 1
    assert data["currency_coverage"] == {"priced": 2, "total": 2}
    assert {c["cur"]: c["listings"] for c in data["currency"]} == {"USD": 1, "Bitcoin (BTC / sats)": 1}
    assert data["categories_coverage"]["with_tag"] == 1
    assert sum(m["v"] for m in data["new_monthly"]) == 2
    assert data["cum_listings"][-1]["v"] == 2
    assert {c["client"]: c["listings"] for c in data["clients"]} == {"(untagged)": 2}
    assert data["geo_attributable"] == 1 and sum(p["listings"] for p in data["map_points"]) == 1
    assert mk.check_invariants(recs, data) == []


def test_the_ledger_states_the_group_last_and_still_reconciles():
    recs, data = mk.build_snapshot([_row("tee", _PAR), _row("s", _VAR, _of("tee")), _row("m", _VAR, _of("tee"))], [], [], NOW)
    assert [e["k"] for e in data["exclusions"]] == [
        "Non-retail bulk publishers", "Carrier / staging clients", "Unrenderable", "Synthetic / test listings",
        "Deleted (NIP-09)", "Withdrawn", "Product options"]
    assert data["exclusions"][-1]["v"] == 2
    assert sum(e["v"] for e in data["exclusions"]) + data["summary"]["active_listings"] == data["summary"]["current_coordinates"]
    # What openmarkets.pub's client refuses, which it republishes the ledger through: a label over
    # 100 characters or carrying a control character (its published validator, read 2026-11-04).
    for e in data["exclusions"]:
        assert 0 < len(e["k"]) <= 100 and not re.search(r"[\x00-\x1f\x7f]", e["k"])


def test_write_pg_counts_only_the_rows_left_live():
    """The anti-shrink gate reads `excl`, so a folded row must not hold a shrunken pool above it."""
    recs = _fold([_row("tee", _PAR), _row("s", _VAR, _of("tee")), _row("m", _VAR, _of("tee"))])
    with pytest.raises(RuntimeError, match="active listings 1 < floor 2"):
        mk.write_pg(None, recs, {}, "run", min_active=2)


def test_check_invariants_catches_two_live_rows_of_one_product():
    recs, data = mk.build_snapshot([_row("tee", _PAR, ["stock", "0"]), _row("s", _VAR, _of("tee"), at=NOW - 100),
                                    _row("m", _VAR, _of("tee"), at=NOW - 200)], [], [], NOW)
    assert mk.check_invariants(recs, data) == []
    next(r for r in recs if r["d_tag"] == "m")["excl"] = None
    assert any("2 live rows for one product" in p for p in mk.check_invariants(recs, data))


def test_check_invariants_catches_a_live_child_beside_its_live_parent():
    recs, data = mk.build_snapshot([_row("tee", _PAR), _row("s", _VAR, _of("tee"))], [], [], NOW)
    assert mk.check_invariants(recs, data) == []
    next(r for r in recs if r["d_tag"] == "s")["excl"] = None
    assert any("a live child beside its live parent" in p for p in mk.check_invariants(recs, data))


@pytest.mark.parametrize("seed", range(40))
def test_the_rule_holds_on_anything_a_stranger_can_publish(seed):
    """Every condition at once, against what must hold whatever the tags say. Input order matters most
    here, because Athena's part order is not DuckDB's scan order and a tie that followed it would
    read as a methodology diff at the agreement gate."""
    rng = random.Random(seed)
    keys = [_pk(c) for c in "abc"]
    ds = ["p" + str(i) for i in range(8)] + [None, ""]
    events = []
    for pubkey in keys:
        for d in rng.sample(ds, 7):
            owner, named = rng.choice(keys), rng.choice(ds + ["absent"])
            tags = [["title", "x"]]
            tags.append(rng.choice([_VAR, _VAR, _VAR, _PAR, ["type", "simple"], ["type"]]))
            if rng.random() < 0.85:
                tags.append(_of(named, owner))
            if rng.random() < 0.2:
                tags.append(["stock", "0"])
            if rng.random() < 0.5:
                tags.append(_price(str(rng.randint(1, 9))))
            events.append(_row(d, *tags, pubkey=pubkey, at=NOW - rng.choice((100, 100, 200))))

    def outcome(evs):
        return {(r["pubkey"], r["d_tag"]): (r["excl"], r["price"]) for r in _fold(evs)}

    got = outcome(events)
    shuffled = events[:]
    rng.shuffle(shuffled)
    assert outcome(shuffled) == got

    recs = _fold(events)
    plain = mk.build_records(events, mk.NO_DELETIONS)
    mk.classify(plain)
    # Only ever from live to `option`, and no seller leaves the pool by it.
    before = {(r["pubkey"], r["d_tag"]): r["excl"] for r in plain}
    for r in recs:
        was = before[r["pubkey"], r["d_tag"]]
        assert r["excl"] == was or (was is None and r["excl"] == "option")
    assert {r["pubkey"] for r in recs if r["excl"] is None} == {r["pubkey"] for r in plain if r["excl"] is None}
    # At most one live row per family, under any parent at all.
    families = collections.Counter((r["pubkey"], r["parent"]) for r in recs
                                   if r["excl"] is None and r["product_type"] == "variation" and r["parent"])
    assert all(n == 1 for n in families.values())
    # check_invariants is a blocking asset check, so a stranger's event that tripped it would stop
    # the nightly refresh: naming someone's live listing, say, or a child naming itself.
    assert mk.check_invariants(*mk.build_snapshot(events, [], [], NOW)) == []


# --- ndjson parsing -------------------------------------------------------------------------

def test_parse_ndjson_gz_roundtrip():
    rows = [{"id": "x", "tags": [["title", "T"]], "d_tag": None},
            {"id": "y", "tags": [], "d_tag": ""}]
    with tempfile.TemporaryDirectory() as d:
        p1 = os.path.join(d, "b.gz")
        p2 = os.path.join(d, "a.gz")
        with gzip.open(p1, "wt") as fh:
            fh.write(json.dumps(rows[0]) + "\n")
        with gzip.open(p2, "wt") as fh:
            fh.write(json.dumps(rows[1]) + "\n\n")  # blank line tolerated
        out = mk.parse_ndjson_gz([p1, p2])
    assert [r["id"] for r in out] == ["y", "x"]  # sorted by path for determinism
    assert out[1]["d_tag"] is None and out[0]["d_tag"] == ""


# --- write_pg gate --------------------------------------------------------------------------

class _ExplodingConn:
    def cursor(self):  # pragma: no cover - reaching this IS the failure
        raise AssertionError("write_pg touched the DB despite the anti-shrink gate")

    def commit(self):  # pragma: no cover
        raise AssertionError("write_pg committed despite the anti-shrink gate")


def test_write_pg_anti_shrink_refuses_before_any_db_touch():
    recs, data = mk.build_snapshot(_fixture_events(), [], [], NOW)
    with pytest.raises(RuntimeError, match="anti-shrink"):
        mk.write_pg(_ExplodingConn(), recs, data, run_id="r1", min_active=10_000)


# --- review-hardening pins (2026-08-23 review findings) ----------------------------------

def test_duck_try_cast_bigint_rejects_what_duckdb_rejects():
    # Probed against duckdb 1.5.x on 2026-08-23 (review finding 1):
    assert mk.duck_try_cast_bigint("0b101") == 5       # unsigned binary parses
    assert mk.duck_try_cast_bigint("0B11") == 3
    assert mk.duck_try_cast_bigint("+0x10") is None    # SIGNED hex/binary do NOT
    assert mk.duck_try_cast_bigint("-0x10") is None
    assert mk.duck_try_cast_bigint("+0b1") is None
    assert mk.duck_try_cast_bigint("0o17") is None     # octal never parses
    assert mk.duck_try_cast_bigint("\t7\n") == 7       # ASCII whitespace padding ok
    assert mk.duck_try_cast_bigint(" 42 ") is None  # NBSP padding is NOT
    assert mk.duck_try_cast_bigint("١٢٣") is None  # Arabic-Indic digits
    assert mk.duck_try_cast_bigint("１２") is None        # fullwidth digits


def test_duck_lower_simple_fold_matches_duckdb():
    # DuckDB lower() is a SIMPLE per-codepoint fold (probed 2026-08-23); Python .lower()
    # is the full algorithm. The three divergent classes:
    assert mk.duck_lower("TESTİNG") == "testing"      # dotted capital I -> bare i
    assert "TESTİNG".lower() != "testing"             # (the full fold grows the string)
    assert mk.duck_lower("ΛΟΓΟΣ") == "λογοσ"  # final sigma stays sigma
    assert mk.duck_lower("ẞ") == "ß"             # capital sharp s -> eszett
    assert mk.duck_lower("Plain ASCII 123") == "plain ascii 123"


def test_synthetic_uses_the_simple_fold():
    # 'TESTİNG' must be flagged synthetic: DuckDB-side lowering yields 'testing' and the
    # reference generator computes synthetic over THAT. Python's full fold would miss it.
    r = mk.build_records([_ev(tags=[["title", "TESTİNG"]])], mk.NO_DELETIONS)
    assert r[0]["synthetic"] is True


def test_build_snapshot_drops_far_future_created_at():
    day = 86400
    evs = [
        _ev(d_tag="ok", eid="a" * 64, created_at=NOW - day, tags=[["title", "Real"]]),
        # year-10000-scale timestamp: without the horizon bound this crashes strftime and
        # permanently wins its coordinate's current-version selection.
        _ev(d_tag="troll", eid="b" * 64, created_at=260000000000, tags=[["title", "Troll"]]),
        # same coordinate as "ok", future-dated: must NOT displace the real version
        _ev(d_tag="ok", eid="c" * 64, created_at=NOW + 30 * day, tags=[["title", "Hijack"]]),
    ]
    recs, data = mk.build_snapshot(evs, [], [], NOW)
    assert data["summary"]["archive_events"] == 1   # both future events dropped pre-count
    assert data["summary"]["active_listings"] == 1
    assert recs[0]["title"] == "real"
    # within the 2-day skew allowance is kept
    _, d2 = mk.build_snapshot([_ev(created_at=NOW + day, tags=[["title", "Skew"]])], [], [], NOW)
    assert d2["summary"]["active_listings"] == 1


def test_strip_nul_recurses_and_write_pg_uses_it():
    assert mk.strip_nul("has\x00nul") == "hasnul"
    assert mk.strip_nul(["a\x00", {"k": "b\x00", "n": 5}, None]) == ["a", {"k": "b", "n": 5}, None]
    assert mk.strip_nul(7) == 7
    # write_pg must sanitize every string-bearing column AND the snapshot document; the
    # compute output itself stays un-sanitized (comparator visibility). Pin the wiring.
    import inspect
    src = inspect.getsource(mk.write_pg)
    # 11 since 2026-10-24: title_tag and content_warning are strings from a stranger's event
    # like the nine before them, and a NUL in one would fail the INSERT and freeze the refresh.
    assert src.count("strip_nul(") >= 11
    assert "Json(strip_nul(data))" in src


# --- implausible geohashes ------------------------------------------------------------------

# A real seller's `g` tags, verbatim: a Spanish postal code published as a prefix ladder.
_POLE_LADDER = [["g", "1"], ["g", "12"], ["g", "125"], ["g", "1252"], ["g", "12520"]]


def test_build_snapshot_implausible_geohash_places_nothing_and_names_no_country():
    """A `g` tag that decodes into Antarctica reaches neither the map nor the country
    counts. Left alone it does both: the nearest-city lookup is planar and unbounded, so
    it answers Chile, and gh_len 5 badges the bubble "exact location"."""
    evs = [_ev(d_tag="seed", eid="a" * 64, tags=[["title", "albahaca roja"]] + _POLE_LADDER)]
    _, data = mk.build_snapshot(evs, [], [], NOW)
    assert data["map_points"] == []
    assert data["countries"] == []
    assert data["geo_attributable"] == 0
    assert data["geo_nonattr"]["NOTAG"]["listings"] == 1


def test_build_snapshot_implausible_geohash_falls_back_to_the_location_tag():
    """With a resolvable `location` tag the listing keeps its real country - and the bubble
    must STILL not be drawn at the pole under that country's name, which is what happens if
    the rejection reaches country attribution but not the map placement."""
    evs = [_ev(d_tag="seed", eid="a" * 64,
               tags=[["title", "garrofo pintat"], ["location", "Madrid, Spain"]] + _POLE_LADDER)]
    recs, data = mk.build_snapshot(evs, [], [], NOW)
    assert [c["cc"] for c in data["countries"]] == ["ES"]
    assert [(p["cc"], p["prec"]) for p in data["map_points"]] == [("ES", "country")]
    assert all(p["lat"] > -60 for p in data["map_points"])
    # cc_src keeps the TEXT's own provenance: stamping the rejection marker unconditionally
    # would erase where every fallen-back country actually came from.
    assert recs[0]["cc_src"] == "text"


def test_build_snapshot_implausible_geohash_is_recorded_not_erased():
    """The decode stays in the row - it is the record of WHY the listing is unplaced - and
    cc_src says it was refused, so a rejection is never silent in the forensic table."""
    evs = [_ev(d_tag="seed", eid="a" * 64, tags=[["title", "albahaca roja"]] + _POLE_LADDER)]
    recs, _ = mk.build_snapshot(evs, [], [], NOW)
    r = recs[0]
    assert r["gh_len"] == 5 and r["gh_xy"][0] < -89.9
    assert r["cc"] is None and r["cc_src"] == "geohash-implausible"


def test_build_snapshot_implausible_geohash_does_not_shift_other_countries():
    """resolve_geo reverse-geocodes the plausible points as ONE batch and zips the answers
    back onto the rows it sent. Filter one side and not the other and every listing after a
    rejected one takes its neighbour's country - silently, and only on days a bad `g` tag is
    live."""
    evs = [
        _ev(pubkey=_pk("a"), d_tag="de", eid="a" * 64, tags=[["title", "berlin"], ["g", "u33dc"]]),
        _ev(pubkey=_pk("b"), d_tag="pole", eid="b" * 64, tags=[["title", "seed"]] + _POLE_LADDER),
        _ev(pubkey=_pk("c"), d_tag="es", eid="c" * 64, tags=[["title", "madrid"], ["g", "ezjmg"]]),
    ]
    _, data = mk.build_snapshot(evs, [], [], NOW)
    assert {c["cc"]: c["listings"] for c in data["countries"]} == {"DE": 1, "ES": 1}
    assert data["geo_nonattr"]["NOTAG"]["listings"] == 1


def test_build_snapshot_short_implausible_geohash_names_no_country_either():
    """The rule must not be gated on the badge length. A 2-character geohash never reaches
    the map (the placement gate is gh_len >= 5, so it falls to the country centroid) - which
    is exactly why gating there would be the worse bug: the listing would keep naming a
    country and the bubble would sit on that country looking entirely ordinary."""
    evs = [_ev(d_tag="seed", eid="a" * 64, tags=[["title", "seed"], ["g", "12"]])]
    recs, data = mk.build_snapshot(evs, [], [], NOW)
    assert recs[0]["gh_len"] == 2 and recs[0]["gh_xy"][0] < -60
    assert data["countries"] == []
    assert data["map_points"] == []


def test_check_invariants_blocks_an_implausible_geohash_placement():
    """The unit tests above only run when someone runs them; this invariant runs on every
    production refresh and raises before anything is written."""
    evs = [_ev(d_tag="de", eid="a" * 64, tags=[["title", "berlin"], ["g", "u33dc"]])]
    recs, data = mk.build_snapshot(evs, [], [], NOW)
    assert mk.check_invariants(recs, data) == []
    data["map_points"][0]["lat"] = -89.98          # a geohash placement that got through
    problems = mk.check_invariants(recs, data)
    assert any("implausible geohash placement" in p for p in problems)


def test_build_snapshot_plausible_geohash_still_places_and_badges_exact():
    evs = [_ev(d_tag="de", eid="a" * 64, tags=[["title", "berlin"], ["g", "u33dc"]])]
    _, data = mk.build_snapshot(evs, [], [], NOW)
    assert [(p["cc"], p["prec"]) for p in data["map_points"]] == [("DE", "geohash")]


def test_map_points_sort_key_includes_prec():
    import inspect
    src = inspect.getsource(mk.build_snapshot)
    assert 'p["prec"])' in src  # ordering must never depend on input row order


# --- the six columns that feed no metric (2026-10-24) ---------------------------------------
# They exist for nostrology-web's Recent listings section: each is a condition one of the three
# marketplaces reads from the listing, and the page drops a row no marketplace can show rather
# than drawing a name that opens on "Listing not available". So a wrong value here is a dead
# link or a lost one, and nothing in the snapshot would ever show it.

def _rec(**kw):
    """The one record build_records makes for one event."""
    return mk.build_records([_ev(**kw)], mk.NO_DELETIONS)[0]


def test_title_tag_is_the_published_title_and_title_stays_folded():
    r = _rec(tags=[["title", "Arcanine for Abra"]])
    assert r["title_tag"] == "Arcanine for Abra"
    assert r["title"] == "arcanine for abra"       # the fold the classifiers compare


def test_no_title_tag_is_null_and_an_empty_one_is_the_empty_string():
    """Conduit's schema takes a title of 1..200 and `["title",""]` fails it, while a listing
    with NO title tag is titled from its content instead. Two different cases, so the column
    is nullable and the empty string has to survive as itself."""
    assert _rec(tags=[])["title_tag"] is None
    assert _rec(tags=[["title"]])["title_tag"] is None          # a 1-element tag is no value
    assert _rec(tags=[["title", ""]])["title_tag"] == ""


def test_event_id_is_the_current_versions_id():
    evs = [_ev(d_tag="d1", eid="a" * 64, created_at=NOW - 100),
           _ev(d_tag="d1", eid="b" * 64, created_at=NOW - 50)]
    recs = mk.build_records(evs, mk.NO_DELETIONS)
    assert [r["event_id"] for r in recs] == ["b" * 64]           # the newer version's


def test_content_warning_is_raw():
    """Plebeian's gate is `?.[1] === 'nsfw'`: raw, case-sensitive, untrimmed. A folded column
    would have the web decline them for a listing they render in full."""
    assert _rec(tags=[["content-warning", "NSFW"]])["content_warning"] == "NSFW"
    assert _rec(tags=[["content-warning", " nsfw"]])["content_warning"] == " nsfw"
    assert _rec(tags=[["content-warning", "nsfw"]])["content_warning"] == "nsfw"
    assert _rec(tags=[])["content_warning"] is None


def test_expires_at_reads_the_nip40_tag_permissively():
    """A form we read as a timestamp can only drop a row from the table; one we refuse to read
    leaves a dead link in it, so the permissive cast is the safe direction here."""
    assert _rec(tags=[["expiration", "1800000000"]])["expires_at"] == 1800000000
    assert _rec(tags=[["expiration", " 1800000000 "]])["expires_at"] == 1800000000
    assert _rec(tags=[["expiration", "not-a-time"]])["expires_at"] is None
    assert _rec(tags=[])["expires_at"] is None
    # NOT the Shopstr `valid_until` field, which only draws an "Outdated" chip there
    assert _rec(tags=[["valid_until", "1800000000"]])["expires_at"] is None


def test_summary_len_is_utf16_units_of_the_summary_tag():
    assert _rec(tags=[["summary", "abc"]])["summary_len"] == 3
    assert _rec(tags=[["summary", "\U0001f600"]])["summary_len"] == 2   # JS String.length
    assert _rec(tags=[])["summary_len"] is None


def test_market_image_reads_the_source_conduit_would_read():
    img = "https://cdn.example-shop.com/a.png"
    assert _rec(content="markdown body", tags=[["image", img]])["market_image"] is True
    assert _rec(content="markdown body", tags=[["image", "https://127.0.0.1/a.png"]])[
        "market_image"] is False
    # JSON content its schema accepts: the image TAGS are not read at all over there
    assert _rec(content=json.dumps({"title": "Tea", "price": 5}),
                tags=[["image", img]])["market_image"] is False
    assert _rec(content=json.dumps({"title": "Tea", "price": 5, "images": [{"url": img}]}),
                tags=[])["market_image"] is True


def test_the_six_columns_are_written_in_the_right_places():
    """The INSERT's column list and the row tuple are two lists that have to stay in step;
    swapping two of them would file one seller's title under another column and no test of the
    values alone would notice. This pins them BY NAME, through the real write_pg."""
    recs, data = mk.build_snapshot(
        [_ev(d_tag="d1", eid="c" * 64,
             content="markdown body",
             tags=[["title", "Arcanine for Abra"], ["price", "190.00"],
                   ["image", "https://cdn.example-shop.com/a.png"],
                   ["summary", "ab"], ["content-warning", "NSFW"],
                   ["expiration", "1900000000"]])], [], [], NOW)
    captured = {}

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, args=None):
            captured.setdefault("executed", []).append(sql)

    class _Conn:
        def cursor(self):
            return _Cur()

        def commit(self):
            captured["committed"] = True

    import psycopg2.extras
    real = psycopg2.extras.execute_values
    psycopg2.extras.execute_values = lambda cur, sql, rows, **kw: captured.update(
        sql=sql, rows=rows)
    try:
        mk.write_pg(_Conn(), recs, data, run_id="r1", min_active=1)
    finally:
        psycopg2.extras.execute_values = real

    cols = [c.strip() for c in captured["sql"].split("(", 1)[1].rsplit(")", 1)[0].split(",")]
    row = captured["rows"][0]
    assert len(cols) == len(row), f"{len(cols)} columns, {len(row)} values"
    by_name = dict(zip(cols, row))
    assert by_name["title"] == "arcanine for abra"
    assert by_name["title_tag"] == "Arcanine for Abra"
    assert by_name["event_id"] == "c" * 64
    assert by_name["summary_len"] == 2
    assert by_name["content_warning"] == "NSFW"
    assert by_name["expires_at"] == 1900000000
    assert by_name["market_image"] is True


def test_title_tag_is_the_tag_conduit_would_read():
    """Their getTagValue skips a tag with no value; this file's own tag_value does not, and
    cannot change (DuckDB parity). So `[["title"], ["title", "Mug"]]` is "Mug" to Conduit, and
    reading None here would skip the 1..200 gate and hand a reader "Product not found"."""
    r = _rec(tags=[["title"], ["title", "Mug"]])
    assert r["title_tag"] == "Mug"
    # the metric fold is unchanged: tag_value's answer, lowercased, whatever this reads
    assert r["title"] == mk.duck_lower(mk.tag_value([["title"], ["title", "Mug"]], "title") or "")
    # and a lone valueless tag is still no title at all
    assert _rec(tags=[["title"]])["title_tag"] is None

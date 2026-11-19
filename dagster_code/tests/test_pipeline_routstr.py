"""Unit tests for pipeline/routstr - the /routstr metrics compute.

Two granularities, mirroring test_pipeline_marketplaces.py:
  (a) primitive pins - URL/endpoint classification, test-marker detection, rounding,
      bucket parsing - the rules the reference generator must match codepoint-for-codepoint;
  (b) methodology via hand-verifiable synthetic fixtures - the ledger merge rules probed
      on the live archive 2026-08-24 (full-coverage-only, newest-wins, 30d-over-3m,
      grid alignment, weekly pre-daily era), the provider registry/exclusions, reviews,
      catalog history, and the write gates (anti-shrink refuses BEFORE any DB touch).
"""

import collections
import datetime as dt
import json

import pytest

from dagster_code.pipeline import routstr as R

# 2026-08-24 00:00:00 UTC - a fixed, digit-checkable NOW. The weekly grid (epoch-aligned,
# Thursday-start) has a boundary at 2026-08-20 00:00 UTC.
NOW = 1787529600
THU = 1787184000
assert THU % R.WEEK_S == 0
DAY = R.DAY_S


def D(n):
    """Midnight UTC, n days before NOW."""
    return NOW - n * DAY


def day_str(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d")


def ts_str(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# --- fixture builders -----------------------------------------------------------------------

_ids = iter(range(10_000_000))


def bucket(ts, req=1, msats=1000, tok=10, models=None, others=None):
    b = {
        "timestamp": ts_str(ts),
        "total_successful": req,
        "total_revenue_msats": float(msats),
        "total_tokens": tok,
        "model_counts": {}, "model_revenue_msats": {}, "model_tokens": {},
    }
    for m, (mr, mm, mt) in (models or {}).items():
        b["model_counts"][m] = mr
        b["model_revenue_msats"][m] = float(mm)
        b["model_tokens"][m] = mt
    if others:
        b["others"], b["others_revenue_msats"], b["others_tokens"] = (
            others[0], float(others[1]), others[2])
    return b


def win(hours_back, interval_min, buckets):
    return {
        "model_usage_mix": {
            "hours_back": hours_back, "interval_minutes": interval_min,
            "metrics": buckets, "total_buckets": len(buckets),
        },
        "summary": {},
    }


def snap(pubkey, created_at, windows, *, gen=None, eid=None, content_override=None):
    content = content_override if content_override is not None else json.dumps({
        "schema": R.ANALYTICS_SCHEMA,
        "generated_at": gen if gen is not None else created_at,
        "windows": windows,
    })
    return {
        "id": eid or f"{next(_ids):064x}",
        "pubkey": pubkey,
        "created_at": created_at,
        "content": content,
        "d_tag": f"{pubkey[:12]}:stats",
    }


def announce(pubkey, created_at, *, d=None, urls=("https://api.example.com",),
             mints=("https://mint.example.com",), version="0.4.4",
             name="A Node", about="", eid=None, tags_extra=()):
    tags = [["d", d if d is not None else pubkey[:12]]]
    tags += [["u", u] for u in urls]
    tags += [["mint", m] for m in mints]
    if version is not None:
        tags.append(["version", version])
    tags += [list(t) for t in tags_extra]
    return {
        "id": eid or f"{next(_ids):064x}",
        "pubkey": pubkey, "created_at": created_at,
        "content": json.dumps({"name": name, "about": about}),
        "tags": tags, "d_tag": d if d is not None else pubkey[:12],
    }


def review(node, created_at, label, *, reviewer=R.CURATED_PUBKEY, eid=None):
    return {
        "id": eid or f"{next(_ids):064x}",
        "pubkey": reviewer, "created_at": created_at, "content": "",
        "tags": [["d", f"{node}:{node[:12]}"], ["k", "38421"], ["p", node],
                 ["node", node], ["t", label]],
        "d_tag": f"{node}:{node[:12]}",
    }


def catalog(created_at, models, *, pubkey=R.CURATED_PUBKEY, d=R.CATALOG_D_TAG, eid=None):
    return {
        "id": eid or f"{next(_ids):064x}",
        "pubkey": pubkey, "created_at": created_at,
        "content": json.dumps({"models": models}),
        "tags": [["d", d]], "d_tag": d,
    }


PK_A = "a" * 64
PK_B = "b" * 64
PK_C = "c" * 64


# --- (a) primitive pins ---------------------------------------------------------------------

def test_round_half_away_pinned():
    assert R.round_half_away(0.5) == 1
    assert R.round_half_away(1.5) == 2      # NOT banker's 2->2, but 1.5 half-away is 2 anyway
    assert R.round_half_away(2.5) == 3      # banker's would give 2
    assert R.round_half_away(-0.5) == -1
    assert R.round_half_away(-2.5) == -3
    assert R.msats_to_sats(1499) == 1
    assert R.msats_to_sats(1500) == 2


def test_classify_url_classes():
    assert R.classify_url("https://api.nonkycai.com") == "clearnet"
    assert R.classify_url("http://x.onion/spec") == "onion"
    assert R.classify_url("http://localhost:8000") == "private"
    assert R.classify_url("http://paygate:8000") == "private"       # dotless container name
    assert R.classify_url("http://foo.local") == "private"
    assert R.classify_url("http://10.1.2.3:8000") == "private"
    assert R.classify_url("http://127.0.0.1") == "private"
    assert R.classify_url("http://172.16.0.1") == "private"
    assert R.classify_url("http://172.32.0.1") == "clearnet"        # outside RFC-1918
    assert R.classify_url("http://192.168.1.4") == "private"
    assert R.classify_url("http://100.90.84.147:8011") == "private"  # CGNAT / Tailscale
    assert R.classify_url("http://100.20.1.1") == "clearnet"        # below CGNAT range
    assert R.classify_url("http://99.68.36.178:8000") == "clearnet"
    assert R.classify_url("wss://relay.damus.io") == "invalid"      # not http(s)
    assert R.classify_url("http://999.1.1.1") == "invalid"
    assert R.classify_url("") == "invalid"


def test_endpoint_class_combinations():
    assert R.endpoint_class(["https://a.com", "http://b.onion"]) == "both"
    assert R.endpoint_class(["https://a.com"]) == "clearnet"
    assert R.endpoint_class(["http://b.onion"]) == "onion"
    assert R.endpoint_class(["http://localhost:1", "http://10.0.0.1"]) == "private-only"
    assert R.endpoint_class([]) == "none"
    assert R.endpoint_class(["nonsense"]) == "none"                 # invalid-only -> none
    # a private mirror does not demote a clearnet provider
    assert R.endpoint_class(["https://a.com", "http://localhost:1"]) == "clearnet"


def test_is_test_marked_word_boundaries_and_tld_exception():
    assert R.is_test_marked("plebchat test paygate", [])
    assert R.is_test_marked("Roustr Staging Environment", [])
    assert R.is_test_marked("llm402-staging-test", [])
    assert not R.is_test_marked("Contest of Champions", [])          # 'test' inside a word
    assert not R.is_test_marked("Attestation Node", [])
    assert R.is_test_marked("x", ["https://staging.routstr.com"])    # host label
    assert R.is_test_marked("x", ["https://api.test.example.com"])
    assert not R.is_test_marked("x", ["https://routstr.rewolf.dev"])  # .dev TLD is NOT a marker
    assert not R.is_test_marked("Remi-chan", ["https://routstr.rewolf.dev"])


def test_current_versions_newest_created_lowest_id_tiebreak():
    e1 = announce(PK_A, 100, eid="ff" * 32)
    e2 = announce(PK_A, 100, eid="aa" * 32)
    e3 = announce(PK_A, 99, eid="00" * 32)
    (cur,) = R.current_versions([e1, e2, e3])
    assert cur["id"] == "aa" * 32  # same created_at -> lowest id wins


def test_parse_bucket_ts():
    assert R._parse_bucket_ts("2026-08-20 00:00:00") == THU
    assert R._parse_bucket_ts("garbage") is None
    assert R._parse_bucket_ts(None) is None


def test_bucket_value_identity_and_rejection():
    b = bucket(D(3), req=5, msats=1500, tok=100,
               models={"m1": (3, 900, 60)}, others=(2, 600, 40))
    v = R._bucket_value(b)
    assert v["req"] == 5 and v["msats"] == 1500 and v["tok"] == 100
    assert v["models"]["m1"] == {"req": 3, "msats": 900, "tok": 60}
    assert v["others"] == {"req": 2, "msats": 600, "tok": 40}
    assert R._bucket_value({"total_successful": -1, "total_revenue_msats": 0.0,
                            "total_tokens": 0}) is None
    assert R._bucket_value({"total_successful": "x", "total_revenue_msats": 0.0,
                            "total_tokens": 0}) is None


# --- (b) the ledger merge rules -------------------------------------------------------------

def test_ledger_full_coverage_excludes_trailing_truncation():
    """A daily bucket at the 3m window's trailing edge (window start INSIDE the bucket)
    must be ignored - the provider truncates it to the window (probed live: 297 -> 3)."""
    gen = D(0) + 3600  # 01:00, so the D(90) bucket is only partially inside a 2160h window
    s = snap(PK_A, gen, {"3m": win(2160, 1440, [bucket(D(90), req=3, msats=33175, tok=17845)])})
    ledger, _, anom = R.build_ledger([s], NOW)
    assert ledger == {}
    assert anom == {"bad_snapshots": 0, "bad_buckets": 0}


def test_ledger_full_coverage_excludes_leading_partial_day():
    gen = D(0) + 3600  # 01:00 today: today's bucket is still accumulating
    s = snap(PK_A, gen, {"30d": win(720, 1440, [bucket(D(0), req=9), bucket(D(1), req=4)])})
    ledger, _, _ = R.build_ledger([s], NOW)
    assert set(ledger) == {(PK_A, "d", D(1))}
    assert ledger[(PK_A, "d", D(1))]["req"] == 4


def test_ledger_newest_snapshot_wins_late_revision():
    """The same day observed by two snapshots with different values -> the newer
    snapshot's value stands (probed live: late accounting revised a day upward)."""
    s1 = snap(PK_A, D(2), {"30d": win(720, 1440, [bucket(D(3), req=279)])})
    s2 = snap(PK_A, D(1), {"30d": win(720, 1440, [bucket(D(3), req=301)])})
    ledger, _, _ = R.build_ledger([s2, s1], NOW)  # input order must not matter
    assert ledger[(PK_A, "d", D(3))]["req"] == 301


def test_ledger_30d_window_preferred_over_3m_within_one_snapshot():
    s = snap(PK_A, D(0), {
        "30d": win(720, 1440, [bucket(D(5), req=650)]),
        "3m": win(2160, 1440, [bucket(D(5), req=610)]),
    })
    ledger, _, _ = R.build_ledger([s], NOW)
    assert ledger[(PK_A, "d", D(5))]["req"] == 650


def test_ledger_misaligned_bucket_is_rejected():
    s = snap(PK_A, D(0), {"30d": win(720, 1440, [bucket(D(2) + 3600, req=1)])})
    ledger, _, anom = R.build_ledger([s], NOW)
    assert ledger == {}
    assert anom["bad_buckets"] == 1


def test_ledger_wrong_interval_window_is_ignored():
    # a 6h-bucket window can never feed the daily grain
    s = snap(PK_A, D(0), {"30d": win(720, 360, [bucket(D(2), req=7)])})
    ledger, _, _ = R.build_ledger([s], NOW)
    assert ledger == {}


def test_ledger_weekly_only_pre_daily_era():
    """Weekly rows survive only for weeks that end before the provider's earliest daily
    coverage; overlapping weeks are dropped so daily+weekly never double count."""
    wk_old = THU - 30 * R.WEEK_S
    wk_overlap = THU - R.WEEK_S  # ends THU, after the daily bucket below (D(9))
    s = snap(PK_A, D(0), {
        "30d": win(720, 1440, [bucket(D(9), req=5, msats=5000)]),
        "1y": win(8760, 10080, [bucket(wk_old, req=100, msats=99000),
                                bucket(wk_overlap, req=7, msats=7000)]),
    })
    ledger, _, _ = R.build_ledger([s], NOW)
    assert (PK_A, "w", wk_old) in ledger
    assert (PK_A, "w", wk_overlap) not in ledger
    assert (PK_A, "d", D(9)) in ledger


def test_ledger_weekly_kept_when_no_daily():
    wk = THU - 10 * R.WEEK_S
    s = snap(PK_A, D(0), {"1y": win(8760, 10080, [bucket(wk, req=3, msats=1000)])})
    ledger, _, _ = R.build_ledger([s], NOW)
    assert set(ledger) == {(PK_A, "w", wk)}


def test_ledger_generated_at_skew_falls_back_to_created_at():
    """A generated_at more than a day from created_at is hostile/garbage - window math
    must anchor on created_at instead."""
    s = snap(PK_A, D(1), {"30d": win(720, 1440, [bucket(D(2), req=8)])},
             gen=D(1) + 10 * DAY)
    ledger, meta, _ = R.build_ledger([s], NOW)
    assert ledger[(PK_A, "d", D(2))]["req"] == 8
    assert meta[PK_A]["last_report_ts"] == D(1)


def test_ledger_horizon_and_bad_content():
    far_future = snap(PK_A, NOW + 3 * DAY, {"30d": win(720, 1440, [bucket(D(2))])})
    garbage = snap(PK_B, D(1), {}, content_override="not json")
    nonobject = snap(PK_C, D(1), {}, content_override='"str"')
    ledger, _, anom = R.build_ledger([far_future, garbage, nonobject], NOW)
    assert ledger == {}
    assert anom["bad_snapshots"] == 2  # the future one is silently dropped, not "bad"


# --- (b) the snapshot -----------------------------------------------------------------------

def _base_fixture():
    """Three providers: A clearnet+reporting, B test-marked+reporting, C announced-only;
    plus a kind-38421 (routstr-provider-announcements) squatter with no u tag; a review flip; two catalog versions."""
    announces = [
        announce(PK_A, D(300), name="Alpha Node", urls=("https://a.example.com",),
                 mints=("https://mint1.x", "https://mint2.y")),
        announce(PK_A, D(5), name="Alpha Node", urls=("https://a.example.com",),
                 mints=("https://mint1.x", "https://mint2.y"), version="0.4.5"),
        announce(PK_B, D(90), name="Beta staging", urls=("https://b.example.com",), mints=()),
        announce(PK_C, D(50), name="Gamma", urls=("http://c.onion",), mints=()),
        # squatter: no u tag at all (lnproxy-style)
        {"id": f"{next(_ids):064x}", "pubkey": "d" * 64, "created_at": D(10),
         "content": "{}", "tags": [["d", "lnproxy-v1"], ["n", "signet"]],
         "d_tag": "lnproxy-v1"},
    ]
    analytics = [
        snap(PK_A, D(1) + 3600, {
            "30d": win(720, 1440, [
                bucket(D(2), req=10, msats=20000, tok=1000,
                       models={"m1": (6, 12000, 600), "m2": (2, 5000, 300)},
                       others=(2, 3000, 100)),
                bucket(D(4), req=5, msats=10000, tok=500, models={"m1": (5, 10000, 500)}),
            ]),
            "1y": win(8760, 10080, [bucket(THU - 20 * R.WEEK_S, req=50, msats=50000, tok=5000,
                                           models={"m1": (50, 50000, 5000)})]),
        }),
        snap(PK_B, D(1) + 7200, {
            "30d": win(720, 1440, [bucket(D(3), req=100, msats=999000, tok=9000)]),
        }),
    ]
    reviews = [
        review(PK_A, D(60), "lgtm"),
        review(PK_A, D(20), "avoid"),
        review(PK_C, D(30), "lgtm", reviewer="e" * 64),  # non-curated: timeline only
    ]
    catalogs = [
        catalog(D(40), ["m1", "m2", "m3"]),
        catalog(D(10), ["m1", "m2"]),
        catalog(D(9), ["mX"], pubkey="f" * 64),  # impostor author: ignored
    ]
    return announces, analytics, catalogs, reviews


def test_build_snapshot_registry_and_exclusions():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    s = data["summary"]
    assert s["providers_announced"] == 3            # A, B, C - squatter not counted
    assert s["providers_reporting"] == 1            # only A is included AND in the ledger
    by_id = {p["id"]: p for p in data["providers"]}
    assert by_id[PK_A[:12]]["class"] == "clearnet"
    assert by_id[PK_A[:12]]["version"] == "0.4.5"   # newest announcement wins
    assert by_id[PK_B[:12]]["excluded"] and by_id[PK_B[:12]]["reason"] == "test"
    assert by_id[PK_C[:12]]["class"] == "onion" and not by_id[PK_C[:12]]["excluded"]
    excl = {e["k"]: e["v"] for e in data["exclusions"]}
    assert excl["Test / staging nodes"] == 1
    assert excl["Kind squatters"] == 1
    assert s["mints"] == 2                          # mint1.x + mint2.y (C has none)


def test_build_snapshot_ledger_aggregates_exclude_test_nodes():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    s = data["summary"]
    # B's 999000 msats are NOT in the network numbers
    assert s["revenue_alltime_sats"] == R.msats_to_sats(20000 + 10000 + 50000)
    assert s["revenue_30d_sats"] == R.msats_to_sats(30000)
    assert s["requests_30d"] == 15 and s["tokens_30d"] == 1500
    # daily series spans D(4)..D(2), zero-filled, and B's day is absent
    assert [d["d"] for d in data["daily"]] == [day_str(D(4)), day_str(D(3)), day_str(D(2))]
    assert [d["msats"] for d in data["daily"]] == [10000, 0, 20000]
    # cumulative = weekly era + daily, monotone, ends at all-time
    assert data["cum_revenue"][-1]["msats"] == 80000
    assert data["cum_revenue"][0]["msats"] == 50000  # the weekly-era point comes first


def test_build_snapshot_models_and_reviews_and_catalog():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    models = {m["model"]: m for m in data["models"]}
    assert models["m1"]["msats_alltime"] == 12000 + 10000 + 50000
    assert models["m1"]["msats_30d"] == 22000
    assert models["m2"]["msats_30d"] == 5000
    assert models["(other)"]["msats_30d"] == 3000
    assert models["m1"]["providers_30d"] == 1
    # review: A's LATEST curated label is avoid; C's non-curated review sets no status
    by_id = {p["id"]: p for p in data["providers"]}
    assert by_id[PK_A[:12]]["review"] == "avoid"
    assert by_id[PK_C[:12]]["review"] is None
    labels = [(rv["label"], rv["reviewer"]) for rv in data["reviews"]]
    assert ("lgtm", "other") in labels and ("avoid", "curated") in labels
    # catalog: impostor ignored, history sorted, tail == current
    assert data["catalog"]["models"] == ["m1", "m2"]
    assert data["catalog"]["history"] == [{"d": day_str(D(40)), "n": 3},
                                          {"d": day_str(D(10)), "n": 2}]
    assert data["summary"]["catalog_models"] == 2


def test_build_snapshot_invariants_pass_and_detect_corruption():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    assert R.check_invariants(state, data) == []
    # seed corruption: break the cumulative endpoint by a whole sat (a 1-msat bump is
    # invisible to the sats-rounded endpoint check but still caught by the msats-level
    # provider-sum check - both paths are exercised here)
    data["cum_revenue"][-1]["msats"] += 1000
    problems = R.check_invariants(state, data)
    assert any("cum_revenue does not end" in p for p in problems)
    assert any("do not sum to the cumulative total" in p for p in problems)
    data["cum_revenue"][-1]["msats"] -= 999  # 1-msat corruption
    problems = R.check_invariants(state, data)
    assert any("do not sum to the cumulative total" in p for p in problems)


def test_build_snapshot_success_rate_from_fresh_30d_summaries():
    a, an, c, r = _base_fixture()
    an[0]["content"] = json.dumps({**json.loads(an[0]["content"]),
                                   "windows": {
        **json.loads(an[0]["content"])["windows"],
        "30d": {**json.loads(an[0]["content"])["windows"]["30d"],
                "summary": {"total_requests": 200, "failed_requests": 10}},
    }})
    state, data = R.build_snapshot(a, an, c, r, NOW)
    assert data["success"]["rate_30d"] == round(1 - 10 / 200, 4)
    assert data["success"]["providers_fresh"] == 1  # B is excluded, C never reported
    assert data["summary"]["success_rate_30d"] == data["success"]["rate_30d"]


def test_build_snapshot_reporting_but_never_announced_provider():
    an = [snap(PK_A, D(1), {
        "30d": win(720, 1440, [bucket(D(2), req=1, msats=1000, tok=10)]),
    })]
    an[0]["content"] = json.dumps({**json.loads(an[0]["content"]),
                                   "endpoint_urls": ["https://solo.example.com"]})
    state, data = R.build_snapshot([], an, [], [], NOW)
    (p,) = data["providers"]
    assert p["id"] == PK_A[:12] and p["name"] == PK_A[:12]
    assert p["class"] == "clearnet" and not p["excluded"]
    assert data["summary"]["providers_announced"] == 0
    assert data["summary"]["providers_reporting"] == 1


class _ExplodingConn:
    def cursor(self):
        raise AssertionError("write_pg touched the DB before the anti-shrink gate")

    def commit(self):
        raise AssertionError("commit before the gate")


def test_write_pg_anti_shrink_refuses_before_any_db_touch():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    with pytest.raises(RuntimeError, match="anti-shrink: ledger rows"):
        R.write_pg(_ExplodingConn(), state, data, run_id="t", min_ledger_rows=10 ** 6,
                   min_providers=0)
    with pytest.raises(RuntimeError, match="anti-shrink: announced providers"):
        R.write_pg(_ExplodingConn(), state, data, run_id="t", min_ledger_rows=0,
                   min_providers=10 ** 6)


def test_strip_nul_recurses_containers():
    assert R.strip_nul({"a": ["x\x00y", {"b": "\x00"}]}) == {"a": ["xy", {"b": ""}]}


# --- (c) review-driven hardening (2026-08-24 review pass) --------------------------------

def with_urls(row, urls=("https://node.example.com",)):
    """Give a reporting-only snapshot fixture reachable endpoint_urls, or its
    provider is (correctly) excluded as private and drops out of the aggregates."""
    c = json.loads(row["content"])
    c["endpoint_urls"] = list(urls)
    row["content"] = json.dumps(c)
    return row


def test_nan_and_infinity_never_crash_the_ledger():
    """json.loads admits NaN/Infinity literals; int() on those raises OUTSIDE isinstance
    guards (B1). A hostile snapshot must degrade (fallback / bad-bucket), never crash."""
    nan_gen = snap(PK_A, D(1), {"30d": win(720, 1440, [bucket(D(2), req=8)])})
    nan_gen["content"] = nan_gen["content"].replace(str(D(1)), "NaN", 1)
    inf_gen = snap(PK_B, D(1), {"30d": win(720, 1440, [bucket(D(2), req=3)])})
    inf_gen["content"] = inf_gen["content"].replace(str(D(1)), "Infinity", 1)
    ledger, meta, anom = R.build_ledger([nan_gen, inf_gen], NOW)
    # generated_at falls back to created_at; the buckets still land
    assert ledger[(PK_A, "d", D(2))]["req"] == 8
    assert ledger[(PK_B, "d", D(2))]["req"] == 3
    assert meta[PK_A]["last_report_ts"] == D(1)

    bad_hb = snap(PK_C, D(1), {"30d": win(float("nan"), 1440, [bucket(D(2))])})
    ledger, _, _ = R.build_ledger([bad_hb], NOW)
    assert ledger == {}

    bad_bucket = snap(PK_A, D(1), {"30d": win(720, 1440, [
        {"timestamp": ts_str(D(2)), "total_successful": 1,
         "total_revenue_msats": float("inf"), "total_tokens": 1}])})
    ledger, _, anom = R.build_ledger([bad_bucket], NOW)
    assert ledger == {}
    assert anom["bad_buckets"] == 1


def test_boundary_exact_window_edges():
    """ts == window_start is fully covered (>= not >); ts + blen == gen_ts is fully
    covered; one bucket earlier is truncated and excluded."""
    hb = 720  # 30 days
    gen = D(0)  # midnight: window is exactly [D(30), D(0)]
    s = snap(PK_A, gen, {"30d": win(hb, 1440, [
        bucket(D(30), req=1),   # starts exactly at window_start -> in
        bucket(D(31), req=2),   # entirely before window_start -> out
        bucket(D(1), req=3),    # ends exactly at gen_ts -> in
    ])})
    ledger, _, _ = R.build_ledger([s], NOW)
    assert set(ledger) == {(PK_A, "d", D(30)), (PK_A, "d", D(1))}


def test_future_day_injection_is_clamped_to_our_clock():
    """A snapshot created inside the +2d horizon with generated_at inside the skew
    allowance must not inject 'fully covered' FUTURE days (S3)."""
    ca = NOW + DAY  # tomorrow, within horizon
    s = snap(PK_A, ca, {"30d": win(720, 1440, [
        bucket(D(0), req=7, msats=5_000_000),   # today: ends NOW+1d > NOW -> out
        bucket(D(1), req=4, msats=1000),        # yesterday: fully past -> in
    ])})
    ledger, _, _ = R.build_ledger([s], NOW)
    assert set(ledger) == {(PK_A, "d", D(1))}
    state, data = R.build_snapshot([], [with_urls(s)], [], [], NOW)
    assert data["summary"]["last_activity"] == day_str(D(1))
    assert R.check_invariants(state, data) == []


def test_newest_wins_tie_on_equal_created_at_higher_id():
    s1 = snap(PK_A, D(1), {"30d": win(720, 1440, [bucket(D(3), req=10)])}, eid="aa" * 32)
    s2 = snap(PK_A, D(1), {"30d": win(720, 1440, [bucket(D(3), req=20)])}, eid="ff" * 32)
    ledger, _, _ = R.build_ledger([s2, s1], NOW)
    assert ledger[(PK_A, "d", D(3))]["req"] == 20  # higher id = later in merge order


def test_weekly_rows_never_enter_the_30d_aggregates():
    wk = THU - R.WEEK_S  # a recent, fully-past week
    s = with_urls(snap(PK_A, D(0), {"1y": win(8760, 10080, [bucket(wk, req=9, msats=9000)])}))
    state, data = R.build_snapshot([], [s], [], [], NOW)
    assert data["summary"]["revenue_alltime_sats"] == 9
    assert data["summary"]["revenue_30d_sats"] == 0
    assert data["summary"]["requests_30d"] == 0
    assert R.check_invariants(state, data) == []


def test_bucket_without_model_breakdown_does_not_trip_invariants():
    """B2: named+others == totals is a THIRD-PARTY property. A bucket with no model
    maps at all is legitimate data and must neither block the refresh nor corrupt
    the ledger-derived model cross-check."""
    b = {"timestamp": ts_str(D(2)), "total_successful": 5,
         "total_revenue_msats": 7000.0, "total_tokens": 50}
    s = with_urls(snap(PK_A, D(1), {"30d": win(720, 1440, [b])}))
    state, data = R.build_snapshot([], [s], [], [], NOW)
    assert data["summary"]["requests_30d"] == 5
    assert data["models"] == []  # nothing named, no residual claimed
    assert R.check_invariants(state, data) == []


def test_review_label_skips_valueless_t_tag():
    ev = review(PK_A, D(5), "ignored")
    ev["tags"] = [["d", "x"], ["node", PK_A], ["t"], ["t", "trusted"]]
    state, data = R.build_snapshot([], [], [], [ev], NOW)
    assert data["reviews"][0]["label"] == "trusted"


def test_model_rows_fold_beyond_cap_preserving_sums():
    models = {f"model-{i:03}": (1, 1000, 10) for i in range(R.MAX_MODEL_ROWS + 5)}
    b = bucket(D(2), req=len(models), msats=1000 * len(models), tok=10 * len(models),
               models=models)
    s = with_urls(snap(PK_A, D(1), {"30d": win(720, 1440, [b])}))
    state, data = R.build_snapshot([], [s], [], [], NOW)
    names = [m["model"] for m in data["models"]]
    assert len(names) == R.MAX_MODEL_ROWS + 1 and names[-1] == "(other)"
    assert sum(m["msats_30d"] for m in data["models"]) == 1000 * len(models)
    assert data["models"][-1]["providers_30d"] == 1
    assert R.check_invariants(state, data) == []


def test_model_id_clip_collisions_merge():
    long_a = "x" * R.MODEL_ID_MAX_LEN + "-alpha"
    long_b = "x" * R.MODEL_ID_MAX_LEN + "-beta"
    b = bucket(D(2), req=3, msats=3000, tok=30,
               models={long_a: (1, 1000, 10), long_b: (2, 2000, 20)})
    v = R._bucket_value(b)
    assert list(v["models"]) == ["x" * R.MODEL_ID_MAX_LEN]
    assert v["models"]["x" * R.MODEL_ID_MAX_LEN] == {"req": 3, "msats": 3000, "tok": 30}


def test_short_id_collision_degrades_to_full_pubkey():
    pk1 = "ab" * 6 + "0" * 52
    pk2 = "ab" * 6 + "1" * 52
    a1 = announce(pk1, D(5), name="One")
    a2 = announce(pk2, D(4), name="Two")
    state, data = R.build_snapshot([a1, a2], [], [], [], NOW)
    ids = {p["id"] for p in data["providers"]}
    assert ids == {pk1, pk2}  # extended to full pubkeys, no collision
    assert R.check_invariants(state, data) == []


def test_provider_daily_top_sums_to_the_network_series(monkeypatch):
    """The page's Top 5/10/20 selector folds ranks N+1..TOP_PROVIDERS into "(others)"
    client-side, so the shipped series must partition the network daily series exactly -
    and stay in rank order. Probed with the cap forced to 1 so a residual actually exists."""
    a, an, c, r = _base_fixture()
    an.append(snap(PK_C, D(1) + 100, {
        "30d": win(720, 1440, [bucket(D(2), req=1, msats=3000, tok=10)])}))
    monkeypatch.setattr(R, "TOP_PROVIDERS", 1)
    state, data = R.build_snapshot(a, an, c, r, NOW)
    ids = [p["id"] for p in data["provider_daily_top"]]
    assert ids == [PK_A[:12], "(others)"]  # A leads on 30d revenue; Gamma folded
    per_day = collections.defaultdict(int)
    for p in data["provider_daily_top"]:
        for pt in p["series"]:
            per_day[pt["d"]] += pt["msats"]
    assert {d["d"]: d["msats"] for d in data["daily"]} == {d["d"]: per_day.get(d["d"], 0) for d in data["daily"]}
    assert R.check_invariants(state, data) == []
    # and the invariant catches a series that no longer partitions
    data["provider_daily_top"][0]["series"][0]["msats"] += 1
    assert any("provider_daily_top" in p for p in R.check_invariants(state, data))


def test_provider_rows_cap_and_partition_invariant(monkeypatch):
    monkeypatch.setattr(R, "MAX_PROVIDER_ROWS", 1)
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    assert len(data["providers"]) == 1
    assert data["providers_total"] == len(state["providers"])
    assert R.check_invariants(state, data) == []


def test_30d_window_is_the_last_30_complete_utc_days():
    """A run at 17:46 UTC used to bound the window at NOW-30d, which made the 30th day
    (its bucket starts before that instant) ineligible: "30d" served 29 days. The bound
    is day-aligned now, so the window is exactly days NOW_day-30 .. NOW_day-1."""
    now = NOW + 17 * 3600 + 46 * 60
    a, an, c, r = _base_fixture()
    an.append(snap(PK_A, now - 60, {
        "3m": win(2160, 1440, [bucket(D(30), req=7, msats=7000, tok=70),
                               bucket(D(31), req=9, msats=9000, tok=90)])}))
    state, data = R.build_snapshot(a, an, c, r, now)
    # D(30) is the 30th complete day back -> inside; D(31) -> outside
    assert data["summary"]["requests_30d"] == 15 + 7
    assert data["summary"]["revenue_30d_sats"] == R.msats_to_sats(30000 + 7000)
    assert R.check_invariants(state, data) == []


def test_absence_in_a_newer_snapshot_does_not_erase_an_observed_day():
    """Buckets are sparse, so a newer fully-covering snapshot that OMITS a day the older
    one reported is not evidence of zero: the ledger keeps the observed day (observed
    history is a floor). Live case: a node that reset its own history."""
    older = snap(PK_A, D(3), {"30d": win(720, 1440, [bucket(D(5), req=4, msats=4000, tok=40)])})
    newer = snap(PK_A, D(1), {"30d": win(720, 1440, [bucket(D(2), req=1, msats=1000, tok=10)])})
    ledger, _, _ = R.build_ledger([newer, older], NOW)
    assert ledger[(PK_A, "d", D(5))]["req"] == 4
    assert ledger[(PK_A, "d", D(2))]["req"] == 1


# --- (d) the MCP substrate (2026-08-29) -----------------------------------------------------
# The Routstr MCP tools read routstr_model_days / routstr_catalog_versions /
# routstr_reviews (plus the two all-time provider columns). These pin (1) that the
# document itself is untouched by the substrate work - the agreement gate compares
# documents, so a new key there would be a methodology change - and (2) that every
# substrate row re-derives from the same state the document is built from.

DOC_KEYS = {
    "generated_utc", "summary", "exclusions", "daily", "cum_revenue", "provider_daily_top",
    "model_daily_top", "providers", "providers_total", "models", "catalog", "reviews",
    "transports", "versions", "mints_top", "success", "coverage", "anomalies",
}


def test_document_shape_is_unchanged_by_the_substrate():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    assert set(data) == DOC_KEYS
    assert set(state) == {"providers", "ledger", "prov_tot", "model_days",
                          "catalog_versions", "reviews"}


def test_model_days_substrate_resums_the_fixture_arithmetic():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    md = state["model_days"]
    # A's two daily buckets + one weekly bucket; B (test-marked) contributes NOTHING.
    assert md["m1"][("d", day_str(D(2)))] == {"msats": 12000, "req": 6, "tok": 600, "provs": {PK_A}}
    assert md["m1"][("d", day_str(D(4)))] == {"msats": 10000, "req": 5, "tok": 500, "provs": {PK_A}}
    assert md["m1"][("w", day_str(THU - 20 * R.WEEK_S))] == {
        "msats": 50000, "req": 50, "tok": 5000, "provs": {PK_A}}
    assert md["m2"] == {("d", day_str(D(2))): {"msats": 5000, "req": 2, "tok": 300, "provs": {PK_A}}}
    assert md["(other)"] == {("d", day_str(D(2))): {"msats": 3000, "req": 2, "tok": 100, "provs": {PK_A}}}
    assert set(md) == {"m1", "m2", "(other)"}
    # ...and the sums are the document's model rows (the invariant, stated by hand once).
    rows = {m["model"]: m for m in data["models"]}
    assert sum(v["msats"] for v in md["m1"].values()) == rows["m1"]["msats_alltime"] == 72000
    assert sum(v["msats"] for (g, _), v in md["m1"].items() if g == "d") == rows["m1"]["msats_30d"]


def test_provider_alltime_request_and_token_totals_reach_the_provider_rows():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    assert state["prov_tot"][PK_A]["req_alltime"] == 10 + 5 + 50
    assert state["prov_tot"][PK_A]["tok_alltime"] == 1000 + 500 + 5000
    assert data["summary"]["requests_alltime"] == 65  # B excluded


def test_catalog_versions_carry_membership_deltas():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    versions = state["catalog_versions"]
    assert [v["models"] for v in versions] == [["m1", "m2", "m3"], ["m1", "m2"]]  # impostor ignored
    assert versions[0]["added"] == ["m1", "m2", "m3"] and versions[0]["removed"] == []
    assert versions[1]["added"] == [] and versions[1]["removed"] == ["m3"]
    assert [v["at"] for v in versions] == [D(40), D(10)]
    assert all(len(v["id"]) == 64 for v in versions)


def test_reviews_state_is_newest_first_with_ids_and_reviewer_keys(monkeypatch):
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    rv = state["reviews"]
    assert [x["at"] for x in rv] == sorted((x["at"] for x in rv), reverse=True)
    assert {x["reviewer_pubkey"] for x in rv} == {R.CURATED_PUBKEY, "e" * 64}
    assert all(len(x["id"]) == 64 for x in rv)
    # The persisted timeline is bounded (open kind) - newest kept.
    monkeypatch.setattr(R, "MAX_REVIEW_TABLE_ROWS", 1)
    state, _ = R.build_snapshot(a, an, c, r, NOW)
    assert [x["at"] for x in state["reviews"]] == [D(20)]


def test_substrate_invariants_detect_corruption():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    assert R.check_invariants(state, data) == []

    state["model_days"]["m1"][("d", day_str(D(2)))]["req"] += 1
    problems = R.check_invariants(state, data)
    assert any("model_days req_alltime for 'm1'" in p for p in problems)
    assert any("model_days req_30d for 'm1'" in p for p in problems)
    state["model_days"]["m1"][("d", day_str(D(2)))]["req"] -= 1

    state["model_days"]["m2"][("d", day_str(D(2)))]["msats"] += 1
    problems = R.check_invariants(state, data)
    assert any("model_daily_top series for 'm2'" in p for p in problems)
    state["model_days"]["m2"][("d", day_str(D(2)))]["msats"] -= 1

    state["catalog_versions"][1]["removed"] = []
    assert any("added/removed delta" in p for p in R.check_invariants(state, data))
    state["catalog_versions"][1]["removed"] = ["m3"]
    state["catalog_versions"].pop()
    assert any("catalog.history" in p for p in R.check_invariants(state, data))

    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    state["reviews"].pop(0)
    problems = R.check_invariants(state, data)
    assert any("routstr_reviews" in p for p in problems)


def test_substrate_invariants_cover_the_folded_model_tail(monkeypatch):
    """Named rows beyond MAX_MODEL_ROWS fold into "(other)"; the substrate keeps them
    as their own rows, so the "(other)" document row must equal residual + folded."""
    monkeypatch.setattr(R, "MAX_MODEL_ROWS", 1)
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    assert [m["model"] for m in data["models"]] == ["m1", "(other)"]
    assert set(state["model_days"]) == {"m1", "m2", "(other)"}
    assert R.check_invariants(state, data) == []


class _RecordingCursor:
    """Enough of a psycopg2 cursor for execute_values: it mogrifies each page and
    executes the joined statement, so the recorded SQL carries the row tuples."""

    class _Conn:
        encoding = "UTF8"

    connection = _Conn()

    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, args=None):
        self.log.append((sql if isinstance(sql, str) else sql.decode(), args))

    def mogrify(self, template, args):
        return repr(tuple(args)).encode()


class _RecordingConn:
    def __init__(self):
        self.log, self.committed = [], False

    def cursor(self):
        return _RecordingCursor(self.log)

    def commit(self):
        self.committed = True


def test_write_pg_writes_every_substrate_table_in_one_transaction():
    a, an, c, r = _base_fixture()
    state, data = R.build_snapshot(a, an, c, r, NOW)
    conn = _RecordingConn()
    stats = R.write_pg(conn, state, data, run_id="t", min_ledger_rows=0, min_providers=0)
    statements = [s for s, _ in conn.log]
    truncates = [s.split()[1] for s in statements if s.startswith("TRUNCATE")]
    assert truncates == ["routstr_providers", "routstr_provider_days", "routstr_model_days",
                         "routstr_catalog_versions", "routstr_reviews"]
    inserts = [s for s in statements if "INSERT INTO" in s]
    assert sum("routstr_snapshot" in s for s in inserts) == 1
    assert conn.committed
    assert stats == {"provider_rows": 3, "ledger_rows": 4, "model_day_rows": 5,
                     "catalog_version_rows": 2, "review_rows": 3}
    # The provider row carries the two new all-time columns in the declared order.
    (prov_sql,) = [s for s in inserts if "INSERT INTO routstr_providers" in s]
    assert "requests_alltime, tokens_alltime" in prov_sql
    (model_sql,) = [s for s in inserts if "INSERT INTO routstr_model_days" in s]
    assert "model, grain, day, msats, requests, tokens, providers" in model_sql


# --- (e) review pass, 2026-08-29 -----------------------------------------------------------

def test_same_second_reviews_keep_document_and_substrate_in_one_order():
    """Two curated reviews signed in the same second on different nodes: the document
    sorts (-at, node), the substrate must follow it exactly (plus a total tie-break) or
    the prefix invariant fires every night on ordinary input."""
    a, an, c, r = _base_fixture()
    t = D(12)
    r += [review(PK_C, t, "lgtm", eid="f" * 64), review(PK_A, t, "avoid", eid="0" * 64),
          review(PK_B, t, "trusted", eid="9" * 64)]
    state, data = R.build_snapshot(a, an, c, r, NOW)
    assert R.check_invariants(state, data) == []
    head = [(x["label"], x["node"][:12]) for x in state["reviews"][:3]]
    assert head == [("avoid", PK_A[:12]), ("trusted", PK_B[:12]), ("lgtm", PK_C[:12])]
    assert [(x["label"], x["node"]) for x in data["reviews"][:3]] == head


def test_nul_in_a_model_id_merges_instead_of_colliding_at_insert():
    """A NUL codepoint in a provider-reported model id: Postgres rejects it, and the
    substrate keys on the id, so 'gpt\\x004' and 'gpt4' must be ONE key at ingest (the
    clip-collision rule) - not two ledger keys that collapse into a duplicate PK when
    write_pg strips the NUL."""
    b = bucket(D(2), req=4, msats=4000, tok=40,
               models={"gpt\x004": (1, 1000, 10), "gpt4": (3, 3000, 30)})
    v = R._bucket_value(b)
    assert v["models"] == {"gpt4": {"req": 4, "msats": 4000, "tok": 40}}
    assert R.model_key("a\x00b" * 40) == ("ab" * 40)[:R.MODEL_ID_MAX_LEN]
    # and a NUL surviving into a JSONB KEY is stripped at persistence, not just values
    assert R.strip_nul({"k\x00": {"x\x00": 1}}) == {"k": {"x": 1}}

"""Unit tests for the nutzaps battery's harness pieces - the parts that turn raw
fetches into comparable values. The battery itself runs against live systems
(`nutzaps_run.py`); these pin the deterministic plumbing - truth derivation from
fixture rows, context derivation, the plan, the comparison rules, the page parser -
and drive the WHOLE question battery against a fake server that answers from the
fixture, so a green live run means the systems agreed, not that the harness
compared nothing. Pure: no network, no database (the page parser needs `node`)."""
import copy
import csv
import datetime as dt
import io
import re
import shutil
import subprocess
import time

import pytest

from loader.nutzap_mints import normalize_mint_url
from nostrology_mcp.metric import grain_redirect, lookup
from nostrology_mcp.verification import nutzaps_plan, nutzaps_questions, nutzaps_run, nutzaps_truth
from nostrology_mcp.verification import report as report_mod
from nostrology_mcp.verification.mcp_parse import Resp
from nostrology_mcp.verification.nutzaps_truth import Truth, epoch_of, next_month

TODAY = "2026-09-30"
NOW = epoch_of(TODAY) + 11 * 3600 + 44 * 60  # the instant the fake pages were "fetched"
A = "https://mint.a.example/Bitcoin"  # the top mint; its path has an upper-case letter
B = "https://mint.b.example"          # configs, few payments
C = "https://cfg.only.example"        # 10 configs, no payment
U = "https://unreviewed.example"      # missing from nutzap_mint_classes: counts
X = "http://127.0.0.1:3338"           # excluded (private host), WITH payments
Y = "https://fake.backend.example"    # excluded, no payments
L = "https://long.example:3000/login?state=" + "x" * 300  # one config, longer than the old 256-char clip
PK = [c * 64 for c in "0123456789abcdef"]


def t(day, hh=12, mm=0):
    return epoch_of(day) + hh * 3600 + mm * 60


def _payments():
    rows = [
        # the web's oldest day D0 = 2025-09-30: one payment BEFORE the 11:44 cutoff, one after
        (t("2025-09-30", 3), PK[0], A, 5), (t("2025-09-30", 20), PK[1], A, 7),
        (t("2025-06-01"), PK[0], A, 100),            # before the web's year
        (t("2026-08-03"), PK[0], A, 21), (t("2026-08-03", 13), PK[2], A, 30),  # past month
        (t("2026-08-17"), PK[1], B, 2),
        (t("2026-09-02"), PK[3], A, 50), (t("2026-09-22", 10), PK[0], A, 10),  # this month
        (t("2026-09-23", 12), PK[4], A, 3),          # the 7d boundary day: after 11:44
        (t("2026-09-23", 9), PK[5], U, 4),           # the 7d boundary day: before 11:44
        (t("2026-09-28"), PK[0], U, 8), (t("2026-09-29"), PK[4], A, 1),
        (t("2026-09-30", 0, 58), PK[2], A, 21),      # today, before the fetch
        (t("2026-09-10"), PK[6], X, 999), (t("2026-08-01"), PK[7], X, 500),   # excluded mint
    ]
    return [{"created_at": a, "pubkey": b, "mint_url": c, "amount_sats": d} for a, b, c, d in rows]


def _configs():
    out = []
    for i in range(10):  # ten pubkeys whose latest config names C (and A), all published long ago
        out.append({"pubkey": f"{i:064x}", "created_at": t("2025-01-01"), "mint_urls": [C, A]})
    # PK[8] once named B, but its LATEST config names only A - B must not count it
    out.append({"pubkey": PK[8], "created_at": t("2025-02-01"), "mint_urls": [B]})
    out.append({"pubkey": PK[8], "created_at": t("2026-09-25"), "mint_urls": [A]})
    out.append({"pubkey": PK[9], "created_at": t("2026-09-26"), "mint_urls": [B, "https://typo.example"]})
    out.append({"pubkey": PK[10], "created_at": t("2026-09-01"), "mint_urls": [X]})
    out.append({"pubkey": PK[13], "created_at": t("2026-09-20"), "mint_urls": [L]})
    return out


def _paid_au(pays):
    """The paid table as schema/nutzap_active_users.sql builds it - distinct senders
    per UTC bucket ACROSS the counted mints - written out here with its own loop,
    not through Truth, so the paid-line checks compare two derivations."""
    import datetime as dt
    out = {}
    for p in pays:
        if p["mint_url"] in (X, Y):
            continue
        d = dt.datetime.fromtimestamp(p["created_at"], dt.timezone.utc).date()
        for grain, start in (("day", d), ("week", d - dt.timedelta(days=d.weekday())), ("month", d.replace(day=1))):
            out.setdefault((grain, start.isoformat()), set()).add(p["pubkey"])
    return [{"granularity": g, "period_start": b, "unique_users": len(s)} for (g, b), s in sorted(out.items())]


def _sql():
    pays = _payments()
    return {
        "FP": [{"all_rows": len(pays) + 3, "max_created_all": max(p["created_at"] for p in pays),
                "pay_rows": len(pays), "max_created_pay": max(p["created_at"] for p in pays),
                "pay_sats": sum(p["amount_sats"] for p in pays), "cfg_rows": 15,
                "max_created_cfg": t("2026-09-26"), "classes_md5": "x", "db_now": NOW}],
        "PAYMENTS": pays,
        "SOURCES": [{"value_source": "none", "n": 3, "sats": 0}, {"value_source": "proof_tags", "n": len(pays), "sats": 0}],
        "CLASSES": [
            {"mint_url": A, "mint_class": "unreviewed", "excluded": False, "class_source": "none"},
            {"mint_url": B, "mint_class": "unreviewed", "excluded": False, "class_source": "none"},
            {"mint_url": X, "mint_class": "private-host", "excluded": True, "class_source": "host-rule"},
            {"mint_url": Y, "mint_class": "fake-backend", "excluded": True, "class_source": "registry"},
        ],
        "CONFIGS": _configs(),
        "MINT_ORDER": [{"mint_url": m, "rank": i + 1} for i, m in enumerate(sorted(
            {p["mint_url"] for p in pays} | {m for c in _configs() for m in c["mint_urls"]} | {A, B, X, Y}))],
        "COLLATION": [{"datcollate": "C", "datctype": "C"}],
        "PAID_AU": _paid_au(pays),
    }


# ---------------------------------------------------------------------------
def test_truth_population_windows_and_totals():
    T = Truth(_sql())
    assert T.excluded == {X, Y}
    assert len(T.raw) == 15 and len(T.pays) == 13  # the two payments on X are raw but never counted
    assert T.daily("2026-09-22", "2026-09-23") == {"2026-09-22": (1, 10), "2026-09-23": (2, 7)}
    assert T.total() == (13, 262, 3, "2025-06-01", "2026-09-30")
    assert T.total("2026-08-01", "2026-08-31") == (3, 53, 2, "2026-08-03", "2026-08-17")
    assert T.total("2020-01-01", "2020-01-31") == (0, 0, 0, None, None)
    assert T.total(mint=X) == (0, 0, 0, None, None)  # excluded stays excluded under a filter
    assert T.distinct_senders() == 6


def test_leaderboard_listing_floor_latest_config_and_order():
    T = Truth(_sql())
    # All: C listed by its 10 configs alone; typo and L (1 config each) are not; B's old config does not count
    assert T.leaderboard() == [(A, 10, 248, 11), (U, 2, 12, 0), (B, 1, 2, 1), (C, 0, 0, 10)]
    assert T.configured_by()[B] == 1 and T.configured_by()[A] == 11
    # a window keeps only the configs published inside it and drops the floor to 1
    lb = T.leaderboard("2026-09-24", "2026-09-30")
    assert lb == [(A, 2, 22, 1), (U, 1, 8, 0), (B, 0, 0, 1), ("https://typo.example", 0, 0, 1)]
    assert [r for r in T.leaderboard("2026-09-20", "2026-09-30") if r[0] == L] == [(L, 0, 0, 1)]
    assert X not in {r[0] for r in T.leaderboard("2026-09-01", "2026-09-30")}  # excluded even with a config


def test_leaderboard_ties_follow_the_database_collation():
    """Aurora sorts text by en_US.UTF-8, which ignores punctuation first: its order is
    fetched (MINT_ORDER) and used for the LAST key only."""
    sql = _sql()
    for k, m in ((14, "https://mint1.example.com"), (15, "https://mint.21linz.at")):
        sql["CONFIGS"].append({"pubkey": PK[k], "created_at": t("2026-09-27"), "mint_urls": [m]})
    sql["MINT_ORDER"] += [{"mint_url": "https://mint1.example.com", "rank": 900}, {"mint_url": "https://mint.21linz.at", "rank": 901}]
    tied = [r[0] for r in Truth(sql).leaderboard("2026-09-27", "2026-09-30") if r[1:] == (0, 0, 1)]
    assert tied == ["https://mint1.example.com", "https://mint.21linz.at"]  # the DB's order, not code point
    del sql["MINT_ORDER"]
    tied = [r[0] for r in Truth(sql).leaderboard("2026-09-27", "2026-09-30") if r[1:] == (0, 0, 1)]
    assert tied == ["https://mint.21linz.at", "https://mint1.example.com"]  # fallback: code point


def test_monthly_active_users_buckets_start_inside_the_window():
    T = Truth(_sql())
    assert T.active_users("month", mint=A)[("2026-08-01", A)] == 2
    assert T.active_users("month", mint=A)[("2026-09-01", A)] == 4  # PK3, PK0, PK4, PK2 - PK4 twice counts once
    assert T.active_users("month", "2026-08-02", "2026-08-31") == {}  # the August bucket starts before 'from'
    assert T.active_users("month", "2026-08-01", "2026-08-01") == {("2026-08-01", A): 2, ("2026-08-01", B): 1}


def test_daily_and_weekly_active_users_bucket_on_utc_days_and_mondays():
    T = Truth(_sql())
    # Tuesday 09-22 and Wednesday 09-23 are one ISO week (Monday 09-21); 09-28 opens the next
    assert T.active_users("week", mint=A)[("2026-09-21", A)] == 2  # PK0 (09-22), PK4 (09-23)
    assert T.active_users("week", mint=U) == {("2026-09-21", U): 1, ("2026-09-28", U): 1}
    assert T.active_users("day", "2026-09-23", "2026-09-23") == {("2026-09-23", A): 1, ("2026-09-23", U): 1}
    # a window from Wednesday 09-23 drops the week that began Monday 09-21, whole
    assert ("2026-09-21", A) not in T.active_users("week", "2026-09-23", "2026-09-30")
    assert T.active_users("week", "2026-09-21", "2026-09-21") == {("2026-09-21", A): 2, ("2026-09-21", U): 1}


def test_network_active_users_deduplicates_across_mints_and_matches_the_paid_rule():
    T = Truth(_sql())
    # week of 09-21: A has PK0 and PK4, U has PK5 -> 3 across mints; X's 09-10 payment never counts
    assert T.network_active_users("week")["2026-09-21"] == 3
    assert "2026-09-10" not in T.network_active_users("day")  # that day's only payment is on excluded X
    for grain in ("day", "week", "month"):
        assert T.network_active_users(grain) == T.paid_au[grain]  # the independent _paid_au agrees
    # PK0 paid through A (09-22) and U (09-28): September's sum over mints over-counts by one
    assert sum(n for (b, _), n in T.active_users("month").items() if b == "2026-09-01") == \
        T.network_active_users("month")["2026-09-01"] + 1


def test_paid_line_rules():
    bv, sm = nutzaps_truth.bracket_violations, nutzaps_truth.sum_matches
    per_mint = {("d1", "a"): 2, ("d1", "b"): 1, ("d2", "a"): 1}
    assert bv(per_mint, {"d1": 2, "d2": 1}) == []  # max 2 <= 2 <= 3
    assert bv(per_mint, {"d1": 4, "d2": 1}) == ["d1: the sum over mints below the network count"]
    assert bv(per_mint, {"d1": 1, "d2": 1}) == ["d1: a mint above the network count"]
    assert bv(per_mint, {"d1": 2}) == ["d2: per-mint rows but no network count"]
    assert bv(per_mint, {"d1": 2, "d2": 1, "d3": 5}) == ["d3: a network count but no per-mint row"]
    assert sm(per_mint, {"d1": 2, "d2": 1}) == (1, 2) and sm(per_mint, {"d1": 3, "d2": 1}) == (2, 2)
    pr = nutzaps_truth.paid_vs_raw
    assert pr({"d1": 2}, {"d1": 2}) == []
    assert pr({"d1": 1, "d3": 4}, {"d1": 2, "d2": 5}) == [
        "d1: paid below raw", "d2: missing from the paid table", "d3: in the paid table, not in the raw rows"]
    assert pr({"d1": 3}, {"d1": 2}) == ["d1: paid above raw"]


def test_a_failing_paid_line_check_never_prints_a_count():
    """Both sides of the paid-line checks ARE the paid series, and the report is
    committed: a failure lists buckets and directions only. The first version
    printed 'paid None / raw 1' per bucket - the network DAU itself - on a
    --reuse over a raw cache from before the PAID_AU section."""
    M, sql, W, ctx, tools = _battery()
    sql = copy.deepcopy(sql)
    for r in sql["PAID_AU"]:
        r["unique_users"] += 7  # every paid bucket wrong, in the same direction
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    failing = [q for q in Q if q["cat"].startswith("Paid line") and report_mod.verdict(q) == "fail"]
    assert len(failing) == 6  # paid-vs-raw and the bracket, at day, week and month
    for q in failing:
        assert q["got"], q["q"]
        for line in q["got"]:
            assert not re.search(r"\d", re.sub(r"\d{4}-\d\d-\d\d", "", line)), line
    del sql["PAID_AU"]
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    got = [q["got"] for q in Q if q["cat"].startswith("Paid line") and report_mod.verdict(q) == "fail"]
    assert got[1:] == [["no paid rows read"]] * 6, got


def test_web_rules_are_rolling_and_partial_at_the_oldest_day():
    T = Truth(_sql())
    s = T.web_daily(NOW)
    assert s[0][0] == "2025-09-30" and s[-1][0] == TODAY and len(s) == 366
    assert s[0][1:] == (1, 1, 7, 7)  # D0 keeps only the payment after the 11:44 cutoff
    assert s[-1][2] == 11 and s[-1][4] == 157  # 11 payments: the 100-sat June one and D0's early 5 are cut off
    w7 = T.web_leaderboard(NOW - 7 * 86400)
    assert w7[U] == (0, 1, 8, True)  # U's 09-23 09:00 payment is before the rolling cutoff
    assert w7[A] == (1, 3, 25, True)  # 09-22 10:00 is before the cutoff; PK8's 09-25 config counts
    assert w7[B] == (1, 0, 0, True)  # PK9's 09-26 config; PK8's older config naming B is not its latest
    assert T.has_page(C) and not T.has_page("https://typo.example")
    assert T.web_mint_detail(NOW, X) is None and T.web_mint_detail(NOW, "https://typo.example") is None
    d = T.web_mint_detail(NOW, A)
    assert (d["all_time_payments"], d["all_time_sats"], d["config_count"]) == (10, 248, 11)
    assert d["mau"][0] == ("2025-09-01", 1)  # the oldest month is partial: PK0's 03:00 payment is cut
    assert T.web_top_mints(NOW) == {A: 143, U: 12, B: 2}


def test_web_mint_mau_is_a_true_timeline():
    """nostrology-web#33 (Per-mint monthly users chart hides months with no activity): the
    restatement of SQL_NUTZAP_MINT_MAU. Months come from the clock, 0 where no sender, and
    the oldest, which the window cuts, appears only where it has one."""
    T = Truth(_sql())
    a = T.web_mint_mau(NOW, A)
    months = [m for m, _ in a]
    assert all(after == next_month(before) for before, after in zip(months, months[1:]))
    assert months[0] == "2025-09-01" and months[-1] == "2026-09-01"  # the cut month has a sender here
    assert dict(a)["2026-07-01"] == 0 and dict(a)["2026-08-01"] == 2  # a month with none; PK0 and PK2
    b = T.web_mint_mau(NOW, B)  # its only sender is in 2026-08: the cut month has no row, this month a 0
    assert b[0] == ("2025-10-01", 0) and b[-2:] == [("2026-08-01", 1), ("2026-09-01", 0)] and len(b) == 12
    forged = {"created_at": t("2030-01-01"), "pubkey": PK[4], "mint_url": B, "amount_sats": 1}
    sql = _sql()
    sql["PAYMENTS"] = sql["PAYMENTS"] + [forged]
    assert Truth(sql).web_mint_mau(NOW, B) == b  # a date past now adds no month


def test_context_derives_every_subject_and_window():
    ctx = nutzaps_run.build_context(_sql(), TODAY)
    # few = configs AND 1..5 payments: B (1 config, 1 payment); U has payments but no config
    assert (ctx["top_mint"], ctx["few_mint"], ctx["config_only_mint"], ctx["excluded_mint"]) == (A, B, C, X)
    assert ctx["excluded_raw_payments"] == 2 and ctx["norm_mint"] == A and ctx["path_case_mint"] == A
    assert (ctx["last7_from"], ctx["last7_to"], ctx["last30_from"]) == ("2026-09-23", "2026-09-29", "2026-08-31")
    assert (ctx["month_from"], ctx["past_month_from"], ctx["past_month_to"]) == ("2026-09-01", "2026-08-01", "2026-08-31")
    assert (ctx["web_d0"], ctx["web_d1"], ctx["roll7_inner"]) == ("2025-09-30", "2025-10-01", "2026-09-24")
    assert (ctx["empty_from"], ctx["empty_to"]) == ("2025-05-01", "2025-05-31")
    assert (ctx["long_mint"], ctx["long_mint_from"]) == (L, "2026-09-20")
    # the first Wednesday two or more weeks back whose ISO week has a counted sender:
    # 09-16 (week of 09-14) and 09-09 (week of 09-07: only X's excluded payment) have
    # none, 09-02 (week of 08-31: PK3 on A) has
    assert ctx["midweek_from"] == "2026-09-02"
    jan = nutzaps_run.build_context(_sql(), "2026-01-15")
    assert (jan["past_month_from"], jan["past_month_to"]) == ("2025-12-01", "2025-12-31")


def test_few_mint_prefers_the_most_configured_then_falls_back():
    sql = _sql()
    for k in (11, 12):
        sql["CONFIGS"].append({"pubkey": PK[k], "created_at": t("2026-01-01"), "mint_urls": [U]})
    assert nutzaps_run.build_context(sql, TODAY)["few_mint"] == U  # U: 2 configs, 2 payments beats B: 1 config
    sql = _sql()
    sql["CONFIGS"] = [c for c in sql["CONFIGS"] if B not in c["mint_urls"]]
    assert nutzaps_run.build_context(sql, TODAY)["few_mint"] == B  # no configured candidate: the fewest payments


def test_plan_ids_are_unique_prefixed_and_drop_absent_subjects():
    ctx = nutzaps_run.build_context(_sql(), TODAY)
    steps = nutzaps_plan.build(ctx)
    ids = [s["id"] for s in steps]
    assert len(set(ids)) == len(ids) and all(i.startswith("NZ") for i in ids)
    assert all(s["tool"] in (nutzaps_plan.METRIC, nutzaps_plan.PAID) for s in steps)
    assert {s["variant"] for s in steps if s.get("variant")} == {
        "host_case", "scheme_case", "trailing_slash", "no_scheme", "default_port", "path_case"}
    assert {"NZ45", "NZ46", "NZ62", "NZ63", "NZ66", "NZ67", "NZ69", "NZ89", "NZ105", "NZ109"} <= set(ids)
    assert not {"NZ87", "NZ88"} & set(ids)  # retired: they asked for day / week while those were withdrawn
    grains = {s["args"].get("grain") for s in steps if s["args"].get("metric") == "mint_active_users"}
    assert grains == {None, "day", "week", "month", "total"}
    mid = next(s for s in steps if s["id"] == "NZ69")["args"]["from"]
    assert mid == ctx["midweek_from"] and dt.date.fromisoformat(mid).weekday() == 2
    ctx2 = dict(ctx, excluded_mint=None, config_only_mint=None, path_case_mint=None, long_mint=None, long_mint_from=None)
    ids2 = {s["id"] for s in nutzaps_plan.build(ctx2)}
    assert not ids2 & {"NZ100", "NZ101", "NZ102", "NZ103", "NZ104", "NZ99", "NZ45", "NZ46"} and "NZ43" in ids2


def test_url_variants_all_normalize_back_and_path_case_does_not():
    for url in (A, B, "http://mint.c.example:3338/x", "https://8333.space:3338"):
        for kind, v in nutzaps_plan.variants(url).items():
            got = normalize_mint_url(v if "://" in v else "https://" + v)
            assert got == url, (kind, v, got)
    assert nutzaps_plan.variants(A)["host_case"] == "https://MINT.A.EXAMPLE/Bitcoin"
    assert nutzaps_plan.variants(A)["no_scheme"] == "mint.a.example/Bitcoin"
    assert "default_port" not in nutzaps_plan.variants("https://8333.space:3338")
    assert nutzaps_plan.path_case_variant(A) == "https://mint.a.example/bitcoin"
    assert nutzaps_plan.path_case_variant(B) is None


def test_comparison_rules():
    assert nutzaps_truth.dense({"2026-01-02": (1, 5)}, "2026-01-01", "2026-01-03") == [
        ("2026-01-01", 0, 0), ("2026-01-02", 1, 5), ("2026-01-03", 0, 0)]
    assert nutzaps_truth.running({"2026-01-02": (1, 5), "2026-01-03": (2, 1)}, "2026-01-01", "2026-01-03") == [
        ("2026-01-01", 0, 0), ("2026-01-02", 1, 5), ("2026-01-03", 3, 6)]
    br = nutzaps_truth.bracket_rows
    assert br({"a": (2, 5)}, {"a": (1, 4)}, {"a": (3, 6)}) == []
    assert br({"a": (4, 5)}, {"a": (1, 4)}, {"a": (3, 6)})[0].startswith("a: page")
    assert br({}, {"a": (1, 1)}, {"a": (1, 1)}) == ["a: in the inner window, missing on the page"]
    assert br({"b": (1,)}, {}, {}) == ["b: on the page, missing from the outer window"]
    assert br({"c": (1,)}, {}, {"c": (2,)}) == []  # absent from the inner window = zero there
    tc = nutzaps_truth.topn_consistent
    assert tc([("a", 5), ("b", 3), ("c", 3)], {"a": 5, "b": 3, "c": 3, "d": 1}, 2)  # a tie at the cut: either one
    assert not tc([("a", 5), ("d", 1)], {"a": 5, "b": 3, "d": 1}, 2)
    tb = nutzaps_truth.topn_bracket
    assert tb([("a", 5), ("b", 3)], {"a": 5, "b": 2, "c": 1}, {"a": 5, "b": 4, "c": 1})
    assert not tb([("a", 5), ("b", 3)], {"a": 5, "b": 2, "c": 4}, {"a": 5, "b": 4, "c": 4})  # c beat the page's floor
    assert nutzaps_truth.fingerprint_moved({"a": 1, "db_now": 5}, {"a": 1, "db_now": 9}) == []
    assert nutzaps_truth.fingerprint_moved({"a": 1, "b": 2}, {"a": 1, "b": 3}) == ["b"]


def test_mint_href_mirrors_the_web_route():
    assert nutzaps_truth.mint_href(A) == "/nutzaps/mints/mint.a.example/Bitcoin"
    assert nutzaps_truth.mint_href(X) == "/nutzaps/mints/127.0.0.1:3338?scheme=http"
    assert nutzaps_truth.mint_href("https://m.example/a?b#c%") == "/nutzaps/mints/m.example/a%3Fb%23c%25"


def test_kit_data_reads_the_leaf_node():
    if shutil.which("node") is None:  # subprocess.run raises on a missing binary, so it cannot be the test
        pytest.skip("node not available")
    html = ('<script>kit.start(app, element, {\n node_ids: [0, 2, 16],\n data: [{type:"data",data:{session:null,'
            'analyticsOff:true},uses:{}},null,{type:"data",data:{detail:{mint_url:"https://m.example/Bitcoin",'
            'note:"a } in a string",total_sats:34762,mau:[{timestamp:1759276800,value:1}]}},uses:{params:["mint"]}}],'
            '\n form: null,\n error: null\n});</script>')
    d = nutzaps_truth.kit_data(html)
    assert d["detail"]["total_sats"] == 34762 and d["detail"]["mau"] == [{"timestamp": 1759276800, "value": 1}]
    assert nutzaps_truth.kit_data("<html>no bootstrap</html>") is None


def test_response_accessors():
    body = ("metric: nutzaps.payment_activity  |  grain: day  |  unit: x  |  series_type: event  |  additive_over_time: true\n"
            "window: 2026-09-01..2026-09-30 (UTC days, inclusive; to defaulted to today - final day in progress)\n"
            "as_of (data-derived - latest kind-9321 (nutzaps) event time): 2026-09-30T00:58:07Z\n"
            "rows: 1  |  server: nostrology-mcp 0.4.0 (abc)\ncolumns: utc_day (date) - d\nmethodology: m\n\n"
            "utc_day,payments,sats\n2026-09-02,1,50\n")
    r = Resp({"id": "x", "tool": "t", "args": {}}, body)
    q = nutzaps_questions
    assert q.grain_of(r) == "day" and q.ident(r) == "nutzaps.payment_activity" and q.as_of(r) == "2026-09-30T00:58:07Z"
    assert q.day_series(r) == {"2026-09-02": (1, 50)} and q.I("\\N") is None and q.N("\\N") is None
    assert q.markdown_series("| utc_day | payments | sats |\n|---|---|---|\n| 2026-09-02 | 1 | 50 |\n") == {
        "2026-09-02": (1, 50)}


# ---------------------------------------------------------------------------
# A fake server answering from the fixture, in the envelope's shape
# ---------------------------------------------------------------------------
METHODOLOGY = ("methodology: The population is the kind-9321 (nutzaps) PAYMENTS carrying a Cashu proof; test and "
               "private mints are excluded. Distinct-sender counts are not additive; the network-wide count is the "
               "PAID nostrology_query_nutzaps.")
GRAINS = {"payment_activity": ("day", "total"), "mint_leaderboard": ("total",), "mint_activity": ("day",),
          "mint_active_users": ("day", "week", "month")}
DEFAULT = {"payment_activity": "day", "mint_leaderboard": "total", "mint_activity": "day", "mint_active_users": "month"}


def _csv(hdr, rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(hdr)
    for r in rows:
        w.writerow(["\\N" if v is None else v for v in r])
    return buf.getvalue()


def fake_server(step, T, today=TODAY):
    a = dict(step["args"])
    if step["tool"] == nutzaps_plan.PAID:
        return "TOOLERROR:Payment required: this tool costs 21 sat per call. Pass the token in the `cashu_token` argument."
    metric, domain = a.get("metric"), a.get("domain", "zapstore")
    if metric == "active_users":
        return "TOOLERROR:'active_users' is a PAID metric, served only by the nostrology_query_nutzaps tool"
    if metric not in GRAINS:
        return "TOOLERROR:metric 'x' is a Zapstore metric - mint_url is a Nutzaps coordinate; see nutzaps.mint_leaderboard"
    grain = a.get("grain") or DEFAULT[metric]
    if grain not in GRAINS[metric]:
        # The server's own redirect clause (0.5.1): the battery's needles are written
        # independently, so this fake is where a drift between the two shows up.
        redirect = grain_redirect("nutzaps", metric, grain)
        return (f"TOOLERROR:metric 'nutzaps.{metric}' serves grain {' | '.join(GRAINS[metric])}, not '{grain}'"
                + (f" - {redirect}" if redirect else ""))
    if a.get("provider_pubkey") or a.get("publisher_pubkey") or (domain == "routstr"):
        return "TOOLERROR:that filter does not apply; nutzaps rows are scoped by mint_url"
    mint = None
    if a.get("mint_url"):
        raw = a["mint_url"] if "://" in a["mint_url"] else "https://" + a["mint_url"]
        mint = normalize_mint_url(raw) if len(raw) <= 1024 else None
        if mint is None:
            return "TOOLERROR:mint_url must be an http(s) Cashu mint URL; call mint_leaderboard"
    frm, to = a.get("from"), a.get("to")
    if frm and to and frm > to:
        return f"TOOLERROR:'from' ({frm}) is after 'to' ({to})"
    win = frm is not None or to is not None
    to_eff = to or (today if win else None)
    if metric == "payment_activity" and grain == "day":
        hdr, rows = ["utc_day", "payments", "sats"], [(d, *v) for d, v in T.daily(frm, to_eff, mint).items()]
    elif metric == "payment_activity":
        hdr, rows = ["payments", "sats", "mints", "first_day", "last_day"], [T.total(frm, to_eff, mint)]
    elif metric == "mint_leaderboard":
        lb = T.leaderboard(frm, to_eff) if win else T.leaderboard()
        hdr, rows = ["mint_url", "payments", "sats", "configured_by"], [r for r in lb if mint is None or r[0] == mint]
    elif metric == "mint_activity":
        hdr, rows = ["utc_day", "mint_url", "payments", "sats"], [(d, m, *v) for (d, m), v in sorted(T.mint_activity(frm, to_eff, mint).items())]
    else:
        hdr, rows = ["period_start", "mint_url", "active_users"], [(b, m, u) for (b, m), u in sorted(T.active_users(grain, frm, to_eff, mint).items())]
    spec = lookup("nutzaps", metric, grain)
    additive = "true" if spec.additive_over_time else "false"
    lines = [f"metric: nutzaps.{metric}  |  grain: {grain}  |  unit: x  |  series_type: event  |  additive_over_time: {additive}"]
    if win:
        lines.append(f"window: {frm or 'series start'}..{to_eff} (UTC days, inclusive"
                     + ("; to defaulted to today - final day in progress" if to is None else "") + ")")
    else:
        lines.append("window: all history (no from/to given)")
    if mint and (not rows or (metric == "payment_activity" and grain == "total" and rows[0][0] == 0)):
        lines[-1] += "  |  " + nutzaps_questions.EMPTY_FILTER_NOTE
    if mint:
        lines.append(f"scope: mint_url={mint}")
    lines.append(f"as_of (data-derived - latest kind-9321 (nutzaps) event time): {nutzaps_questions.iso(T.fp['max_created_all'])}")
    lines.append(f"rows: {len(rows)}  |  server: nostrology-mcp 0.0.0 (fake)")
    types = {"utc_day": "date", "period_start": "date", "first_day": "date", "last_day": "date", "mint_url": "text"}
    desc = {c.name: c.description for c in spec.columns}
    lines.append("columns: " + "; ".join(f"{h} ({types.get(h, 'integer, ' + h)}) - {desc.get(h, 'd')}" for h in hdr))
    if "mint_url" in hdr:
        lines.append("untrusted: mint_url are authored by arbitrary Nostr keys.")
    # The one caveat a battery question reads beyond the shared methodology above.
    across = [c for c in spec.caveats if c.startswith("Across separate windows")]
    lines.append(" ".join([METHODOLOGY, *across]))
    if a.get("format") == "markdown":
        table = "| " + " | ".join(hdr) + " |\n|---|---|---|\n" + "".join(
            "| " + " | ".join(str(v) for v in r) + " |\n" for r in rows)
    else:
        table = _csv(hdr, rows)
    return "\n".join(lines) + "\n\n" + table


def fake_web(T, ctx):
    def pts(series):
        return [{"timestamp": epoch_of(d), "daily_count": n, "cumulative_count": cn, "daily_sats": s, "cumulative_sats": cs}
                for d, n, cn, s, cs in series]

    def items(cutoff):
        lb = sorted(T.web_leaderboard(cutoff).items(), key=lambda kv: (-kv[1][2], -kv[1][1], -kv[1][0], kv[0]))
        return [{"mint_url": m, "config_count": c, "payment_count": p, "total_amount_sats": s, "has_page": h}
                for m, (c, p, s, h) in lb]

    tops = sorted(T.web_top_mints(NOW).items(), key=lambda kv: -kv[1])[:20]
    W = {"overview": {"data": {"dailySeries": pts(T.web_daily(NOW)), "nutzapsMAU": []}},
         "mints_page": {"data": {"mintStats": items(None), "period": "all", "topMintsCumulative": [
             {"mint_url": m, "total_sats": s,
              "daily": [{"timestamp": epoch_of(d), "cumulative_sats": c} for d, c in T.web_mint_cumulative(NOW, m)]}
             for m, s in tops]}}}
    for period, n in (("all", None), ("7d", 7), ("30d", 30), ("1y", 365)):
        W["api_" + period] = {"data": {"items": items(None if n is None else NOW - n * 86400), "period": period}}
    for role in ("top", "few", "config_only"):
        det = T.web_mint_detail(NOW, ctx[role + "_mint"])
        W["mint_" + role] = {"data": {"detail": {
            **{k: det[k] for k in ("mint_url", "total_payments", "total_sats", "config_count", "all_time_payments",
                                   "all_time_sats")},
            "daily": pts(det["daily"]), "mau": [{"timestamp": epoch_of(m), "value": v} for m, v in det["mau"]]}}}
    W["mint_excluded"] = {"status": 404, "data": None}
    for v in W.values():
        v.setdefault("status", 200)
        v["t0"], v["t1"] = NOW, NOW
    return W


def _battery(mutate=None):
    sql = _sql()
    T = Truth(sql)
    ctx = nutzaps_run.build_context(sql, TODAY)
    M = {}
    for s in nutzaps_plan.build(ctx):
        body = fake_server(s, T)
        M[s["id"]] = Resp(s, mutate(s, body) if mutate else body)
    tools = {"nostrology_query_nutzaps": {"description": "THIS TOOL IS PAID: 21 sat per call", "inputSchema": {
                 "properties": {"metric": {"enum": ["active_users"]}, "cashu_token": {}}}},
             "nostrology_query_metric": {"description": "", "inputSchema": {"properties": {
                 "domain": {"enum": ["nutzaps", "routstr", "zapstore"]}, "mint_url": {}}}}}
    return M, sql, fake_web(T, ctx), ctx, tools


def test_battery_passes_against_a_consistent_fake_server():
    M, sql, W, ctx, tools = _battery()
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    fails = [(q["n"], q["cat"], q["q"], q["got"], q["want"]) for q in Q if report_mod.verdict(q) == "fail"]
    assert not fails, fails[:5]
    cats = {q["cat"] for q in Q}
    assert len(Q) > 150 and len(cats) == 15  # every section ran and none raised
    # the fixture straddles both rolling cutoffs, so the brackets are genuinely exercised
    assert sum(1 for q in Q if report_mod.verdict(q) == "bounded") >= 3
    assert nutzaps_run.self_test(M, sql, W, ctx, tools, Q) == 0


def test_results_json_serializes_every_question(tmp_path):
    """The per-(day, mint) views are tuple-keyed dicts; the 2026-09-30 local run wrote
    its HTML and then died half-way through the results JSON on them."""
    import json
    M, sql, W, ctx, tools = _battery()
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    out = tmp_path / "r.json"
    nutzaps_run.write_results(str(out), {"run_date": TODAY}, ctx, Q)
    doc = json.loads(out.read_text())
    assert len(doc["questions"]) == len(Q) and doc["ctx"]["top_mint"] == A
    assert nutzaps_run.jsonable({("2026-09-01", A): 1, "s": {1, 2}}) == {f"2026-09-01|{A}": 1, "s": [1, 2]}


def test_battery_fails_when_the_server_is_wrong():
    def drop_top_row(step, body):
        if step["id"] == "NZ30":  # the All leaderboard loses its first data row
            head, _, table = body.partition("\n\n")
            lines = table.splitlines()
            return head + "\n\n" + "\n".join(lines[:1] + lines[2:]) + "\n"
        return body

    M, sql, W, ctx, tools = _battery(drop_top_row)
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    failed = {q["cat"] for q in Q if report_mod.verdict(q) == "fail"}
    assert {"Tool - mint_leaderboard@total", "Cross-call reconciliation", "Live page - /nutzaps/mints leaderboard"} <= failed


def test_battery_fails_when_a_chart_loses_a_month():
    """The unbroken-months question is not vacuous: a page that drops a 0 month from its
    series, as they all did until nostrology-web#33 (Per-mint monthly users chart hides
    months with no activity), fails it."""
    M, sql, W, ctx, tools = _battery()
    mau = W["mint_top"]["data"]["detail"]["mau"]
    del mau[next(i for i, p in enumerate(mau) if 0 < i < len(mau) - 1 and p["value"] == 0)]
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    failed = [q["q"] for q in Q if report_mod.verdict(q) == "fail"]
    assert any("months run unbroken" in q for q in failed), failed


def test_a_forged_later_month_is_no_disagreement_between_chart_and_tool(monkeypatch):
    """The page draws no month past now, by design, and the tool lists whatever the archive
    holds: a date in a later month must not read as the chart losing it. Other questions may
    still fail on such a row, so this holds the one comparison."""
    real = _payments
    forged = {"created_at": t("2030-01-01"), "pubkey": PK[4], "mint_url": A, "amount_sats": 1}
    monkeypatch.setitem(globals(), "_payments", lambda: real() + [forged])
    M, sql, W, ctx, tools = _battery()
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    held = [q for q in Q if "equal mint_active_users@month" in q["q"]]
    assert held
    assert not [q["q"] for q in held if report_mod.verdict(q) == "fail"]


def test_battery_fails_against_the_0_5_0_rejections_and_flag():
    """The 0.5.1 questions are not vacuous: a server that rejects a grain WITHOUT the
    redirect clause, and still says additive_over_time: true on payment_activity@total
    (with the old mints description and no caveat), fails exactly those questions."""
    def as_0_5_0(step, body):
        if body.startswith("TOOLERROR:") and " serves grain " in body:
            return body.split(" - ", 1)[0]
        a = step["args"]
        if a.get("metric") == "payment_activity" and a.get("grain") == "total" and not body.startswith("TOOLERROR:"):
            head, sep, table = body.partition("\n\n")
            head = head.replace("additive_over_time: false", "additive_over_time: true", 1)
            head = head.replace(" - a distinct count, so it does not add across windows", "")
            head = re.sub(r" Across separate windows only payments and sats add up[^\n]*", "", head)
            return head + sep + table
        return body

    M, sql, W, ctx, tools = _battery(as_0_5_0)
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    failed = sorted(q["q"][:60] for q in Q if report_mod.verdict(q) == "fail")
    assert failed == sorted(q[:60] for q in [
        "grain=week on payment_activity is rejected, naming the grains it serves and the call that answers a week?",
        "grain=day on mint_leaderboard is rejected, pointing to the per-mint daily rows?",
        "grain=total on mint_activity is rejected, pointing to each mint's totals?",
        "...and says why no sum of buckets replaces a total (distinct counts do not add)?",
        "...and says additive_over_time: true exactly on the day series of payments and sats",
        "payment_activity@total says only its payments and sats add up across windows, not its mints?",
    ]), failed


def test_battery_fails_when_an_excluded_mint_leaks():
    def leak(step, body):
        if step["id"] == "NZ100":
            return body.replace("utc_day,payments,sats\n", "utc_day,payments,sats\n2026-09-10,1,999\n")
        return body

    M, sql, W, ctx, tools = _battery(leak)
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    assert any(report_mod.verdict(q) == "fail" and "excluded mint returns no rows" in q["q"] for q in Q)


def test_the_paid_line_fails_when_the_paid_table_or_a_per_mint_row_disagrees():
    # (1) the paid table counts one sender fewer in one week than the raw rows hold
    M, sql, W, ctx, tools = _battery()
    sql = copy.deepcopy(sql)
    row = next(r for r in sql["PAID_AU"] if r["granularity"] == "week" and r["unique_users"] >= 2)
    row["unique_users"] -= 1
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    bad = [q["q"] for q in Q if report_mod.verdict(q) == "fail"]
    # exactly the two week checks: the paid table vs the raw rows, and (the busiest
    # mint now above the lowered network count) the bracket
    assert len(bad) == 2 and all(q.startswith("week: ") for q in bad), bad
    assert any("the paid table counts the senders" in q for q in bad), bad
    # (2) the tool inflates one per-mint day row above the network count
    def inflate(step, body):
        if step["id"] == "NZ62":
            head, _, table = body.partition("\n\n")
            lines = table.splitlines()
            b, m, n = lines[1].split(",")
            return head + "\n\n" + "\n".join([lines[0], f"{b},{m},{int(n) + 5}"] + lines[2:]) + "\n"
        return body
    M, sql, W, ctx, tools = _battery(inflate)
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    failed = [q["q"] for q in Q if report_mod.verdict(q) == "fail"]
    assert any(q.startswith("day: in every bucket no mint's count exceeds") for q in failed), failed
    assert any("day buckets, every mint" in q for q in failed) and any("sits inside its (week" in q for q in failed)
    # (3) a cached raw read without the paid section scores FAIL, never a vacuous PASS
    M, sql, W, ctx, tools = _battery()
    del sql["PAID_AU"]
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    assert any(q["q"].startswith("The paid table (active_user_counts") and report_mod.verdict(q) == "fail" for q in Q)


def test_a_malformed_response_fails_its_section_instead_of_crashing():
    M, sql, W, ctx, tools = _battery()
    W = copy.deepcopy(W)
    W["overview"]["data"] = {}  # the page lost its dailySeries
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    bad = [q for q in Q if q["cat"] == "Live page - /nutzaps" and report_mod.verdict(q) == "fail"]
    assert len(bad) == 1 and "KeyError" in bad[0]["got"]


def test_run_delegates_domain_nutzaps(monkeypatch):
    from nostrology_mcp.verification import run as run_mod
    seen = {}
    monkeypatch.setattr(nutzaps_run, "main", lambda argv: (seen.__setitem__("argv", argv), 0)[1])
    assert run_mod.main(["--domain", "nutzaps", "--reuse", "--self-test"]) == 0
    assert seen["argv"] == ["--reuse", "--self-test"]


def _gone(pid, within=3.0):
    """True once pid no longer exists (a SIGTERMed orphan is reaped by launchd/init)."""
    import os
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


def test_run_script_does_not_hang_on_an_orphan_holding_the_output(tmp_path):
    """The 2026-09-30 hang: over SSH-over-SSM the ProxyCommand's session-manager-plugin
    inherited ssh's stderr and outlived ssh, so a pipe-based subprocess.run waited for
    an EOF that never came. A child that exits while ITS child keeps stdout and stderr
    open must return at once - and that grandchild must be killed with the group."""
    pidfile = tmp_path / "pid"
    argv = ["sh", "-c", f"sleep 30 & echo $! > {pidfile}; printf '#FP\\n[{{\"a\": 1}}]\\n#END\\n'"]
    t0 = time.monotonic()
    assert nutzaps_truth.run_script("select 1;\n", timeout=20, argv=argv) == {"FP": [{"a": 1}]}
    assert time.monotonic() - t0 < 10
    assert _gone(int(pidfile.read_text()))


def test_run_script_reports_failures_and_timeouts(monkeypatch):
    monkeypatch.setattr(nutzaps_truth, "RETRY_DELAY", 0)
    with pytest.raises(RuntimeError, match=r"failed \(3\).*boom"):
        nutzaps_truth.run_script("x", argv=["sh", "-c", "echo boom >&2; exit 3"])
    with pytest.raises(RuntimeError, match=r"failed 2 times.*\(255\)"):  # ssh's own failure is retried
        nutzaps_truth.run_script("x", attempts=2, argv=["sh", "-c", "echo closed >&2; exit 255"])
    t0 = time.monotonic()
    with pytest.raises(RuntimeError, match="timed out"):
        nutzaps_truth.run_script("x", timeout=1, argv=["sh", "-c", "sleep 30"])
    assert time.monotonic() - t0 < 10

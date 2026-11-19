"""The Routstr battery, structured by TOOL and METRIC rather than by chart.

The same two rules as the Zapstore battery (`questions.py`): never hard-code an
expected value (every `want` is recomputed from the reference re-derivation `R`,
the substrate rows `T` or the live page `W` on each run), and say in `how` which
independent path the check rests on.

The Zapstore battery was organised around what the SITE shows (chart 1..4, the
cards, the app page). This one is organised around what a CALLER - typically a model
client - asks the tools, one section per metric or record, plus the contract the model
relies on (domain routing, rejections, caps, formats), and the whole-document agreement
with the reference implementation and the live page.
"""
import json
import math
import re

from .routstr_truth import Truth, ranked, shift


def sats(msats):
    """The pipeline's msats -> sats rule: round half away from zero, once."""
    v = msats / 1000.0
    return int(math.floor(v + 0.5)) if v >= 0 else int(math.ceil(v - 0.5))


def I(v):
    return None if v in (None, "\\N", "") else int(v)


def F(v):
    return None if v in (None, "\\N", "") else float(v)


def argmax(pairs):
    """(key, value) with the largest value; ties -> the later/greater key, both sides."""
    return max(pairs, key=lambda kv: (kv[1], kv[0]))


# ---------------------------------------------------------------------------
def build(M, sql, R, W, ctx):
    """M: id -> Resp. sql: substrate sections. R: the reference re-derivation
    (crosscheck/, DuckDB over S3). W: the page-embedded document. ctx: run
    context."""
    T = Truth(sql)
    doc = T.doc
    Q = []

    def ask(cat, question, calls, got, want, how):
        Q.append(dict(n=len(Q) + 1, cat=cat, q=question, calls=calls,
                      got=got, want=want, how=how, bounded=False,
                      ok=(bool(got) == want) if isinstance(want, bool) else (got == want)))

    top, exc, tmodel, fmodel = ctx["top_provider"], ctx["excluded_provider"], ctx["top_model"], ctx["folded_model"]
    R30F, R30T = ctx["refresh_30d_from"], ctx["refresh_30d_to"]
    L7F, L7T = ctx["last7_from"], ctx["last7_to"]
    L30F, L30T = ctx["last30_from"], ctx["last30_to"]
    MF, TODAY = ctx["month_from"], ctx["today"]
    rs = R["summary"]

    # =======================================================================
    C = "Tool - network_summary"
    r01 = M["RS01"].dicts()[0]
    for col in ("providers_announced", "providers_reporting", "providers_active_30d",
                "revenue_30d_sats", "requests_30d", "tokens_30d", "revenue_alltime_sats",
                "requests_alltime", "tokens_alltime", "models_30d", "catalog_models", "mints"):
        ask(C, f"How big is Routstr - network_summary.{col}?", ["RS01"], I(r01[col]), rs[col],
            "REFERENCE: the DuckDB re-derivation over the raw S3 archive, summary." + col)
    ask(C, "first_activity / last_activity bracket the ledger?", ["RS01"],
        (r01["first_activity"], r01["last_activity"]), (rs["first_activity"], rs["last_activity"]),
        "REFERENCE summary.first_activity/last_activity")
    ask(C, "The daily and weekly ledger starts are the coverage the reference derives?", ["RS01"],
        (r01["daily_ledger_from"], r01["weekly_ledger_from"]),
        (R["coverage"]["daily_from"], R["coverage"]["weekly_from"]), "REFERENCE coverage block")
    ask(C, "success_rate_30d in the headline row equals the reference's reliability rate?", ["RS01"],
        F(r01["success_rate_30d"]), R["success"]["rate_30d"], "REFERENCE success.rate_30d")
    ask(C, "The three exclusion counts (test, private, squatters) match the reference ledger?", ["RS01"],
        (I(r01["excluded_test_nodes"]), I(r01["excluded_private_nodes"]), I(r01["kind_squatters"])),
        tuple(e["v"] for e in R["exclusions"]), "REFERENCE exclusions[] in precedence order")
    ask(C, "revenue_alltime_sats is the all-time msats of the substrate ledger, rounded once?", ["RS01"],
        I(r01["revenue_alltime_sats"]), sats(sum(v[0] for v in T.provider_totals().values())),
        "SQL: SUM(msats) over routstr_provider_days (both grains), included providers, /1000 rounded half away from zero")
    ask(C, "requests_alltime / tokens_alltime are the substrate's all-time sums?", ["RS01"],
        (I(r01["requests_alltime"]), I(r01["tokens_alltime"])),
        tuple(sum(v[i] for v in T.provider_totals().values()) for i in (1, 2)),
        "SQL: SUM over routstr_provider_days, included providers, both grains")
    ask(C, "providers_reporting is the number of included providers with any ledger row?", ["RS01"],
        I(r01["providers_reporting"]), len(T.provider_totals()),
        "SQL: COUNT(DISTINCT pubkey) in routstr_provider_days among included providers")
    ask(C, "The refresh instant is the served document's generation instant?", ["RS01"],
        r01["refresh_generated_at"], T.doc_generated, "SQL: routstr_snapshot.generated_utc of the newest row")

    # =======================================================================
    C = "Tool - network_activity"
    r02 = M["RS02"].dicts()
    series = [(r["utc_day"], I(r["msats"]), I(r["requests"]), I(r["tokens"]), I(r["active_providers"])) for r in r02]
    ask(C, "The full daily series is the reference's daily[] row for row?", ["RS02"], series,
        [(d["d"], d["msats"], d["req"], d["tok"], d["prov"]) for d in R["daily"]],
        "REFERENCE daily[] (the independent ledger reconstruction)")
    nd = T.network_days()
    ask(C, "Every day's four measures re-sum from the raw substrate buckets?", ["RS02"],
        {d: (a, b, c, p) for d, a, b, c, p in series}, nd,
        "SQL: per-day SUM over routstr_provider_days grain='d', included providers, DISTINCT pubkeys present")
    ask(C, "The series is dense from the first daily-covered day to the last ledger day?", ["RS02"],
        (series[0][0], series[-1][0], len(series)),
        (R["coverage"]["daily_from"], rs["last_activity"], len(nd)),
        "REFERENCE coverage.daily_from .. summary.last_activity; SQL day count")
    ask(C, "Which day had the most reported revenue, and how much (msats)?", ["RS02"],
        argmax([(d, m) for d, m, *_ in series]), argmax([(d, v[0]) for d, v in nd.items()]),
        "argmax over the series vs argmax over the SQL re-sum")
    ask(C, "Which day had the most successful requests?", ["RS02"],
        argmax([(d, q) for d, _, q, *_ in series]), argmax([(d, v[1]) for d, v in nd.items()]),
        "argmax over requests, both sides")
    ask(C, "What is the largest number of providers active on one day?", ["RS02"],
        max(p for *_, p in series), max(v[3] for v in nd.values()), "max of the per-day distinct count")
    ask(C, "How many days had zero reported revenue (the floor semantics - a gap, not zero traffic)?", ["RS02"],
        sum(1 for _, m, *_ in series if m == 0), sum(1 for v in nd.values() if v[0] == 0),
        "count of zero-msats days, both sides; the caveat says these are unreported, not idle")
    ask(C, "Summing refresh-30..refresh-1 reproduces the 30d headline (sats, requests, tokens)?", ["RS02", "RS01"],
        (sats(sum(m for d, m, *_ in series if R30F <= d <= R30T)),
         sum(q for d, _, q, *_ in series if R30F <= d <= R30T),
         sum(t for d, _, _, t, _ in series if R30F <= d <= R30T)),
        (rs["revenue_30d_sats"], rs["requests_30d"], rs["tokens_30d"]),
        f"client-side window {R30F}..{R30T} over the series vs REFERENCE summary 30d")
    r03 = M["RS03"].dicts()
    ask(C, f"'Last 7 days' (a client's recipe: {L7F}..{L7T}) - msats, requests, tokens?", ["RS03"],
        (sum(I(r["msats"]) for r in r03), sum(I(r["requests"]) for r in r03), sum(I(r["tokens"]) for r in r03)),
        T.network_sum(L7F, L7T), "SQL: SUM over routstr_provider_days grain='d' in the window, included providers")
    ask(C, "The server-side 7-day window equals slicing the full series client-side?", ["RS03", "RS02"],
        [(r["utc_day"], I(r["msats"])) for r in r03], [(d, m) for d, m, *_ in series if L7F <= d <= L7T],
        "window consistency: the same metric, windowed by the server vs by the caller")
    ask(C, "The 7-day window echoes its bounds in the envelope?", ["RS03"],
        M["RS03"].env.get("window", "").startswith(f"{L7F}..{L7T}"), True, "envelope window line")
    r04 = M["RS04"].dicts()
    ask(C, f"'Last 30 days' from TODAY ({L30F}..{L30T}) - revenue in sats?", ["RS04"],
        sats(sum(I(r["msats"]) for r in r04)), sats(T.network_sum(L30F, L30T)[0]),
        "SQL window sum; NOTE this is today-anchored and differs from the page's refresh-anchored 30d by design")
    ask(C, "The today-anchored and refresh-anchored 30d windows differ only by the days that moved?", ["RS04", "RS02"],
        sum(I(r["msats"]) for r in r04) - sum(m for d, m, *_ in series if R30F <= d <= R30T),
        sum(m for d, m, *_ in series if L30F <= d <= L30T) - sum(m for d, m, *_ in series if R30F <= d <= R30T),
        "the difference of the two windows, from the tool vs from the series")
    r05 = M["RS05"].dicts()
    ask(C, f"'This month' ({MF}, open-ended to) - requests so far?", ["RS05"],
        sum(I(r["requests"]) for r in r05), T.network_sum(MF, None)[1],
        "SQL: SUM over grain='d' rows from the month's first day")
    ask(C, "An open-ended 'to' defaults to today and flags the final day as in progress?", ["RS05"],
        "in progress" in M["RS05"].body and M["RS05"].env.get("window", "").startswith(f"{MF}..{TODAY}"), True,
        "the incomplete-day rule must be declared; the ledger never holds today anyway")
    ask(C, "The series carries no row for today or later (the refresh day is never a ledger day)?", ["RS02"],
        max(d for d, *_ in series) < ctx["refresh_day"], True, "max(utc_day) vs the refresh day")
    ask(C, "active_providers is declared non-additive in the methodology?", ["RS02"],
        "never sum" in M["RS02"].body.lower() or "not additive" in M["RS02"].body.lower()
        or "distinct" in M["RS02"].body.lower(), True, "methodology text check")

    # =======================================================================
    C = "Tool - cumulative_revenue"
    r06 = M["RS06"].dicts()
    cum = [(r["utc_day"], I(r["cumulative_msats"])) for r in r06]
    ask(C, "The running total is the reference's cum_revenue[] row for row?", ["RS06"], cum,
        [(c["d"], c["msats"]) for c in R["cum_revenue"]], "REFERENCE cum_revenue[]")
    ask(C, "It ends at the all-time revenue headline (sats)?", ["RS06", "RS01"],
        sats(cum[-1][1]), rs["revenue_alltime_sats"], "last row /1000 vs REFERENCE summary")
    ask(C, "It is monotone non-decreasing?", ["RS06"], all(a[1] <= b[1] for a, b in zip(cum, cum[1:])), True,
        "pairwise scan")
    ask(C, "It starts in the weekly pre-daily era, before the daily series does?", ["RS06", "RS02"],
        cum[0][0] < series[0][0] and cum[0][0] >= R["coverage"]["weekly_from"], True,
        "first row vs the daily series' first day and the weekly coverage start")
    ask(C, "The last cumulative point equals the SUM of every substrate bucket, both grains?", ["RS06"],
        cum[-1][1], sum(v[0] for v in T.provider_totals().values()),
        "SQL: SUM(msats) over routstr_provider_days, included providers")
    r07 = M["RS07"].dicts()
    ask(C, "Windowed to the refresh 30d, the last minus first-before rows give the 30d revenue?", ["RS07", "RS06"],
        I(r07[-1]["cumulative_msats"]) - next((v for d, v in reversed(cum) if d < R30F), 0),
        sum(m for d, m, *_ in series if R30F <= d <= R30T),
        "difference of running totals across the window vs the windowed daily sum")
    ask(C, "The windowed call keeps the running total's absolute values (not re-based)?", ["RS07", "RS06"],
        [(r["utc_day"], I(r["cumulative_msats"])) for r in r07], [(d, v) for d, v in cum if R30F <= d <= R30T],
        "server window vs client slice of the same running total")

    # =======================================================================
    C = "Tool - success_rate"
    r08 = M["RS08"].dicts()[0]
    ask(C, "The 30d success rate, requests and failures reported are the reference's?", ["RS08"],
        (F(r08["success_rate_30d"]), I(r08["requests_reported"]), I(r08["failed_reported"]), I(r08["providers_fresh"])),
        (R["success"]["rate_30d"], R["success"]["requests_reported"], R["success"]["failed_reported"],
         R["success"]["providers_fresh"]), "REFERENCE success block")
    if r08["success_rate_30d"] not in ("\\N", ""):
        ask(C, "The rate is failed/(reported) to four decimals, as the methodology states?", ["RS08"],
            F(r08["success_rate_30d"]),
            round(1 - I(r08["failed_reported"]) / I(r08["requests_reported"]), 4) if I(r08["requests_reported"]) else None,
            "recomputed from the row's own numerator and denominator")
    ask(C, "The reliability figures are declared self-reported and fresh-provider-only?", ["RS08"],
        "self-reported" in M["RS08"].body.lower() and "fresh" in M["RS08"].body.lower(), True, "methodology text")
    ask(C, "It is a snapshot: no time axis, grain=current only?", ["RS08"],
        M["RS08"].env.get("series_type") == "snapshot" or "snapshot" in M["RS08"].body.splitlines()[0], True,
        "envelope identity line")

    # =======================================================================
    C = "Tool - provider_leaderboard"
    r10 = M["RS10"].dicts()
    tot_all0 = T.provider_totals()
    by_pk = {r["provider_pubkey"]: r for r in r10}
    ref_prov = R["providers"]
    ask(C, "How many providers, and how many of them excluded?", ["RS10"],
        (len(r10), sum(r["excluded"] == "true" for r in r10)),
        (R["providers_total"], sum(1 for p in ref_prov if p["excluded"])), "REFERENCE providers_total / providers[]")
    ask(C, "Ranked by all-time revenue, the top 10 (by 12-char key prefix) match the reference?", ["RS10"],
        [r["provider_pubkey"][:12] for r in r10[:10]], [p["id"] for p in ref_prov[:10]],
        "REFERENCE providers[] order (msats_alltime desc)")
    ask(C, "Every provider's 30d and all-time revenue equals the reference row?", ["RS10"],
        sorted((r["provider_pubkey"][:12], I(r["msats_30d"]), I(r["msats_alltime"])) for r in r10),
        sorted((p["id"], p["msats_30d"], p["msats_alltime"]) for p in ref_prov),
        "REFERENCE providers[] msats_30d / msats_alltime, keyed by prefix")
    ask(C, "Every provider's 30d requests and tokens equal the reference row?", ["RS10"],
        sorted((r["provider_pubkey"][:12], I(r["requests_30d"]), I(r["tokens_30d"])) for r in r10),
        sorted((p["id"], p["req_30d"], p["tok_30d"]) for p in ref_prov), "REFERENCE providers[] req_30d / tok_30d")
    ask(C, "Name, endpoint class, version and review label match the reference registry?", ["RS10"],
        sorted((r["provider_pubkey"][:12], r["provider_name"], r["endpoint_class"], r["version"],
                None if r["review"] == "\\N" else r["review"]) for r in r10),
        sorted((p["id"], p["name"], p["class"], p["version"], p["review"]) for p in ref_prov),
        "REFERENCE providers[] registry columns")
    ask(C, "first_seen / last_announce / last_report dates match the reference?", ["RS10"],
        sorted((r["provider_pubkey"][:12], r["first_seen"][:10], r["last_announce"][:10], r["last_report"][:10]) for r in r10),
        sorted((p["id"], p["first_seen"] or "\\N", p["last_announce"] or "\\N", p["last_report"] or "\\N") for p in ref_prov),
        "REFERENCE providers[] dates (NULL rendered as \\N)")
    ask(C, "All-time requests and tokens per included provider re-sum from the substrate (both grains)?", ["RS10"],
        sorted((pk, I(r["requests_alltime"]), I(r["tokens_alltime"])) for pk, r in by_pk.items() if pk in tot_all0),
        sorted((pk, v[1], v[2]) for pk, v in tot_all0.items()),
        "SQL: SUM(requests), SUM(tokens) over routstr_provider_days per included pubkey")
    ask(C, "NUANCE - excluded providers show ZERO usage on the leaderboard (the page's rule) even where the ledger holds their self-reports?",
        ["RS10"], sorted((pk, I(r["msats_alltime"]), I(r["requests_alltime"])) for pk, r in by_pk.items() if r["excluded"] == "true"),
        sorted((pk, 0, 0) for pk, r in by_pk.items() if r["excluded"] == "true"),
        "the document accumulates included providers only; an excluded node's own figures are reachable via "
        "provider_activity + provider_pubkey (checked below) - the leaderboard row is the page's table, not the ledger")
    ask(C, "Included providers' revenue sums to the network all-time headline?", ["RS10", "RS01"],
        sats(sum(I(r["msats_alltime"]) for r in r10 if r["excluded"] == "false")), I(r01["revenue_alltime_sats"]),
        "cross-call reconciliation: leaderboard rows -> headline")
    ask(C, "Excluded providers carry a reason, included ones do not?", ["RS10"],
        all((r["excl_reason"] != "\\N") == (r["excluded"] == "true") for r in r10), True, "column pairing scan")
    ask(C, "Which provider tops the 30d window, and with how many sats?", ["RS10"],
        (max(r10, key=lambda r: (I(r["msats_30d"]), r["provider_pubkey"]))["provider_pubkey"][:12],
         sats(max(I(r["msats_30d"]) for r in r10))),
        (max(ref_prov, key=lambda p: (p["msats_30d"], p["id"]))["id"], sats(max(p["msats_30d"] for p in ref_prov))),
        "argmax over msats_30d, both sides")
    ask(C, "How many providers had any 30d activity (the page's active_30d count)?", ["RS10", "RS01"],
        sum(1 for r in r10 if r["excluded"] == "false" and (I(r["msats_30d"]) or I(r["requests_30d"]) or I(r["tokens_30d"]))),
        rs["providers_active_30d"], "count of included rows with nonzero 30d columns vs REFERENCE summary")
    r11 = M["RS11"].dicts()
    ask(C, "Filtered to the top provider, exactly its row comes back?", ["RS11", "RS10"],
        [(r["provider_pubkey"], I(r["msats_alltime"])) for r in r11], [(top, I(by_pk[top]["msats_alltime"]))],
        "provider_pubkey filter vs the unfiltered table's row")
    r12 = M["RS12"].dicts()
    if exc in T.providers:
        ask(C, "Filtered to an excluded provider, its row is served AND flagged excluded with a reason?", ["RS12"],
            [(r["provider_pubkey"], r["excluded"], r["excl_reason"]) for r in r12],
            [(exc, "true", T.providers[exc]["excl_reason"])], "SQL: routstr_providers.excl_reason")
    ask(C, "The unfiltered call is capped (row_cap) but the cap does not bind on today's registry?", ["RS10"],
        (M["RS10"].declared_rows(), bool(M["RS10"].truncated)), (len(r10), False), "rows: line vs rows parsed")
    ask(C, "Rows are ordered by all-time revenue desc (the page's default sort)?", ["RS10"],
        [I(r["msats_alltime"]) for r in r10] == sorted((I(r["msats_alltime"]) for r in r10), reverse=True), True,
        "monotone scan of msats_alltime")

    # =======================================================================
    C = "Tool - provider_activity"
    r13 = {r["provider_pubkey"]: r for r in M["RS13"].dicts()}
    tot_all = T.provider_totals()
    ask(C, "grain=total, no window: every included provider's all-time msats/requests/tokens?", ["RS13"],
        sorted((pk, I(r["msats"]), I(r["requests"]), I(r["tokens"])) for pk, r in r13.items()),
        sorted((pk, v[0], v[1], v[2]) for pk, v in tot_all.items()),
        "SQL: SUM over routstr_provider_days per pubkey, BOTH grains, included providers")
    ask(C, "...and its daily/weekly bucket counts and first/last days?", ["RS13"],
        sorted((pk, I(r["daily_buckets"]), I(r["weekly_buckets"]), r["first_day"], r["last_day"]) for pk, r in r13.items()),
        sorted((pk, v[3], v[4], v[5], v[6]) for pk, v in tot_all.items()), "SQL: COUNT per grain, MIN/MAX(day)")
    ask(C, "The unwindowed totals equal the leaderboard's all-time columns?", ["RS13", "RS10"],
        sorted((pk, I(r["msats"])) for pk, r in r13.items()),
        sorted((pk, I(r["msats_alltime"])) for pk, r in by_pk.items() if r["excluded"] == "false" and pk in r13),
        "cross-call: substrate SUM vs document-derived column")
    r14 = {r["provider_pubkey"]: r for r in M["RS14"].dicts()}
    tot30 = T.provider_totals(R30F, R30T)
    ask(C, f"Refresh-anchored 30d window ({R30F}..{R30T}) equals the leaderboard's 30d columns for every provider?",
        ["RS14", "RS10"], sorted((pk, I(r["msats"]), I(r["requests"]), I(r["tokens"])) for pk, r in r14.items()),
        sorted((pk, I(r["msats_30d"]), I(r["requests_30d"]), I(r["tokens_30d"])) for pk, r in by_pk.items()
               if r["excluded"] == "false" and (I(r["msats_30d"]) or I(r["requests_30d"]) or I(r["tokens_30d"]))),
        "THE parity gate: the windowed SUM vs the page's fixed 30d figures")
    ask(C, "...and equals an independent daily-only re-sum of the substrate in that window?", ["RS14"],
        sorted((pk, I(r["msats"]), I(r["weekly_buckets"])) for pk, r in r14.items()),
        sorted((pk, v[0], 0) for pk, v in tot30.items()), "SQL: grain='d' rows only inside the window; weekly must be 0")
    ask(C, "Summing the 30d rows over providers gives the network 30d headline?", ["RS14", "RS01"],
        (sats(sum(I(r["msats"]) for r in r14.values())), sum(I(r["requests"]) for r in r14.values())),
        (I(r01["revenue_30d_sats"]), I(r01["requests_30d"])), "cross-call reconciliation to network_summary")
    r15 = M["RS15"].dicts()
    tot7 = T.provider_totals(L7F, L7T)
    ask(C, f"Which providers served the most tokens last week ({L7F}..{L7T})?", ["RS15"],
        sorted(((r["provider_pubkey"], I(r["tokens"])) for r in r15), key=lambda kv: (-kv[1], kv[0]))[:5],
        ranked(tot7, 2)[:5], "SQL: SUM(tokens) per provider over grain='d' rows in the window, top 5")
    ask(C, "...with revenue and requests for the same week?", ["RS15"],
        sorted((r["provider_pubkey"], I(r["msats"]), I(r["requests"])) for r in r15),
        sorted((pk, v[0], v[1]) for pk, v in tot7.items()), "SQL window sums")
    ask(C, "Rows are ordered by msats desc then pubkey (the documented ordering)?", ["RS15"],
        [(r["provider_pubkey"]) for r in r15],
        [pk for pk, _ in sorted(((pk, v[0]) for pk, v in tot7.items()), key=lambda kv: (-kv[1], kv[0]))],
        "ordering_note vs a client-side sort of the SQL totals")
    r16 = M["RS16"].dicts()
    ask(C, f"'Last 30 days' from today ({L30F}..{L30T}) per provider?", ["RS16"],
        sorted((r["provider_pubkey"], I(r["msats"])) for r in r16),
        sorted((pk, v[0]) for pk, v in T.provider_totals(L30F, L30T).items()), "SQL window sums")
    ask(C, "The provider sums of that window equal the network series' window sum?", ["RS16", "RS04"],
        sum(I(r["msats"]) for r in r16), sum(I(r["msats"]) for r in r04), "cross-call: per-provider vs network")
    r17 = M["RS17"].dicts()
    ask(C, "grain=day for the top provider: its rows re-sum to its all-time total?", ["RS17", "RS13"],
        (sum(I(r["msats"]) for r in r17), len(r17)), (I(r13[top]["msats"]), len(T.provider_rows(pubkey=top, daily_only=False))),
        "SUM and COUNT of the filtered day rows vs the grain=total row and the SQL row count")
    ask(C, "Its day rows ARE the substrate rows (day, grain, msats, requests, tokens)?", ["RS17"],
        [(r["utc_day"], r["grain"], I(r["msats"]), I(r["requests"]), I(r["tokens"])) for r in r17],
        [(r["day"], r["grain"], r["msats"], r["requests"], r["tokens"])
         for r in sorted(T.provider_rows(pubkey=top, daily_only=False), key=lambda r: (r["day"], r["grain"]))],
        "SQL rows, ordered by day")
    ask(C, "The provider_pubkey filter is echoed in the envelope scope line?", ["RS17"],
        M["RS17"].env.get("scope"), f"provider_pubkey={top}", "envelope scope line")
    r18 = M["RS18"]
    n_inc_rows = len([r for r in T.pdays if r["pubkey"] in T.included])
    ask(C, "Unfiltered grain=day serves every included bucket up to the 2,000-row cap, announcing truncation only when it binds?",
        ["RS18"], (len(r18.rows), bool(r18.truncated), "provider_pubkey" in (r18.truncated[0] if r18.truncated else "")),
        (min(2000, n_inc_rows), n_inc_rows > 2000, n_inc_rows > 2000),
        f"SQL: {n_inc_rows} included-provider buckets vs the cap; a truncated: line must name the filter remedy when present")
    ask(C, "Its rows: line states the true row count (N of M when capped, N otherwise)?", ["RS18"],
        re.search(r"rows:\s*(\d+(?: of \d+)?)", r18.body).group(1),
        (f"{min(2000, n_inc_rows)} of {n_inc_rows}" if n_inc_rows > 2000 else str(n_inc_rows)), "envelope rows: line")
    ask(C, "The capped day series stays ordered by day, then pubkey (never re-sorted by the cap)?", ["RS18"],
        [(r["utc_day"], r["provider_pubkey"]) for r in r18.dicts()] ==
        sorted((r["utc_day"], r["provider_pubkey"]) for r in r18.dicts()), True, "ordering scan of the served rows")
    r19 = M["RS19"].dicts()
    ask(C, "An excluded provider is reachable by filter, flagged, and never in the unfiltered total?", ["RS19", "RS13"],
        ([(r["provider_pubkey"], r["excluded"]) for r in r19], exc in r13),
        ([(exc, "true")] if T.provider_rows(pubkey=exc, daily_only=False) else [], False),
        "SQL: the excluded subject's substrate rows exist; exclusion applies to the unfiltered call only")
    r20 = M["RS20"].dicts()
    ask(C, "Filter + window together: the top provider's last-7-day rows?", ["RS20"],
        [(r["utc_day"], I(r["msats"])) for r in r20],
        [(r["day"], r["msats"]) for r in T.provider_rows(L7F, L7T, pubkey=top)], "SQL rows in the window for that pubkey")

    # =======================================================================
    C = "Tool - model_leaderboard"
    r21 = M["RS21"].dicts()
    ref_models = R["models"]
    ask(C, "The model rows are the reference's models[] in order (incl. the '(other)' fold)?", ["RS21"],
        [(r["model"], I(r["msats_30d"]), I(r["msats_alltime"]), I(r["providers_30d"])) for r in r21],
        [(m["model"], m["msats_30d"], m["msats_alltime"], m["providers_30d"]) for m in ref_models],
        "REFERENCE models[]")
    ask(C, "Requests and tokens (30d and all-time) per model match the reference?", ["RS21"],
        [(r["model"], I(r["requests_30d"]), I(r["tokens_30d"]), I(r["requests_alltime"]), I(r["tokens_alltime"])) for r in r21],
        [(m["model"], m["req_30d"], m["tok_30d"], m["req_alltime"], m["tok_alltime"]) for m in ref_models],
        "REFERENCE models[]")
    ask(C, "Which model leads the leaderboard (ranked by 30d revenue), and how many sats all time?", ["RS21"],
        (r21[0]["model"], sats(I(r21[0]["msats_alltime"]))),
        (ref_models[0]["model"], sats(ref_models[0]["msats_alltime"])), "first row vs REFERENCE models[0] (ranked by 30d then all-time)")
    ask(C, "Which model earned the most ALL TIME (a question the ranking does not answer directly)?", ["RS21"],
        max(((r["model"], I(r["msats_alltime"])) for r in r21 if r["model"] != "(other)"), key=lambda kv: (kv[1], kv[0])),
        max(((m, v[0]) for m, v in T.model_totals().items() if m != "(other)"), key=lambda kv: (kv[1], kv[0])),
        "argmax over msats_alltime: leaderboard named rows vs SQL SUM over routstr_model_days (all models, beyond the fold)")
    mt_all = T.model_totals()
    named = [r for r in r21 if r["model"] != "(other)"]
    ask(C, "Every named model's all-time msats re-sums from routstr_model_days (both grains)?", ["RS21"],
        sorted((r["model"], I(r["msats_alltime"])) for r in named), sorted((r["model"], mt_all[r["model"]][0]) for r in named),
        "SQL: SUM(msats) per model over routstr_model_days")
    mt30 = T.model_totals(R30F, R30T)
    ask(C, "Every named model's 30d msats re-sums from the daily rows in the refresh window?", ["RS21"],
        sorted((r["model"], I(r["msats_30d"])) for r in named),
        sorted((r["model"], mt30.get(r["model"], (0,))[0]) for r in named), "SQL: grain='d' rows in refresh-30..refresh-1")
    ask(C, "The named rows plus '(other)' sum to the network's all-time revenue?", ["RS21", "RS01"],
        sats(sum(I(r["msats_alltime"]) for r in r21)), I(r01["revenue_alltime_sats"]),
        "cross-call reconciliation: the model partition covers every msat")
    ask(C, "How many models had 30d activity (models_30d)?", ["RS21", "RS01"],
        len({m for m, v in mt30.items() if (v[0] or v[1] or v[2]) and m != "(other)"}), I(r01["models_30d"]),
        "SQL: DISTINCT named models with a nonzero daily bucket in the window vs the headline ('(other)' is a residual, not a model)")
    r22 = M["RS22"].dicts()
    ask(C, "Filtered to the top model, exactly its row comes back?", ["RS22"],
        [(r["model"], I(r["msats_alltime"])) for r in r22], [(tmodel, I(r21[0]["msats_alltime"]))],
        "model filter vs the unfiltered top row")
    r29 = M["RS29"].dicts()
    ask(C, "The '(other)' residual is addressable by filter?", ["RS29"], [r["model"] for r in r29],
        ["(other)"] if any(r["model"] == "(other)" for r in r21) else [], "filter on the fold row")
    ask(C, "Model ids are declared third-party text (untrusted)?", ["RS21"],
        "untrusted" in M["RS21"].body and "model" in M["RS21"].body.split("untrusted")[1][:80], True, "envelope untrusted line")

    # =======================================================================
    C = "Tool - model_activity"
    r23 = {r["model"]: r for r in M["RS23"].dicts()}
    ask(C, "grain=total, no window: every model's all-time msats/requests/tokens?", ["RS23"],
        sorted((m, I(r["msats"]), I(r["requests"]), I(r["tokens"])) for m, r in r23.items()),
        sorted((m, v[0], v[1], v[2]) for m, v in mt_all.items()), "SQL: SUM per model over routstr_model_days, both grains")
    ask(C, "How many distinct models were ever reported (beyond the leaderboard's 100-row fold)?", ["RS23", "RS21"],
        (len(r23), len(r23) > len(named)), (len(mt_all), len(mt_all) > len(named)),
        "SQL: COUNT(DISTINCT model); the fold means the leaderboard shows fewer")
    ask(C, "The leaderboard's named rows agree with model_activity's totals?", ["RS23", "RS21"],
        sorted((r["model"], I(r["msats_alltime"])) for r in named), sorted((r["model"], I(r23[r["model"]]["msats"])) for r in named),
        "cross-call: document-derived vs substrate-derived")
    r24 = {r["model"]: r for r in M["RS24"].dicts()}
    ask(C, "Refresh-anchored 30d window per model equals the leaderboard's 30d columns?", ["RS24", "RS21"],
        sorted((r["model"], I(r24.get(r["model"], {"msats": 0})["msats"])) for r in named),
        sorted((r["model"], I(r["msats_30d"])) for r in named), "THE parity gate, model side")
    ask(C, "...and a bound window never counts a weekly bucket?", ["RS24"],
        all(I(r["weekly_buckets"]) == 0 for r in r24.values()), True, "weekly_buckets scan")
    r25 = M["RS25"].dicts()
    mt7 = T.model_totals(L7F, L7T)
    ask(C, f"Which models earned the most last week ({L7F}..{L7T})? Top 5.", ["RS25"],
        [(r["model"], I(r["msats"])) for r in r25[:5]], ranked(mt7, 0)[:5], "SQL: per-model window sums, ranked")
    ask(C, "The week's model sums equal the week's provider sums (two partitions of one ledger)?", ["RS25", "RS15"],
        sum(I(r["msats"]) for r in r25), sum(I(r["msats"]) for r in r15), "cross-call reconciliation")
    r26 = M["RS26"].dicts()
    ask(C, "grain=day for the top model IS its substrate rows?", ["RS26"],
        [(r["utc_day"], r["grain"], I(r["msats"]), I(r["providers"])) for r in r26],
        [(r["day"], r["grain"], r["msats"], r["providers"]) for r in
         sorted(T.model_rows(model=tmodel, daily_only=False), key=lambda r: (r["day"], r["grain"]))],
        "SQL rows for that model, ordered by day")
    ask(C, "The top model's day rows re-sum to its leaderboard all-time figure?", ["RS26", "RS21"],
        sum(I(r["msats"]) for r in r26), I(r21[0]["msats_alltime"]), "SUM of day rows vs the document column")
    r27 = M["RS27"]
    ask(C, "The unfiltered day series serves every bucket up to the 2,000-row cap, announcing truncation (with the model= remedy) only when it binds?",
        ["RS27"], (len(r27.rows), bool(r27.truncated), "model" in (r27.truncated[0] if r27.truncated else "")),
        (min(2000, len(T.mdays)), len(T.mdays) > 2000, len(T.mdays) > 2000),
        f"SQL: {len(T.mdays)} routstr_model_days rows vs the cap")
    r28 = M["RS28"].dicts()
    if ctx["has_folded_model"]:
        ask(C, "A model folded into '(other)' on the leaderboard is its own row here?", ["RS28", "RS21"],
            ([(r["model"], I(r["msats"])) for r in r28], fmodel in {r["model"] for r in r21}),
            ([(fmodel, mt_all[fmodel][0])], False), "SQL total for a model absent from the leaderboard's named rows")
    ask(C, "The model filter is echoed in the envelope scope line?", ["RS28"],
        M["RS28"].env.get("scope"), f"model={fmodel}", "envelope scope line")
    ask(C, "Model activity is declared included-providers-only in the methodology?", ["RS23"],
        "included" in M["RS23"].body.lower() and "excluded" in M["RS23"].body.lower(), True, "methodology text")

    # =======================================================================
    C = "Tool - catalog_size"
    r30 = M["RS30"].dicts()
    ask(C, "One row per archived catalog version with the model count - the reference's history?", ["RS30"],
        [(r["utc_day"], I(r["catalog_models"])) for r in r30], [(h["d"], h["n"]) for h in R["catalog"]["history"]],
        "REFERENCE catalog.history[]")
    ask(C, "The version ordinal counts up from 1 in time order?", ["RS30"],
        [I(r["version_ordinal"]) for r in r30], list(range(1, len(r30) + 1)), "ordinal scan")
    ask(C, "The last row's size is the current catalog size headline?", ["RS30", "RS01"],
        I(r30[-1]["catalog_models"]), I(r01["catalog_models"]), "cross-call: last version vs network_summary")
    ask(C, "Every substrate catalog version is represented (same count as routstr_catalog_versions)?", ["RS30"],
        len(r30), len(T.catalog_versions), "SQL: COUNT(*) routstr_catalog_versions")
    ask(C, "When did the catalog last change, and to how many models?", ["RS30"],
        (r30[-1]["utc_day"], I(r30[-1]["catalog_models"])), (T.catalog_versions[0]["day"], T.catalog_versions[0]["model_count"]),
        "SQL: newest routstr_catalog_versions row")

    # =======================================================================
    C = "Tool - provider_facets"
    r31 = M["RS31"].dicts()
    ask(C, "The three facets (transport, version, mint host) are the reference's breakdowns in order?", ["RS31"],
        [(r["facet"], r["value"], I(r["providers"])) for r in r31],
        [("transport", e["k"], e["v"]) for e in R["transports"]] + [("version", e["k"], e["v"]) for e in R["versions"]]
        + [("mint_host", e["k"], e["v"]) for e in R["mints_top"]], "REFERENCE transports/versions/mints_top")
    ask(C, "The transport facet sums to providers_announced (announced network, excluded included)?", ["RS31", "RS01"],
        sum(I(r["providers"]) for r in r31 if r["facet"] == "transport"), I(r01["providers_announced"]), "SUM vs headline")
    ask(C, "The transport counts re-derive from the registry's endpoint classes?", ["RS31"],
        sorted((r["value"], I(r["providers"])) for r in r31 if r["facet"] == "transport"),
        sorted((c, n) for c, n in _endpoint_classes(T).items() if n),
        "SQL: COUNT per endpoint_class over announced providers in routstr_providers")
    ask(C, "The mint-host facet's top host is the most-accepted mint across the registry?", ["RS31"],
        next((r["value"], I(r["providers"])) for r in r31 if r["facet"] == "mint_host"),
        max(((h, n) for h, n in _mint_hosts(T).items()), key=lambda kv: (kv[1], kv[0])),
        "SQL: hosts of routstr_providers.mints over announced providers, counted per provider")
    ask(C, "Which version do most providers run?", ["RS31"],
        next((r["value"], I(r["providers"])) for r in r31 if r["facet"] == "version"),
        (R["versions"][0]["k"], R["versions"][0]["v"]), "REFERENCE versions[0]")
    ask(C, "The facets declare that they count announcements, not usage?", ["RS31"],
        "not usage" in M["RS31"].body.lower() or "announcements" in M["RS31"].body.lower(), True, "methodology text")

    # =======================================================================
    C = "Record - provider_profile"
    r40 = M["RS40"].dicts()
    ask(C, "The directory has one row per provider, matching the leaderboard's population?", ["RS40", "RS10"],
        sorted(r["provider_pubkey"] for r in r40), sorted(by_pk), "record vs metric keys")
    ask(C, "Names, endpoint class, version, announced flag and review match the registry table?", ["RS40"],
        sorted((r["provider_pubkey"], r["provider_name"], r["endpoint_class"], r["version"], r["announced"],
                None if r["review"] == "\\N" else r["review"]) for r in r40),
        sorted((pk, p["name"], p["endpoint_class"], p["version"], "true" if p["announced"] else "false", p["review"])
               for pk, p in T.providers.items()), "SQL: routstr_providers columns")
    ask(C, "Endpoint URL and mint counts per provider match the registry arrays?", ["RS40"],
        sorted((r["provider_pubkey"], len(json.loads(r["urls"])), len(json.loads(r["mints"]))) for r in r40),
        sorted((pk, p["n_urls"], p["n_mints"]) for pk, p in T.providers.items()),
        "SQL: jsonb_array_length(urls), jsonb_array_length(mints)")
    ask(C, "Excluded providers carry excl_reason; included ones \\N?", ["RS40"],
        sorted((r["provider_pubkey"], r["excluded"], r["excl_reason"]) for r in r40),
        sorted((pk, "true" if p["excluded"] else "false", p["excl_reason"] or "\\N") for pk, p in T.providers.items()),
        "SQL: routstr_providers.excluded / excl_reason")
    r41 = M["RS41"].dicts()
    ask(C, "Scoped to the top provider: one row, and its about text ships?", ["RS41"],
        (len(r41), r41[0]["provider_pubkey"] if r41 else None, "about" in M["RS41"].hdr), (1, top, True),
        "single-provider scope")
    r42 = M["RS42"].dicts()
    if exc in T.providers:
        ask(C, "'Why is provider X missing from the totals?' - the profile answers with excluded + reason?", ["RS42"],
            [(r["provider_pubkey"], r["excluded"], r["excl_reason"]) for r in r42],
            [(exc, "true", T.providers[exc]["excl_reason"])], "SQL: the excluded subject's reason")
    ask(C, "The reason vocabulary is exactly the exclusions record's categories (test | private)?", ["RS40"],
        sorted({r["excl_reason"] for r in r40 if r["excl_reason"] != "\\N"}),
        sorted({p["excl_reason"] for p in T.providers.values() if p["excl_reason"]}), "SQL DISTINCT excl_reason")
    ask(C, "Never-announced reporters are present with announced=false and a prefix name?", ["RS40"],
        sorted((r["provider_pubkey"], r["provider_name"]) for r in r40 if r["announced"] == "false"),
        sorted((pk, p["name"]) for pk, p in T.providers.items() if not p["announced"]), "SQL: announced=false rows")
    ask(C, "Third-party text columns are declared untrusted?", ["RS40"],
        "untrusted" in M["RS40"].body and "provider_name" in M["RS40"].body, True, "envelope untrusted line")
    ask(C, "The scope-less call is allowed (the directory is not scope-required)?", ["RS40"],
        not M["RS40"].error, True, "no error on the unscoped record")

    # =======================================================================
    C = "Record - provider_reviews"
    r43 = M["RS43"].dicts()
    ask(C, "Every review event, newest first: (day, node, label, curated) matches the substrate?", ["RS43"],
        [(r["reviewed_at"][:10], r["node_pubkey"], r["label"], r["curated"]) for r in r43],
        [(r["day"], r["node"], r["label"], "true" if r["curated"] else "false") for r in T.reviews],
        "SQL: routstr_reviews ordered reviewed_utc desc")
    ask(C, "The newest rows are the reference's review timeline head (label, day, node prefix)?", ["RS43"],
        [(r["label"], r["reviewed_at"][:10], r["node_pubkey"][:12]) for r in r43[:len(R["reviews"])]],
        [(r["label"], r["d"], r["node"]) for r in R["reviews"]], "REFERENCE reviews[] (capped timeline)")
    ask(C, "How many reviews exist, and how many are by the curated key?", ["RS43"],
        (len(r43), sum(r["curated"] == "true" for r in r43)), (len(T.reviews), sum(1 for r in T.reviews if r["curated"])),
        "SQL COUNT")
    ask(C, "Each provider's review label on the leaderboard is its newest CURATED review?", ["RS43", "RS10"],
        sorted((pk, None if r["review"] == "\\N" else r["review"]) for pk, r in by_pk.items() if r["review"] != "\\N"),
        sorted(_newest_curated(T).items()), "SQL: newest curated routstr_reviews row per node vs the leaderboard column")
    r44 = M["RS44"].dicts()
    rev = ctx["reviewed_node"]
    ask(C, "Scoped to one reviewed node: exactly its reviews?", ["RS44"],
        [(r["reviewed_at"][:10], r["label"]) for r in r44], [(r["day"], r["label"]) for r in T.reviews if r["node"] == rev],
        "SQL rows for that node")
    r45 = M["RS45"].dicts()
    ask(C, f"Windowed from the month's first day ({MF}): this month's reviews?", ["RS45"],
        [(r["reviewed_at"][:10], r["node_pubkey"]) for r in r45], [(r["day"], r["node"]) for r in T.reviews if r["day"] >= MF],
        "SQL rows with reviewed_utc >= month start")
    ask(C, "The record names the node's display name beside its key (name is display data)?", ["RS43"],
        "node_name" in M["RS43"].hdr and all(r["node_name"] for r in r43), True, "column contract")

    # =======================================================================
    C = "Record - catalog_versions"
    r46 = M["RS46"].dicts()
    ask(C, "Every archived version, newest first, with its size?", ["RS46"],
        [(r["version_at"][:10], I(r["model_count"]), r["event_id"]) for r in r46],
        [(v["day"], v["model_count"], v["event_id"]) for v in T.catalog_versions], "SQL: routstr_catalog_versions")
    ask(C, "The added / removed lists per version match the substrate deltas?", ["RS46"],
        [(r["event_id"], json.loads(r["added"]), json.loads(r["removed"])) for r in r46],
        [(v["event_id"], v["added"], v["removed"]) for v in T.catalog_versions],
        "SQL: added/removed JSONB, rendered as JSON arrays")
    ask(C, "The deltas re-derive from consecutive model lists (set difference)?", ["RS46"],
        [(r["event_id"], json.loads(r["added"]), json.loads(r["removed"])) for r in r46], _catalog_deltas(T),
        "recomputed: each version's models minus the previous version's, in time order")
    ask(C, "The newest version's model list is the current catalog (model_catalog)?", ["RS46", "RS48"],
        json.loads(r46[0]["models"]) if r46 else [], [r["model"] for r in M["RS48"].dicts()], "cross-record")
    ask(C, "Sizes over versions equal catalog_size's series?", ["RS46", "RS30"],
        [(r["version_at"][:10], I(r["model_count"])) for r in reversed(r46)], [(r["utc_day"], I(r["catalog_models"])) for r in r30],
        "cross-call: record vs metric")
    r47 = M["RS47"].dicts()
    ask(C, f"How did the catalog change this month (from {MF})?", ["RS47"],
        [(r["version_at"][:10], json.loads(r["added"]), json.loads(r["removed"])) for r in r47],
        [(v["day"], v["added"], v["removed"]) for v in T.catalog_versions if v["day"] >= MF],
        "SQL rows with version_utc >= month start")
    ask(C, "How many models were added and removed across all versions (net = current - first)?", ["RS46"],
        (sum(len(json.loads(r["added"])) for r in r46), sum(len(json.loads(r["removed"])) for r in r46)),
        (sum(len(v["added"]) for v in T.catalog_versions), sum(len(v["removed"]) for v in T.catalog_versions)),
        "SQL totals of the delta lists")
    ask(C, "The first archived version has no removals (nothing to diff against)?", ["RS46"],
        json.loads(r46[-1]["removed"]) if r46 else None, [], "oldest row")

    # =======================================================================
    C = "Record - model_catalog"
    r48 = M["RS48"].dicts()
    ask(C, "The current curated list is the reference's catalog.models?", ["RS48"],
        [r["model"] for r in r48], R["catalog"]["models"], "REFERENCE catalog.models")
    ask(C, "Its size is the headline catalog_models?", ["RS48", "RS01"], len(r48), I(r01["catalog_models"]), "cross-call")
    ask(C, "Every row carries the catalog's updated day and the version event id?", ["RS48"],
        ({r["catalog_updated_at"][:10] for r in r48}, {r["version_event_id"] for r in r48}),
        ({R["catalog"]["updated"]}, {T.catalog_versions[0]["event_id"]}), "REFERENCE catalog.updated; SQL newest version id")
    ask(C, "The list is sorted and deduplicated (the pipeline's normalisation)?", ["RS48"],
        [r["model"] for r in r48] == sorted(set(r["model"] for r in r48)), True, "scan")

    # =======================================================================
    C = "Record - exclusions"
    r49 = M["RS49"].dicts()
    ask(C, "The three categories and counts are the reference's exclusions ledger?", ["RS49"],
        [(r["category"], I(r["providers"])) for r in r49], [(e["k"], e["v"]) for e in R["exclusions"]], "REFERENCE exclusions[]")
    ask(C, "Each category carries its 'why' text, verbatim from the reference?", ["RS49"],
        [r["why"] for r in r49], [e["why"] for e in R["exclusions"]], "REFERENCE exclusions[].why")
    ask(C, "Test + private counts equal the excluded rows in the registry?", ["RS49", "RS10"],
        sum(I(r["providers"]) for r in r49 if "squat" not in r["category"].lower()),
        sum(1 for r in r10 if r["excluded"] == "true"), "cross-call: ledger vs flagged rows (squatters are not providers)")
    ask(C, "Squatters are a ledger-only entry - no provider row carries that reason?", ["RS49", "RS40"],
        any("squat" in r["excl_reason"].lower() for r in r40), False, "no row-level squatter reason")

    # =======================================================================
    C = "Contract - domain routing, rejections, caps, formats"
    ident = lambda k: (M[k].env.get("metric") or M[k].env.get("record") or "").split("  |  ")[0]
    ask(C, "A bare 'network_summary' (no domain) resolves to routstr by name?", ["RS50"], ident("RS50"), "routstr.network_summary",
        "identity line after name-based domain resolution")
    ask(C, "...and serves the same row as the explicit call?", ["RS50", "RS01"], M["RS50"].rows, M["RS01"].rows, "row equality")
    ask(C, "A bare 'provider_leaderboard' resolves too?", ["RS51"], ident("RS51"), "routstr.provider_leaderboard", "identity line")
    ask(C, "A bare 'provider_profile' record resolves too?", ["RS52"], ident("RS52"), "routstr.provider_profile", "identity line")
    ask(C, "domain='zapstore' with a Routstr-only name: the NAME wins (documented), no error?", ["RS65"],
        (not M["RS65"].error, ident("RS65")), (True, "routstr.network_summary"), "resolve_domain rule: a unique name wins over domain")
    ask(C, "provider_pubkey on a Zapstore metric is rejected, naming the Routstr metrics that take it?", ["RS53"],
        M["RS53"].error and "provider" in M["RS53"].body and "routstr" in M["RS53"].body, True, "tool error text")
    ask(C, "publisher_pubkey on a Routstr metric is rejected, pointing at provider_pubkey?", ["RS54"],
        M["RS54"].error and "provider_pubkey" in M["RS54"].body, True, "tool error text")
    ask(C, "publisher_pubkey on a Routstr record is rejected?", ["RS63"], M["RS63"].error, True, "tool error")
    ask(C, "from/to on the provider_leaderboard snapshot are rejected with the reason?", ["RS55"],
        M["RS55"].error and "no time axis" in M["RS55"].body, True, "snapshot metrics have no time axis")
    ask(C, "grain=total on network_activity (a day-only series) is rejected and the served grains are named?", ["RS56"],
        M["RS56"].error and "serves grain" in M["RS56"].body, True, "tool error text")
    ask(C, "...and the rejection says which columns add up and where the 30d and all-time totals are?", ["RS56"],
        bool(M["RS56"].error and "sum the day rows over from/to" in M["RS56"].body
             and "active_providers does not" in M["RS56"].body and "routstr.network_summary" in M["RS56"].body), True,
        "the redirect clause (0.5.1) - the ask a model client made six times on 2026-08-30")
    ask(C, "grain=current on provider_activity is rejected?", ["RS57"], M["RS57"].error and "serves grain" in M["RS57"].body, True,
        "tool error text")
    ask(C, "An inverted window (from after to) is rejected?", ["RS58"], M["RS58"].error and "after" in M["RS58"].body, True, "tool error text")
    ask(C, "An unknown provider_pubkey returns an empty table, not an error (metric)?", ["RS59"],
        (not M["RS59"].error, len(M["RS59"].rows)), (True, 0), "the documented empty-not-error rule")
    ask(C, "...and on a record?", ["RS68"], (not M["RS68"].error, len(M["RS68"].rows)), (True, 0), "same rule")
    ask(C, "An npub value is rejected with the decode-to-hex hint?", ["RS60"],
        M["RS60"].error and "npub" in M["RS60"].body and "hex" in M["RS60"].body, True, "tool error text")
    ask(C, "A model filter over 60 chars is rejected and the remedy names model_leaderboard?", ["RS61"],
        M["RS61"].error and "model_leaderboard" in M["RS61"].body, True, "tool error text")
    ask(C, "An unknown model id returns an empty table (exact match, no fuzzy)?", ["RS69"],
        (not M["RS69"].error, len(M["RS69"].rows)), (True, 0), "exact-match rule")
    ask(C, "from/to on model_catalog (not windowable) are rejected?", ["RS62"], M["RS62"].error and "no time axis" in M["RS62"].body, True,
        "record window rule")
    ask(C, "An unknown metric name under domain=routstr is rejected?", ["RS66"], M["RS66"].error and "unknown metric" in M["RS66"].body, True,
        "tool error text")
    ask(C, "An all-future window returns an empty series rather than an error?", ["RS67"],
        (not M["RS67"].error, len(M["RS67"].rows)), (True, 0), "a window far in the future")
    md = M["RS64"].body
    ask(C, "format=markdown renders a table, with the same values as CSV for the same window?", ["RS64", "RS03"],
        [int(re.search(r"\|\s*" + d + r"\s*\|\s*(\d+)", md).group(1)) for d in sorted(M["RS03"].series())[:4]] if md.count("|") > 10 else None,
        [M["RS03"].series()[d] for d in sorted(M["RS03"].series())[:4]], "the same days parsed out of both renderings")
    keys = ["RS01", "RS02", "RS06", "RS08", "RS10", "RS13", "RS21", "RS23", "RS30", "RS31"]
    ask(C, "Every Routstr metric declares the SAME data-derived freshness - the refresh instant?", keys,
        {M[k].as_of_day() for k in keys}, {ctx["refresh_day"]}, "as_of across all ten metrics vs the served document's day")
    rkeys = ["RS40", "RS43", "RS46", "RS48", "RS49"]
    ask(C, "...and so does every record?", rkeys, {M[k].as_of_day() for k in rkeys}, {ctx["refresh_day"]}, "as_of across the five records")
    ask(C, "The declared row count matches the rows returned on every uncapped call?", keys + rkeys,
        [M[k].declared_rows() == len(M[k].rows) for k in keys + rkeys], [True] * len(keys + rkeys), "envelope rows: vs parsed CSV")
    ask(C, "Every Routstr response says its usage numbers are self-reported?", ["RS01", "RS10", "RS21", "RS13"],
        all("self-reported" in M[k].body.lower() for k in ("RS01", "RS10", "RS21", "RS13")), True, "methodology honesty check")
    ask(C, "Revenue columns are declared msats everywhere except network_summary's *_sats?", ["RS10", "RS13", "RS21", "RS01"],
        all("msats" in M[k].body for k in ("RS10", "RS13", "RS21")) and "revenue_30d_sats" in M["RS01"].hdr, True, "column contract")
    ask(C, "The envelope names the domain on every call (a client routes on it)?", keys,
        all(M[k].env.get("metric", "").startswith("routstr.") for k in keys), True, "identity line prefix")

    # =======================================================================
    C = "Reference re-derivation vs served document"
    diffs = ctx["reference_diffs"]
    ask(C, "The independent DuckDB re-derivation over the raw archive reproduces the served document with ZERO diffs?",
        [], len(diffs), 0, "crosscheck/build_routstr_snapshot.py pinned to the document's generated_utc, field-by-field diff")
    ask(C, "Both sides read the same archive (event counts per kind)?", [], R["summary"]["archive_events"],
        doc["summary"]["archive_events"], "summary.archive_events on both documents")
    for key in ("summary", "daily", "providers", "models", "catalog", "reviews", "exclusions"):
        ask(C, f"Section '{key}' agrees between the reference and the served document?", [],
            len([d for d in diffs if d.startswith(f"$.{key}")]), 0, "diff paths under that section")

    # =======================================================================
    C = "Live page - /routstr"
    ask(C, "The page-embedded document IS the served row the tools read (whole-document equality)?", [],
        W == doc, True, "SvelteKit payload evaluated by node vs routstr_snapshot.data")
    ws = W["summary"]
    for col in ("providers_announced", "providers_active_30d", "revenue_30d_sats", "revenue_alltime_sats", "requests_30d", "tokens_30d"):
        ask(C, f"The page's headline {col} equals network_summary?", ["RS01"], I(r01[col]), ws[col], "the page-embedded summary")
    ask(C, "The page's top provider row (Providers tab) equals provider_leaderboard's first row?", ["RS10"],
        (r10[0]["provider_pubkey"][:12], r10[0]["provider_name"], I(r10[0]["msats_30d"]), I(r10[0]["msats_alltime"])),
        (W["providers"][0]["id"], W["providers"][0]["name"], W["providers"][0]["msats_30d"], W["providers"][0]["msats_alltime"]),
        "page providers[0]")
    ask(C, "The page's top model row (Models tab) equals model_leaderboard's first row?", ["RS21"],
        (r21[0]["model"], I(r21[0]["msats_30d"]), I(r21[0]["providers_30d"])),
        (W["models"][0]["model"], W["models"][0]["msats_30d"], W["models"][0]["providers_30d"]), "page models[0]")
    ask(C, "The page's daily revenue chart is network_activity's msats series?", ["RS02"],
        [(d, m) for d, m, *_ in series], [(x["d"], x["msats"]) for x in W["daily"]], "page daily[]")
    top_line = next((p for p in W["provider_daily_top"] if p["id"] == top[:12]), None)
    ask(C, "The page's daily chart line for the top provider re-sums from provider_activity's day rows?", ["RS17"],
        [(r["utc_day"], I(r["msats"])) for r in r17 if r["grain"] == "d" and I(r["msats"])],
        [(p["d"], p["msats"]) for p in top_line["series"]] if top_line else None,
        "page provider_daily_top[] entry keyed by the top provider's id (the chart ranks by 30d, the table by all-time)")
    ask(C, "The page and the tools read the SAME refresh?", ["RS01"], r01["refresh_generated_at"],
        W["generated_utc"].replace(" ", "T"), "generated_utc embedded in the page vs the envelope")

    return Q


# ---------------------------------------------------------------------------
def _mint_hosts(T):
    from urllib.parse import urlsplit
    counts = {}
    for p in T.providers.values():
        if not p["announced"]:
            continue
        hosts = set()
        for m in (p["mints"] or []):
            h = urlsplit(m).hostname  # the pipeline's rule: a scheme-less mint has no host and is not counted
            if h:
                hosts.add(h.lower())
        for h in hosts:
            counts[h] = counts.get(h, 0) + 1
    return counts


def _endpoint_classes(T):
    out = {}
    for p in T.providers.values():
        if p["announced"]:
            out[p["endpoint_class"]] = out.get(p["endpoint_class"], 0) + 1
    return out


def _newest_curated(T):
    out = {}
    for r in sorted(T.reviews, key=lambda r: (r["at"], r["event_id"]), reverse=True):
        if r["curated"] and r["node"] not in out and r["node"] in T.providers:
            out[r["node"]] = r["label"]
    return out


def _catalog_deltas(T):
    versions = list(reversed(T.catalog_versions))  # oldest first
    out, prev = [], set()
    for v in versions:
        cur = set(v["models"])
        out.append((v["event_id"], sorted(cur - prev), sorted(prev - cur)))
        prev = cur
    return list(reversed(out))

"""The nutzaps battery, organised by TOOL and METRIC, like the Routstr one.

Same two rules as every battery here: never hard-code an expected value (every
`want` is recomputed on each run from the raw SQL rows `T` or the live pages `W`),
and say in `how` which independent path the check rests on. The contract under
test is docs/design/nutzaps-mcp-design.md.

Verdicts: PASS = equal. BOUNDED = the page's ROLLING window (`NOW() - N days`) sits
inside the two whole-UTC-day windows the tool can express around it (design record
sec 5). A page-vs-SQL check evaluates the page's own rule at the instant the page
was fetched, so it is exact (PASS/FAIL) - BOUNDED only if the cutoff moved a
payment across it during the fetch itself.

Every section runs inside a guard: a section that raises records one FAIL naming
the exception instead of taking the whole run down with it.
"""
import datetime as dt
import re
from collections import defaultdict

from .nutzaps_plan import METRIC, path_case_variant
from .nutzaps_truth import (DAY, ROW_CAP, Truth, bracket_rows, bracket_violations, bucket, dense, day_of, next_month, paid_vs_raw,
                            running, shift, topn_bracket, topn_consistent)

SKEW = 2  # seconds of clock skew allowed between the workstation and the database's NOW()
ROLLING = {"7d": 7, "30d": 30, "1y": 365}


# ---------------------------------------------------------------------------
# response accessors - total by construction (a malformed answer scores, never raises)
# ---------------------------------------------------------------------------
def I(v):
    try:
        return None if v in (None, "\\N", "") else int(v)
    except (TypeError, ValueError):
        return None


def N(v):
    """A nullable text cell: the COPY-text sentinel is None."""
    return None if v in (None, "\\N") else v


def rows(r):
    return r.dicts() if r is not None and not r.error else []


def ident(r):
    return (r.env.get("metric") or "").split("  |  ")[0] if r is not None else ""


def grain_of(r):
    m = re.search(r"grain:\s*(\w+)", (r.env.get("metric") or "") if r is not None else "")
    return m.group(1) if m else None


def day_series(r):
    return {x["utc_day"]: (I(x.get("payments")), I(x.get("sats"))) for x in rows(r)}


def total_row(r):
    x = rows(r)
    if not x:
        return None
    x = x[0]
    return (I(x.get("payments")), I(x.get("sats")), I(x.get("mints")), N(x.get("first_day")), N(x.get("last_day")))


def lb_rows(r):
    return [(x.get("mint_url"), I(x.get("payments")), I(x.get("sats")), I(x.get("configured_by"))) for x in rows(r)]


NOTE = "  |  nothing matches the filter"
# The exact nutzaps note that ends the window line of a filtered call matching
# nothing - read from the committed spec (metric._NUTZAP_EMPTY_FILTER_NOTE), so a
# deployed server that says anything else is a deploy lag or a drift, and FAILs.
from nostrology_mcp.metric import _NUTZAP_EMPTY_FILTER_NOTE  # noqa: E402

EMPTY_FILTER_NOTE = "nothing matches the filter - " + _NUTZAP_EMPTY_FILTER_NOTE


def nothing_note(r):
    """The window line of a filtered call that matched nothing ENDS with the note."""
    w = (r.env.get("window") or "") if r is not None else ""
    return w.endswith("  |  " + EMPTY_FILTER_NOTE)


def ma_rows(r):
    return {(x.get("utc_day"), x.get("mint_url")): (I(x.get("payments")), I(x.get("sats"))) for x in rows(r)}


def au_rows(r):
    return {(x.get("period_start"), x.get("mint_url")): I(x.get("active_users")) for x in rows(r)}


def as_of(r):
    line = next((l for l in (r.body if r is not None else "").splitlines() if l.startswith("as_of")), "")
    m = re.search(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ)", line)
    return m.group(1) if m else None


def iso(epoch):
    return dt.datetime.fromtimestamp(int(epoch), dt.timezone.utc).isoformat().replace("+00:00", "Z")


def by_day(pairs):
    """{(day, mint): (p, s)} -> {day: (p, s)} summed over mints."""
    out = {}
    for (d, _), (p, s) in pairs.items():
        a = out.setdefault(d, [0, 0])
        a[0] += p or 0
        a[1] += s or 0
    return {d: tuple(out[d]) for d in sorted(out)}


def markdown_series(body):
    out = {}
    for line in body.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 3 and re.fullmatch(r"\d{4}-\d\d-\d\d", cells[0]):
            out[cells[0]] = (I(cells[1]), I(cells[2]))
    return out


def web_lb(items):
    return {i["mint_url"]: (i["config_count"], i["payment_count"], i["total_amount_sats"], i["has_page"])
            for i in items or []}


def web_daily_rows(points):
    return [(day_of(p["timestamp"]), p["daily_count"], p["cumulative_count"], p["daily_sats"], p["cumulative_sats"])
            for p in points or []]


def web_detail(det):
    if not det:
        return None
    return {"mint_url": det["mint_url"], "total_payments": det["total_payments"], "total_sats": det["total_sats"],
            "config_count": det["config_count"], "all_time_payments": det["all_time_payments"],
            "all_time_sats": det["all_time_sats"], "daily": web_daily_rows(det["daily"]),
            "mau": [(day_of(p["timestamp"]), p["value"]) for p in det["mau"]]}


# ---------------------------------------------------------------------------
def build(M, sql, W, ctx, tools=None):
    """M: id -> Resp (mcp_parse). sql: the raw SQL sections. W: the page reads
    (nutzaps_truth.collect_web). ctx: nutzaps_run.build_context. tools: tools/list
    name -> {description, inputSchema}."""
    T = Truth(sql)
    Q = []

    def ask(cat, question, calls, got, want, how):
        Q.append(dict(n=len(Q) + 1, cat=cat, q=question, calls=calls, got=got, want=want, how=how,
                      bounded=False, ok=(bool(got) == want) if isinstance(want, bool) else (got == want)))

    def bound(cat, question, calls, got, inner, outer, ok, how):
        """A bracket check: BOUNDED when the two whole-day windows differ, and then
        `ok` is the bracket rule; when they coincide it is an exact check."""
        Q.append(dict(n=len(Q) + 1, cat=cat, q=question, calls=calls, got=got,
                      want={"inner": inner, "outer": outer} if inner != outer else inner,
                      how=how, bounded=inner != outer, ok=bool(ok)))

    def rolling(cat, question, calls, got, fn, t0, t1, how):
        """A page vs the SQL restatement of the page's own rolling rule, evaluated at
        both ends of the fetch (+-SKEW): exact when both agree, which is all but always."""
        a, b = fn(t0 - SKEW), fn(t1 + SKEW)
        if a == b:
            ask(cat, question, calls, got, a, how + " - evaluated at the fetch instant")
        else:
            Q.append(dict(n=len(Q) + 1, cat=cat, q=question, calls=calls, got=got, want={"at_t0": a, "at_t1": b},
                          how=how + " - a payment crossed the cutoff during the fetch; either end is correct",
                          bounded=True, ok=got in (a, b)))

    def section(name, fn):
        try:
            fn()
        except Exception as e:  # noqa: BLE001 - a crash in one section must not lose the others
            Q.append(dict(n=len(Q) + 1, cat=name, q=f"Section '{name}' completed without raising?", calls=[],
                          got=f"{type(e).__name__}: {e}", want="no exception", bounded=False, ok=False,
                          how="harness guard - a malformed response or a harness bug; read the traceback by "
                              "re-running with --reuse"))

    top, few, cfg_only, exc = ctx["top_mint"], ctx["few_mint"], ctx["config_only_mint"], ctx["excluded_mint"]
    TODAY = ctx["today"]
    L7F, L7T, L30F, L30T = ctx["last7_from"], ctx["last7_to"], ctx["last30_from"], ctx["last30_to"]
    MF, PMF, PMT = ctx["month_from"], ctx["past_month_from"], ctx["past_month_to"]
    D0, D1 = ctx["web_d0"], ctx["web_d1"]
    EF, ET = ctx["empty_from"], ctx["empty_to"]
    MID = ctx["midweek_from"]
    g = M.get

    DAY_WINDOWS = [("NZ02", L7F, L7T, "last 7 days"), ("NZ03", L30F, L30T, "last 30 days"),
                   ("NZ04", MF, TODAY, "this month, open-ended to"), ("NZ05", PMF, PMT, "the past full month"),
                   ("NZ06", D0, TODAY, "the web's calendar year, from its partial oldest day")]
    TOTAL_WINDOWS = [("NZ07", None, None, "all time"), ("NZ08", L7F, L7T, "last 7 days"),
                     ("NZ09", L30F, L30T, "last 30 days"), ("NZ10", MF, TODAY, "this month, open-ended to"),
                     ("NZ11", PMF, PMT, "the past full month"), ("NZ12", D0, TODAY, "from the web's oldest day D0"),
                     ("NZ13", D1, TODAY, "from D0 + 1")]
    LB_WINDOWS = [("NZ30", None, None, "All (no window)"), ("NZ31", L7F, L7T, "last 7 days"),
                  ("NZ32", L30F, L30T, "last 30 days"), ("NZ33", MF, TODAY, "this month, open-ended to"),
                  ("NZ34", PMF, PMT, "the past full month"), ("NZ35", D0, TODAY, "from D0 (1y outer)"),
                  ("NZ36", D1, TODAY, "from D0 + 1 (1y inner)"),
                  ("NZ37", ctx["roll7_outer"], TODAY, "7d outer"), ("NZ38", ctx["roll7_inner"], TODAY, "7d inner"),
                  ("NZ39", ctx["roll30_outer"], TODAY, "30d outer"), ("NZ40", ctx["roll30_inner"], TODAY, "30d inner")]

    # =======================================================================
    def s_payment_day():
        C = "Tool - payment_activity@day"
        s01 = day_series(g("NZ01"))
        ask(C, "An omitted grain resolves to day (the documented default)?", ["NZ01"], grain_of(g("NZ01")), "day",
            "envelope identity line, grain:")
        ask(C, "The whole daily series (no window): payments and sats per UTC day equal the SQL re-count?", ["NZ01"],
            s01, T.daily(), "SQL: every value_source='proof_tags' row, excluded mints (nutzap_mint_classes.excluded) "
            "dropped in Python, bucketed by the UTC day of created_at, amount_sats summed")
        ask(C, "The series is sparse - no zero rows?", ["NZ01"], all((p or 0) > 0 for p, _ in s01.values()), True,
            "scan of the payments column")
        for cid, frm, to, label in DAY_WINDOWS:
            got = day_series(g(cid))
            ask(C, f"{label} ({frm}..{to}): the windowed series equals the SQL re-count?", [cid], got,
                T.daily(frm, to), "SQL re-count over created_at in [from 00:00Z, to+1 00:00Z)")
            ask(C, f"{label}: the server-side window equals slicing the unwindowed series client-side?", [cid, "NZ01"],
                got, {d: v for d, v in s01.items() if frm <= d <= to}, "window consistency within the tool")
        ask(C, f"Filtered to the top mint ({top}): its daily series equals the SQL re-count for that mint?", ["NZ15"],
            day_series(g("NZ15")), T.daily(mint=top), "SQL rows with mint_url = the subject")
        ask(C, f"Filtered to a mint with configs but few payments ({few})?", ["NZ19"], day_series(g("NZ19")),
            T.daily(mint=few), "SQL rows with mint_url = the subject")
        ask(C, f"Filter + window: the top mint over the last 30 days ({L30F}..{L30T})?", ["NZ21"],
            day_series(g("NZ21")), T.daily(L30F, L30T, top), "SQL rows for that mint inside the window")
        best = max(s01.items(), key=lambda kv: ((kv[1][1] or 0), kv[0])) if s01 else None
        tb = max(T.daily().items(), key=lambda kv: (kv[1][1], kv[0])) if T.pays else None
        ask(C, "Which day carried the most sats, and how many?", ["NZ01"], (best[0], best[1][1]) if best else None,
            (tb[0], tb[1][1]) if tb else None, "argmax over the series vs argmax over the SQL re-count")

    # =======================================================================
    def s_payment_total():
        C = "Tool - payment_activity@total"
        for cid, frm, to, label in TOTAL_WINDOWS:
            ask(C, f"{label}{'' if frm is None else f' ({frm}..{to})'}: payments, sats, mints, first_day, last_day "
                   "equal the SQL re-count?", [cid], total_row(g(cid)), T.total(frm, to),
                "SQL: COUNT, SUM(amount_sats), COUNT(DISTINCT mint_url), MIN/MAX UTC day over the window, in Python")
        r14 = rows(g("NZ14"))
        ask(C, f"A window with no payment ({EF}..{ET}, the month before the first) still returns exactly ONE row, "
               "with zeros and NULL days?", ["NZ14"], (len(r14), total_row(g("NZ14"))),
            (1, (0, 0, 0, None, None)), "design record: 'the row still comes back, with zeros'")
        ask(C, "Filtered to the top mint, all time?", ["NZ16"], total_row(g("NZ16")), T.total(mint=top),
            "SQL re-count for that mint")
        ask(C, f"Filtered to the top mint from the web's oldest day D0 ({D0}, open-ended)?", ["NZ17"],
            total_row(g("NZ17")), T.total(D0, TODAY, top), "SQL re-count for that mint in the window")
        ask(C, f"Filtered to the top mint from D0 + 1 ({D1}, open-ended)?", ["NZ18"], total_row(g("NZ18")),
            T.total(D1, TODAY, top), "SQL re-count for that mint in the window")
        ask(C, "Filtered to the few-payments mint, all time?", ["NZ20"], total_row(g("NZ20")), T.total(mint=few),
            "SQL re-count for that mint")
        src = {k: v["n"] for k, v in T.sources.items()}
        excl_pay = len([p for p in T.raw if p[2] in T.excluded])
        t07 = total_row(g("NZ07"))
        ask(C, "THE POPULATION - all-time payments are the proof-carrying rows on counted mints, not every "
               "kind-9321 (nutzaps) row?", ["NZ07"], t07[:2] if t07 else None,
            (len(T.pays), sum(p[3] for p in T.pays)),
            f"SQL: {sum(src.values())} kind-9321 (nutzaps) rows by value_source {src}; {len(T.raw)} are payments, "
            f"{excl_pay} of them on the {len(T.excluded)} excluded mints - those and every non-payment row must "
            "be left out")

    # =======================================================================
    def s_leaderboard():
        C = "Tool - mint_leaderboard@total"
        ask(C, "An omitted grain resolves to total (its only grain)?", ["NZ30"], grain_of(g("NZ30")), "total",
            "envelope identity line, grain:")
        for cid, frm, to, label in LB_WINDOWS:
            ask(C, f"{label}{'' if frm is None else f' ({frm}..{to})'}: every listed mint in the documented order, "
                   "with payments, sats and configured_by?", [cid], lb_rows(g(cid)), T.leaderboard(frm, to),
                "SQL: payments per mint in the window; configured_by = pubkeys whose LATEST "
                "kind-10019 (nutzap-configs) lists the mint (published inside the window when there is one); listed when "
                "payments >= 1 or configured_by >= 10 (All) / >= 1 (window); excluded mints never; ordered by "
                f"sats, payments, configured_by desc, then mint_url asc in the database's collation "
                f"({T.collation}, fetched as MINT_ORDER) - all in Python")
        r30 = lb_rows(g("NZ30"))
        ties = []
        for cid, frm, to, _ in LB_WINDOWS:
            got = lb_rows(g(cid))
            groups = {}
            for m, p, s_, c in got:
                groups.setdefault((s_, p, c), []).append(m)
            ties += [ms for ms in groups.values() if len(ms) > 1]
        ask(C, "Rows tied on sats, payments AND configured_by come back in mint_url order under the database's "
               "collation (not code-point order)?", [c for c, *_ in LB_WINDOWS],
            [ms for ms in ties if ms != sorted(ms, key=T.url_key)], [],
            f"{len(ties)} tie groups across the windows; order checked against MINT_ORDER ({T.collation}); "
            f"{sum(1 for ms in ties if ms != sorted(ms))} of them order differently by code point - INTERPRETED: the "
            "record says 'then mint_url', and the tool sorts it the way the database sorts text")
        ask(C, "The subject 'top mint by all-time sats' is the All view's first row?", ["NZ30"],
            r30[0][0] if r30 else None, top, "first row vs the SQL-derived subject")
        ask(C, "All keeps the 10-config floor: no listed mint has zero payments and fewer than 10 configs?", ["NZ30"],
            sorted(m for m, p, s, c in r30 if p == 0 and (c or 0) < 10), [], "scan of the All rows")
        floor1 = [r for r in T.leaderboard(L30F, L30T) if r[1] == 0 and r[3] < 10]
        ask(C, "A window drops the floor to 1: config-only mints with fewer than 10 recent configs are listed?", ["NZ32"],
            sorted(m for m, p, s, c in lb_rows(g("NZ32")) if p == 0 and (c or 0) < 10),
            sorted(r[0] for r in floor1), "SQL: the last-30-days rows with no payment and 1..9 configs")
        listed = {m for cid, *_ in LB_WINDOWS for m, *_ in lb_rows(g(cid))}
        ask(C, "No excluded mint is ever listed, All or windowed?", [c for c, *_ in LB_WINDOWS],
            sorted(listed & T.excluded), [], f"intersection with the {len(T.excluded)} mints nutzap_mint_classes excludes")
        ask(C, "Filtered to the top mint (no window): exactly its All row?", ["NZ41", "NZ30"], lb_rows(g("NZ41")),
            [r for r in r30 if r[0] == top], "mint_url filter vs the unfiltered table's row")
        ask(C, "Filtered to the few-payments mint: its All row?", ["NZ42"], lb_rows(g("NZ42")),
            [r for r in T.leaderboard() if r[0] == few], "SQL row for that mint")
        if cfg_only:
            ask(C, f"Filtered to a config-only mint ({cfg_only}): listed with zero payments by its configs alone?",
                ["NZ43"], lb_rows(g("NZ43")), [r for r in T.leaderboard() if r[0] == cfg_only],
                "SQL row: no payment, configured_by >= 10")
        ask(C, f"Filter + window: the top mint's past-year row (from {D1})?", ["NZ44"], lb_rows(g("NZ44")),
            [r for r in T.leaderboard(D1, TODAY) if r[0] == top], "SQL row for that mint in the window")
        ask(C, "The columns are exactly mint_url, payments, sats, configured_by - no senders column, windowed or not?",
            ["NZ30", "NZ31"], [g(c).hdr if g(c) else None for c in ("NZ30", "NZ31")],
            [["mint_url", "payments", "sats", "configured_by"]] * 2,
            "2026-09-30 review: a per-mint distinct-sender count over a one-day window is per-mint DAU, the paid "
            "number, so the column was removed")

    # =======================================================================
    def s_mint_activity():
        C = "Tool - mint_activity@day"
        ask(C, "An omitted grain resolves to day?", ["NZ50"], grain_of(g("NZ50")), "day", "envelope identity line")
        ask(C, "Every (day, mint) row with a payment, no window, equals the SQL re-count?", ["NZ50"],
            ma_rows(g("NZ50")), T.mint_activity(), "SQL: counted payments per (UTC day, mint_url)")
        ask(C, f"Last 30 days ({L30F}..{L30T})?", ["NZ51"], ma_rows(g("NZ51")), T.mint_activity(L30F, L30T),
            "SQL re-count in the window")
        ask(C, "Filtered to the top mint?", ["NZ52"], ma_rows(g("NZ52")), T.mint_activity(mint=top),
            "SQL re-count for that mint")
        ask(C, f"From the web's oldest day D0 ({D0}), the ledger behind the cumulative-by-mint chart?", ["NZ53"],
            ma_rows(g("NZ53")), T.mint_activity(D0, TODAY), "SQL re-count in the window")
        ds = [x.get("utc_day") for x in rows(g("NZ50"))]
        ask(C, "Rows come back in day order?", ["NZ50"], ds == sorted(ds), True, "scan of utc_day")

    # =======================================================================
    def s_active_users():
        C = "Tool - mint_active_users"
        ask(C, "An omitted grain resolves to month (the documented default)?", ["NZ60"], grain_of(g("NZ60")), "month",
            "envelope identity line")
        ask(C, "grain=day and grain=week are served as asked?", ["NZ62", "NZ63"],
            (grain_of(g("NZ62")), grain_of(g("NZ63"))), ("day", "week"), "envelope identity line")
        starts = {"day": "UTC day", "week": "Monday of the ISO week", "month": "1st of the month"}
        checks = [("NZ60", "month", None, None, None, "month buckets, every mint, no window"),
                  ("NZ61", "month", None, None, top, "the top mint by month"),
                  ("NZ62", "day", None, None, None, "day buckets, every mint, no window"),
                  ("NZ63", "week", None, None, None, "ISO-week buckets, every mint, no window"),
                  ("NZ64", "month", None, None, few, "the few-payments mint by month"),
                  ("NZ65", "month", PMF, PMT, None, f"the past full month ({PMF}..{PMT}), every mint"),
                  ("NZ66", "day", L30F, L30T, top, f"the top mint by day over the last 30 days ({L30F}..{L30T})"),
                  ("NZ67", "week", None, None, top, "the top mint by week"),
                  ("NZ68", "month", shift(PMF, 1), PMT, None,
                   f"a window from the 2nd of the past month ({shift(PMF, 1)}..{PMT}) - no month STARTS inside it"),
                  ("NZ69", "week", MID, L7T, None,
                   f"a window from a Wednesday ({MID}..{L7T}) - the week that began that Monday is left out whole")]
        for cid, grain, frm, to, mint, label in checks:
            ask(C, f"{label}: distinct senders per (bucket, mint) equal the SQL re-count?", [cid], au_rows(g(cid)),
                T.active_users(grain, frm, to, mint),
                f"SQL: DISTINCT sender pubkeys per (bucket, mint) in Python, the bucket = the {starts[grain]} of the "
                "payment's UTC day; a window keeps the buckets STARTING inside it")
        r = g("NZ89")
        ask(C, "grain=total is REJECTED, naming the grains served in order (day | week | month)?", ["NZ89"],
            bool(r and r.error and "serves grain day | week | month" in r.body), True,
            "invalid-grain listing error; grains are listed finest first")
        ask(C, "...and says why no sum of buckets replaces a total (distinct counts do not add)?", ["NZ89"],
            bool(r and r.error and "do not add across buckets or mints" in r.body), True,
            "the redirect clause after the grains (0.5.1)")
        months = [k[0] for k in au_rows(g("NZ60"))]
        ask(C, "Month buckets start on the 1st?", ["NZ60"], sorted({m for m in months if not m.endswith("-01")}), [],
            "scan of period_start")
        weeks = [k[0] for k in au_rows(g("NZ63"))]
        ask(C, "Week buckets start on a Monday?", ["NZ63"],
            sorted({w for w in weeks if w and dt.date.fromisoformat(w).weekday() != 0}), [], "scan of period_start")
        ask(C, f"The Wednesday-start window ({MID}) keeps no week that began before it?", ["NZ69"],
            sorted({w for w, _ in au_rows(g("NZ69")) if w and w < MID}), [], "scan of period_start")
        # The two leave-it-out-whole windows only test something if the bucket they
        # leave out HAS senders; otherwise an overlap bug and the correct rule agree.
        monday = bucket(MID, "week")
        ask(C, f"...and the week it leaves out (Monday {monday}) and the month NZ68 leaves out ({PMF}) have senders, "
               "so both windows discriminate?", ["NZ69", "NZ68"],
            (bool(T.active_users("week", monday, monday)), bool(T.active_users("month", PMF, PMF))), (True, True),
            "SQL re-count of the excluded buckets")

    # =======================================================================
    def s_cross():
        C = "Cross-call reconciliation"
        pairs = [("NZ01", "NZ07", "all time"), ("NZ02", "NZ08", "last 7 days"), ("NZ03", "NZ09", "last 30 days"),
                 ("NZ04", "NZ10", "this month"), ("NZ05", "NZ11", "past month"), ("NZ06", "NZ12", "from D0")]
        for dcid, tcid, label in pairs:
            s, t = day_series(g(dcid)), total_row(g(tcid))
            ask(C, f"{label}: the day rows sum to payment_activity@total (payments, sats) and span its first/last day?",
                [dcid, tcid], (sum(p or 0 for p, _ in s.values()), sum(x or 0 for _, x in s.values()),
                               min(s) if s else None, max(s) if s else None),
                (t[0], t[1], t[3], t[4]) if t else None, "grain=day summed vs grain=total over the same window")
        for lcid, tcid, label in [("NZ30", "NZ07", "All"), ("NZ31", "NZ08", "last 7 days"), ("NZ32", "NZ09", "last 30 days"),
                                  ("NZ33", "NZ10", "this month"), ("NZ34", "NZ11", "past month"),
                                  ("NZ35", "NZ12", "from D0"), ("NZ36", "NZ13", "from D0 + 1")]:
            lb, t = lb_rows(g(lcid)), total_row(g(tcid))
            ask(C, f"{label}: the leaderboard's rows sum to payment_activity@total, and its paying mints are its mints count?",
                [lcid, tcid], (sum(p or 0 for _, p, *_ in lb), sum(s or 0 for _, _, s, *_ in lb),
                               sum(1 for _, p, *_ in lb if (p or 0) >= 1)),
                (t[0], t[1], t[2]) if t else None, "mint_leaderboard summed over mints vs grain=total")
        for mcid, dcid, label in [("NZ50", "NZ01", "no window"), ("NZ51", "NZ03", "last 30 days"),
                                  ("NZ53", "NZ06", "from D0")]:
            ask(C, f"{label}: mint_activity summed per day is payment_activity@day?", [mcid, dcid],
                by_day(ma_rows(g(mcid))), day_series(g(dcid)), "two metrics, one ledger")
        ask(C, "mint_activity filtered to the top mint IS payment_activity@day filtered to it?", ["NZ52", "NZ15"],
            {d: v for (d, _), v in sorted(ma_rows(g("NZ52")).items())}, day_series(g("NZ15")),
            "design record: mint_activity@day filtered == payment_activity@day filtered")
        for ucid, fcid, mint, label in [("NZ60", "NZ61", top, "top mint"), ("NZ60", "NZ64", few, "few mint")]:
            ask(C, f"Unfiltered rows for one mint equal the filtered call ({label})?", [ucid, fcid],
                {k: v for k, v in au_rows(g(ucid)).items() if k[1] == mint}, au_rows(g(fcid)), "filter == client-side slice")
        t16, r41 = total_row(g("NZ16")), lb_rows(g("NZ41"))
        ask(C, "The top mint's leaderboard row and its payment_activity@total agree, on exactly one mint?", ["NZ41", "NZ16"],
            (r41[0][1], r41[0][2], 1) if r41 else None, (t16[0], t16[1], t16[2]) if t16 else None,
            "two metrics' headline for one mint (the per-mint page's all-time figures)")
        t18, r44 = total_row(g("NZ18")), lb_rows(g("NZ44"))
        ask(C, "...and over the past-year window from D0 + 1?", ["NZ44", "NZ18"], (r44[0][1], r44[0][2]) if r44 else None,
            (t18[0], t18[1]) if t18 else None, "the design record's two paths to the page's past-year headline")
        t20, r42 = total_row(g("NZ20")), lb_rows(g("NZ42"))
        ask(C, "The few-payments mint's leaderboard row and its total agree?", ["NZ42", "NZ20"],
            (r42[0][1], r42[0][2]) if r42 else None, (t20[0], t20[1]) if t20 else None, "same, second mint")
        m65 = {m for (_, m) in au_rows(g("NZ65"))}
        ask(C, "Past month: the mints with monthly active users are exactly the mints with payments that month?",
            ["NZ65", "NZ34"], sorted(m65), sorted(m for m, p, s, c in lb_rows(g("NZ34")) if (p or 0) >= 1),
            "a bucket has a sender exactly when the mint has a payment in it")
        d62, a50 = au_rows(g("NZ62")), ma_rows(g("NZ50"))
        ask(C, "Day buckets: a (day, mint) has active users exactly when mint_activity has payments there, and never "
               "more senders than payments?", ["NZ62", "NZ50"],
            (sorted(set(d62) ^ set(a50)), sorted(k for k in d62 if k in a50 and (d62[k] or 0) > (a50[k][0] or 0))),
            ([], []), "two metrics over one population: every payment has one sender")
        for ucid, fcid, mint, grain, frm, to, label in [
                ("NZ62", "NZ66", top, "day", L30F, L30T, "top mint by day, last 30 days"),
                ("NZ63", "NZ67", top, "week", None, None, "top mint by week")]:
            ask(C, f"Unfiltered {grain} rows for one mint equal the filtered call ({label})?", [ucid, fcid],
                {k: v for k, v in au_rows(g(ucid)).items()
                 if k[1] == mint and (frm is None or k[0] >= frm) and (to is None or k[0] <= to)},
                au_rows(g(fcid)), "filter == client-side slice")
        for coarse_id, grain in (("NZ63", "week"), ("NZ60", "month")):
            fine, coarse, bad = defaultdict(list), au_rows(g(coarse_id)), []
            for (d, m), n in d62.items():
                fine[(bucket(d, grain), m)].append(n or 0)
            for k in sorted(set(fine) | set(coarse)):
                if not fine.get(k) or k not in coarse or not max(fine[k]) <= coarse[k] <= sum(fine[k]):
                    bad.append(f"{k[0]} {k[1]}: days {fine.get(k)} vs {grain} {coarse.get(k)}")
            ask(C, f"Every (day, mint) sits inside its ({grain}, mint): the {grain} count is at least the busiest day "
                   f"and at most the sum of its days, with no {grain} lacking its days?", ["NZ62", coarse_id], bad[:8], [],
                "distinct counts nest: a day's senders are a subset of its week's and month's")

    # =======================================================================
    def s_filter():
        C = "Contract - mint_url filter, normalization, exclusion"
        base = day_series(g("NZ98"))
        norm = ctx["norm_mint"]
        ask(C, f"The stored spelling ({norm}) returns that mint's rows?", ["NZ98"], base, T.daily(mint=norm),
            "SQL re-count for the mint")
        for cid in sorted(k for k in M if M[k].call.get("variant") and M[k].call["variant"] != "path_case"):
            r = M[cid]
            ask(C, f"The {r.call['variant'].replace('_', ' ')} spelling '{r.call['args']['mint_url']}' returns the same "
                   "rows as the stored spelling?", [cid, "NZ98"], day_series(r), base,
                "design record sec 3: scheme and host case, a default port, trailing slashes folded; no scheme = https")
            ask(C, f"...and the scope line echoes the NORMALIZED value ({r.call['variant'].replace('_', ' ')})?", [cid],
                r.env.get("scope"), f"mint_url={norm}", "envelope scope line")
        if "NZ99" in M:
            lower = path_case_variant(ctx["path_case_mint"])
            ask(C, f"The PATH keeps its case: '{lower}' is a different mint from '{ctx['path_case_mint']}'?", ["NZ99"],
                (day_series(g("NZ99")), g("NZ99").env.get("scope")), (T.daily(mint=lower), f"mint_url={lower}"),
                "SQL re-count for the lower-cased URL (normally none) - a case-folding bug would return the "
                "original mint's rows here")
        if exc:
            n_raw = len(T.select(None, None, exc, raw=True))
            how = (f"{exc} is excluded in nutzap_mint_classes ({T.classes.get(exc, {}).get('mint_class')}) and has "
                   f"{n_raw} raw payment rows the filter could reach")
            for cid, label in [("NZ100", "payment_activity@day"), ("NZ102", "mint_leaderboard"),
                               ("NZ103", "mint_activity"), ("NZ104", "mint_active_users")]:
                ask(C, f"{label} filtered to an excluded mint returns no rows (no error)?", [cid],
                    (bool(g(cid) and g(cid).error), len(rows(g(cid)))), (False, 0), how)
            ask(C, "payment_activity@total filtered to an excluded mint returns exactly ONE row of zeros with NULL "
                   "first_day/last_day?", ["NZ101"],
                (bool(g("NZ101") and g("NZ101").error), len(rows(g("NZ101"))), total_row(g("NZ101"))),
                (False, 1, (0, 0, 0, None, None)),
                how + "; grain=total always answers with one row, and an excluded mint contributes nothing to it "
                "(the implemented contract, pinned in test_nutzaps_contract_pg.py::test_excluded_mints_never_surface)")
            seen = set()
            for cid in ("NZ30", "NZ35", "NZ50", "NZ53", "NZ60"):
                seen |= {x.get("mint_url") for x in rows(g(cid))}
            ask(C, "No unfiltered nutzaps response mentions an excluded mint?", ["NZ30", "NZ35", "NZ50", "NZ53", "NZ60"],
                sorted(seen & T.excluded), [], "every mint_url column vs the excluded set")
            ex = ["NZ100", "NZ101", "NZ102", "NZ103", "NZ104"]
            ask(C, "Every call filtered to the excluded mint says why it is empty: the window line ends with the "
                   "'nothing matches the filter - ...' note (excluded test/private mint, not in the archive, no "
                   "payments in the window; mint_leaderboard lists the URLs)?", ex,
                [nothing_note(g(c)) for c in ex], [True] * len(ex),
                "2026-09-30 review; payment_activity@total's zero row counts as matching nothing")
        nm = ["NZ105", "NZ106", "NZ107", "NZ108", "NZ109"]
        ask(C, "A well-formed URL the archive has never seen (https://no-such-mint.invalid): no rows on every metric, "
               "and payment_activity@total one row of zeros?", nm,
            [(bool(g(c) and g(c).error), len(rows(g(c)))) for c in nm] + [total_row(g("NZ106"))],
            [(False, 0), (False, 1), (False, 0), (False, 0), (False, 0), (0, 0, 0, None, None)],
            "no SQL rows by construction (.invalid is reserved); matching nothing is an answer, not an error")
        ask(C, "...and each says 'nothing matches the filter' with the same note?", nm, [nothing_note(g(c)) for c in nm],
            [True] * len(nm), "window line")
        filtered = [c for c in ("NZ100", "NZ101", "NZ102", "NZ103", "NZ104") if c in M] + nm
        ask(C, "No filtered call answers with the old 'window: empty (no rows in source table)'?", filtered,
            sorted(c for c in filtered if "empty (no rows in source table)" in g(c).body), [],
            "that text blamed the source table for what the filter did")
        plain = ["NZ14", "NZ84", "NZ15", "NZ16", "NZ41"]
        ask(C, "The note appears ONLY where a filter matched nothing (not on an unfiltered empty window, not on a "
               "filtered call with rows)?", plain, [NOTE in (g(c).env.get("window") or "") if g(c) else None for c in plain],
            [False] * len(plain), "window line of two unfiltered empty answers and three filtered answers with rows")
        lm = ctx.get("long_mint")
        if lm:
            r45 = lb_rows(g("NZ45"))
            ask(C, f"The longest live mint URL ({len(lm)} chars, over the old 256-char clip) round-trips WHOLE in a "
                   "leaderboard row and in the scope echo?", ["NZ45"],
                ([r[0] for r in r45], g("NZ45").env.get("scope") if g("NZ45") else None),
                ([r[0] for r in T.leaderboard(ctx["long_mint_from"], TODAY) if r[0] == lm], f"mint_url={lm}"),
                f"SQL: listed in the window from its newest config ({ctx['long_mint_from']}); the column and the echo "
                "now cap at 2048")
            ask(C, "...and as a payment_activity filter: same echo, the rows SQL has for it?", ["NZ46"],
                (g("NZ46").env.get("scope") if g("NZ46") else None, day_series(g("NZ46"))),
                (f"mint_url={lm}", T.daily(mint=lm)), "scope line and rows")

    # =======================================================================
    def s_rejections():
        C = "Contract - routing and rejections"

        def err(cid, *needles):
            r = g(cid)
            return bool(r and r.error and all(n.lower() in r.body.lower() for n in needles))

        ask(C, "A bare 'payment_activity' (no domain) resolves to nutzaps by name?", ["NZ70"], ident(g("NZ70")),
            "nutzaps.payment_activity", "identity line after name-based domain resolution")
        ask(C, "...and serves the same rows as the explicit call?", ["NZ70", "NZ01"], rows(g("NZ70")), rows(g("NZ01")),
            "row equality")
        ask(C, "A bare 'mint_leaderboard' resolves too?", ["NZ71"], ident(g("NZ71")), "nutzaps.mint_leaderboard",
            "identity line")
        ask(C, "active_users on the FREE tool (domain='nutzaps') is refused and names the paid tool nostrology_query_nutzaps?",
            ["NZ72"], err("NZ72", "nostrology_query_nutzaps"), True, "design record sec 3: the unknown-metric error names the paid tool")
        ask(C, "...and so does a bare active_users (no domain)?", ["NZ73"], err("NZ73", "nostrology_query_nutzaps"), True,
            "same rule; the record does not say which domain, so both are asked")
        ask(C, "A non-http(s) mint_url is rejected with the remedy (call mint_leaderboard)?", ["NZ74"],
            err("NZ74", "mint_leaderboard"), True, "design record sec 3: rejected with the remedy")
        ask(C, "A mint_url with an out-of-range port is rejected with the remedy?", ["NZ75"],
            err("NZ75", "mint_leaderboard"), True, "loader normalizer: a port outside 0..65535 does not normalize")
        ask(C, "A mint_url over 1,024 bytes is rejected with the remedy?", ["NZ76"], err("NZ76", "mint_leaderboard"), True,
            "the loader's cap")
        ask(C, "mint_url on a Zapstore metric is rejected, pointing at the nutzaps metrics?", ["NZ77"],
            err("NZ77", "mint_url", "nutzaps"), True, "only nutzaps metrics take mint_url")
        ask(C, "mint_url on a Routstr metric is rejected?", ["NZ85"], err("NZ85", "mint_url"), True, "same rule")
        ask(C, "provider_pubkey on a nutzaps metric is rejected, naming mint_url as the filter that applies?", ["NZ78"],
            err("NZ78", "mint_url"), True, "design record: no other entity filter applies")
        ask(C, "publisher_pubkey on a nutzaps metric is rejected?", ["NZ79"], err("NZ79", "mint_url"), True, "same rule")
        ask(C, "An inverted window (from after to) is rejected?", ["NZ80"], err("NZ80", "after"), True, "window rule")
        # Since 0.5.1 a rejected grain also names the call that answers the ask.
        ask(C, "grain=week on payment_activity is rejected, naming the grains it serves and the call that answers a week?",
            ["NZ81"], err("NZ81", "serves grain day | total", "for one week, call grain=total with from/to spanning it"),
            True, "payment_activity serves day | total; one week = one grain=total call")
        ask(C, "grain=day on mint_leaderboard is rejected, pointing to the per-mint daily rows?", ["NZ82"],
            err("NZ82", "serves grain total", "call nutzaps.mint_activity (grain day)"), True,
            "total only; the daily rows per mint are mint_activity")
        ask(C, "grain=total on mint_activity is rejected, pointing to each mint's totals?", ["NZ83"],
            err("NZ83", "serves grain day", "call nutzaps.mint_leaderboard (grain total)"), True,
            "day only; per-mint totals over a window are mint_leaderboard")
        ask(C, "An all-future window returns an empty series, not an error?", ["NZ84"],
            (not (g("NZ84") and g("NZ84").error), len(rows(g("NZ84")))), (True, 0), "2099-01-01..2099-01-31")
        wids = ["NZ02", "NZ08", "NZ31", "NZ51", "NZ65"]
        ask(C, "No nutzaps metric is a snapshot: windows are accepted on all of them (the totals and the leaderboard too)?",
            wids, [bool(g(c) and not g(c).error) for c in wids], [True] * len(wids), "windowed calls succeed")
        ask(C, "format=markdown renders the same values as CSV for the same window?", ["NZ86", "NZ02"],
            markdown_series(g("NZ86").body) if g("NZ86") else None, day_series(g("NZ02")),
            "the same days parsed out of both renderings")

    # =======================================================================
    def s_envelope():
        C = "Contract - envelope"
        ok_ids = sorted(k for k, r in M.items() if r.call.get("tool") == METRIC and not r.error
                        and (r.call["args"].get("domain") == "nutzaps" or ident(r).startswith("nutzaps.")))
        ask(C, "Every successful nutzaps call's identity line names nutzaps.<the metric asked>?", ok_ids,
            sorted(k for k in ok_ids if ident(M[k]) != "nutzaps." + M[k].call["args"]["metric"]), [],
            "identity line vs the call's metric")
        ask(C, "...and declares an event series (series_type: event), never a snapshot?", ok_ids,
            sorted(k for k in ok_ids if "series_type: event" not in (M[k].env.get("metric") or "")), [],
            "identity line")
        # The flag speaks for the whole row: true only where every column adds up across
        # days or windows. payment_activity@total said true until 0.5.1 despite its mints.
        additive = {("payment_activity", "day"), ("mint_activity", "day")}
        ask(C, "...and says additive_over_time: true exactly on the day series of payments and sats (payment_activity "
            "and mint_activity at grain day), false on every grain with a distinct count, a min or max, or a config count?",
            ok_ids, sorted(k for k in ok_ids if ("additive_over_time: true" in (M[k].env.get("metric") or ""))
                           != ((M[k].call["args"]["metric"], grain_of(M[k])) in additive)), [],
            "identity line's additive_over_time vs the metric and grain served")
        ask(C, "payment_activity@total says only its payments and sats add up across windows, not its mints?", ["NZ07"],
            bool(g("NZ07") and "only payments and sats add up" in g("NZ07").body
                 and "a distinct count, so it does not add across windows" in g("NZ07").body), True,
            "the mints column's description and the methodology")
        wins = [("NZ02", L7F, L7T), ("NZ05", PMF, PMT), ("NZ31", L7F, L7T), ("NZ65", PMF, PMT), ("NZ11", PMF, PMT)]
        ask(C, "A windowed call echoes the resolved window (from..to, UTC days, inclusive)?", [c for c, *_ in wins],
            [(g(c).env.get("window") or "").startswith(f"{f}..{t} (UTC days, inclusive") if g(c) else False for c, f, t in wins],
            [True] * len(wins), "envelope window line")
        opens = ["NZ04", "NZ10", "NZ33"]
        ask(C, "An open-ended to resolves to today and says the final day is in progress?", opens,
            [(g(c).env.get("window") or "").startswith(f"{MF}..{TODAY}") and "in progress" in (g(c).env.get("window") or "")
             if g(c) else False for c in opens], [True] * len(opens), "envelope window line")
        sc = ["NZ15", "NZ16", "NZ41", "NZ52", "NZ61", "NZ66", "NZ67"]
        ask(C, "Filtered calls echo the mint_url in a scope line?", sc, [g(c).env.get("scope") if g(c) else None for c in sc],
            [f"mint_url={top}"] * len(sc), "envelope scope line")
        un = ["NZ01", "NZ30", "NZ60"]
        ask(C, "Unfiltered calls carry no scope line?", un, [("scope" in g(c).env) if g(c) else None for c in un],
            [False] * len(un), "a filtered answer must be distinguishable from a network-wide one, and vice versa")
        ut = ["NZ30", "NZ50", "NZ60", "NZ62", "NZ63"]
        ask(C, "Where the mint_url column ships, the untrusted line names it?", ut,
            ["mint_url" in (g(c).env.get("untrusted") or "") if g(c) else False for c in ut], [True] * len(ut),
            "design record: mint_url is third-party text (a sender's u tag)")
        ask(C, "Every successful call carries a methodology line?", ok_ids,
            sorted(k for k in ok_ids if len(M[k].methodology) < 40), [], "the line that travels with the numbers")
        m01 = (g("NZ01").methodology if g("NZ01") else "").lower()
        ask(C, "The methodology states the population: Cashu proofs only, test and private mints excluded?", ["NZ01"],
            ("proof" in m01, "test" in m01 and "private" in m01), (True, True), "text check")
        m60 = (g("NZ60").methodology if g("NZ60") else "").lower()
        ask(C, "mint_active_users' methodology warns that per-mint senders do not sum, and points at the paid tool?",
            ["NZ60"], (("not additive" in m60) or ("over-count" in m60) or ("do not sum" in m60),
                       "nostrology_query_nutzaps" in m60), (True, True), "design record sec 4")
        c01 = g("NZ01").env.get("columns", "") if g("NZ01") else ""
        ask(C, "The column contract declares a sats column and no msats unit?", ["NZ01"],
            ("sats (integer" in c01, "msats)" in c01), (True, False), "envelope columns line, pinned loosely")
        uncapped = [k for k in ok_ids if not M[k].truncated and M[k].call["args"].get("format") != "markdown"]
        ask(C, "The rows: line states the rows returned on every uncapped call?", uncapped,
            sorted(k for k in uncapped if M[k].declared_rows() != len(M[k].rows)), [], "rows: vs parsed CSV")

        def of_total(r):
            m = re.search(r"rows:\s*\d+\s+of\s+(\d+)", r.body)
            return int(m.group(1)) if m else None
        ask(C, "A truncated: line appears only where the cap binds - every truncated call returned exactly the cap, "
               "of a larger declared total?", ok_ids,
            sorted(k for k in ok_ids if M[k].truncated and (len(M[k].rows) != ROW_CAP or (of_total(M[k]) or 0) <= ROW_CAP)),
            [], "a total counted before the window (the whole table) once printed 'rows: 4 of 861' and a truncated: "
                "line on windowed calls the cap never touched; the rows: check above skips truncated calls")
        n50, n60 = len(T.mint_activity()), len(T.active_users("month"))
        n62, n63 = len(T.active_users("day")), len(T.active_users("week"))
        n30 = len(T.leaderboard())
        capped = ("NZ50", "NZ60", "NZ62", "NZ63", "NZ30")
        got = [(bool(g(c).truncated), g(c).declared_rows()) if g(c) else None for c in capped]
        ask(C, "The 2,000-row cap announces itself with a truncated: line exactly when it binds?", list(capped),
            got, [(n > ROW_CAP, min(n, ROW_CAP)) for n in (n50, n60, n62, n63, n30)],
            f"SQL populations: {n50} (day, mint) pairs, {n60} (month, mint), {n62} (day, mint) and {n63} (week, mint) "
            f"buckets, {n30} All rows - the cap "
            f"{'binds' if max(n50, n60, n62, n63, n30) > ROW_CAP else 'does NOT bind on live data today, so only the no-truncation half is exercised'}")
        want_as_of = iso(T.fp["max_created_all"]) if T.fp.get("max_created_all") else None
        ask(C, "as_of is data-derived - the newest kind-9321 (nutzaps) event's created_at - and identical on every call?",
            ok_ids, sorted({as_of(M[k]) for k in ok_ids}, key=str), [want_as_of],
            "SQL: MAX(created_at) over events_kind_9321_nutzaps (all rows); the record is silent on as_of, the "
            "free metrics are expected to share the paid metric's probe")

    # =======================================================================
    def s_paid():
        C = "Paid tool - unchanged"
        b110 = g("NZ110").body if g("NZ110") else ""
        ask(C, "Called WITHOUT a token, nostrology_query_nutzaps answers with its payment terms (an error result, "
               "nothing charged)?", ["NZ110"], bool(g("NZ110") and g("NZ110").error and "Payment required" in b110
                                                   and "cashu_token" in b110), True, "the free path of the paid tool")
        price = re.search(r"costs (\d+) sat", b110)
        desc = (tools or {}).get("nostrology_query_nutzaps", {}).get("description", "")
        dprice = re.search(r"PAID: (\d+) sat", desc)
        ask(C, "The terms carry a price in sats, the same as the tool's own description?", ["NZ110"],
            int(price.group(1)) if price else None, int(dprice.group(1)) if dprice else "tools/list not read",
            "challenge text vs tools/list description")
        ask(C, "The terms never name the unadvertised test mint?", ["NZ110"], "testnut" in b110.lower(), False,
            "the test instrument stays private")
        b111 = g("NZ111").body if g("NZ111") else ""
        ask(C, "A windowed paid call without a token also returns the terms and no data?", ["NZ111"],
            bool(g("NZ111") and g("NZ111").error and "Payment required" in b111 and "unique_users" not in b111), True,
            "no table without payment")
        if tools:
            paid = tools.get("nostrology_query_nutzaps", {}).get("inputSchema", {}).get("properties", {})
            ask(C, "tools/list still carries the paid tool with metric active_users and a cashu_token argument?", [],
                (bool(paid), (paid.get("metric") or {}).get("enum"), "cashu_token" in paid),
                (True, ["active_users"], True), "the paid schema is untouched (design record sec 4)")
            free = tools.get(METRIC, {}).get("inputSchema", {}).get("properties", {})
            ask(C, "The free tool's schema advertises domain 'nutzaps' and the mint_url filter?", [],
                ("nutzaps" in ((free.get("domain") or {}).get("enum") or []), "mint_url" in free), (True, True),
                "tools/list inputSchema of nostrology_query_metric")

    # =======================================================================
    def s_paid_line():
        C = "Paid line - free per-mint counts vs the paid table"
        present = {gr: bool(T.paid_au.get(gr)) for gr in ("day", "week", "month")}
        ask(C, "The paid table (active_user_counts_kind_9321_nutzaps) was read, at every grain?", [], present,
            {"day": True, "week": True, "month": True}, "SQL section PAID_AU")
        # Failures print buckets and the side that broke, never a count: both the paid
        # table and the raw network re-count ARE the paid series.
        for cid, grain in (("NZ62", "day"), ("NZ63", "week"), ("NZ60", "month")):
            paid = dict(T.paid_au.get(grain, {}))
            ask(C, f"{grain}: the paid table counts the senders the raw rows hold, deduplicated across mints (buckets "
                   "that disagree)?", [], paid_vs_raw(paid, T.network_active_users(grain))[:8] if paid
                else ["no paid rows read"], [],
                "SQL: DISTINCT senders per bucket over every counted mint, recomputed in Python from the raw rows - "
                "the free and the paid numbers rest on one population")
            ask(C, f"{grain}: in every bucket no mint's count exceeds the paid network count, and the sum over mints "
                   "never falls below it (buckets that break the bracket)?", [cid],
                bracket_violations(au_rows(g(cid)), paid)[:8] if paid else ["no paid rows read"], [],
                "the paid table vs the TOOL's per-mint rows: a sender counts once per mint used, so max <= paid <= sum")

    # =======================================================================
    def s_overview():
        C = "Live page - /nutzaps"
        ov = W["overview"]
        web = web_daily_rows(ov["data"]["dailySeries"])
        rolling(C, "The page's daily series - both charts, daily and cumulative - equals the SQL restatement of its "
                   "own rule?", [], web, lambda t: T.web_daily(t), ov["t0"], ov["t1"],
                "SQL restated: created_at >= NOW() - 365 d, calendar from that day to today, zero-filled, running sums")
        ask(C, "The page's calendar runs from D0 (today - 365 d, the partial oldest day) to today?", [],
            (web[0][0], web[-1][0]) if web else None, (D0, TODAY), "first and last point")
        s06 = day_series(g("NZ06"))
        ask(C, "Every day after D0: the page's daily payments and sats equal payment_activity@day (zero-filled)?",
            ["NZ06"], [(d, n, s) for d, n, _, s, _ in web[1:]], dense(s06, D1, TODAY),
            "trap 1 of design record sec 5 avoided: compare the days after the partial oldest day")
        w0 = (web[0][1], web[0][3]) if web else None
        full0 = s06.get(D0, (0, 0))
        bound(C, f"The partial oldest day ({D0}): the page's payments and sats sit between zero and the tool's whole day?",
              ["NZ06"], w0, (0, 0), full0, w0 is not None and all(0 <= a <= b for a, b in zip(w0, full0)),
              "the page cuts D0 at the time of day it was fetched; the tool can only take D0 whole")
        ask(C, "The cumulative lines, re-based after D0, are the running sums of payment_activity@day from D0 + 1?",
            ["NZ06"], [(d, cn - web[0][2], cs - web[0][4]) for d, _, cn, _, cs in web[1:]], running(s06, D1, TODAY),
            "cumulative = running sum from the window start (design record sec 1)")
        t13 = total_row(g("NZ13"))
        ask(C, "The page's end-of-year cumulative, less D0, is payment_activity@total from D0 + 1?", ["NZ13"],
            (web[-1][2] - web[0][2], web[-1][4] - web[0][4]) if web else None, t13[:2] if t13 else None,
            "last cumulative point vs the one-row total")

    # =======================================================================
    def s_mints_page():
        C = "Live page - /nutzaps/mints leaderboard"
        api = {p: W["api_" + p]["data"] for p in ("all", "7d", "30d", "1y")}
        page_all = W["mints_page"]["data"]["mintStats"]
        ask(C, "The page's All table IS what /api/nutzaps?period=all serves (one function)?", [],
            page_all, api["all"]["items"], "SSR payload vs the JSON endpoint")
        ask(C, "The API echoes the period asked for?", [], [api[p]["period"] for p in api], list(api), "period field")
        wall = web_lb(api["all"]["items"])
        ask(C, "All: the page's rows equal the SQL restatement of its rule?", [], wall, T.web_leaderboard(None),
            "SQL restated: latest config per pubkey, payments all time, floor 10, excluded never; has_page = a payment "
            "ever or 10 configs")
        r30 = lb_rows(g("NZ30"))
        ask(C, "All: PARITY - every page row equals mint_leaderboard with no window (payments, sats, Configured By)?",
            ["NZ30"], {m: (v[1], v[2], v[0]) for m, v in wall.items()}, {m: (p, s, c) for m, p, s, c in r30},
            "design record sec 3: exact at All")
        ask(C, "All: the page's order is the tool's order (the page has no final tie-break, so sort keys are compared)?",
            ["NZ30"], [(i["total_amount_sats"], i["payment_count"], i["config_count"]) for i in api["all"]["items"]],
            [(s, p, c) for m, p, s, c in r30], "sats, payments, configured_by sequence")
        ask(C, "All: every listed mint has a page?", [], all(v[3] for v in wall.values()), True, "has_page on every All row")
        allm = {m for m, *_ in r30}
        for period, (outer_id, inner_id) in (("7d", ("NZ37", "NZ38")), ("30d", ("NZ39", "NZ40")), ("1y", ("NZ35", "NZ36"))):
            w = web_lb(api[period]["items"])
            t0, t1 = W["api_" + period]["t0"], W["api_" + period]["t1"]
            n = ROLLING[period]
            rolling(C, f"{period}: the page's rows equal the SQL restatement of its rolling rule?", [], w,
                    lambda t, n=n: T.web_leaderboard(t - n * DAY), t0, t1,
                    f"SQL restated: payments and latest configs with created_at >= NOW() - {n} d (no upper bound), floor 1")
            pw = {m: (v[1], v[2], v[0]) for m, v in w.items()}
            inner = {m: (p, s, c) for m, p, s, c in lb_rows(g(inner_id))}
            outer = {m: (p, s, c) for m, p, s, c in lb_rows(g(outer_id))}
            bad = bracket_rows(pw, inner, outer)

            def summ(d):
                return (len(d), sum(v[0] for v in d.values()), sum(v[1] for v in d.values()))
            bound(C, f"{period}: every page row sits between mint_leaderboard from {g(outer_id).call['args']['from'] if g(outer_id) else '?'} "
                     f"and from {g(inner_id).call['args']['from'] if g(inner_id) else '?'} (payments, sats, Configured By)?",
                  [inner_id, outer_id], summ(pw), summ(inner), summ(outer), not bad,
                  "rolling vs whole UTC days (design record sec 3, 'Parity with the page'); shown as (mints, payments, "
                  "sats); per-mint violations: " + ("; ".join(bad[:6]) if bad else "none"))
            ask(C, f"{period}: has_page marks exactly the mints the tool's All view lists?", ["NZ30"],
                sorted(m for m, v in w.items() if v[3]), sorted(m for m in w if m in allm),
                "the per-mint page exists for exactly the All-listed mints")

    # =======================================================================
    def s_topn():
        C = "Live page - /nutzaps/mints cumulative-by-mint chart"
        mp = W["mints_page"]
        tops = mp["data"]["topMintsCumulative"]
        head = [(e["mint_url"], e["total_sats"]) for e in tops]
        a, b = T.web_top_mints(mp["t0"] - SKEW), T.web_top_mints(mp["t1"] + SKEW)
        ask(C, "The chart's top 20 (mint, trailing-365 d sats) are the SQL restatement's 20 largest (tie-tolerant)?", [],
            topn_consistent(head, a, 20) or topn_consistent(head, b, 20), True,
            "SQL restated: counted payments with created_at >= NOW() - 365 d, SUM(amount_sats) per mint; the page "
            "ranks with no tie-break, so the totals are compared, not the order among equals")
        inner = {m: s for m, p, s, c in lb_rows(g("NZ36"))}
        outer = {m: s for m, p, s, c in lb_rows(g("NZ35"))}
        for n in (5, 10, 20):
            ranked_inner = [m for m, *_ in lb_rows(g("NZ36")) if inner.get(m)][:n]
            ranked_outer = [m for m, *_ in lb_rows(g("NZ35")) if outer.get(m)][:n]
            bound(C, f"Top {n}: ranking with mint_leaderboard over the whole-day windows around the cutoff reproduces "
                     f"the chart's top {n}?", ["NZ35", "NZ36"], [m for m, _ in head[:n]], ranked_inner, ranked_outer,
                  topn_bracket(head[:n], inner, outer),
                  "design record sec 1: rank with mint_leaderboard@total over the window; each page total must lie "
                  "between the D0 + 1 and D0 windows, and no mint off the chart may beat the chart's smallest total")
        ma = ma_rows(g("NZ53"))
        got, want = {}, {}
        for e in tops:
            m = e["mint_url"]
            line = [(day_of(p["timestamp"]), p["cumulative_sats"]) for p in e["daily"]]
            if not line:
                continue
            got[m] = [(d, c - line[0][1]) for d, c in line[1:]]
            s = {d: v for (d, mm), v in ma.items() if mm == m}
            want[m] = [(d, cs) for d, _, cs in running(s, D1, TODAY)]
        ask(C, "Every line, re-based after D0, is the running sum of mint_activity@day for that mint?", ["NZ53"], got, want,
            "cumulative = running sum from the window start; the partial D0 is left out (design record sec 5)")
        if tops:
            m0 = tops[0]["mint_url"]
            rolling(C, f"The first line ({m0}) equals the SQL restatement, D0 included?", [],
                    [(day_of(p["timestamp"]), p["cumulative_sats"]) for p in tops[0]["daily"]],
                    lambda t: T.web_mint_cumulative(t, m0), mp["t0"], mp["t1"], "SQL restated per-mint running sum")

    # =======================================================================
    def s_mint_pages():
        C = "Live page - /nutzaps/mints/<mint>"
        calls = {"top": ("NZ41", "NZ15", "NZ61"), "few": ("NZ42", "NZ19", "NZ64"), "config_only": ("NZ43", None, None)}
        for role, mint in (("top", top), ("few", few), ("config_only", cfg_only)):
            key = "mint_" + role
            if not mint or key not in W:
                continue
            page = W[key]
            det = web_detail((page["data"] or {}).get("detail"))
            lbid, dayid, mauid = calls[role]
            rolling(C, f"{role} mint ({mint}): the whole page - headline, daily series, MAU - equals the SQL restatement?",
                    [], det, lambda t, m=mint: T.web_mint_detail(t, m), page["t0"], page["t1"],
                    "SQL restated: getNutzapMintDetail's three queries (trailing 365 d rolling, all-time listing, per-mint MAU)")
            r = lb_rows(g(lbid))
            ask(C, f"{role}: the all-time headline (payments, sats, Configured By) equals mint_leaderboard with mint_url, "
                   "no window?", [lbid], (det["all_time_payments"], det["all_time_sats"], det["config_count"]) if det else None,
                (r[0][1], r[0][2], r[0][3]) if r else None, "design record sec 1: the per-mint headline")
            if dayid:
                s = day_series(g(dayid))
                ask(C, f"{role}: every day after D0 equals payment_activity@day with mint_url (zero-filled)?", [dayid],
                    [(d, n, x) for d, n, _, x, _ in det["daily"][1:]] if det else None, dense(s, D1, TODAY),
                    "the page's daily chart, the partial oldest day left out")
            if mauid:
                au = {k[0]: v for k, v in au_rows(g(mauid)).items()}
                mau = dict(det["mau"]) if det else {}
                partial = D0[:7] + "-01"  # the month of the page's cutoff - the ONLY partial one
                months = sorted(mau)
                ask(C, f"{role}: the chart's months run unbroken, none missing from its axis?", [],
                    [(a, b) for a, b in zip(months, months[1:]) if b != next_month(a)], [],
                    "nostrology-web#33 (Per-mint monthly users chart hides months with no activity): the x-axis has "
                    "one slot per row, so a month with no row vanishes with no sign")
                ask(C, f"{role}: monthly active users after the partial month {partial} equal mint_active_users@month?",
                    [mauid], sorted((m, v) for m, v in mau.items() if m > partial and v),
                    sorted((m, v) for m, v in au.items() if partial < m <= TODAY),
                    "trap 1 of design record sec 5: the per-mint MAU's oldest month is partial; the page draws a month "
                    "with no sender as 0, and the tool, whose series is sparse, omits it; a forged date in a later "
                    "month is on the tool and, by design, not on the page")
                bound(C, f"{role}: the partial month {partial} sits between zero and the tool's whole month?",
                      [mauid], mau.get(partial, 0), 0, au.get(partial, 0), 0 <= mau.get(partial, 0) <= au.get(partial, 0),
                      "the page counts only the senders after its cutoff inside that month (absent = 0)")
            if role == "top":
                t17, t18 = total_row(g("NZ17")), total_row(g("NZ18"))
                w = (det["total_payments"], det["total_sats"]) if det else None
                lo, hi = (t18[:2] if t18 else None), (t17[:2] if t17 else None)
                bound(C, "top: the past-year headline sits between payment_activity@total from D0 + 1 and from D0?",
                      ["NZ17", "NZ18"], w, lo, hi, bool(w and lo and hi and all(a <= b <= c for a, b, c in zip(lo, w, hi))),
                      "rolling 365 d vs whole UTC days")
        if exc and "mint_excluded" in W:
            ask(C, f"The excluded mint's page 404s and the tool returns no rows for it - the same exclusion?", ["NZ102"],
                (W["mint_excluded"]["status"], len(rows(g("NZ102")))), (404, 0), "design record sec 2: 'as its page 404s'")

    for name, fn in [("Tool - payment_activity@day", s_payment_day), ("Tool - payment_activity@total", s_payment_total),
                     ("Tool - mint_leaderboard@total", s_leaderboard), ("Tool - mint_activity@day", s_mint_activity),
                     ("Tool - mint_active_users", s_active_users), ("Cross-call reconciliation", s_cross),
                     ("Contract - mint_url filter, normalization, exclusion", s_filter),
                     ("Contract - routing and rejections", s_rejections), ("Contract - envelope", s_envelope),
                     ("Paid tool - unchanged", s_paid), ("Paid line - free per-mint counts vs the paid table", s_paid_line),
                     ("Live page - /nutzaps", s_overview),
                     ("Live page - /nutzaps/mints leaderboard", s_mints_page),
                     ("Live page - /nutzaps/mints cumulative-by-mint chart", s_topn),
                     ("Live page - /nutzaps/mints/<mint>", s_mint_pages)]:
        section(name, fn)
    return Q

"""Run the nutzaps MCP verification battery end to end.

    python3 -m nostrology_mcp.verification.nutzaps_run              # full run
    python3 -m nostrology_mcp.verification.nutzaps_run --reuse      # re-verify cached fetches
    python3 -m nostrology_mcp.verification.nutzaps_run --self-test  # prove the harness can fail
    python3 -m nostrology_mcp.verification.run --domain nutzaps     # the same, via the shared entry point

Order matters, as in the other batteries: the independent sides come FIRST - the
raw rows from Aurora (psql as nostrology_ro), then the live pages - because every
subject and window in the MCP plan is derived from them. After the MCP calls the
table fingerprint is read AGAIN: if the nightly refresh (Dagster nutzaps_daily,
01:45 UTC) moved the tables in between, the run aborts rather than compare two
datasets. Raw fetches are cached under `raw/<date>/` so `--reuse` re-verifies
without touching the deployment, the site or the MCP endpoint.

The MCP endpoint is always the MARKED url (mcp_client.DEFAULT_URL,
`https://nostrolo.gy/mcp?traffic_type=internal`); `--url` exists for a local
server (`http://127.0.0.1:<port>/mcp`) only.
"""
import argparse
import copy
import datetime as dt
import html
import json
import os
import re
import sys

from . import mcp_parse, nutzaps_plan, nutzaps_questions, nutzaps_truth
from . import report as report_mod
from .mcp_client import DEFAULT_URL, McpClient
from .nutzaps_truth import Truth, shift

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_OUT = os.path.join(REPO, "nostrology_mcp", "verification", "battery-artifacts")
FEW_MAX_PAYMENTS = 5  # "a mint with configs but few payments"
LONG_URL_CHARS = 256  # the text-cell clip the mint_url column and scope echo no longer apply (cap 2048)


def build_context(sql, today):
    """Every subject and window, derived from the raw rows - never hard-coded."""
    T = Truth(sql)
    lb = T.leaderboard()
    if not lb:
        raise SystemExit("the SQL truth lists no counted mint - nothing to verify (is the nutzaps table empty?)")
    top = lb[0][0]
    few = sorted((r for r in lb if r[0] != top and r[3] >= 1 and 1 <= r[1] <= FEW_MAX_PAYMENTS),
                 key=lambda r: (-r[3], r[1], r[0]))
    if not few:
        few = sorted((r for r in lb if r[0] != top and r[1] >= 1), key=lambda r: (r[1], -r[3], r[0]))
    cfg_only = sorted((r for r in lb if r[1] == 0), key=lambda r: (-r[3], r[0]))
    # The longest counted mint URL, when it is longer than the renderer's old 256-char
    # clip: it must round-trip whole (row and scope echo). A window from its newest
    # event lists it (floor 1) even when All does not.
    seen = ({p[2] for p in T.pays} | {m for _, ms in T.configs.values() for m in ms}) - T.excluded
    longest = max(sorted(seen), key=len) if seen else None
    long_mint = longest if longest and len(longest) > LONG_URL_CHARS else None
    long_days = ([nutzaps_truth.day_of(t) for t, ms in T.configs.values() if long_mint in ms]
                 + [p[4] for p in T.pays if p[2] == long_mint]) if long_mint else []
    # An excluded mint WITH raw payments, so the filter has rows it must refuse to reach.
    exc = sorted(T.excluded, key=lambda m: (-len(T.select(None, None, m, raw=True)), m))
    https = [r[0] for r in lb if r[0].startswith("https://")]
    first = T.first_day() or today
    month_from = today[:7] + "-01"
    pm_to = shift(month_from, -1)
    pm_from = pm_to[:7] + "-01"
    d0 = shift(today, -365)
    # a Wednesday at least two weeks back whose week HAS senders: a week window from
    # it must leave that week out whole (buckets START inside a window), which only
    # tests something if there is a sender to leave out
    two_weeks = dt.date.fromisoformat(shift(today, -14))
    wed = two_weeks - dt.timedelta(days=(two_weeks.weekday() - 2) % 7)
    weeks = {b for b, _ in T.active_users("week")}
    for _ in range(52):
        if (wed - dt.timedelta(days=2)).isoformat() in weeks:
            break
        wed -= dt.timedelta(days=7)
    midweek = wed.isoformat()
    return {
        "today": today,
        "last7_from": shift(today, -7), "last7_to": shift(today, -1),
        "last30_from": shift(today, -30), "last30_to": shift(today, -1),
        "month_from": month_from,
        "past_month_from": pm_from, "past_month_to": pm_to, "past_month_second": shift(pm_from, 1),
        "midweek_from": midweek,
        # the web's trailing year: its calendar starts on the PARTIAL day D0
        "web_d0": d0, "web_d1": shift(d0, 1),
        # the page's rolling 7d / 30d, bracketed by whole days
        "roll7_outer": shift(today, -7), "roll7_inner": shift(today, -6),
        "roll30_outer": shift(today, -30), "roll30_inner": shift(today, -29),
        "empty_from": shift(first, -31), "empty_to": shift(first, -1),
        "top_mint": top,
        "few_mint": few[0][0] if few else top,
        "config_only_mint": cfg_only[0][0] if cfg_only else None,
        "excluded_mint": exc[0] if exc else None,
        "excluded_raw_payments": len(T.select(None, None, exc[0], raw=True)) if exc else 0,
        "norm_mint": top if top.startswith("https://") else (https[0] if https else top),
        "path_case_mint": next((r[0] for r in lb if nutzaps_plan.path_case_variant(r[0])), None),
        "long_mint": long_mint,
        "long_mint_from": max(long_days) if long_days else None,
    }


def list_tools(client):
    """tools/list over the same initialized session: name -> {description, inputSchema}."""
    client._rid += 1
    _, raw = client._post({"jsonrpc": "2.0", "id": 100 + client._rid, "method": "tools/list"})
    resp = client._parse(raw) or {}
    return {t["name"]: {"description": t.get("description", ""), "inputSchema": t.get("inputSchema", {})}
            for t in (resp.get("result") or {}).get("tools", [])}


def jsonable(v):
    """A question value as JSON: the per-(day, mint) and per-(bucket, mint) views are
    dicts keyed by tuples, which json cannot key - they become 'a|b' strings."""
    if isinstance(v, dict):
        return {("|".join(map(str, k)) if isinstance(k, tuple) else k): jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [jsonable(x) for x in (sorted(v, key=str) if isinstance(v, set) else v)]
    return v


def write_results(path, meta, ctx, Q):
    doc = {"meta": meta, "ctx": ctx, "questions": Q}
    text = json.dumps(jsonable(doc), indent=1, default=str)  # serialize fully BEFORE opening the file
    with open(path, "w") as fh:
        fh.write(text)


def count(Q, v):
    return sum(1 for q in Q if report_mod.verdict(q) == v)


def self_test(M, sql, W, ctx, tools, Q):
    """Two proofs that the harness can fail. (1) Corrupting ONE expected value adds
    exactly one FAIL. (2) Removing the newest counted payment from the SQL side - a
    one-row disagreement in the data - makes the MCP-vs-SQL checks FAIL."""
    n0 = count(Q, "fail")
    q2 = copy.deepcopy(Q)
    target = next(q for q in q2 if report_mod.verdict(q) == "pass" and not isinstance(q["want"], bool) and not q["bounded"])
    target["want"], target["ok"] = f"__corrupted__{target['want']}", False
    n1 = count(q2, "fail")
    sql2 = copy.deepcopy(sql)
    excluded = {r["mint_url"] for r in sql2["CLASSES"] if r["excluded"]}
    counted = [i for i, r in enumerate(sql2["PAYMENTS"]) if r["mint_url"] not in excluded]
    del sql2["PAYMENTS"][counted[-1]]
    n3 = count(nutzaps_questions.build(M, sql2, W, ctx, tools), "fail")
    print(f"self-test: baseline {n0} FAIL; one corrupted expectation -> {n1} (expected {n0 + 1}); "
          f"one payment row dropped from the SQL side -> {n3} (expected more than {n0})")
    return 0 if (n1 == n0 + 1 and n3 > n0) else 1


def paths_html(Q):
    n = len(Q)
    paid = [q for q in Q if q["cat"].startswith("Paid line")]
    sqln = sum(1 for q in Q if (q["how"].startswith("SQL") or " SQL" in q["how"]) and q not in paid)
    liven = sum(1 for q in Q if q["cat"].startswith("Live"))
    return f"""<p>No answer is scored against the MCP server's own reasoning, and no expected value is stored in the
harness - both sides are recomputed on every run. The independent paths:</p>
<ul>
<li><strong>Independent SQL over the raw tables</strong> ({sqln} of {n} checks) - every kind-9321 (nutzaps) payment row
(<code>value_source = 'proof_tags'</code>, on every mint), every kind-10019 (nutzap-configs) row and
<code>nutzap_mint_classes</code>, read whole via <code>ssh $NOSTROLOGY_RO_SSH</code> -&gt; <code>psql</code> as
<code>nostrology_ro</code>. The population rule (excluded mints), the latest-config-per-pubkey rule, the listing
floor, the ordering, every window, rank, sum and distinct count are then recomputed in Python from the design
record - never by the tool's SQL. For the paid-line checks only, the same read takes the PAID table
(<code>active_user_counts_kind_9321_nutzaps</code>), to hold the free per-mint counts against it; the report
lists disagreements, never the paid series.</li>
<li><strong>The paid line</strong> ({len(paid)} checks, which test the paid table and the per-mint counts against
each other rather than the tool alone) - the paid table must count the senders the raw rows hold, and in every
bucket no mint's free count may exceed it or the sum over mints fall below it.</li>
<li><strong>The live website</strong> ({liven} checks) - what <code>nostrolo.gy/nutzaps</code>,
<code>/nutzaps/mints</code> (and the <code>/api/nutzaps?period=</code> JSON it is built from) and
<code>/nutzaps/mints/&lt;mint&gt;</code> render, read out of the SvelteKit SSR payload by <code>node</code>. The site
runs its OWN SQL (nostrology-web <code>postgres.ts</code>) on ROLLING windows; each page is compared twice - with the
SQL restatement of its own rule at the instant it was fetched (exact), and with the tool's whole-day windows that
bracket its cutoff (BOUNDED).</li>
<li><strong>Cross-call and contract checks</strong> - day rows summing to totals, the leaderboard summing to the
totals, per-mint ledgers and filters agreeing with each other, the rejections and their remedies, mint URL
normalization, the excluded-mint filter, the envelope lines, the paid tool's unchanged free path.</li>
</ul>
<p>Run <code>python3 -m nostrology_mcp.verification.nutzaps_run --reuse --self-test</code> to confirm the harness can
still fail: one corrupted expectation must add exactly one FAIL, and one payment row dropped from the SQL side must
make the MCP-vs-SQL checks fail.</p>"""


BOUNDED_NOTE = """<div class="note">
<h3>Bounded, not exact: the site cuts on a timestamp, the tool on whole UTC days</h3>
<p>The site's 365-day series and its 7d / 30d / 1y leaderboard periods cut on <code>NOW() - N days</code>, a mid-day
timestamp; the tool's <code>from</code>/<code>to</code> are inclusive UTC calendar days, so the page's oldest day (and
the per-mint MAU's oldest month) can only be taken whole or dropped whole. The battery fetches both whole-day windows
(from D0 and from D0 + 1) and scores the page BOUNDED when it sits strictly between them; the same page is ALSO
compared with the SQL restatement of its own rolling rule at the fetch instant, which is exact. Design record
sec 5.</p>
{items}
</div>"""


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--reuse", action="store_true", help="re-verify from the cached raw fetches")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--url", default=None, help="MCP endpoint override (a LOCAL server only)")
    ap.add_argument("--tag", default=None,
                    help="suffix for this run's report files and raw cache (e.g. 'local' for a run against a local "
                         "server, so the deployed run's report stays the canonical one and its --reuse never reads "
                         "a local server's responses)")
    args = ap.parse_args(argv)
    if args.tag and not re.fullmatch(r"[a-z0-9-]+", args.tag):
        sys.exit("--tag must be lowercase letters, digits and hyphens")
    sfx = f"-{args.tag}" if args.tag else ""

    run_date = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    raw_dir = os.path.join(args.out, "raw", run_date + sfx)
    os.makedirs(raw_dir, exist_ok=True)
    p = {k: os.path.join(raw_dir, f"{k}.nutzaps.json") for k in ("sql", "web", "fp_after", "tools")}

    # -- 1. independent sides first -------------------------------------------
    if args.reuse and os.path.exists(p["sql"]):
        sql = json.load(open(p["sql"]))
        print("reused raw SQL rows")
    else:
        print("querying Aurora read-only (raw kind-9321 (nutzaps) payments, kind-10019 (nutzap-configs), mint classes)...")
        sql = nutzaps_truth.run_sql()
        json.dump(sql, open(p["sql"], "w"))
    print("  " + ", ".join(f"{k}={len(v)}" for k, v in sql.items()))

    ctx = build_context(sql, run_date)
    print(f"  subjects: top={ctx['top_mint']} few={ctx['few_mint']} config_only={ctx['config_only_mint']} "
          f"excluded={ctx['excluded_mint']} ({ctx['excluded_raw_payments']} raw payments)")

    if args.reuse and os.path.exists(p["web"]):
        W = json.load(open(p["web"]))
        print("reused live pages")
    else:
        print("reading the live pages (operator-marked)...")
        W = nutzaps_truth.collect_web({"top": ctx["top_mint"], "few": ctx["few_mint"],
                                       "config_only": ctx["config_only_mint"]}, ctx["excluded_mint"])
        json.dump(W, open(p["web"], "w"))
    empty = [k for k, v in W.items() if k != "mint_excluded" and (v["status"] != 200 or not v["data"])]
    if empty:
        sys.exit(f"live-page extraction returned nothing for {empty} - the page markup or route likely changed. "
                 "Fix nutzaps_truth.collect_web rather than scoring empty against empty.")

    steps = nutzaps_plan.build(ctx)

    # -- 2. the MCP side -------------------------------------------------------
    M, retried, tools = {}, [], None
    if args.reuse and all(os.path.exists(os.path.join(raw_dir, s["id"] + ".txt")) for s in steps):
        for s in steps:
            M[s["id"]] = mcp_parse.load(os.path.join(raw_dir, s["id"] + ".txt"))
        tools = json.load(open(p["tools"])) if os.path.exists(p["tools"]) else None
        print(f"reused {len(M)} MCP responses")
    else:
        url = args.url or DEFAULT_URL
        if "nostrolo.gy" in url and url != DEFAULT_URL:
            sys.exit(f"refusing {url}: the public endpoint is called ONLY as {DEFAULT_URL} (the traffic marker)")
        client = McpClient(url=url)
        client.init()
        print(f"MCP session {client.session_id} at {url}; running {len(steps)} calls...")
        tools = list_tools(client)
        json.dump(tools, open(p["tools"], "w"), indent=1)
        for s in steps:
            text, attempts = client.call(s["tool"], s["args"])
            if attempts:
                retried.append(s["id"])
            M[s["id"]] = mcp_parse.Resp(s, text or "ERROR:empty response")
            mcp_parse.save(M[s["id"]], os.path.join(raw_dir, s["id"] + ".txt"))
        print(f"  {len(M)} responses" + (f"; retried {retried}" if retried else ""))

    # -- 3. did the data move while we read it? --------------------------------
    if args.reuse and os.path.exists(p["fp_after"]):
        fp_after = json.load(open(p["fp_after"]))
    else:
        fp_after = nutzaps_truth.run_fingerprint()
        json.dump(fp_after, open(p["fp_after"], "w"))
    moved = nutzaps_truth.fingerprint_moved(sql["FP"][0], fp_after)
    if moved:
        print(f"ABORT: the nutzap tables changed between the reads ({', '.join(moved)}) - the nightly refresh "
              "ran mid-battery. Rerun without --reuse.")
        return 2

    # -- 4. verify -------------------------------------------------------------
    Q = nutzaps_questions.build(M, sql, W, ctx, tools)
    if args.self_test:
        return self_test(M, sql, W, ctx, tools, Q)

    fp = sql["FP"][0]
    server = next((re.search(r"server:\s*(.+)$", r.body, re.M).group(1).strip() for r in M.values()
                   if re.search(r"server:\s*(.+)$", r.body, re.M)), "nostrology-mcp")
    bnd = [q for q in Q if report_mod.verdict(q) == "bounded"]
    meta = {
        "run_date": run_date,
        "url": args.url or DEFAULT_URL,
        "server": server,
        "retried_calls": retried,
        "title": "Nutzaps MCP verification battery" + (" (local server)" if args.url else ""),
        "subtitle": (f"{len(Q)} questions put to the <code>nostrology</code> MCP server's free <code>nutzaps</code> domain "
                     f"(<code>nostrology_query_metric</code>, <code>domain='nutzaps'</code>) "
                     + (f"on a LOCAL server (<code>{html.escape(args.url)}</code>) built from the nostrology-data working "
                        f"tree, reading the dev Aurora cluster read-only, " if args.url else
                        "on the AWS <code>dev</code> environment, ")
                     + f"each answer verified against an independent path. Run {run_date}, against "
                     f"kind-9321 (nutzaps) data up to {html.escape(nutzaps_questions.iso(fp['max_created_all']))}."),
        "paths_html": paths_html(Q),
        "bounded_note_html": BOUNDED_NOTE.format(items="<ul>" + "".join(
            f"<li><strong>{html.escape(q['cat'])}</strong> - {html.escape(q['q'])}</li>" for q in bnd) + "</ul>"),
        "findings_html": findings_html(Q, ctx, sql),
        "footer_html": (f'<p class="sub" style="margin-top:2rem">Server: <code>{html.escape(server)}</code> at '
                        f'<code>{html.escape(args.url or DEFAULT_URL)}</code>. Data: {fp["pay_rows"]} payment rows, '
                        f'{fp["cfg_rows"]} config rows, newest kind-9321 (nutzaps) event '
                        f'<code>{html.escape(nutzaps_questions.iso(fp["max_created_all"]))}</code> (unchanged across the run: '
                        f'fingerprint read before and after). Subjects derived at run time: top mint '
                        f'<code>{html.escape(ctx["top_mint"])}</code>, few-payments mint <code>{html.escape(ctx["few_mint"])}</code>, '
                        f'config-only mint <code>{html.escape(str(ctx["config_only_mint"]))}</code>, excluded mint '
                        f'<code>{html.escape(str(ctx["excluded_mint"]))}</code>. Harness: '
                        f'<code>nostrology_mcp/verification/nutzaps_*.py</code>.</p>'),
    }
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H%MZ")
    html_path = os.path.join(args.out, f"{stamp}-nutzaps-mcp-verification-battery{sfx}.html")
    json_path = os.path.join(args.out, f"{stamp}-nutzaps-results{sfx}.json")
    open(html_path, "w").write(report_mod.render(Q, meta))
    write_results(json_path, meta, ctx, Q)

    nfail = count(Q, "fail")
    print(f"\nTOTAL {len(Q)}  PASS {count(Q, 'pass')}  BOUNDED {len(bnd)}  FAIL {nfail}")
    for q in Q:
        if report_mod.verdict(q) == "fail":
            print(f"  FAIL #{q['n']:>3} [{q['cat']}] {q['q']}")
            print(f"        MCP={str(q['got'])[:300]!r}")
            print(f"        ind={str(q['want'])[:300]!r}")
    print(f"\nreport: {html_path}")
    return 1 if nfail else 0


def findings_html(Q, ctx, sql):
    T = Truth(sql)
    nfail = count(Q, "fail")
    verdict = ("every check agreed" if not nfail else f"<strong>{nfail}</strong> check(s) disagreed - see the list above")
    biggest = max(len(T.mint_activity()), len(T.leaderboard()),
                  *(len(T.active_users(g)) for g in ("day", "week", "month")))
    cap = (f"the 2,000-row cap does not bind on any nutzaps metric (the largest population is {biggest} rows), so "
           "only the no-truncation half of that contract is checked" if biggest <= 2000 else
           f"the 2,000-row cap binds (a population of {biggest} rows) and its truncated: line is checked")
    return f"""<div class="note" style="border-left-color:var(--good)">
<h3>What this run establishes</h3>
<p><strong>{verdict}.</strong> The population is the one the design record names: {len(T.pays)} kind-9321 (nutzaps)
payments on counted mints out of {len(T.raw)} proof-carrying rows ({len(T.raw) - len(T.pays)} sit on the
{len(T.excluded)} mints <code>nutzap_mint_classes</code> excludes), from {T.distinct_senders()} distinct senders, and
{len(T.configs)} latest kind-10019 (nutzap-configs).</p>
<p><strong>Windows the way a caller asks them</strong>: last 7 days <code>{ctx['last7_from']}..{ctx['last7_to']}</code>,
last 30 days <code>{ctx['last30_from']}..{ctx['last30_to']}</code>, this month from <code>{ctx['month_from']}</code>
(open-ended), the past month <code>{ctx['past_month_from']}..{ctx['past_month_to']}</code>, and the web's trailing year
from its partial oldest day <code>{ctx['web_d0']}</code>.</p>
<p><strong>Row cap</strong>: {cap}.</p>
<p><strong>The paid line, measured</strong> (recomputed from the raw rows, not read from the tool or the paid
table): summed over mints, the per-mint distinct senders equal the network-wide count deduplicated across mints on
{paid_line(T, ctx)}; everywhere else the sum over-counts the senders who used more than one mint. Per-mint day and week buckets are free by the operator's decision of
2026-09-30, taken with these numbers in hand (design record sec 4).</p>
</div>"""


def paid_line(T, ctx):
    """How much of the paid series the free per-mint rows reproduce, from the raw
    rows: over the web's trailing year (the design record's measure) and all history."""
    parts = []
    for grain in ("day", "week", "month"):
        per_mint, net = T.active_users(grain), T.network_active_users(grain)
        year = {b: n for b, n in net.items() if b >= ctx["web_d1"]}
        ye, yn = nutzaps_truth.sum_matches(per_mint, year)
        ae, an = nutzaps_truth.sum_matches(per_mint, net)
        parts.append(f"{ye} of {yn} {grain}s with payments in the trailing year "
                     f"({round(100 * ye / yn) if yn else 0}%; all history: {ae} of {an})")
    return "; ".join(parts)


if __name__ == "__main__":
    sys.exit(main())

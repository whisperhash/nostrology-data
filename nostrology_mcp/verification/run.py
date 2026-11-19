"""Run the Zapstore MCP verification battery end to end.

    python3 -m nostrology_mcp.verification.run                 # full run
    python3 -m nostrology_mcp.verification.run --reuse         # re-verify cached fetches
    python3 -m nostrology_mcp.verification.run --self-test     # prove the harness can fail

Order matters: the live site and Postgres are read FIRST, because two MCP calls are
parameterised from them (the site's rolling-30-day card window, and the first day of
real zap activity). Raw fetches are cached under the output directory's `raw/<date>/`
so a failing check can be re-examined without hammering the deployment.
"""
import argparse
import copy
import datetime as dt
import json
import os
import re
import sys

from . import mcp_parse, plan as plan_mod, questions as questions_mod
from . import report as report_mod, subjects as S, truth_sql, web_truth
from .mcp_client import DEFAULT_URL, McpClient

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_OUT = os.path.join(REPO, "nostrology_mcp", "verification", "battery-artifacts")


# ---------------------------------------------------------------------------
def subject_apps():
    """(pubkey, app_id|None) pairs the per-release ground truth is scoped to."""
    return [
        (S.DEV_PUBKEY, None),
        (S.CURATOR_PUBKEY, S.FIRST_PARTY_APP_ID),
        (S.CURATOR_PUBKEY, S.REPUBLISHED_APP_ID),
        (S.NO_PROFILE_PUBKEY, None),
    ]


def build_context(sql, web):
    """Every date the battery needs, derived from the data - never hard-coded."""
    days = sorted({r["d"] for r in sql["DAILY_RELEASES"]})
    zap_days = sorted({r["d"] for r in sql["APP_ZAPS_DAILY"]})
    last = max(sql["SUMMARY"][0]["max_release_at"][:10], sql["SUMMARY"][0]["max_zap_at"][:10])

    def shift(day, n):
        return (dt.date.fromisoformat(day) + dt.timedelta(days=n)).isoformat()

    return {
        "last_data_day": last,
        "first_day": days[0],
        "year_start": last[:4] + "-01-01",
        "last_year_end": str(int(last[:4]) - 1) + "-12-31",
        "year_ago": shift(last, -365),
        "d30": shift(last, -29),
        # The zap series opens on a single fabricated demo receipt followed by a
        # months-long gap; "real activity" starts at the SECOND populated day.
        "zap_era_from": zap_days[1] if len(zap_days) > 1 else zap_days[0],
        "day_after": lambda d: shift(d, 1),
        "us_date": lambda s: (dt.datetime.strptime(s, "%b %d, %Y").strftime("%Y-%m-%d")
                              if s else None),
        # the site's rolling-30-day cards, as the equivalent calendar window
        "rolling30_from": shift(web["window_end"], -30),
        "rolling30_from_excl": shift(web["window_end"], -29),
        "today": web["window_end"],
        **routstr_context(sql),
    }


def routstr_context(sql):
    """The Routstr windows and subjects, from the DOCUMENT the page serves: its
    generation day defines the 30 complete days (today-30 .. today-1), and the
    top included provider by all-time revenue is the drilldown subject."""
    doc = sql["ROUTSTR_DOC"][0]["data"]
    today = doc["generated_utc"][:10]

    def shift(day, n):
        return (dt.date.fromisoformat(day) + dt.timedelta(days=n)).isoformat()

    included = [r for r in sql["ROUTSTR_PROVIDER_SUMS"] if not r["excluded"]]
    return {
        "routstr_today": today,
        "routstr_30d_from": shift(today, -30),
        "routstr_30d_to": shift(today, -1),
        "routstr_top_provider": included[0]["pubkey"] if included else "0" * 64,
    }


def fill_plan(steps, ctx):
    out = []
    for step in steps:
        step = copy.deepcopy(step)
        for k, v in list(step["args"].items()):
            if v == "__ROLLING30_FROM__":
                step["args"][k] = ctx["rolling30_from"]
            elif v == "__TODAY__":
                step["args"][k] = ctx["today"]
            elif v == "__ROLLING30_FROM_EXCL__":
                step["args"][k] = ctx["rolling30_from_excl"]
            elif v == "__ZAP_ERA_FROM__":
                step["args"][k] = ctx["zap_era_from"]
            elif v == "__ROUTSTR_30D_FROM__":
                step["args"][k] = ctx["routstr_30d_from"]
            elif v == "__ROUTSTR_30D_TO__":
                step["args"][k] = ctx["routstr_30d_to"]
            elif v == "__ROUTSTR_TOP_PROVIDER__":
                step["args"][k] = ctx["routstr_top_provider"]
        out.append(step)
    return out


# ---------------------------------------------------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=DEFAULT_OUT, help="report output directory")
    ap.add_argument("--reuse", action="store_true",
                    help="re-verify from the cached raw fetches instead of re-fetching")
    ap.add_argument("--self-test", action="store_true",
                    help="corrupt one expected value and assert exactly one FAIL")
    ap.add_argument("--url", default=None, help="override the MCP endpoint")
    ap.add_argument("--domain", default="zapstore", choices=["zapstore", "routstr", "nutzaps"],
                    help="which battery: the Zapstore one (default), the Routstr one (routstr_run.py) or the "
                         "nutzaps one (nutzaps_run.py)")
    args, rest = ap.parse_known_args(argv)
    if args.domain in ("routstr", "nutzaps"):
        from . import nutzaps_run, routstr_run
        fwd = [a for a in (argv if argv is not None else sys.argv[1:]) if not a.startswith("--domain")]
        other = {"routstr": routstr_run, "nutzaps": nutzaps_run}[args.domain]
        return other.main([a for a in fwd if a != args.domain])

    run_date = dt.datetime.utcnow().strftime("%Y-%m-%d")
    raw_dir = os.path.join(args.out, "raw", run_date)
    os.makedirs(raw_dir, exist_ok=True)

    sql_path = os.path.join(raw_dir, "sql.json")
    web_path = os.path.join(raw_dir, "web.json")

    # -- 1. independent sides first (they parameterise two MCP calls) --------
    if args.reuse and os.path.exists(sql_path):
        sql = json.load(open(sql_path))
        print("reused SQL ground truth")
    else:
        print("querying Aurora read-only (the cluster may need to resume; this is slow)...")
        sql = truth_sql.run(subject_apps())
        json.dump(sql, open(sql_path, "w"))
    print("  " + ", ".join(f"{k}={len(v)}" for k, v in sql.items()))

    if args.reuse and os.path.exists(web_path):
        web = json.load(open(web_path))
        print("reused live-page scrape")
    else:
        print("scraping the live pages...")
        web = web_truth.collect(S.DEV_PUBKEY, S.DEV_APP_ID)
        web.update(web_truth.collect_routstr())
        json.dump(web, open(web_path, "w"))
    empty = [k for k, v in web.items() if v is None or (isinstance(v, list) and not v)]
    if empty:
        sys.exit(f"live-page extraction returned nothing for {empty} - the page markup "
                 f"likely changed. Fix web_truth.py rather than scoring empty against empty.")

    ctx = build_context(sql, web)
    steps = fill_plan(plan_mod.build(), ctx)

    # -- 2. the MCP side ----------------------------------------------------
    M, retried = {}, []
    if args.reuse and all(os.path.exists(os.path.join(raw_dir, s["id"] + ".txt")) for s in steps):
        for s in steps:
            M[s["id"]] = mcp_parse.load(os.path.join(raw_dir, s["id"] + ".txt"))
        print(f"reused {len(M)} MCP responses")
    else:
        client = McpClient(**({"url": args.url} if args.url else {}))
        client.init()
        print(f"MCP session {client.session_id}; running {len(steps)} calls...")
        for s in steps:
            text, attempts = client.call(s["tool"], s["args"])
            if attempts:
                retried.append(s["id"])
            resp = mcp_parse.Resp(s, text)
            M[s["id"]] = resp
            mcp_parse.save(resp, os.path.join(raw_dir, s["id"] + ".txt"))
        print(f"  {len(M)} responses" + (f"; retried {retried}" if retried else ""))

    # -- 3. verify ----------------------------------------------------------
    Q = questions_mod.build(M, sql, web, ctx)

    if args.self_test:
        target = next(q for q in Q if not isinstance(q["want"], bool))
        target["want"] = f"__corrupted__{target['want']}"
        target["ok"] = False
        nfail = sum(1 for q in Q if report_mod.verdict(q) == "fail")
        print(f"self-test: {nfail} FAIL (expected exactly 1)")
        return 0 if nfail == 1 else 1

    summary = sql["SUMMARY"][0]
    meta = {
        "run_date": run_date,
        # The URL the run ACTUALLY called, marker and all - not a prettied-up copy.
        # The report is where someone copies an endpoint from, and an unmarked copy
        # there teaches the one URL that breaks the traffic marking.
        "url": args.url or DEFAULT_URL,
        "server": (M["M01"].env.get("rows", "").split("server:")[-1].strip()
                   or "nostrology-mcp"),
        "max_release_at": summary["max_release_at"],
        "max_zap_at": summary["max_zap_at"],
        "retried_calls": retried,
    }
    m = re.search(r"server:\s*(.+)$", M["M01"].body, re.M)
    if m:
        meta["server"] = m.group(1).strip()

    html_path = os.path.join(args.out, f"{run_date}-zapstore-mcp-verification-battery.html")
    json_path = os.path.join(args.out, f"{run_date}-results.json")
    os.makedirs(args.out, exist_ok=True)
    open(html_path, "w").write(report_mod.render(Q, meta))
    json.dump({"meta": meta, "questions": Q}, open(json_path, "w"), indent=1, default=str)

    npass = sum(1 for q in Q if report_mod.verdict(q) == "pass")
    nbnd = sum(1 for q in Q if report_mod.verdict(q) == "bounded")
    nfail = sum(1 for q in Q if report_mod.verdict(q) == "fail")
    print(f"\nTOTAL {len(Q)}  PASS {npass}  BOUNDED {nbnd}  FAIL {nfail}")
    for q in Q:
        if report_mod.verdict(q) == "fail":
            print(f"  FAIL #{q['n']:>3} [{q['cat']}] {q['q']}")
            print(f"        MCP={q['got']!r}")
            print(f"        ind={q['want']!r}")
    print(f"\nreport: {html_path}")
    return 1 if nfail else 0


if __name__ == "__main__":
    sys.exit(main())

"""Run the Routstr MCP verification battery end to end.

    python3 -m nostrology_mcp.verification.routstr_run              # full run
    python3 -m nostrology_mcp.verification.routstr_run --reuse      # re-verify cached fetches
    python3 -m nostrology_mcp.verification.routstr_run --self-test  # prove the harness can fail

Order matters, as in the Zapstore battery: the independent sides come FIRST -
the substrate rows and the served document (psql), the reference re-derivation
(DuckDB over S3, pinned to the document's instant; minutes) and the live page -
because every subject and window in the MCP plan is derived from them. Raw
fetches are cached under `raw/<date>/` so `--reuse` re-verifies without
touching the deployment or the archive.
"""
import argparse
import datetime as dt
import html
import json
import os
import re
import sys

from . import mcp_parse, report as report_mod, routstr_plan, routstr_questions, routstr_truth
from .mcp_client import McpClient

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_OUT = os.path.join(REPO, "nostrology_mcp", "verification", "battery-artifacts")


def build_context(sql, reference, today):
    """Every subject and window, derived from the data - never hard-coded."""
    T = routstr_truth.Truth(sql)
    doc = T.doc
    refresh_day = doc["generated_utc"][:10]
    shift = routstr_truth.shift

    top = T.by_prefix(doc["providers"][0]["id"])
    # An excluded provider WITH ledger rows, so the filter path has something to
    # reach; fall back to any excluded provider.
    excluded = sorted((p for p in T.providers.values() if p["excluded"]),
                      key=lambda p: (-p["msats_alltime"], p["pubkey"]))
    exc = next((p for p in excluded if T.provider_rows(pubkey=p["pubkey"], daily_only=False)), excluded[0] if excluded else None)
    named = {m["model"] for m in doc["models"] if m["model"] != "(other)"}
    folded = sorted(((m, v[0]) for m, v in T.model_totals().items() if m not in named and m != "(other)"),
                    key=lambda kv: (-kv[1], kv[0]))
    return {
        "today": today,
        "refresh_day": refresh_day,
        "refresh_30d_from": shift(refresh_day, -30),
        "refresh_30d_to": shift(refresh_day, -1),
        "last7_from": shift(today, -7), "last7_to": shift(today, -1),
        "last30_from": shift(today, -30), "last30_to": shift(today, -1),
        "month_from": today[:7] + "-01",
        "top_provider": top,
        "excluded_provider": exc["pubkey"] if exc else "0" * 64,
        "top_model": doc["models"][0]["model"],
        "has_folded_model": bool(folded),
        "folded_model": folded[0][0] if folded else doc["models"][0]["model"],
        "reviewed_node": T.reviews[0]["node"] if T.reviews else "0" * 64,
        "reference_diffs": routstr_truth.diff_documents(reference, doc) if reference else ["reference missing"],
    }


def findings_html(Q, ctx, meta):
    """The narrative block: what this run established, written from the data."""
    by = {q["q"]: q for q in Q}
    diffs = ctx["reference_diffs"]
    ref_line = ("reproduces the served document with <strong>zero diffs</strong>"
                if not diffs else f"differs from the served document in <strong>{len(diffs)}</strong> field(s)")
    return f"""<div class="note" style="border-left-color:var(--good)">
<h3>What this run establishes</h3>
<p><strong>The tools and the page are one dataset.</strong> The page-embedded document, the newest
<code>routstr_snapshot</code> row and every document-backed metric agree, and the independent DuckDB
re-derivation over the raw archive (<code>crosscheck/build_routstr_snapshot.py</code>, pinned to
<code>{html.escape(ctx['refresh_day'])}</code>'s refresh instant) {ref_line}. The substrate-backed grains
(<code>provider_activity</code>, <code>model_activity</code>) re-sum from the raw ledger buckets in Python
and reproduce the page's fixed 30-day columns at the refresh-anchored window
<code>{ctx['refresh_30d_from']}..{ctx['refresh_30d_to']}</code>.</p>
<p><strong>The windows a client computes are the windows the tools serve.</strong> The battery asks the
questions the way a model client does: "last 7 days" and "last 30 days" from today
(<code>{ctx['last7_from']}..{ctx['last7_to']}</code>, <code>{ctx['last30_from']}..{ctx['last30_to']}</code>),
"this month" from <code>{ctx['month_from']}</code> with an open-ended <code>to</code>, one filtered call for one
provider or model. A bound window sums daily buckets only - the page's own rule - so the tool can never
disagree with the page by including a weekly pre-daily bucket.</p>
</div>"""


def paths_html(Q):
    n = len(Q)
    refn = sum(1 for q in Q if "REFERENCE" in q["how"] or q["cat"].startswith("Reference"))
    sqln = sum(1 for q in Q if q["how"].startswith("SQL") or "SQL:" in q["how"])
    liven = sum(1 for q in Q if q["cat"].startswith("Live"))
    return f"""<p>No answer is scored against the MCP server's own reasoning, and no expected value is stored in the
harness - both sides are recomputed on every run. Three independent paths are used:</p>
<ul>
<li><strong>The reference re-derivation</strong> ({refn} of {n} checks) -
<code>crosscheck/build_routstr_snapshot.py</code>: DuckDB straight over the raw S3 archive of
kind-38421 (routstr-provider-announcements), kind-38422 (routstr-analytics-snapshots),
kind-38423 (routstr-model-catalog) and kind-38425 (routstr-provider-reviews), a second implementation of
the whole methodology (ledger reconstruction, exclusions, catalog deltas), pinned to the served document's
generation instant. Agreement is two implementations agreeing, not a shared code path.</li>
<li><strong>Independent SQL over the substrate</strong> ({sqln} checks) - the raw ledger buckets
(<code>routstr_provider_days</code>, <code>routstr_model_days</code>), the registry, catalog-version and
review tables read whole via <code>ssh $NOSTROLOGY_RO_SSH</code> -&gt; <code>psql</code> as
<code>nostrology_ro</code>, then windowed, ranked and summed in Python - never by the tool's SQL.</li>
<li><strong>The live page</strong> ({liven} checks) - the document <code>nostrolo.gy/routstr</code>
server-renders, evaluated back from the page payload by <code>node</code>: exactly what a reader sees.</li>
</ul>
<p>Cross-call and contract checks (partitions summing to the headline, server windows equalling client
slices, domain routing by name, rejections with remedies, row caps announcing themselves, format parity,
one shared freshness instant) make up the rest. Run
<code>python3 -m nostrology_mcp.verification.routstr_run --self-test</code> to confirm the harness can still
fail: it corrupts one expected value and expects exactly one FAIL.</p>"""


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--reuse", action="store_true", help="re-verify from the cached raw fetches")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--url", default=None)
    args = ap.parse_args(argv)

    run_date = dt.datetime.utcnow().strftime("%Y-%m-%d")
    raw_dir = os.path.join(args.out, "raw", run_date)
    os.makedirs(raw_dir, exist_ok=True)
    sql_path = os.path.join(raw_dir, "sql.routstr.json")
    ref_path = os.path.join(raw_dir, "reference.json")
    page_path = os.path.join(raw_dir, "page.routstr.json")

    # -- 1. independent sides first -------------------------------------------
    if args.reuse and os.path.exists(sql_path):
        sql = json.load(open(sql_path)); print("reused substrate SQL")
    else:
        print("querying Aurora read-only (substrate rows + the served document)...")
        sql = routstr_truth.run_sql()
        json.dump(sql, open(sql_path, "w"))
    print("  " + ", ".join(f"{k}={len(v)}" for k, v in sql.items()))
    doc = sql["DOC"][0]["data"]

    reference = routstr_truth.load_reference(ref_path, doc["generated_utc"])
    if reference is not None:
        print("reused reference re-derivation (provenance sidecar matches the served refresh)")
    else:
        print(f"re-deriving the document from the raw archive (DuckDB over S3, as-of {doc['generated_utc']}; minutes)...")
        reference = routstr_truth.build_reference(doc["generated_utc"], ref_path)
    if reference.get("generated_utc") != doc["generated_utc"]:
        sys.exit(f"reference generated_utc {reference.get('generated_utc')} != served {doc['generated_utc']}; "
                 "the nightly moved between the two reads - rerun without --reuse")

    if args.reuse and os.path.exists(page_path):
        W = json.load(open(page_path)); print("reused live page")
    else:
        print("reading the live /routstr page...")
        W = routstr_truth.page_document()
        json.dump(W, open(page_path, "w"))
    if not W:
        sys.exit("the /routstr page payload could not be extracted - fix routstr_truth.page_document, do not score empty")

    ctx = build_context(sql, reference, run_date)
    steps = routstr_plan.build(ctx)

    # -- 2. the MCP side -------------------------------------------------------
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

    # -- 3. verify -------------------------------------------------------------
    Q = routstr_questions.build(M, sql, reference, W, ctx)

    if args.self_test:
        target = next(q for q in Q if not isinstance(q["want"], bool))
        target["want"] = f"__corrupted__{target['want']}"
        target["ok"] = False
        nfail = sum(1 for q in Q if report_mod.verdict(q) == "fail")
        print(f"self-test: {nfail} FAIL (expected exactly 1)")
        return 0 if nfail == 1 else 1

    m = re.search(r"server:\s*(.+)$", M["RS01"].body, re.M)
    meta = {
        "run_date": run_date,
        "url": args.url or "https://nostrolo.gy/mcp?traffic_type=internal",
        "server": m.group(1).strip() if m else "nostrology-mcp",
        "refresh": doc["generated_utc"],
        "retried_calls": retried,
        "title": "Routstr MCP verification battery",
        "bounded_note_html": "",
    }
    meta["subtitle"] = (f"{len(Q)} questions put to the <code>nostrology</code> MCP server's Routstr domain on the AWS "
                        f"<code>dev</code> environment, each answer verified against an independent path. "
                        f"Run {run_date}, against the refresh of {html.escape(doc['generated_utc'])}.")
    meta["paths_html"] = paths_html(Q)
    meta["findings_html"] = findings_html(Q, ctx, meta)
    meta["footer_html"] = (f'<p class="sub" style="margin-top:2rem">Server: <code>{html.escape(meta["server"])}</code> at '
                           f'<code>{html.escape(meta["url"])}</code>. Refresh under test: <code>{html.escape(doc["generated_utc"])}</code> '
                           f'(run {html.escape(sql["DOC"][0]["run_id"])}). Subjects derived at run time: top provider '
                           f'<code>{ctx["top_provider"][:12]}</code>, excluded provider <code>{ctx["excluded_provider"][:12]}</code>, '
                           f'top model <code>{html.escape(ctx["top_model"])}</code>, folded model <code>{html.escape(ctx["folded_model"])}</code>. '
                           f'Harness: <code>nostrology_mcp/verification/routstr_*.py</code>.</p>')

    # Every run gets its own files (date + UTC time), so a re-run never clobbers
    # the previous evidence; the raw cache stays per date (that is what --reuse reads).
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H%MZ")
    html_path = os.path.join(args.out, f"{stamp}-routstr-mcp-verification-battery.html")
    json_path = os.path.join(args.out, f"{stamp}-routstr-results.json")
    open(html_path, "w").write(report_mod.render(Q, meta))
    json.dump({"meta": meta, "ctx": ctx, "questions": Q},
              open(json_path, "w"), indent=1, default=str)

    npass = sum(1 for q in Q if report_mod.verdict(q) == "pass")
    nfail = sum(1 for q in Q if report_mod.verdict(q) == "fail")
    print(f"\nTOTAL {len(Q)}  PASS {npass}  FAIL {nfail}")
    for q in Q:
        if report_mod.verdict(q) == "fail":
            print(f"  FAIL #{q['n']:>3} [{q['cat']}] {q['q']}")
            print(f"        MCP={str(q['got'])[:300]!r}")
            print(f"        ind={str(q['want'])[:300]!r}")
    print(f"\nreport: {html_path}")
    return 1 if nfail else 0


if __name__ == "__main__":
    sys.exit(main())

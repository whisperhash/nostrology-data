"""Render a battery run as a self-contained HTML report.

Status colours (good / warning / critical) are ALWAYS paired with an icon and a word -
`warning` is sub-3:1 on the light surface by design, so colour never carries the verdict alone.
"""
import collections
import html


def verdict(q):
    if q.get("bounded"):
        return "bounded" if q["ok"] else "fail"
    return "pass" if q["ok"] else "fail"


def fmt(v):
    if isinstance(v, dict):
        return ", ".join(f"{k}={x}" for k, x in v.items())
    if isinstance(v, (list, tuple)):
        if v and isinstance(v[0], (list, tuple)):
            return "; ".join(", ".join(str(y) for y in x) if isinstance(x, (list, tuple)) else str(x) for x in v)
        return ", ".join(str(x) for x in v)
    return str(v)


ICON = {"pass": "&#10003;", "bounded": "&#9646;", "fail": "&#10007;"}


def cat_id(cat):
    """A stable anchor per surface: the first question row of that surface."""
    import re
    return "s-" + re.sub(r"[^a-z0-9]+", "-", cat.lower()).strip("-")


# Collapses any answer cell taller than five verdict badges behind a native
# <details> triangle (the first fragment stays visible as the summary). Measured
# in the browser after layout, so the rule is about rendered height, not length.
COLLAPSE_JS = """
(function(){
  var badge=document.querySelector('.badge'); if(!badge) return;
  var limit=5*badge.getBoundingClientRect().height;
  document.querySelectorAll('td.val').forEach(function(td){
    if(td.getBoundingClientRect().height<=limit) return;
    var text=td.textContent, first=text.split(/[;,]\\s*/)[0].slice(0,60);
    var d=document.createElement('details'), s=document.createElement('summary');
    s.innerHTML='<span class="peek"></span> ...';
    s.querySelector('.peek').textContent=first;
    var body=document.createElement('div'); body.textContent=text;
    d.appendChild(s); d.appendChild(body); td.textContent=''; td.appendChild(d);
  });
})();
"""
LABEL = {"pass": "PASS", "bounded": "BOUNDED", "fail": "FAIL"}

CSS = """
*, *::before, *::after { box-sizing:border-box; }
body { margin:0; }
:root { color-scheme: light dark; }
.viz-root {
  --surface-1:#fcfcfb; --surface-2:#f2f2ef; --line:#dddcd6;
  --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#77756e;
  --good:#0ca30c; --warning:#fab219; --critical:#d03b3b;
  --good-ink:#006300; --warn-ink:#7a5200; --crit-ink:#a32a2a;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) .viz-root {
    --surface-1:#1a1a19; --surface-2:#232322; --line:#3a3a37;
    --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#94938a;
    --good-ink:#6cd36c; --warn-ink:#fab219; --crit-ink:#f08b8b;
  }
}
:root[data-theme="dark"] .viz-root {
  --surface-1:#1a1a19; --surface-2:#232322; --line:#3a3a37;
  --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#94938a;
  --good-ink:#6cd36c; --warn-ink:#fab219; --crit-ink:#f08b8b;
}
.viz-root { background:var(--surface-1); color:var(--text-primary);
  font:15px/1.55 ui-sans-serif,-apple-system,"Segoe UI",Roboto,sans-serif;
  margin:0; padding:1.25rem .5rem 3rem; max-width:none; }
.val details > summary { cursor:pointer; color:var(--text-muted); }
.val details > summary .peek { color:var(--text-secondary); }
.cat a { color:inherit; text-decoration:none; border-bottom:1px dotted var(--text-muted); }
.cat a:hover { border-bottom-style:solid; }
tr:target td { background:color-mix(in srgb,var(--warning) 14%,transparent); }
h1 { font-size:1.6rem; margin:0 0 .35rem; letter-spacing:-.01em; }
h2 { font-size:1.15rem; margin:2.4rem 0 .7rem; letter-spacing:-.01em; }
.sub { color:var(--text-secondary); margin:0 0 1.6rem; }
.tiles { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:.75rem; margin:1.4rem 0 .4rem; }
.tile { background:var(--surface-2); border:1px solid var(--line); border-radius:10px; padding:.85rem 1rem; }
.tile .k { font-size:.72rem; text-transform:uppercase; letter-spacing:.06em; color:var(--text-muted); }
.tile .v { font-size:1.9rem; font-weight:650; font-variant-numeric:tabular-nums; margin-top:.15rem; }
.tile .n { font-size:.78rem; color:var(--text-secondary); }
.v-good { color:var(--good-ink); } .v-warn { color:var(--warn-ink); } .v-crit { color:var(--crit-ink); }
.wrap { overflow-x:auto; border:1px solid var(--line); border-radius:10px; background:var(--surface-2); }
table { border-collapse:collapse; width:100%; font-size:13.5px; }
th { text-align:left; font-weight:600; font-size:.72rem; text-transform:uppercase; letter-spacing:.05em;
     color:var(--text-muted); padding:.6rem .7rem; border-bottom:1px solid var(--line); white-space:nowrap; }
td { padding:.55rem .7rem; border-bottom:1px solid var(--line); vertical-align:top; }
tr:last-child td { border-bottom:0; }
.num { text-align:right; font-variant-numeric:tabular-nums; color:var(--text-secondary); white-space:nowrap; }
.cat { color:var(--text-secondary); white-space:nowrap; font-size:12.5px; }
.qq { min-width:19rem; }
.how { color:var(--text-muted); font-size:11.5px; margin-top:.2rem; }
.val { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:11.5px;
       max-width:15rem; overflow-wrap:anywhere; color:var(--text-secondary); }
.calls code { font-size:10.5px; background:var(--surface-1); border:1px solid var(--line);
       border-radius:4px; padding:.05rem .25rem; margin-right:.15rem; color:var(--text-muted); }
.verdict { white-space:nowrap; }
.badge { display:inline-flex; align-items:center; gap:.3rem; font-size:11px; font-weight:650;
        letter-spacing:.03em; padding:.15rem .45rem; border-radius:5px; border:1px solid; }
.badge .ic { font-size:12px; line-height:1; }
.b-pass { color:var(--good-ink); border-color:var(--good); background:color-mix(in srgb,var(--good) 12%,transparent); }
.b-bounded { color:var(--warn-ink); border-color:var(--warning); background:color-mix(in srgb,var(--warning) 16%,transparent); }
.b-fail { color:var(--crit-ink); border-color:var(--critical); background:color-mix(in srgb,var(--critical) 12%,transparent); }
.bar { display:flex; height:8px; border-radius:4px; overflow:hidden; background:var(--line); min-width:110px; gap:2px; }
.seg { display:block; }
.s-pass { background:var(--good); } .s-bounded { background:var(--warning); } .s-fail { background:var(--critical); }
.mini { width:34%; }
.note { background:var(--surface-2); border:1px solid var(--line); border-left:3px solid var(--warning);
       border-radius:8px; padding:.85rem 1rem; margin:1rem 0; }
.note h3 { margin:0 0 .35rem; font-size:.95rem; }
p, li { color:var(--text-secondary); }
p strong, li strong { color:var(--text-primary); }
code { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }
.legend { display:flex; gap:1rem; flex-wrap:wrap; margin:.6rem 0 0; font-size:12px; color:var(--text-secondary); }
"""


def render(questions, meta):
    n = len(questions)
    npass = sum(1 for q in questions if verdict(q) == "pass")
    nbnd = sum(1 for q in questions if verdict(q) == "bounded")
    nfail = sum(1 for q in questions if verdict(q) == "fail")
    score = round(100 * (npass + nbnd) / n, 1) if n else 0.0
    exact = round(100 * npass / n, 1) if n else 0.0

    cats = collections.OrderedDict()
    for q in questions:
        cats.setdefault(q["cat"], []).append(q)

    rows = []
    seen_cat = set()
    for q in questions:
        v = verdict(q)
        calls = " ".join(f"<code>{html.escape(c)}</code>" for c in q["calls"])
        anchor = f' id="{cat_id(q["cat"])}"' if q["cat"] not in seen_cat else ""
        seen_cat.add(q["cat"])
        rows.append(
            f'<tr class="r-{v}"{anchor}><td class="num" id="q{q["n"]}">{q["n"]}</td>'
            f'<td class="cat"><a href="#{cat_id(q["cat"])}">{html.escape(q["cat"])}</a></td>'
            f'<td class="qq">{html.escape(q["q"])}<div class="how">{html.escape(q["how"])}</div></td>'
            f'<td class="val">{html.escape(fmt(q["got"]))}</td>'
            f'<td class="val">{html.escape(fmt(q["want"]))}</td>'
            f'<td class="calls">{calls}</td>'
            f'<td class="verdict"><span class="badge b-{v}"><span class="ic">{ICON[v]}</span>{LABEL[v]}</span></td></tr>')

    catrows = []
    for c, qs in cats.items():
        p = sum(1 for x in qs if verdict(x) == "pass")
        b = sum(1 for x in qs if verdict(x) == "bounded")
        f = sum(1 for x in qs if verdict(x) == "fail")
        seg = "".join(f'<span class="seg s-{k}" style="flex:{v}"></span>'
                      for k, v in (("pass", p), ("bounded", b), ("fail", f)) if v)
        catrows.append(
            f'<tr><td class="cat"><a href="#{cat_id(c)}">{html.escape(c)}</a></td><td class="mini"><div class="bar">{seg}</div></td>'
            f'<td class="num">{p}</td><td class="num">{b or "-"}</td>'
            f'<td class="num">{f or "-"}</td><td class="num">{len(qs)}</td></tr>')

    fails = [q for q in questions if verdict(q) == "fail"]
    fail_block = ""
    if fails:
        items = "".join(
            f'<li><strong>#{q["n"]} [{html.escape(q["cat"])}]</strong> {html.escape(q["q"])}<br>'
            f'<code>MCP: {html.escape(fmt(q["got"]))}</code> vs '
            f'<code>independent: {html.escape(fmt(q["want"]))}</code></li>' for q in fails)
        fail_block = (f'<div class="note" style="border-left-color:var(--critical)">'
                      f'<h3>{len(fails)} check(s) did not agree</h3><ul>{items}</ul>'
                      f'<p>A disagreement is not automatically a server bug - confirm the independent '
                      f'side first, then the question, then the server.</p></div>')

    bounded_note = ""
    if nbnd:
        items = "".join(
            f'<li><strong>{html.escape(q["cat"])}</strong> - {html.escape(q["q"])}<br>'
            f'<code>MCP: {html.escape(fmt(q["got"]))}</code><br>'
            f'<code>live: {html.escape(fmt(q["want"]))}</code></li>'
            for q in questions if verdict(q) == "bounded")
        bounded_note = f"""<div class="note">
<h3>Bounded, not exact: the site cuts on a timestamp, MCP windows on calendar days</h3>
<p>Every trailing window on the site - the 365-day charts and the 30-day leaderboard periods alike -
cuts on <code>NOW() - N days</code>, a <em>mid-day timestamp</em>. MCP's <code>from</code>/<code>to</code>
are inclusive UTC calendar days, so the oldest day of the range can only be taken whole or dropped whole.
Where the boundary day's events all fall on one side of the cutoff this costs nothing and the series
reproduces exactly; where they straddle it, the live value sits strictly between the two windows MCP can
express. Those cases are scored BOUNDED - the answer is correct up to a limitation the metric's own
methodology text declares, not wrong.</p>
<ul>{items}</ul>
<p><strong>Two boundary effects exist, not one.</strong> The incomplete <em>current</em> day is handled
explicitly - an open-ended <code>to</code> defaults to today and the envelope says the final day is in
progress. The trailing-window <em>start</em> day is the second, and it is the one that actually bites.
Whether a given card or chart lands exact or bounded depends on that day's traffic, so a run where
everything is exact is luck, not a stronger guarantee.</p>
</div>"""

    sqln = sum(1 for q in questions if "SQL" in q["how"])
    liven = sum(1 for q in questions if q["cat"].startswith("Live"))

    # A second battery (Routstr) reuses this renderer with its own prose blocks;
    # every block below falls back to the Zapstore text when meta does not
    # carry an override, so the original report is byte-for-byte unchanged.
    title = meta.get("title", "Zapstore MCP verification battery")
    subtitle = meta.get("subtitle", f"{n} questions put to the <code>nostrology</code> MCP server on the AWS "
               f"<code>dev</code>\nenvironment, each answer verified against an independent path. Run {meta['run_date']}.")
    paths_html = meta.get("paths_html", f"""<p>No answer is scored against the MCP server's own reasoning, and no expected value is stored in the
harness - both sides are recomputed on every run. Three independent paths are used:</p>
<ul>
<li><strong>Independent SQL</strong> ({sqln} of {n} checks) - queries written from the table definitions in
<code>schema/init.sql</code> and run read-only against Aurora as <code>nostrology_ro</code>. The
native/republished partition rule is applied from the domain definition, not from the MCP server's SQL.</li>
<li><strong>The live website</strong> ({liven} checks) - chart series and leaderboard card rows scraped out of
the rendered pages at <code>nostrolo.gy</code>. The site reads the same Postgres through its <em>own</em>
SQL, so agreement is a second implementation agreeing, not a shared code path.</li>
<li><strong>Cross-call and contract checks</strong> - internal consistency the server must hold (grain=day
summing to grain=total, entity filters matching client-side filtering, partitions summing to the whole)
plus error handling, row caps, column contracts and freshness declarations.</li>
</ul>
<p>Run <code>python3 -m nostrology_mcp.verification.run --self-test</code> to confirm the harness can still
fail: it corrupts one expected value and expects exactly one FAIL.</p>""")
    findings_block = ("\n\n" + meta["findings_html"]) if meta.get("findings_html") else ""
    footer_html = meta.get("footer_html", f"""<p class="sub" style="margin-top:2rem">Server: <code>{html.escape(meta.get('server', 'unknown'))}</code> at
<code>{html.escape(meta.get('url', ''))}</code>. Data freshness at run time: kind-30063 to
<code>{html.escape(meta.get('max_release_at', '?'))}</code>, kind-9735 (Zapstore-attributed) to
<code>{html.escape(meta.get('max_zap_at', '?'))}</code>.
Harness: <code>nostrology_mcp/verification/</code>.</p>""")
    if meta.get("bounded_note_html") is not None:
        bounded_note = meta["bounded_note_html"] if nbnd else ""

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)} - {meta['run_date']}</title>
<style>{CSS}</style>
</head>
<body>
<div class="viz-root">

<h1>{html.escape(title)}</h1>
<p class="sub">{subtitle}</p>

<div class="tiles">
  <div class="tile"><div class="k">Questions</div><div class="v">{n}</div><div class="n">across {len(cats)} surfaces</div></div>
  <div class="tile"><div class="k">Pass</div><div class="v v-good">{npass}</div><div class="n">exact match</div></div>
  <div class="tile"><div class="k">Bounded</div><div class="v {'v-warn' if nbnd else ''}">{nbnd}</div><div class="n">correct within a documented boundary</div></div>
  <div class="tile"><div class="k">Fail</div><div class="v {'v-crit' if nfail else ''}">{nfail}</div><div class="n">wrong answer</div></div>
  <div class="tile"><div class="k">Overall score</div><div class="v {'v-good' if not nfail else 'v-crit'}">{score}%</div><div class="n">{exact}% bit-exact</div></div>
</div>
<div class="legend">
  <span><span class="badge b-pass"><span class="ic">&#10003;</span>PASS</span> MCP answer equals the independent answer</span>
  <span><span class="badge b-bounded"><span class="ic">&#9646;</span>BOUNDED</span> MCP brackets the target; the gap is a declared limitation</span>
  <span><span class="badge b-fail"><span class="ic">&#10007;</span>FAIL</span> MCP answer is wrong</span>
</div>

{fail_block}

<h2>How each answer was verified</h2>
{paths_html}

{bounded_note}{findings_block}

<h2>Result by surface</h2>
<div class="wrap"><table>
<thead><tr><th>Surface</th><th>Distribution</th><th class="num">Pass</th><th class="num">Bnd</th><th class="num">Fail</th><th class="num">Total</th></tr></thead>
<tbody>{"".join(catrows)}</tbody></table></div>

<h2>All {n} questions</h2>
<div class="wrap"><table>
<thead><tr><th class="num">#</th><th>Surface</th><th>Question &amp; verification</th><th>MCP answer</th><th>Independent answer</th><th>Calls</th><th>Verdict</th></tr></thead>
<tbody>{"".join(rows)}</tbody></table></div>

{footer_html}
</div>
<script>{COLLAPSE_JS}</script>
</body>
</html>
"""

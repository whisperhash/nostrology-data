"""The third answer: the live nostrology-web pages.

The site reads the same Postgres through its OWN SQL (`nostrology-web/src/lib/db/
postgres.ts`), so when a scraped chart endpoint agrees with an MCP-derived one,
that is a second implementation agreeing - not a shared code path. This module
only extracts; it asserts nothing.

Two shapes are parsed:
  * chart series - SvelteKit inlines them as `{timestamp:...,value:...}` literals
  * leaderboard cards - server-rendered <table> rows

Both are layout-coupled by nature. `run.py` treats an extraction that yields
nothing as a harness failure, not as a passing comparison of empty against empty.
"""
import datetime as dt
import html
import re
import subprocess

BASE = "https://nostrolo.gy"


def fetch(path, timeout=180):
    # The leaderboards page server-renders ~1.8 MB of table rows, so this is a
    # slow fetch by design rather than a hung one - keep the timeout generous.
    proc = subprocess.run(["curl", "-sS", "--compressed", "--max-time", str(timeout), BASE + path],
                          capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"fetch {path} failed: {proc.stderr[-300:]}")
    return proc.stdout


def _balanced_array(doc, start):
    depth = 0
    for k in range(start, len(doc)):
        if doc[k] == "[":
            depth += 1
        elif doc[k] == "]":
            depth -= 1
            if depth == 0:
                return doc[start:k + 1]
    raise ValueError("unbalanced array literal")


def _points(text):
    return [(int(a), float(b))
            for a, b in re.findall(r"timestamp:(\d+),value:(-?[\d.]+)", text)]


def _day(ts):
    return dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")


def series(doc, key, within=None):
    """Extract a `key:[{timestamp,value}...]` series, optionally inside `within`."""
    scope = doc
    if within:
        i = doc.find(within + ":{")
        if i < 0:
            raise ValueError(f"container {within} not found")
        scope = doc[i:]
    j = scope.find(key + ":[")
    if j < 0:
        raise ValueError(f"series {key} not found")
    pts = _points(_balanced_array(scope, scope.index("[", j)))
    if not pts:
        raise ValueError(f"series {key} parsed empty")
    return [(_day(t), v) for t, v in pts]


def cards(doc):
    """title -> list of rendered <td> cell lists, for the leaderboard cards."""
    def txt(t):
        return html.unescape(re.sub("<[^>]+>", "", t)).strip()

    out = {}
    for chunk in re.split(r"<h3[^>]*>", doc)[1:]:
        title = txt(chunk.split("</h3>")[0])
        rows = []
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", chunk.split("</table>")[0], re.S):
            cells = [txt(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
            if len(cells) >= 3:
                rows.append(cells)
        if rows:
            out[title] = rows
    return out


def _int(s):
    return int(re.sub(r"[^\d-]", "", s))


def collect_routstr():
    """The /routstr page's headline figures, read from the snapshot document the
    page SSR-embeds (SvelteKit serializes the load data into the HTML), so the
    numbers are exactly what the page renders after its own msats->sats step."""
    page = fetch("/routstr")

    def num(key):
        m = re.search(key + r":(\d+)", page)
        return int(m.group(1)) if m else None

    m = re.search(r'generated_utc:"([^"]+)"', page)
    return {
        "routstr_generated_utc": m.group(1) if m else None,
        "routstr_providers_announced": num("providers_announced"),
        "routstr_providers_reporting": num("providers_reporting"),
        "routstr_providers_active_30d": num("providers_active_30d"),
        "routstr_revenue_30d_sats": num("revenue_30d_sats"),
        "routstr_revenue_alltime_sats": num("revenue_alltime_sats"),
        "routstr_requests_30d": num("requests_30d"),
        "routstr_tokens_30d": num("tokens_30d"),
    }


def collect(dev_pubkey, dev_app_id):
    """Fetch every page the battery compares against and return parsed values."""
    overview = fetch("/zapstore")
    leaderboards = fetch("/zapstore/leaderboards")
    app_page = fetch(f"/zapstore/apps/{dev_pubkey}/{dev_app_id}")
    pub_page = fetch(f"/zapstore/publishers/{dev_pubkey}")

    card = cards(leaderboards)
    plain = re.sub(r"\s+", " ", html.unescape(re.sub("<[^>]+>", " ", pub_page)))
    app_plain = re.sub(r"\s+", " ", html.unescape(re.sub("<[^>]+>", " ", app_page)))

    def find(rx, doc, cast=_int):
        m = re.search(rx, doc)
        return cast(m.group(1)) if m else None

    cum_total = series(overview, "total")
    return {
        # -- chart 1: cumulative apps (total / native / republished)
        "cum_total": cum_total,
        "cum_native": series(overview, "native"),
        "cum_github": series(overview, "github"),
        # -- chart 2: native share
        "native_share": series(overview, "daily", within="nativeShare"),
        # -- chart 3: daily releases
        "rel_native": series(overview, "native", within="releasesDaily"),
        "rel_new_native": series(overview, "newNative", within="releasesDaily"),
        "rel_github": series(overview, "github", within="releasesDaily"),
        # -- chart 4: zaps
        "zaps_daily": series(overview, "daily", within="aggZaps"),
        "zaps_cumulative": series(overview, "cumulative", within="aggZaps"),
        # the charted window, read off the data rather than assumed
        "window_start": cum_total[0][0],
        "window_end": cum_total[-1][0],
        # -- leaderboard cards (rows 1..4 of each)
        "card_stacks": [(r[1], _int(r[2])) for r in card["App Stack Appearances"][:4]],
        "card_pub_apps": [_int(r[2]) for r in card["Total Apps"][:4]],
        "card_app_zaps": [(r[1], _int(r[2]), _int(r[3])) for r in card["Zaps"][:4]],
        "card_pub_zaps": [(r[1], _int(r[2]), _int(r[3])) for r in card["Total Zaps"][:4]],
        # -- detail pages
        "app_releases_total": find(r"([\d,]+) total releases?\.", app_plain),
        "app_first_release": find(r"First release: ([A-Z][a-z]{2} \d+, \d{4})", app_plain, str),
        "app_last_release": find(r"Most recent release: ([A-Z][a-z]{2} \d+, \d{4})", app_plain, str),
        "app_sats": find(r"([\d,]+) sats", app_plain),
        "pub_app_count": find(r"([\d,]+) apps? published\.", plain),
        "pub_zaps_1y": find(r"([\d,]+) Zapstore-attributed zaps in the past year\.", plain),
        "pub_sats_1y": find(r"([\d,]+) sats received in the past year\.", plain),
    }


def us_date_to_iso(s):
    return dt.datetime.strptime(s, "%b %d, %Y").strftime("%Y-%m-%d") if s else None

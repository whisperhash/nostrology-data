"""The independent sides of the Routstr battery.

Three sides, none of which is the MCP server's own SQL:

1. **The reference re-derivation** - `crosscheck/build_routstr_snapshot.py`,
   DuckDB straight over the raw S3 archive of kind-38421
   (routstr-provider-announcements) / kind-38422 (routstr-analytics-snapshots) /
   kind-38423 (routstr-model-catalog) / kind-38425 (routstr-provider-reviews),
   pinned to the served document's generation instant. It is a SECOND
   IMPLEMENTATION of the whole methodology (the Zapstore battery's analogue is
   the website's own SQL), so agreement is evidence rather than a shared code
   path. It is slow (minutes) and needs read access to the archive (AWS_PROFILE),
   so its output is cached beside the run's raw fetches.
2. **Independent SQL over the substrate tables** - the RAW ledger rows
   (`routstr_provider_days`, `routstr_model_days`) plus the registry, catalog
   and review tables, fetched whole and sliced in Python. Every window, sort and
   sum the questions need is recomputed here, never by the tool's SQL.
3. **The live page** - the document `nostrolo.gy/routstr` server-renders
   (SvelteKit serializes the load data into the HTML as a JS object literal;
   `node` evaluates it back), i.e. exactly what a reader of the page sees.
"""
import datetime as dt
import json
import os
import re
import subprocess
from collections import defaultdict

from . import truth_sql, web_truth

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
REFERENCE_PY = os.path.join(REPO_ROOT, ".venv-crosscheck", "bin", "python3")
REFERENCE_SCRIPT = os.path.join(REPO_ROOT, "crosscheck", "build_routstr_snapshot.py")


def _aws_profile():
    """The profile that reads the archive: AWS_PROFILE, read when the reference runs (which also
    needs NOZZL_ARCHIVE_BUCKET, and reads that itself)."""
    profile = os.environ.get("AWS_PROFILE", "").strip()
    if not profile:
        raise RuntimeError("set AWS_PROFILE to a profile that can read the nozzl archive")
    return profile


# ---------------------------------------------------------------------------
# 2. SQL over the substrate (raw rows; sliced in Python by the questions)
# ---------------------------------------------------------------------------
SECTIONS = {
    "DOC": """
        select data, run_id, to_char(generated_utc at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') generated
        from routstr_snapshot order by generated_utc desc, id desc limit 1
    """,
    "PROVIDERS": """
        select pubkey, provider_id, name, endpoint_class, version, announced, review,
               excluded, excl_reason,
               to_char(first_seen_utc at time zone 'UTC', 'YYYY-MM-DD') first_seen,
               to_char(last_announce_utc at time zone 'UTC', 'YYYY-MM-DD') last_announce,
               to_char(last_report_utc at time zone 'UTC', 'YYYY-MM-DD') last_report,
               msats_alltime, msats_30d, requests_30d, tokens_30d, requests_alltime, tokens_alltime,
               jsonb_array_length(urls) n_urls, jsonb_array_length(mints) n_mints, mints
        from routstr_providers order by msats_alltime desc, pubkey
    """,
    "PROVIDER_DAYS": """
        select pubkey, grain, day::text as day, msats, requests, tokens
        from routstr_provider_days order by pubkey, day, grain
    """,
    "MODEL_DAYS": """
        select model, grain, day::text as day, msats, requests, tokens, providers
        from routstr_model_days order by model, day, grain
    """,
    "CATALOG_VERSIONS": """
        select event_id, to_char(version_utc at time zone 'UTC', 'YYYY-MM-DD') as day,
               model_count, models, added, removed
        from routstr_catalog_versions order by version_utc desc, event_id desc
    """,
    "REVIEWS": """
        select event_id, node, label, to_char(reviewed_utc at time zone 'UTC', 'YYYY-MM-DD') as day,
               extract(epoch from reviewed_utc)::bigint as at, reviewer_pubkey, curated
        from routstr_reviews order by reviewed_utc desc, node, event_id
    """,
}


def run_sql():
    return truth_sql.run_sections(SECTIONS)


# ---------------------------------------------------------------------------
# 1. The reference re-derivation (DuckDB over S3, crosscheck/)
# ---------------------------------------------------------------------------
def build_reference(generated_utc, out_path, timeout=3600):
    """Run the generator pinned to the served document's instant, BOTH as the
    created_at horizon (--as-of) and as the archive-state cutoff
    (--ingested-before): rows the archive received after the production run
    built its document are dropped, so the comparison survives the 12h chunk
    roll. `generated_utc` is the document's own header ('YYYY-MM-DD HH:MM:SSZ')."""
    as_of = generated_utc.replace(" ", "T").rstrip("Z")
    creds = subprocess.run(
        ["aws", "configure", "export-credentials", "--profile", _aws_profile(), "--format", "env"],
        capture_output=True, text=True, check=True).stdout
    env = dict(os.environ)
    for line in creds.splitlines():
        if line.startswith("export "):
            k, _, v = line[len("export "):].partition("=")
            env[k] = v
    proc = subprocess.run([REFERENCE_PY, REFERENCE_SCRIPT, "--as-of", as_of, "--ingested-before", as_of,
                           "--out", out_path],
                          capture_output=True, text=True, env=env, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"reference generator failed: {proc.stderr[-800:]}")
    sha = subprocess.run(["git", "-C", REPO_ROOT, "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    json.dump({"as_of": as_of, "ingested_before": as_of, "generator_sha": sha,
               "built_at": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")},
              open(out_path + ".meta.json", "w"), indent=1)
    return json.load(open(out_path))


def load_reference(out_path, generated_utc):
    """A cached reference is trusted only with its provenance sidecar naming the
    same instant for both pins - never a hand-placed or unpinned file."""
    meta_path = out_path + ".meta.json"
    if not (os.path.exists(out_path) and os.path.exists(meta_path)):
        return None
    meta = json.load(open(meta_path))
    as_of = generated_utc.replace(" ", "T").rstrip("Z")
    if meta.get("as_of") != as_of or meta.get("ingested_before") != as_of:
        return None
    ref = json.load(open(out_path))
    return ref if ref.get("generated_utc") == generated_utc else None


def diff_documents(a, b, path="$"):
    """crosscheck/compare_routstr_snapshots.py's rule, restated: exact equality field by field,
    int/float identity tolerated for equal numerics."""
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append(f"{path}.{k}: only in served")
            elif k not in b:
                out.append(f"{path}.{k}: only in reference")
            else:
                out += diff_documents(a[k], b[k], f"{path}.{k}")
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append(f"{path}: length {len(a)} vs {len(b)}")
        for i, (x, y) in enumerate(zip(a, b)):
            out += diff_documents(x, y, f"{path}[{i}]")
    elif a != b and not (isinstance(a, (int, float)) and isinstance(b, (int, float)) and a == b):
        out.append(f"{path}: {a!r} vs {b!r}")
    return out


# ---------------------------------------------------------------------------
# 3. The live page's embedded document
# ---------------------------------------------------------------------------
def _balanced_object(text, start):
    """The JS object literal starting at text[start] == '{', string-aware."""
    depth, i, in_str, esc = 0, start, None, False
    while i < len(text):
        c = text[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == in_str:
                in_str = None
        elif c in "\"'":
            in_str = c
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
        i += 1
    raise ValueError("unbalanced object literal")


def page_document(html=None):
    """The snapshot document exactly as the /routstr page received it. The
    SvelteKit payload is a JS literal (unquoted keys, `.9721`), so `node`
    evaluates it rather than a regex approximating JSON."""
    html = html if html is not None else web_truth.fetch("/routstr")
    m = re.search(r"\{daily:\[", html)
    if not m:
        return None
    literal = _balanced_object(html, m.start())
    proc = subprocess.run(
        ["node", "-e", "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{"
                       "process.stdout.write(JSON.stringify((0,eval)('('+s+')')))})"],
        input=literal, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"node could not evaluate the page payload: {proc.stderr[-300:]}")
    return json.loads(proc.stdout)


# ---------------------------------------------------------------------------
# Views the questions slice
# ---------------------------------------------------------------------------
def shift(day, n):
    return (dt.date.fromisoformat(day) + dt.timedelta(days=n)).isoformat()


class Truth:
    """Python views over the SQL sections. Every method recomputes from the raw
    substrate rows - the tool's windowed SUMs are never consulted."""

    def __init__(self, sql):
        self.doc = sql["DOC"][0]["data"]
        self.doc_generated = sql["DOC"][0]["generated"]
        self.providers = {r["pubkey"]: r for r in sql["PROVIDERS"]}
        self.pdays = sql["PROVIDER_DAYS"]
        self.mdays = sql["MODEL_DAYS"]
        self.catalog_versions = sql["CATALOG_VERSIONS"]
        self.reviews = sql["REVIEWS"]

    # -- registry -----------------------------------------------------------
    @property
    def included(self):
        return {pk for pk, p in self.providers.items() if not p["excluded"]}

    def by_prefix(self, prefix):
        hits = [pk for pk in self.providers if pk.startswith(prefix)]
        return hits[0] if len(hits) == 1 else None

    # -- provider ledger ----------------------------------------------------
    def provider_rows(self, frm=None, to=None, pubkey=None, daily_only=None):
        """Raw provider buckets in a window. A BOUND window counts daily
        buckets only (the page's own windowed rule); unbounded counts both."""
        if daily_only is None:
            daily_only = frm is not None or to is not None
        return [r for r in self.pdays
                if (pubkey is None or r["pubkey"] == pubkey)
                and (frm is None or r["day"] >= frm) and (to is None or r["day"] <= to)
                and (not daily_only or r["grain"] == "d")]

    def provider_totals(self, frm=None, to=None, include_excluded=False):
        """pubkey -> (msats, requests, tokens, daily, weekly, first, last)."""
        out = {}
        for r in self.provider_rows(frm, to):
            if not include_excluded and r["pubkey"] not in self.included:
                continue
            t = out.setdefault(r["pubkey"], [0, 0, 0, 0, 0, r["day"], r["day"]])
            t[0] += r["msats"]; t[1] += r["requests"]; t[2] += r["tokens"]
            t[3 if r["grain"] == "d" else 4] += 1
            t[5] = min(t[5], r["day"]); t[6] = max(t[6], r["day"])
        return {k: tuple(v) for k, v in out.items()}

    def network_days(self):
        """day -> (msats, requests, tokens, distinct included providers with a
        daily bucket that day), daily grain, included providers only - the
        network_activity contract (a provider that reported a zero day counts
        as present, as in the pipeline's day_net rule)."""
        out = {}
        for r in self.pdays:
            if r["grain"] != "d" or r["pubkey"] not in self.included:
                continue
            t = out.setdefault(r["day"], [0, 0, 0, set()])
            t[0] += r["msats"]; t[1] += r["requests"]; t[2] += r["tokens"]
            t[3].add(r["pubkey"])
        if not out:
            return {}
        # the pipeline zero-fills the series between the first and last daily
        # day (a gap is a floor, not a missing row), so the view is dense too
        d, last = min(out), max(out)
        dense = {}
        while d <= last:
            v = out.get(d)
            dense[d] = (v[0], v[1], v[2], len(v[3])) if v else (0, 0, 0, 0)
            d = shift(d, 1)
        return dense

    def network_sum(self, frm=None, to=None):
        rows = [v for d, v in self.network_days().items()
                if (frm is None or d >= frm) and (to is None or d <= to)]
        return (sum(v[0] for v in rows), sum(v[1] for v in rows), sum(v[2] for v in rows))

    # -- model ledger -------------------------------------------------------
    def model_rows(self, frm=None, to=None, model=None, daily_only=None):
        if daily_only is None:
            daily_only = frm is not None or to is not None
        return [r for r in self.mdays
                if (model is None or r["model"] == model)
                and (frm is None or r["day"] >= frm) and (to is None or r["day"] <= to)
                and (not daily_only or r["grain"] == "d")]

    def model_totals(self, frm=None, to=None):
        out = {}
        for r in self.model_rows(frm, to):
            t = out.setdefault(r["model"], [0, 0, 0, 0, 0])
            t[0] += r["msats"]; t[1] += r["requests"]; t[2] += r["tokens"]
            t[3 if r["grain"] == "d" else 4] += 1
        return {k: tuple(v) for k, v in out.items()}


def ranked(totals, key=0):
    """[(entity, value)] sorted by the chosen measure desc, then entity asc."""
    return sorted(((k, v[key]) for k, v in totals.items()), key=lambda kv: (-kv[1], kv[0]))


def group_sum(rows, key, fields):
    out = defaultdict(lambda: [0] * len(fields))
    for r in rows:
        t = out[r[key]]
        for i, f in enumerate(fields):
            t[i] += r[f]
    return {k: tuple(v) for k, v in out.items()}

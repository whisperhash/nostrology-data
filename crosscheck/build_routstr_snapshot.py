#!/usr/bin/env python3
"""
Build the /routstr dataset from the Nozzl S3 archive - the INDEPENDENT re-derivation
of the metrics that nostrology-data's routstr_refresh Dagster job computes in
production (dagster_code/pipeline/routstr.py, daily; the page serves the newest
routstr_snapshot row). This script's job is the agreement gate: re-derive the snapshot
here, over the raw archive via DuckDB, and diff it against the production row with
crosscheck/compare_routstr_snapshots.py (ZERO diffs required; full procedure in that
script's header). A methodology change lands in BOTH implementations or the gate fails.

    (the venv: crosscheck/requirements.in, on a Python 3.14 with a maintained certificate store. AWS_PROFILE names a profile
    that can read the archive, NOZZL_ARCHIVE_BUCKET the archive's bucket.)
    eval "$(aws configure export-credentials --profile "$AWS_PROFILE" --format env)"
    .venv-crosscheck/bin/python -I crosscheck/build_routstr_snapshot.py \
        --as-of <the production row's generated_utc, as YYYY-MM-DDTHH:MM:SS> \
        --ingested-before <the same instant> \
        --out static.json

`--ingested-before` pins the ARCHIVE STATE (rows ingested later are dropped), so the
gate reproduces a past run even after the 12h chunk roll has landed newer events;
without it only a same-chunk-window run agrees (2026-08-30 lesson: 35 diffs, all
input drift, on a reference built 13h after the run).

SOURCES (all addressable, ALL versions kept by the archive)
    kind-38421 (routstr-provider-announcements)  s3://$NOZZL_ARCHIVE_BUCKET/c=addressable/k=38421/
    kind-38422 (routstr-analytics-snapshots)     .../k=38422/  (reduced to the LAST snapshot
                per (pubkey, UTC day) carrying the routstr.analytics.snapshot.v1 schema tag -
                the same two SQL-side filters as production's extract template)
    kind-38423 (routstr-model-catalog)           .../k=38423/
    kind-38425 (routstr-provider-reviews)        .../k=38425/

METHODOLOGY - the full statement lives in docs/dataset-reference/routstr-dataset-reference.md; the
engineering rules this file must share exactly with the production compute:
  * horizon: events dated past as_of+2d are dropped before everything;
  * a GENUINE announcement carries >= 1 valued `u` tag and a valued `d` tag
    (kind-38421 (routstr-provider-announcements) is squatted - lnproxy fee quotes
    live on it);
  * the ledger merges the snapshots' trailing-window buckets (daily buckets from the
    30d/3m windows, weekly from 1y) under FULL-COVERAGE-ONLY (a bucket wholly inside
    [generated_at - hours_back*3600, generated_at]) and NEWEST-WINS (max (created_at,
    id) snapshot; within one snapshot 30d beats 3m), on the UTC-midnight / epoch-week
    grid, integer msats (round half away from zero);
  * weekly rows only for a provider's pre-daily era (week fully before its earliest
    daily-covered date);
  * generated_at is trusted for window math only within a day of created_at;
  * exclusion buckets, precedence-ordered: test-marked (name/host-label test|staging,
    TLD exempt), private-endpoint-only; kind squatters are a registry-side ledger entry.
"""
import argparse
import collections
import datetime as dt
import json
import math
import os
import re
import ssl
import sys
from urllib.parse import urlsplit

import duckdb

# DuckDB's httpfs reads none of Python's TLS settings: it verifies against the first existing path
# in its own short list of well-known bundles, which on macOS is only the OS's static bundle (still
# holding two roots compromised in 2025), and it ignores SSL_CERT_FILE. On macOS, point it at the venv
# interpreter's own default store instead, and refuse an interpreter whose default is missing or is
# that same static file: a bare python3 from PATH may be such a build. Elsewhere the list finds the
# distribution's maintained bundle, and an empty ca_cert_file keeps that default.
CA_FILE = ssl.get_default_verify_paths().openssl_cafile if sys.platform == "darwin" else ""
APPLE_STATIC = os.path.realpath(CA_FILE).endswith("/etc/ssl/cert.pem") if CA_FILE else False  # tls-posture-ok: the refusal names the file it refuses
if CA_FILE and (not os.path.isfile(CA_FILE) or APPLE_STATIC):
    sys.exit(f"{sys.executable}: its default CA bundle {CA_FILE!r} is missing or is the OS's static one; "
             "run under .venv-crosscheck (crosscheck/requirements.in)")

# The archive's root: None reads s3://$NOZZL_ARCHIVE_BUCKET when first needed (archive_root());
# a self-test or a caller points it at a local tree instead.
BUCKET = None


def archive_root():
    """The nozzl archive's root: BUCKET when set, else s3://$NOZZL_ARCHIVE_BUCKET."""
    if BUCKET:
        return BUCKET
    bucket = os.environ.get("NOZZL_ARCHIVE_BUCKET", "").strip()
    if not bucket:
        sys.exit("set NOZZL_ARCHIVE_BUCKET to the nozzl archive's S3 bucket (the pipeline reads it too)")
    return "s3://" + bucket
CURATED = "4ad6fa2d16e2a9b576c863b4cf7404a70d4dc320c0c447d10ad6ff58993eacc8"
CATALOG_D = "routstr-21-models"
SCHEMA_TAG = "routstr.analytics.snapshot.v1"
DAY = 86400
WEEK = 7 * DAY
TESTWORD = re.compile(r"(^|[^a-z0-9])(test|testing|staging)([^a-z0-9]|$)")
# Document-size caps - shared verbatim with the production compute (routstr.py).
MODEL_ID_MAX = 60  # model ids: NUL codepoints removed, then clipped (mirrors pipeline model_key)
MAX_MODEL_ROWS = 100
MAX_REVIEW_ROWS = 200
MAX_PROVIDER_ROWS = 250
MAX_URLS = 20
MAX_MINTS = 50
URL_MAX = 200
CATALOG_MAX = 100


def rha(v):
    """round half away from zero"""
    return int(math.floor(v + 0.5)) if v >= 0 else int(math.ceil(v - 0.5))


def sats(msats):
    return rha(msats / 1000.0)


def fin(v):
    """A usable JSON number or None: json.loads admits NaN/Infinity and int() on those
    raises outside isinstance guards; bools read as absent (True is an int subclass)."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return v if math.isfinite(v) else None


def day_of(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d")


# --- extraction (DuckDB over raw S3) --------------------------------------------------------

def fetch(con, as_of, ingested_before=None):
    """`ingested_before` (epoch seconds, optional) pins the ARCHIVE STATE: rows
    ingested after it are dropped, so a reference built days after a production
    run still reads what that run could have read. Without it the created_at
    horizon alone admits events archived later (the 12h chunk roll), and the
    comparator reports input drift (archive_events differ) instead of agreement.
    The production run reads the archive right after flushing the WALs at its
    start, so its snapshot's generated_utc is the natural cutoff (events
    ingested in the ~minute between flush and build are the residual risk)."""
    horizon = as_of + 2 * DAY
    ingest_pred = f"AND ingested_at <= {int(ingested_before)}" if ingested_before else ""
    con.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW'")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET ca_cert_file = '{CA_FILE}'")
    con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1')")

    def all_versions(kind):
        return con.execute(f"""
            SELECT id, pubkey, created_at, content, tags, d_tag
            FROM read_parquet('{archive_root()}/c=addressable/k={kind}/p=*/dt=*/*.parquet')
            WHERE created_at <= {horizon} {ingest_pred}
            QUALIFY row_number() OVER (PARTITION BY id ORDER BY ingested_at) = 1
        """).fetchall()

    announce = all_versions(38421)
    catalog = all_versions(38423)
    reviews = all_versions(38425)
    # NOTE the horizon is NOT in this query: production reduces per-day in Athena
    # (which knows no NOW) and applies the horizon afterwards in Python, so the
    # reference must reduce-then-filter in the same order or the two can pick
    # different day-winners around a future-dated event.
    analytics = con.execute(f"""
        WITH src AS (
          SELECT id, pubkey, created_at, content
          FROM read_parquet('{archive_root()}/c=addressable/k=38422/p=*/dt=*/*.parquet')
          WHERE len(list_filter(tags, t -> t[1] = 'schema' AND t[2] = '{SCHEMA_TAG}')) > 0 {ingest_pred}
          QUALIFY row_number() OVER (PARTITION BY id ORDER BY ingested_at) = 1
        )
        SELECT id, pubkey, created_at, content
        FROM src
        QUALIFY row_number() OVER (
          PARTITION BY pubkey, strftime(to_timestamp(created_at) AT TIME ZONE 'UTC', '%Y-%m-%d')
          ORDER BY created_at DESC, id ASC) = 1
    """).fetchall()
    analytics = [r for r in analytics if r[2] <= horizon]
    return announce, analytics, catalog, reviews


# --- classification -------------------------------------------------------------------------

def url_class(u):
    try:
        p = urlsplit(u.strip())
    except ValueError:
        return "invalid"
    if p.scheme not in ("http", "https") or not p.hostname:
        return "invalid"
    h = p.hostname.lower().rstrip(".")
    if h.endswith(".onion"):
        return "onion"
    if h == "localhost" or h.endswith((".local", ".internal", ".lan")):
        return "private"
    if re.fullmatch(r"[0-9.]+", h):
        try:
            o = [int(x) for x in h.split(".")]
        except ValueError:
            return "invalid"
        if len(o) != 4 or any(x > 255 for x in o):
            return "invalid"
        priv = (o[0] in (0, 10, 127) or (o[0] == 172 and 16 <= o[1] <= 31)
                or (o[0] == 192 and o[1] == 168) or (o[0] == 100 and 64 <= o[1] <= 127))
        return "private" if priv else "clearnet"
    return "private" if "." not in h else "clearnet"


def net_class(urls):
    ks = {url_class(u) for u in urls if u and u.strip()}
    if "clearnet" in ks and "onion" in ks:
        return "both"
    if "clearnet" in ks:
        return "clearnet"
    if "onion" in ks:
        return "onion"
    if "private" in ks:
        return "private-only"
    return "none"


def clip(s, n=80):
    """Third-party content-JSON text: non-strings are absent, not a crash."""
    return s.strip()[:n] if isinstance(s, str) else ""


def test_marked(name, urls):
    if isinstance(name, str) and TESTWORD.search(name.lower()):
        return True
    for u in urls or []:
        try:
            h = urlsplit(u.strip()).hostname or ""
        except ValueError:
            continue
        labels = h.lower().rstrip(".").split(".")
        for lab in (labels[:-1] if len(labels) > 1 else labels):
            if TESTWORD.search(lab):
                return True
    return False


def tval(tags, name):
    for t in tags or []:
        if t and t[0] == name:
            return t[1] if len(t) >= 2 else None
    return None


def tall(tags, name):
    return [t[1] for t in (tags or []) if t and t[0] == name and len(t) >= 2]


# --- main -----------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", required=True,
                    help="pin NOW to this UTC instant (YYYY-MM-DDTHH:MM:SS)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--ingested-before", default=None,
                    help="drop archive rows ingested after this UTC instant (YYYY-MM-DDTHH:MM:SS) - "
                         "pins the archive state a past production run read; use its generated_utc")
    args = ap.parse_args()
    NOW = int(dt.datetime.fromisoformat(args.as_of).replace(tzinfo=dt.timezone.utc).timestamp())
    cutoff = (int(dt.datetime.fromisoformat(args.ingested_before).replace(tzinfo=dt.timezone.utc).timestamp())
              if args.ingested_before else None)

    con = duckdb.connect()
    announce, analytics, catalog_rows, review_rows_raw = fetch(con, NOW, cutoff)
    print(f"[fetch] announce={len(announce)} analytics_daily={len(analytics)} "
          f"catalog={len(catalog_rows)} reviews={len(review_rows_raw)}", file=sys.stderr)

    # ---- registry: kind-38421 (routstr-provider-announcements)
    genuine, squat_pks = [], set()
    for (eid, pk, ca, content, tags, d_tag) in announce:
        if tall(tags, "u") and tval(tags, "d") is not None:
            genuine.append((eid, pk, ca, content, tags, d_tag))
        else:
            squat_pks.add(pk)
    squat_pks -= {g[1] for g in genuine}

    # current version per (pubkey, d_tag), then the provider's newest coordinate
    best_coord, latest = {}, {}
    for g in genuine:
        key = (g[1], g[5])
        cur = best_coord.get(key)
        if cur is None or (-g[2], g[0]) < (-cur[2], cur[0]):
            best_coord[key] = g
    for g in best_coord.values():
        cur = latest.get(g[1])
        if cur is None or (-g[2], g[0]) < (-cur[2], cur[0]):
            latest[g[1]] = g
    first_ann = {}
    for g in genuine:
        if g[1] not in first_ann or g[2] < first_ann[g[1]]:
            first_ann[g[1]] = g[2]

    # ---- ledger: kind-38422 (routstr-analytics-snapshots), candidates -> select
    cands = collections.defaultdict(list)  # (pk, grain, ts) -> [(created_at, id, prio, val)]
    meta = {}
    anomalies = {"bad_snapshots": 0, "bad_buckets": 0}
    for (eid, pk, ca, content) in analytics:
        try:
            c = json.loads(content)
            assert isinstance(c, dict)
        except (ValueError, TypeError, AssertionError):
            anomalies["bad_snapshots"] += 1
            continue
        g = fin(c.get("generated_at"))
        gen = int(g) if g is not None and abs(int(g) - ca) <= DAY else ca
        old = meta.get(pk)
        if old is None or gen > old[0] or (gen == old[0] and (ca, eid) < (old[2], old[3])):
            meta[pk] = (gen, c, ca, eid)
        wins = c.get("windows")
        if not isinstance(wins, dict):
            anomalies["bad_snapshots"] += 1
            continue
        for grain, names, imin, blen in (("d", ("30d", "3m"), 1440, DAY),
                                         ("w", ("1y",), 10080, WEEK)):
            for prio, wname in enumerate(names):
                w = wins.get(wname)
                mix = w.get("model_usage_mix") if isinstance(w, dict) else None
                if not isinstance(mix, dict) or mix.get("interval_minutes") != imin:
                    continue
                hb = fin(mix.get("hours_back"))
                if hb is None or hb <= 0:
                    continue
                wstart = gen - int(hb) * 3600
                for b in (mix.get("metrics") or []):
                    if not isinstance(b, dict):
                        anomalies["bad_buckets"] += 1
                        continue
                    try:
                        ts = int(dt.datetime.strptime(b["timestamp"], "%Y-%m-%d %H:%M:%S")
                                 .replace(tzinfo=dt.timezone.utc).timestamp())
                    except (ValueError, TypeError, KeyError):
                        anomalies["bad_buckets"] += 1
                        continue
                    if ts % blen != 0:
                        anomalies["bad_buckets"] += 1
                        continue
                    # full coverage by the provider's window AND by our clock (a
                    # skew-allowed future snapshot must not inject future days)
                    if ts < wstart or ts + blen > gen or ts + blen > NOW:
                        continue
                    def iv(v):
                        n = fin(v)
                        if n is None:
                            raise ValueError("non-finite")
                        return int(n)

                    def mv_(v):
                        n = fin(v if v is not None else 0)
                        if n is None:
                            raise ValueError("non-finite")
                        return rha(float(n))

                    try:
                        models = {}
                        counts = b.get("model_counts") or {}
                        revs = b.get("model_revenue_msats") or {}
                        toks = b.get("model_tokens") or {}
                        # clipped ids; clip-collisions MERGE (same rule as production)
                        for m in counts:
                            key = str(m).replace(chr(0), "")[:MODEL_ID_MAX]
                            slot = models.setdefault(key, {"req": 0, "msats": 0, "tok": 0})
                            slot["req"] += iv(counts.get(m, 0))
                            slot["msats"] += mv_(revs.get(m, 0) or 0)
                            slot["tok"] += iv(toks.get(m, 0) or 0)
                        val = {
                            "req": iv(b["total_successful"]),
                            "msats": mv_(b["total_revenue_msats"]),
                            "tok": iv(b["total_tokens"]),
                            "models": models,
                            "others": {
                                "req": iv(b.get("others", 0) or 0),
                                "msats": mv_(b.get("others_revenue_msats", 0) or 0),
                                "tok": iv(b.get("others_tokens", 0) or 0),
                            },
                        }
                        if val["req"] < 0 or val["msats"] < 0 or val["tok"] < 0:
                            raise ValueError
                    except (ValueError, TypeError, KeyError, OverflowError):
                        anomalies["bad_buckets"] += 1
                        continue
                    cands[(pk, grain, ts)].append((ca, eid, -prio, val))
    ledger = {}
    for key, obs in cands.items():
        ledger[key] = max(obs, key=lambda o: (o[0], o[1], o[2]))[3]
    first_daily = {}
    for (pk, grain, ts) in ledger:
        if grain == "d" and (pk not in first_daily or ts < first_daily[pk]):
            first_daily[pk] = ts
    ledger = {k: v for k, v in ledger.items()
              if not (k[1] == "w" and k[0] in first_daily and k[2] + WEEK > first_daily[k[0]])}

    # ---- providers
    providers = {}
    for pk, g in latest.items():
        (eid, _, ca, content, tags, d_tag) = g
        try:
            cj = json.loads(content or "{}")
            cj = cj if isinstance(cj, dict) else {}
        except ValueError:
            cj = {}
        urls = [u[:URL_MAX] for u in tall(tags, "u")][:MAX_URLS]
        name = clip(cj.get("name")) or pk[:12]
        providers[pk] = dict(
            provider_id=d_tag, name=name, about=clip(cj.get("about")),
            urls=urls, mints=sorted({m.strip()[:URL_MAX] for m in tall(tags, "mint")
                                     if m.strip()})[:MAX_MINTS],
            version=(tval(tags, "version") or "").strip()[:40],
            cls=net_class(urls), test=test_marked(cj.get("name"), urls),
            announced=True, first_seen=first_ann[pk], last_announce=ca)
    for pk, (gen, c, _, _) in meta.items():
        if pk not in providers:
            urls = [u[:URL_MAX] for u in (c.get("endpoint_urls") or [])
                    if isinstance(u, str)][:MAX_URLS]
            providers[pk] = dict(provider_id=None, name=pk[:12], about="", urls=urls,
                                 mints=[], version="", cls=net_class(urls),
                                 test=test_marked(None, urls), announced=False,
                                 first_seen=None, last_announce=None)
    for pk, p in providers.items():
        p["last_report"] = meta[pk][0] if pk in meta else None
        fl = min((ts for (lp, g, ts) in ledger if lp == pk), default=None)
        if fl is not None and (p["first_seen"] is None or fl < p["first_seen"]):
            p["first_seen"] = fl
        p["excl"] = "test" if p["test"] else (
            "private" if p["cls"] in ("private-only", "none") else None)
    included = {pk for pk, p in providers.items() if p["excl"] is None}

    # ---- reviews: kind-38425 (routstr-provider-reviews)
    rev_rows = []
    for (eid, pk, ca, content, tags, d_tag) in sorted(review_rows_raw,
                                                      key=lambda r: (r[2], r[0])):
        node = tval(tags, "node") or tval(tags, "p")
        # first t tag WITH a value - a valueless ["t"] before the real label must not
        # hide it (production scans past valueless t tags; tval would return None)
        label = next((t[1] for t in tags or [] if t and t[0] == "t" and len(t) >= 2), None)
        if not node or not label:
            continue
        rev_rows.append(dict(node=node, label=label.strip().lower()[:24], at=ca,
                             reviewer="curated" if pk == CURATED else "other"))
    latest_rev = {}
    for r in rev_rows:
        if r["reviewer"] == "curated":
            cur = latest_rev.get(r["node"])
            if cur is None or r["at"] >= cur["at"]:
                latest_rev[r["node"]] = r
    for pk, p in providers.items():
        p["review"] = latest_rev[pk]["label"] if pk in latest_rev else None

    # ---- catalog: kind-38423 (routstr-model-catalog)
    cat_hist, cat_models, cat_updated = [], [], None
    for (eid, pk, ca, content, tags, d_tag) in sorted(catalog_rows, key=lambda r: (r[2], r[0])):
        if pk != CURATED or d_tag != CATALOG_D:
            continue
        try:
            ms = json.loads(content).get("models")
            assert isinstance(ms, list)
        except (ValueError, TypeError, AssertionError, AttributeError):
            continue
        ms = sorted({str(m).replace(chr(0), "")[:MODEL_ID_MAX] for m in ms})[:CATALOG_MAX]
        cat_hist.append({"d": day_of(ca), "n": len(ms)})
        cat_models, cat_updated = ms, day_of(ca)

    # ---- aggregations
    inc = {k: v for k, v in ledger.items() if k[0] in included}
    win30 = (NOW // DAY - 30) * DAY  # last 30 COMPLETE UTC days (day-aligned)
    daynet = collections.defaultdict(lambda: dict(msats=0, req=0, tok=0, prov=set()))
    pday = collections.defaultdict(dict)
    mday = collections.defaultdict(dict)
    ptot = collections.defaultdict(lambda: collections.Counter())
    mtot = collections.defaultdict(lambda: collections.Counter())
    mprov30 = collections.defaultdict(set)
    cum = collections.defaultdict(int)
    for (pk, grain, ts), v in sorted(inc.items()):
        ptot[pk]["msats_alltime"] += v["msats"]
        ptot[pk]["req_alltime"] += v["req"]
        ptot[pk]["tok_alltime"] += v["tok"]
        for m, mv in v["models"].items():
            for f, k2 in (("msats", "msats_alltime"), ("req", "req_alltime"), ("tok", "tok_alltime")):
                mtot[m][k2] += mv[f]
        oth = v["others"]
        if oth["req"] or oth["msats"] or oth["tok"]:
            for f, k2 in (("msats", "msats_alltime"), ("req", "req_alltime"), ("tok", "tok_alltime")):
                mtot["(other)"][k2] += oth[f]
        if grain == "w":
            cum[day_of(ts + WEEK - DAY)] += v["msats"]
            continue
        d = day_of(ts)
        cum[d] += v["msats"]
        daynet[d]["msats"] += v["msats"]
        daynet[d]["req"] += v["req"]
        daynet[d]["tok"] += v["tok"]
        daynet[d]["prov"].add(pk)
        pday[pk][d] = pday[pk].get(d, 0) + v["msats"]
        if ts >= win30:
            for f in ("msats", "req", "tok"):
                ptot[pk][f + "_30d"] += v[f]
            for m, mv in v["models"].items():
                for f in ("msats", "req", "tok"):
                    mtot[m][f + "_30d"] += mv[f]
                mprov30[m].add(pk)
            if oth["req"] or oth["msats"] or oth["tok"]:
                for f in ("msats", "req", "tok"):
                    mtot["(other)"][f + "_30d"] += oth[f]
                mprov30["(other)"].add(pk)
        for m, mv in v["models"].items():
            mday[m][d] = mday[m].get(d, 0) + mv["msats"]
        if oth["msats"]:
            mday["(other)"][d] = mday["(other)"].get(d, 0) + oth["msats"]

    daily = []
    if daynet:
        ds = sorted(daynet)
        d0, d1 = dt.date.fromisoformat(ds[0]), dt.date.fromisoformat(ds[-1])
        cur = d0
        while cur <= d1:
            k = cur.isoformat()
            r = daynet.get(k)
            daily.append({"d": k, "msats": r["msats"] if r else 0,
                          "req": r["req"] if r else 0, "tok": r["tok"] if r else 0,
                          "prov": len(r["prov"]) if r else 0})
            cur += dt.timedelta(days=1)

    cum_rev, run = [], 0
    for d in sorted(cum):
        run += cum[d]
        cum_rev.append({"d": d, "msats": run})

    def series(dm):
        return [{"d": d, "msats": dm[d]} for d in sorted(dm)]

    # display ids: 12-hex prefix, full pubkey for any colliding prefix set
    sid = {}
    bypfx = collections.defaultdict(list)
    for pk in providers:
        bypfx[pk[:12]].append(pk)
    for pfx, grp in bypfx.items():
        for pk in grp:
            sid[pk] = pfx if len(grp) == 1 else pk

    inc_sorted = sorted(included, key=lambda pk: (-ptot[pk]["msats_30d"],
                                                  -ptot[pk]["msats_alltime"], pk))
    top_p = [pk for pk in inc_sorted if ptot[pk]["msats_alltime"] > 0][:20]
    oday = collections.defaultdict(int)
    for pk in included:
        if pk not in top_p:
            for d, ms in pday[pk].items():
                oday[d] += ms
    ptop = [{"id": sid[pk], "name": providers[pk]["name"], "series": series(pday[pk])}
            for pk in top_p]
    if any(oday.values()):
        ptop.append({"id": "(others)", "name": "(others)", "series": series(oday)})

    mnames = sorted((m for m in mtot if m != "(other)"),
                    key=lambda m: (-mtot[m]["msats_30d"], -mtot[m]["msats_alltime"], m))
    top_m = [m for m in mnames if mtot[m]["msats_alltime"] > 0][:6]
    moday = collections.defaultdict(int)
    for m, dm in mday.items():
        if m not in top_m:
            for d, ms in dm.items():
                moday[d] += ms
    mtop = [{"id": m, "series": series(mday[m])} for m in top_m]
    if any(moday.values()):
        mtop.append({"id": "(others)", "series": series(moday)})

    prov_rows = [{
        "id": sid[pk], "name": providers[pk]["name"], "class": providers[pk]["cls"],
        "version": providers[pk]["version"], "mints": len(providers[pk]["mints"]),
        "review": providers[pk]["review"],
        "first_seen": day_of(providers[pk]["first_seen"]) if providers[pk]["first_seen"] else None,
        "last_announce": day_of(providers[pk]["last_announce"]) if providers[pk]["last_announce"] else None,
        "last_report": day_of(providers[pk]["last_report"]) if providers[pk]["last_report"] else None,
        "msats_alltime": ptot[pk]["msats_alltime"], "msats_30d": ptot[pk]["msats_30d"],
        "req_30d": ptot[pk]["req_30d"], "tok_30d": ptot[pk]["tok_30d"],
        "excluded": providers[pk]["excl"] is not None, "reason": providers[pk]["excl"],
    } for pk in sorted(providers, key=lambda pk: (-ptot[pk]["msats_alltime"],
                                                  -(providers[pk]["last_announce"] or 0), pk))
    ][:MAX_PROVIDER_ROWS]

    # fold named rows beyond MAX_MODEL_ROWS into "(other)" (sums preserved)
    named_shown = mnames[:MAX_MODEL_ROWS]
    for m in mnames[MAX_MODEL_ROWS:]:
        for k2, v2 in mtot[m].items():
            mtot["(other)"][k2] += v2
        mprov30["(other)"] |= mprov30[m]
    model_rows = [{
        "model": m, "msats_30d": mtot[m]["msats_30d"], "req_30d": mtot[m]["req_30d"],
        "tok_30d": mtot[m]["tok_30d"], "msats_alltime": mtot[m]["msats_alltime"],
        "req_alltime": mtot[m]["req_alltime"], "tok_alltime": mtot[m]["tok_alltime"],
        "providers_30d": len(mprov30[m]),
    } for m in named_shown + (["(other)"] if "(other)" in mtot else [])]

    ann_pks = [pk for pk, p in providers.items() if p["announced"]]
    transports = collections.Counter(providers[pk]["cls"] for pk in ann_pks)
    versions = collections.Counter(providers[pk]["version"] or "(untagged)" for pk in ann_pks)
    mint_hosts = collections.Counter()
    all_mints = set()
    for pk in ann_pks:
        hs = set()
        for mu in providers[pk]["mints"]:
            all_mints.add(mu)
            try:
                h = urlsplit(mu).hostname
            except ValueError:
                h = None
            if h:
                hs.add(h.lower())
        for h in hs:
            mint_hosts[h] += 1

    def top(counter, n):
        head = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:n]
        rest = sum(v for k, v in counter.items() if k not in dict(head))
        out = [{"k": k, "v": v} for k, v in head]
        if rest:
            out.append({"k": "(other)", "v": rest})
        return out

    st, sf, fresh = 0, 0, 0
    for pk in included:
        if pk not in meta or meta[pk][0] < NOW - 7 * DAY:
            continue
        s = ((meta[pk][1].get("windows") or {}).get("30d") or {}).get("summary") or {}
        try:
            t_, f_ = int(fin(s["total_requests"])), int(fin(s["failed_requests"]))
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if t_ < 0 or f_ < 0 or f_ > t_:
            continue
        st, sf, fresh = st + t_, sf + f_, fresh + 1
    rate = round(1.0 - sf / st, 4) if st else None

    w30 = [r for r in daily if int(dt.datetime.strptime(r["d"], "%Y-%m-%d")
                                   .replace(tzinfo=dt.timezone.utc).timestamp()) >= win30]
    active30 = {k[0] for k, v in inc.items()
                if k[1] == "d" and k[2] >= win30 and (v["msats"] or v["req"] or v["tok"])}
    led_days = sorted({day_of(ts) for (_, g, ts) in inc if g == "d"}
                      | {day_of(ts + WEEK - DAY) for (_, g, ts) in inc if g == "w"})
    tot = {f: sum(v[f] for v in inc.values()) for f in ("msats", "req", "tok")}

    data = {
        "generated_utc": dt.datetime.fromtimestamp(NOW, dt.timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%SZ"),
        "summary": {
            "archive_events": {"announcements": len(announce),
                               "analytics_daily": len(analytics),
                               "catalog": len(catalog_rows), "reviews": len(review_rows_raw)},
            "providers_announced": len(ann_pks),
            "providers_reporting": len({k[0] for k in inc}),
            "providers_active_30d": len(active30),
            "revenue_30d_sats": sats(sum(r["msats"] for r in w30)),
            "requests_30d": sum(r["req"] for r in w30),
            "tokens_30d": sum(r["tok"] for r in w30),
            "revenue_alltime_sats": sats(tot["msats"]),
            "requests_alltime": tot["req"],
            "tokens_alltime": tot["tok"],
            "first_activity": led_days[0] if led_days else None,
            "last_activity": led_days[-1] if led_days else None,
            "models_30d": sum(1 for m in mnames if mtot[m]["msats_30d"]
                              or mtot[m]["req_30d"] or mtot[m]["tok_30d"]),
            "catalog_models": len(cat_models) if cat_models else None,
            "mints": len(all_mints),
            "success_rate_30d": rate,
        },
        "exclusions": [
            {"k": "Test / staging nodes",
             "v": sum(1 for p in providers.values() if p["excl"] == "test"),
             "why": "the announcement name or an endpoint host label says test/staging - "
                    "self-reported usage from non-production nodes"},
            {"k": "Private-endpoint-only nodes",
             "v": sum(1 for p in providers.values() if p["excl"] == "private"),
             "why": "every announced endpoint is loopback/LAN/CGNAT or a bare container "
                    "hostname, or no usable endpoint was announced at all - no "
                    "marketplace client could ever reach them"},
            {"k": "Kind squatters",
             "v": len(squat_pks),
             "why": "kind-38421 (routstr-provider-announcements) events without the "
                    "u+d tag shape every Routstr consumer requires (live example: "
                    "lnproxy fee quotes) - not Routstr providers at all"},
        ],
        "daily": daily,
        "cum_revenue": cum_rev,
        "provider_daily_top": ptop,
        "model_daily_top": mtop,
        "providers": prov_rows,
        "providers_total": len(providers),
        "models": model_rows,
        "catalog": {"updated": cat_updated, "models": cat_models, "history": cat_hist},
        "reviews": [{"node": sid.get(r["node"], r["node"][:12]), "label": r["label"],
                     "d": day_of(r["at"]), "reviewer": r["reviewer"],
                     "name": (providers.get(r["node"], {}).get("name") or r["node"][:12])}
                    for r in sorted(rev_rows, key=lambda r: (-r["at"], r["node"]))
                    ][:MAX_REVIEW_ROWS],
        "transports": top(transports, 10),
        "versions": top(versions, 10),
        "mints_top": top(mint_hosts, 10),
        "success": {"rate_30d": rate, "requests_reported": st, "failed_reported": sf,
                    "providers_fresh": fresh},
        "coverage": {
            "daily_from": min((day_of(ts) for (_, g, ts) in inc if g == "d"), default=None),
            "weekly_from": min((day_of(ts) for (_, g, ts) in inc if g == "w"), default=None),
            "snapshots_used": len(analytics),
        },
        "anomalies": anomalies,
    }
    with open(args.out, "w") as fh:
        json.dump(data, fh, indent=1)
    print(f"[out] {args.out}: {len(json.dumps(data))} bytes; "
          f"{data['summary']['providers_announced']} announced, "
          f"{data['summary']['revenue_alltime_sats']} sats all-time", file=sys.stderr)


if __name__ == "__main__":
    main()

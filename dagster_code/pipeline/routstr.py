"""The /routstr metrics compute - one snapshot per run from the Routstr marketplace kinds.

PRODUCTION HOME of the Routstr methodology. The independent cross-check is
crosscheck/build_routstr_snapshot.py (DuckDB straight over the raw S3 archive on
a workstation); crosscheck/compare_routstr_snapshots.py diffs the two snapshot documents
field-by-field and the contract is ZERO diffs. Change methodology IN BOTH PLACES or the
agreement gate fails. Metric semantics live in docs/dataset-reference/routstr-dataset-reference.md.

INPUTS (parsed NDJSON rows from the four Athena UNLOAD extracts - see sql/):
  * announce_events - kind-38421 (routstr-provider-announcements), ALL versions,
                      id-deduped, each {id, pubkey, created_at, content, tags, d_tag}
  * analytics_rows  - kind-38422 (routstr-analytics-snapshots), the LAST snapshot per
                      (pubkey, UTC day) carrying the routstr.analytics.snapshot.v1 schema
                      tag (both filters are IN the SQL - extract_routstr_analytics_daily.sql -
                      and mirrored by the reference generator), each
                      {id, pubkey, created_at, content, d_tag}
  * catalog_events  - kind-38423 (routstr-model-catalog), ALL versions, same shape as
                      announce_events
  * review_events   - kind-38425 (routstr-provider-reviews), ALL versions, same shape

THE LEDGER - how history is reconstructed from trailing windows:
  Each analytics snapshot embeds trailing windows (24h/7d/30d/3m/1y) whose
  `model_usage_mix.metrics` arrays carry UTC-grid-aligned activity buckets - daily
  buckets (1440 min) in the 30d/3m windows, weekly buckets (10080 min, epoch-aligned,
  i.e. Thursday-start) in the 1y window. Buckets are SPARSE: a provider omits
  no-activity buckets, so absence is not evidence, and the ledger only ever WRITES
  buckets that are present - a day observed in an older snapshot survives a newer
  fully-covering snapshot that omits it (live: a node reset its own history; we keep
  what we archived - observed history is a floor). Probed 2026-08-24 on the live
  archive: the grid is stable across snapshots, so buckets from different snapshots
  merge; two edge rules make the merge sound:
    1. FULL-COVERAGE ONLY: a bucket is trusted only when its whole [start, start+len)
       span lies inside the window ([generated_at - hours_back*3600, generated_at]).
       At the trailing edge the provider truncates the bucket to the window (probed:
       a 297-request day re-reported as 3 when the window slid past it), and at the
       leading edge the current day is still accumulating.
    2. NEWEST WINS: among fully-covering observations of the same (provider, grain,
       bucket) the newest snapshot's value wins (probed: late accounting revised a
       day upward across consecutive snapshots); within one snapshot the 30d window
       is preferred over 3m (they agree when both fully cover).
  Weekly rows are kept only for a provider's PRE-DAILY era (week fully before its
  earliest daily-covered date), so daily+weekly never overlap per provider and
  all-time sums add the two grains directly.

REVENUE UNITS: bucket revenue is msats (float on the wire, integral in practice);
every value is rounded half-away-from-zero to an int at ingest and all arithmetic is
integer msats. Headline cards convert to sats (round half-up) at doc-build; series
carry msats and the page divides for display.

CAVEAT THAT SHAPES EVERYTHING: analytics numbers are SELF-REPORTED by each provider's
node, unverifiable by construction. The exclusion ledger (test/staging nodes,
private-endpoint-only nodes) keeps the obvious non-production reporters out of the
headline aggregates; the rest is served as reported and labeled as such.

OUTPUTS: `build_snapshot` returns (state, data) - the persistable substrate records and
the page's snapshot dict; `write_pg` persists them in ONE transaction, behind the
MIN_LEDGER_ROWS / MIN_PROVIDERS anti-shrink gates: TRUNCATE+INSERT of the five
substrate tables (routstr_providers, routstr_provider_days, routstr_model_days,
routstr_catalog_versions, routstr_reviews) plus one appended routstr_snapshot row.

THE SUBSTRATE IS THE MCP TOOLS' SOURCE. The /routstr page reads only the document; the
nostrology_mcp Routstr metrics (domain `routstr`) read the document for every number the
page publishes and the substrate tables for per-entity drilldowns and caller-chosen
windows. The rule that keeps the two from diverging: THIS module is the only place a
Routstr number is computed - the tools select, filter and window pre-computed rows, and
every substrate table is tied to the document by `check_invariants` (model days re-sum
to the model rows and the chart series; catalog versions re-derive the catalog history;
reviews carry the timeline the document truncates). A methodology change therefore
lands here (and in the reference generator) and reaches the page and the tools together.
"""
import collections
import datetime as dt
import gzip
import json
import math
import re
from urllib.parse import urlsplit

# The curated Routstr key: sole author the SDK trusts for the kind-38423
# (routstr-model-catalog) "routstr-21-models" list and the default reviewer for
# kind-38425 (routstr-provider-reviews) (routstr-sdk ModelManager.ts).
CURATED_PUBKEY = "4ad6fa2d16e2a9b576c863b4cf7404a70d4dc320c0c447d10ad6ff58993eacc8"
CATALOG_D_TAG = "routstr-21-models"

# Review labels, from routstr-sdk ModelManager.ts (fetchReviewLabels): a provider is
# enabled only when its LATEST curated review is positive. Labels outside both sets
# (live example: "bad") are "other" - rendered verbatim, treated as not-positive.
REVIEW_POSITIVE = {"trusted", "verified", "lgtm"}
REVIEW_NEGATIVE = {"suspicious", "avoid", "blacklisted", "removed"}

# The schema tag value the analytics extract filters on (also enforced in SQL).
ANALYTICS_SCHEMA = "routstr.analytics.snapshot.v1"

# Window names by ledger grain, in PRIORITY order within one snapshot.
DAILY_WINDOWS = ("30d", "3m")
WEEKLY_WINDOWS = ("1y",)
DAY_S = 86400
WEEK_S = 7 * DAY_S
DAILY_INTERVAL_MIN = 1440
WEEKLY_INTERVAL_MIN = 10080

# A snapshot's content.generated_at is used for window-coverage math (it is the
# aggregation instant); it is trusted only within a day of the event's created_at,
# else created_at substitutes (one hostile generated_at would otherwise shift every
# coverage decision for that snapshot).
GENERATED_AT_MAX_SKEW_S = DAY_S

# Test/staging node detection: word-boundary match on the announcement name and on
# every endpoint host LABEL except the TLD (routstr.rewolf.dev is a real node on the
# .dev TLD; staging.routstr.com is the staging host). Lowercased inputs.
_TEST_WORD_RE = re.compile(r"(^|[^a-z0-9])(test|testing|staging)([^a-z0-9]|$)")

# Provider display names are third-party text; bound them (the page escapes, this
# bounds the document).
NAME_MAX_LEN = 80

# Top-N caps for the per-provider / per-model daily chart series and version/mint lists.
# The provider series ship the top 20 (rank order) plus the "(others)" residual, and the
# page's Top 5/10/20 selector folds ranks N+1..20 into the residual client-side - so the
# residual identity `sum(provider_daily_top) == daily` per day is part of the contract
# (pinned by test_provider_daily_top_sums_to_the_network_series).
TOP_PROVIDERS = 20
TOP_MODELS = 6
TOP_VERSIONS = 10
TOP_MINTS = 10

# Document-size caps against hostile growth: every list in the served JSONB whose
# cardinality a third party controls is bounded. Kind-38421 (routstr-provider-
# announcements), kind-38422 (routstr-analytics-snapshots) and kind-38425
# (routstr-provider-reviews) are all OPEN kinds - anyone can flood providers, model
# identifiers or review events, and the whole document is SSR-embedded in the page.
# Folding/truncation rules are shared with the reference generator.
MODEL_ID_MAX_LEN = 60      # model identifiers, clipped at ingest (clip-collisions merge)
MAX_MODEL_ROWS = 100       # named model rows in the doc; the tail folds into "(other)"
MAX_REVIEW_ROWS = 200      # newest review events shown
MAX_PROVIDER_ROWS = 250    # provider rows in the doc (summary counts stay uncapped)
MAX_URLS_PER_PROVIDER = 20
MAX_MINTS_PER_PROVIDER = 50
URL_MAX_LEN = 200
CATALOG_MAX_MODELS = 100

# Anti-shrink floors for the serving write - the never-wipe posture of
# pipeline/kinds.py MIN_ROWS, ~80% of the 2026-08-24 measured values (ledger rows
# 1,088; providers with a genuine announcement 49). NOT integrity gates: a value
# below the floor aborts BEFORE the TRUNCATE, preserving the previous serving data,
# and lowering a floor is a deliberate code change. The ledger only grows (the
# archive keeps every version), so a shrink signals an extract/merge regression.
MIN_LEDGER_ROWS = 850
MIN_PROVIDERS = 39

# The review timeline persisted to routstr_reviews (the MCP `provider_reviews` record's
# source) keeps the NEWEST rows up to this bound - kind-38425 (routstr-provider-reviews)
# is an open kind, so an unbounded table would let one flooder grow every nightly
# write. The document's own cap (MAX_REVIEW_ROWS) is far smaller; the table exists so
# a per-provider timeline is complete well past what the page shows.
MAX_REVIEW_TABLE_ROWS = 5000


# --- NDJSON input ---------------------------------------------------------------------------

def parse_ndjson_gz(paths) -> list[dict]:
    """Parse Athena UNLOAD .gz JSON-lines parts into row dicts (one object per line)."""
    rows: list[dict] = []
    for p in sorted(paths):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


def tag_value(tags, name):
    """First tag named `name`, its second element (None when absent or 1-element)."""
    for t in tags or []:
        if t and t[0] == name:
            return t[1] if len(t) >= 2 else None
    return None


def tag_values_all(tags, name):
    """Every tag named `name` -> its second element, skipping 1-element tags."""
    return [t[1] for t in (tags or []) if t and t[0] == name and len(t) >= 2]


def current_versions(events) -> list[dict]:
    """Current version per (pubkey, d_tag): newest created_at, lowest id breaking ties,
    per NIP-01 (the same rule pipeline/marketplaces.py uses)."""
    groups: dict = {}
    for ev in events:
        key = (ev["pubkey"], ev.get("d_tag"))
        cur = groups.get(key)
        if cur is None or (-ev["created_at"], ev["id"]) < (-cur["created_at"], cur["id"]):
            groups[key] = ev
    return list(groups.values())


# --- units / rounding -----------------------------------------------------------------------

def round_half_away(v: float) -> int:
    """Round half away from zero - one explicit rule shared with the reference generator
    (Python's built-in round() is banker's rounding and would diverge on .5 values)."""
    return int(math.floor(v + 0.5)) if v >= 0 else int(math.ceil(v - 0.5))


def msats_to_sats(msats: int) -> int:
    """Headline conversion; msats are non-negative here so half-up == half-away."""
    return round_half_away(msats / 1000.0)


def model_key(m) -> str:
    """A model identifier as the ledger keys it: str(), NUL codepoints removed, clipped
    at MODEL_ID_MAX_LEN. Shared verbatim with the reference generator."""
    return str(m).replace(chr(0), "")[:MODEL_ID_MAX_LEN]


def finite_num(v):
    """A JSON number that is actually usable, or None. json.loads accepts NaN/Infinity
    literals by default and int() on those raises OUTSIDE any isinstance guard
    (ValueError for NaN, OverflowError for Infinity) - so every numeric field read
    from third-party content goes through here first. Bools are excluded (True is an
    int subclass and a hostile `"generated_at": true` should read as absent)."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return v if math.isfinite(v) else None


# --- provider classification (kind-38421 routstr-provider-announcements) --------------------

def classify_url(u: str) -> str:
    """One endpoint URL -> 'clearnet' | 'onion' | 'private' | 'invalid'.

    'private': loopback/RFC-1918/CGNAT (Tailscale 100.64/10) IPs, localhost, dotless
    hostnames (docker-internal like `paygate`), and .local/.internal/.lan suffixes -
    endpoints no marketplace client could reach.
    """
    try:
        parts = urlsplit(u.strip())
    except ValueError:
        return "invalid"
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return "invalid"
    host = parts.hostname.lower().rstrip(".")
    if host.endswith(".onion"):
        return "onion"
    if host == "localhost" or host.endswith((".local", ".internal", ".lan")):
        return "private"
    if re.fullmatch(r"[0-9.]+", host):
        try:
            o = [int(x) for x in host.split(".")]
        except ValueError:
            return "invalid"
        if len(o) != 4 or any(x > 255 for x in o):
            return "invalid"
        if (o[0] == 10 or o[0] == 127 or (o[0] == 172 and 16 <= o[1] <= 31)
                or (o[0] == 192 and o[1] == 168) or (o[0] == 100 and 64 <= o[1] <= 127)
                or o[0] == 0):
            return "private"
        return "clearnet"
    if "." not in host:
        return "private"
    return "clearnet"


def endpoint_class(urls) -> str:
    """A provider's endpoint set -> 'clearnet' | 'onion' | 'both' | 'private-only' | 'none'."""
    kinds = {classify_url(u) for u in urls if u and u.strip()}
    has_clear, has_onion = "clearnet" in kinds, "onion" in kinds
    if has_clear and has_onion:
        return "both"
    if has_clear:
        return "clearnet"
    if has_onion:
        return "onion"
    if "private" in kinds:
        return "private-only"
    return "none"


def is_test_marked(name: str, urls) -> bool:
    """True when the display name or any endpoint host label (TLD excluded) carries a
    test/staging word."""
    if isinstance(name, str) and _TEST_WORD_RE.search(name.lower()):
        return True
    for u in urls or []:
        try:
            host = urlsplit(u.strip()).hostname or ""
        except ValueError:
            continue
        labels = host.lower().rstrip(".").split(".")
        for label in labels[:-1] if len(labels) > 1 else labels:
            if _TEST_WORD_RE.search(label):
                return True
    return False


# --- the analytics ledger (kind-38422 routstr-analytics-snapshots) --------------------------

def _parse_bucket_ts(s):
    """Bucket timestamp 'YYYY-MM-DD HH:MM:SS' (UTC) -> epoch seconds, or None."""
    try:
        return int(dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                   .replace(tzinfo=dt.timezone.utc).timestamp())
    except (ValueError, TypeError):
        return None


def _bucket_value(b):
    """One window bucket -> the ledger observation, or None when malformed.

    Values are integer msats/counts from here on. Per-model maps keep the bucket's
    named models plus the residual the provider itself aggregated as `others`
    (buckets are top-N-truncated on the wire, so '(other)' is part of the data
    model, not a display trick). Identity named+others == totals probed exact on
    1,810 live buckets (2026-08-24).
    """
    def _i(v):
        n = finite_num(v)
        if n is None:
            raise ValueError("non-finite count")
        return int(n)

    def _ms(v):
        n = finite_num(v if v is not None else 0)
        if n is None:
            raise ValueError("non-finite msats")
        return round_half_away(float(n))

    try:
        req = _i(b["total_successful"])
        msats = _ms(b["total_revenue_msats"])
        tok = _i(b["total_tokens"])
        if req < 0 or msats < 0 or tok < 0:
            return None
        # Model identifiers are third-party strings: NUL-stripped (Postgres rejects the
        # codepoint, and routstr_model_days keys on the id - two ids differing only by
        # a NUL must merge here, not collide at INSERT), clipped at MODEL_ID_MAX_LEN,
        # and clip-collisions MERGE (two hostile 60+-char names folding to one key add
        # up rather than silently dropping one). Both rules are mirrored by the
        # reference generator.
        models = {}
        counts = b.get("model_counts") or {}
        revs = b.get("model_revenue_msats") or {}
        toks = b.get("model_tokens") or {}
        for m in counts:
            key = model_key(m)
            mv = models.setdefault(key, {"req": 0, "msats": 0, "tok": 0})
            mv["req"] += _i(counts.get(m, 0))
            mv["msats"] += _ms(revs.get(m, 0) or 0)
            mv["tok"] += _i(toks.get(m, 0) or 0)
        others = {
            "req": _i(b.get("others", 0) or 0),
            "msats": _ms(b.get("others_revenue_msats", 0) or 0),
            "tok": _i(b.get("others_tokens", 0) or 0),
        }
        return {"req": req, "msats": msats, "tok": tok, "models": models, "others": others}
    except (TypeError, ValueError, KeyError, OverflowError):
        return None


def build_ledger(analytics_rows, now_ts: int):
    """Reconstruct the per-provider activity ledger from the daily analytics snapshots.

    Returns (ledger, providers_meta, anomalies):
      ledger:  {(pubkey, grain, bucket_start_ts): observation dict}  with grain 'd'|'w',
               weekly rows already restricted to each provider's pre-daily era
      providers_meta: {pubkey: {"last_report_ts", "last_content"}} - the newest parsed
               snapshot per provider (feeds success-rate + reachability fallback)
      anomalies: {"bad_snapshots": n, "bad_buckets": n}
    """
    horizon = now_ts + 2 * DAY_S
    bad_snapshots = 0
    bad_buckets = 0
    ledger: dict = {}
    meta: dict = {}

    # Newest-wins by processing in (created_at, id) order; within one snapshot the
    # FIRST window in priority order that fully covers a bucket claims it.
    rows = sorted(analytics_rows, key=lambda r: (r["created_at"], r["id"]))
    for row in rows:
        if row["created_at"] > horizon:
            continue
        try:
            c = json.loads(row["content"])
            if not isinstance(c, dict):
                raise ValueError("content is not an object")
        except (ValueError, TypeError):
            bad_snapshots += 1
            continue

        gen = finite_num(c.get("generated_at"))
        gen_ts = int(gen) if (gen is not None
                              and abs(int(gen) - row["created_at"]) <= GENERATED_AT_MAX_SKEW_S
                              ) else row["created_at"]

        m = meta.get(row["pubkey"])
        if m is None or gen_ts > m["last_report_ts"]:
            meta[row["pubkey"]] = {"last_report_ts": gen_ts, "last_content": c}

        windows = c.get("windows")
        if not isinstance(windows, dict):
            bad_snapshots += 1
            continue

        row_obs: dict = {}
        for grain, names, interval_min, blen in (
            ("d", DAILY_WINDOWS, DAILY_INTERVAL_MIN, DAY_S),
            ("w", WEEKLY_WINDOWS, WEEKLY_INTERVAL_MIN, WEEK_S),
        ):
            for wname in names:
                w = windows.get(wname)
                if not isinstance(w, dict):
                    continue
                mix = w.get("model_usage_mix")
                if not isinstance(mix, dict) or mix.get("interval_minutes") != interval_min:
                    continue
                hours_back = finite_num(mix.get("hours_back"))
                if hours_back is None or hours_back <= 0:
                    continue
                window_start = gen_ts - int(hours_back) * 3600
                for b in (mix.get("metrics") or []):
                    if not isinstance(b, dict):
                        bad_buckets += 1
                        continue
                    ts = _parse_bucket_ts(b.get("timestamp"))
                    # Grid alignment is part of the merge contract (epoch-aligned:
                    # daily at UTC midnight, weekly on the epoch Thursday grid).
                    if ts is None or ts % blen != 0:
                        bad_buckets += 1
                        continue
                    # Full-coverage rule: trailing-edge truncation + leading partial -
                    # AND the bucket must be complete by OUR clock, not just the
                    # provider's claimed generated_at: a snapshot dated inside the
                    # now+2d skew allowance could otherwise inject "fully covered"
                    # buckets for future days into the ledger.
                    if ts < window_start or ts + blen > gen_ts or ts + blen > now_ts:
                        continue
                    val = _bucket_value(b)
                    if val is None:
                        bad_buckets += 1
                        continue
                    row_obs.setdefault((grain, ts), val)
        for (grain, ts), val in row_obs.items():
            ledger[(row["pubkey"], grain, ts)] = val

    # Weekly rows only for each provider's pre-daily era.
    first_daily: dict = {}
    for (pk, grain, ts) in ledger:
        if grain == "d" and (pk not in first_daily or ts < first_daily[pk]):
            first_daily[pk] = ts
    for key in [k for k in ledger if k[1] == "w"]:
        pk, _, ts = key
        fd = first_daily.get(pk)
        if fd is not None and ts + WEEK_S > fd:
            del ledger[key]

    return ledger, meta, {"bad_snapshots": bad_snapshots, "bad_buckets": bad_buckets}


# --- the snapshot ---------------------------------------------------------------------------

def _day(ts: int) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d")


def _clip_name(s) -> str:
    """Bound third-party display text; non-string content-JSON values (a hostile
    provider controls the JSON) are treated as absent, not crashed on."""
    if not isinstance(s, str):
        return ""
    return s.strip()[:NAME_MAX_LEN]


def build_snapshot(announce_events, analytics_rows, catalog_events, review_events, now_ts: int):
    """The full metrics compute. Pure: rows in, (state, data) out - no AWS, no DB, no clock
    (now_ts is the caller's). `data` is exactly the page's snapshot JSON; `state` carries
    the substrate rows write_pg persists."""
    NOW = int(now_ts)
    horizon = NOW + 2 * DAY_S
    announce_events = [e for e in announce_events if e["created_at"] <= horizon]
    catalog_events = [e for e in catalog_events if e["created_at"] <= horizon]
    review_events = [e for e in review_events if e["created_at"] <= horizon]
    analytics_in = [r for r in analytics_rows if r["created_at"] <= horizon]

    # ---- kind-38421 (routstr-provider-announcements): the provider registry
    # A GENUINE Routstr announcement carries >= 1 `u` endpoint tag and a `d` tag - the
    # discriminator every Routstr consumer uses (kind-38421 (routstr-provider-announcements) is squatted: lnproxy
    # publishes fee quotes on it with neither tag).
    genuine, squatters = [], set()
    for ev in announce_events:
        if tag_values_all(ev.get("tags"), "u") and tag_value(ev.get("tags"), "d") is not None:
            genuine.append(ev)
        else:
            squatters.add(ev["pubkey"])
    squatters -= {e["pubkey"] for e in genuine}

    current = current_versions(genuine)
    # Provider identity is the PUBKEY; a node that re-announced under several d-tags
    # (observed live) is one provider whose current announcement is its newest
    # current-version coordinate.
    latest_by_pk: dict = {}
    for ev in current:
        cur = latest_by_pk.get(ev["pubkey"])
        if cur is None or (-ev["created_at"], ev["id"]) < (-cur["created_at"], cur["id"]):
            latest_by_pk[ev["pubkey"]] = ev

    first_announce = {}
    for ev in genuine:
        t = first_announce.get(ev["pubkey"])
        if t is None or ev["created_at"] < t:
            first_announce[ev["pubkey"]] = ev["created_at"]

    # ---- kind-38422 (routstr-analytics-snapshots): the ledger
    ledger, report_meta, anomalies = build_ledger(analytics_in, NOW)

    # ---- provider records (announced or reporting)
    providers: dict = {}
    for pk, ev in latest_by_pk.items():
        tags = ev.get("tags") or []
        try:
            content = json.loads(ev.get("content") or "{}")
            if not isinstance(content, dict):
                content = {}
        except ValueError:
            content = {}
        # Announced URL/mint lists are third-party and unbounded on the wire; both
        # the classification inputs and the persisted lists are capped.
        urls = [u[:URL_MAX_LEN] for u in tag_values_all(tags, "u")][:MAX_URLS_PER_PROVIDER]
        providers[pk] = {
            "pubkey": pk,
            "provider_id": ev.get("d_tag"),
            "name": _clip_name(content.get("name")) or pk[:12],
            "about": _clip_name(content.get("about")),
            "urls": urls,
            "mints": sorted({m.strip()[:URL_MAX_LEN] for m in tag_values_all(tags, "mint")
                             if m.strip()})[:MAX_MINTS_PER_PROVIDER],
            "version": (tag_value(tags, "version") or "").strip()[:40],
            "class": endpoint_class(urls),
            "test": is_test_marked(content.get("name"), urls),
            "announced": True,
            "first_seen_ts": first_announce[pk],
            "last_announce_ts": ev["created_at"],
        }
    for pk, m in report_meta.items():
        if pk not in providers:
            # Reporting but never (genuinely) announced: classify from the snapshot's
            # own endpoint_urls; named by pubkey prefix.
            urls = [u[:URL_MAX_LEN] for u in (m["last_content"].get("endpoint_urls") or [])
                    if isinstance(u, str)][:MAX_URLS_PER_PROVIDER]
            providers[pk] = {
                "pubkey": pk, "provider_id": None, "name": pk[:12], "about": "",
                "urls": urls, "mints": [], "version": "",
                "class": endpoint_class(urls),
                "test": is_test_marked(None, urls),
                "announced": False, "first_seen_ts": None, "last_announce_ts": None,
            }
    for pk, p in providers.items():
        m = report_meta.get(pk)
        p["last_report_ts"] = m["last_report_ts"] if m else None
        first_led = min((ts for (lpk, g, ts) in ledger if lpk == pk), default=None)
        if first_led is not None and (p["first_seen_ts"] is None or first_led < p["first_seen_ts"]):
            p["first_seen_ts"] = first_led
        # The exclusion bucket, precedence-ordered (one bucket per provider).
        p["excl"] = ("test" if p["test"] else
                     "private" if p["class"] in ("private-only", "none") else None)

    included = {pk for pk, p in providers.items() if p["excl"] is None}

    # ---- kind-38425 (routstr-provider-reviews): latest curated label per node
    review_rows = []
    for ev in sorted(review_events, key=lambda e: (e["created_at"], e["id"])):
        node = tag_value(ev.get("tags"), "node") or tag_value(ev.get("tags"), "p")
        label = None
        for t in ev.get("tags") or []:
            if t and t[0] == "t" and len(t) >= 2:
                label = t[1]
                break
        if not node or not label:
            continue
        review_rows.append({
            "node": node, "label": label.strip().lower()[:24],
            "at": ev["created_at"],
            "reviewer": "curated" if ev["pubkey"] == CURATED_PUBKEY else "other",
            # Persisted (routstr_reviews) but not in the document: the event id and
            # the reviewer key make a timeline row relay-verifiable.
            "id": ev["id"], "reviewer_pubkey": ev["pubkey"],
        })
    latest_review: dict = {}
    for r in review_rows:
        if r["reviewer"] != "curated":
            continue
        cur = latest_review.get(r["node"])
        if cur is None or r["at"] >= cur["at"]:
            latest_review[r["node"]] = r
    for pk, p in providers.items():
        r = latest_review.get(pk)
        p["review"] = r["label"] if r else None

    # ---- kind-38423 (routstr-model-catalog): the curated list + its history
    catalog_versions = [
        ev for ev in sorted(catalog_events, key=lambda e: (e["created_at"], e["id"]))
        if ev["pubkey"] == CURATED_PUBKEY and ev.get("d_tag") == CATALOG_D_TAG
    ]
    catalog_history, catalog_models, catalog_updated = [], [], None
    # Every archived curated version, oldest first, with the membership delta against
    # the previous version - the substrate behind the MCP `catalog_versions` record
    # ("which models were added / removed"). The document keeps only the size history.
    catalog_version_rows = []
    for ev in catalog_versions:
        try:
            c = json.loads(ev["content"])
            models = c.get("models") if isinstance(c, dict) else None
            if not isinstance(models, list):
                continue
            models = sorted({model_key(m) for m in models})[:CATALOG_MAX_MODELS]
        except (ValueError, TypeError):
            continue
        catalog_history.append({"d": _day(ev["created_at"]), "n": len(models)})
        prev = set(catalog_models)
        catalog_version_rows.append({
            "id": ev["id"], "at": ev["created_at"], "models": models,
            "added": sorted(set(models) - prev), "removed": sorted(prev - set(models)),
        })
        catalog_models, catalog_updated = models, _day(ev["created_at"])

    # ---- aggregations over the ledger (included providers only)
    inc_ledger = {k: v for k, v in ledger.items() if k[0] in included}

    day_net: dict = collections.defaultdict(lambda: {"msats": 0, "req": 0, "tok": 0, "prov": set()})
    prov_day: dict = collections.defaultdict(dict)     # pk -> day -> msats
    model_day: dict = collections.defaultdict(dict)    # model -> day -> msats
    model_tot: dict = collections.defaultdict(lambda: collections.Counter())
    prov_tot: dict = collections.defaultdict(collections.Counter)
    cum_events: dict = collections.defaultdict(int)    # date -> msats (weekly at week END)
    # The per-model bucket substrate (routstr_model_days): model -> (grain, day) -> all
    # three measures + the set of included providers reporting it in that bucket. Same
    # grain rule as the provider ledger (weekly rows = a provider's pre-daily era only),
    # so summing every row re-derives the model rows' ALL-TIME figures exactly. Built
    # beside `model_day` (daily msats only, feeding the chart series) rather than
    # replacing it, so the document stays byte-identical to the reference generator's;
    # the invariants tie the two together instead.
    model_days: dict = collections.defaultdict(
        lambda: collections.defaultdict(lambda: {"msats": 0, "req": 0, "tok": 0, "provs": set()}))

    def _acc_model_days(grain, d, models, others, pk):
        for mname, mv in models.items():
            md = model_days[mname][(grain, d)]
            md["msats"] += mv["msats"]
            md["req"] += mv["req"]
            md["tok"] += mv["tok"]
            md["provs"].add(pk)
        if others["req"] or others["msats"] or others["tok"]:
            md = model_days["(other)"][(grain, d)]
            md["msats"] += others["msats"]
            md["req"] += others["req"]
            md["tok"] += others["tok"]
            md["provs"].add(pk)

    # "30d" = the last 30 COMPLETE UTC days: day-aligned, so a run at 17:46 does not
    # lose the 30th day to a partial-day bound (it did: 29 days served as "30d").
    win30_lo = (NOW // DAY_S - 30) * DAY_S  # bucket_ts >= this AND bucket day complete
    for (pk, grain, ts), v in sorted(inc_ledger.items()):
        prov_tot[pk]["msats_alltime"] += v["msats"]
        prov_tot[pk]["req_alltime"] += v["req"]
        prov_tot[pk]["tok_alltime"] += v["tok"]
        for mname, mv in v["models"].items():
            model_tot[mname]["msats_alltime"] += mv["msats"]
            model_tot[mname]["req_alltime"] += mv["req"]
            model_tot[mname]["tok_alltime"] += mv["tok"]
        if v["others"]["req"] or v["others"]["msats"] or v["others"]["tok"]:
            model_tot["(other)"]["msats_alltime"] += v["others"]["msats"]
            model_tot["(other)"]["req_alltime"] += v["others"]["req"]
            model_tot["(other)"]["tok_alltime"] += v["others"]["tok"]
        if grain == "w":
            cum_events[_day(ts + WEEK_S - DAY_S)] += v["msats"]
            _acc_model_days("w", _day(ts), v["models"], v["others"], pk)
            continue
        d = _day(ts)
        _acc_model_days("d", d, v["models"], v["others"], pk)
        cum_events[d] += v["msats"]
        rec = day_net[d]
        rec["msats"] += v["msats"]
        rec["req"] += v["req"]
        rec["tok"] += v["tok"]
        rec["prov"].add(pk)
        prov_day[pk][d] = prov_day[pk].get(d, 0) + v["msats"]
        in30 = ts >= win30_lo
        if in30:
            prov_tot[pk]["msats_30d"] += v["msats"]
            prov_tot[pk]["req_30d"] += v["req"]
            prov_tot[pk]["tok_30d"] += v["tok"]
            for mname, mv in v["models"].items():
                model_tot[mname]["msats_30d"] += mv["msats"]
                model_tot[mname]["req_30d"] += mv["req"]
                model_tot[mname]["tok_30d"] += mv["tok"]
                model_tot[mname]["providers_30d_" + pk] = 1
            if v["others"]["req"] or v["others"]["msats"] or v["others"]["tok"]:
                model_tot["(other)"]["msats_30d"] += v["others"]["msats"]
                model_tot["(other)"]["req_30d"] += v["others"]["req"]
                model_tot["(other)"]["tok_30d"] += v["others"]["tok"]
                model_tot["(other)"]["providers_30d_" + pk] = 1
        for mname, mv in v["models"].items():
            model_day[mname][d] = model_day[mname].get(d, 0) + mv["msats"]
        if v["others"]["msats"]:
            model_day["(other)"][d] = model_day["(other)"].get(d, 0) + v["others"]["msats"]

    # zero-filled contiguous network daily series
    daily = []
    if day_net:
        days = sorted(day_net)
        d0, d1 = dt.date.fromisoformat(days[0]), dt.date.fromisoformat(days[-1])
        d = d0
        while d <= d1:
            k = d.isoformat()
            rec = day_net.get(k)
            daily.append({
                "d": k,
                "msats": rec["msats"] if rec else 0,
                "req": rec["req"] if rec else 0,
                "tok": rec["tok"] if rec else 0,
                "prov": len(rec["prov"]) if rec else 0,
            })
            d += dt.timedelta(days=1)

    # cumulative observed revenue (weekly era + daily), cumsum in msats
    cum_revenue, run = [], 0
    for d in sorted(cum_events):
        run += cum_events[d]
        cum_revenue.append({"d": d, "msats": run})

    def _series(day_map):
        return [{"d": d, "msats": day_map[d]} for d in sorted(day_map)]

    # 12-hex display ids, extended to the FULL pubkey for any colliding prefix set -
    # a 48-bit prefix collision against a known pubkey is grindable, and failing the
    # refresh on one (the old invariant) would have made that a DoS.
    sid = {}
    by_pfx = collections.defaultdict(list)
    for pk in providers:
        by_pfx[pk[:12]].append(pk)
    for pfx, group in by_pfx.items():
        if len(group) == 1:
            sid[group[0]] = pfx
        else:
            for pk in group:
                sid[pk] = pk

    # top providers by 30d revenue (then all-time, then id) + "(others)" aggregate
    inc_sorted = sorted(
        included,
        key=lambda pk: (-prov_tot[pk]["msats_30d"], -prov_tot[pk]["msats_alltime"], pk))
    top_pks = [pk for pk in inc_sorted
               if prov_tot[pk]["msats_alltime"] > 0][:TOP_PROVIDERS]
    others_day: dict = collections.defaultdict(int)
    for pk in included:
        if pk in top_pks:
            continue
        for d, ms in prov_day[pk].items():
            others_day[d] += ms
    provider_daily_top = [
        {"id": sid[pk], "name": providers[pk]["name"], "series": _series(prov_day[pk])}
        for pk in top_pks
    ]
    if any(others_day.values()):
        provider_daily_top.append({"id": "(others)", "name": "(others)",
                                   "series": _series(others_day)})

    # top models by 30d revenue + "(others)" = everything else INCLUDING the buckets'
    # own residual '(other)' aggregation
    model_names = sorted(
        (m for m in model_tot if m != "(other)"),
        key=lambda m: (-model_tot[m]["msats_30d"], -model_tot[m]["msats_alltime"], m))
    top_model_names = [m for m in model_names if model_tot[m]["msats_alltime"] > 0][:TOP_MODELS]
    mo_day: dict = collections.defaultdict(int)
    for m, dm in model_day.items():
        if m in top_model_names:
            continue
        for d, ms in dm.items():
            mo_day[d] += ms
    model_daily_top = [{"id": m, "series": _series(model_day[m])} for m in top_model_names]
    if any(mo_day.values()):
        model_daily_top.append({"id": "(others)", "series": _series(mo_day)})

    def _prov_row(pk):
        p, t = providers[pk], prov_tot[pk]
        return {
            "id": sid[pk], "name": p["name"], "class": p["class"],
            "version": p["version"], "mints": len(p["mints"]),
            "review": p["review"],
            "first_seen": _day(p["first_seen_ts"]) if p["first_seen_ts"] else None,
            "last_announce": _day(p["last_announce_ts"]) if p["last_announce_ts"] else None,
            "last_report": _day(p["last_report_ts"]) if p["last_report_ts"] else None,
            "msats_alltime": t["msats_alltime"], "msats_30d": t["msats_30d"],
            "req_30d": t["req_30d"], "tok_30d": t["tok_30d"],
            "excluded": p["excl"] is not None, "reason": p["excl"],
        }

    # Provider rows are capped (announcements are an open kind, floodable); the
    # summary counts and the exclusion ledger stay uncapped, and `providers_total`
    # tells the page (and the partition invariant) whether the list was truncated.
    providers_rows = [_prov_row(pk) for pk in sorted(
        providers, key=lambda pk: (-prov_tot[pk]["msats_alltime"],
                                   -(providers[pk]["last_announce_ts"] or 0), pk))
    ][:MAX_PROVIDER_ROWS]

    # Named model rows beyond MAX_MODEL_ROWS fold into the "(other)" row together
    # with the buckets' own residuals - "(other)" means "everything not individually
    # listed", and folding preserves every sum the invariants re-derive.
    named_shown = model_names[:MAX_MODEL_ROWS]
    folded = model_names[MAX_MODEL_ROWS:]
    if folded and "(other)" not in model_tot:
        model_tot["(other)"] = collections.Counter()
    for m in folded:
        t = model_tot["(other)"]
        for k2, v2 in model_tot[m].items():
            if k2.startswith("providers_30d_"):
                t[k2] = 1
            else:
                t[k2] += v2

    models_rows = []
    for m in named_shown + (["(other)"] if "(other)" in model_tot else []):
        t = model_tot[m]
        models_rows.append({
            "model": m,
            "msats_30d": t["msats_30d"], "req_30d": t["req_30d"], "tok_30d": t["tok_30d"],
            "msats_alltime": t["msats_alltime"], "req_alltime": t["req_alltime"],
            "tok_alltime": t["tok_alltime"],
            "providers_30d": sum(1 for k in t if k.startswith("providers_30d_")),
        })

    # ---- current-announcement facets (all genuine providers, excluded included - these
    # describe the ANNOUNCED network, not the reported-usage pool)
    ann_pks = [pk for pk, p in providers.items() if p["announced"]]
    transports = collections.Counter(providers[pk]["class"] for pk in ann_pks)
    versions = collections.Counter(
        providers[pk]["version"] or "(untagged)" for pk in ann_pks)
    mint_hosts = collections.Counter()
    all_mints = set()
    for pk in ann_pks:
        hosts = set()
        for mu in providers[pk]["mints"]:
            all_mints.add(mu)
            try:
                h = urlsplit(mu).hostname
            except ValueError:
                h = None
            if h:
                hosts.add(h.lower())
        for h in hosts:
            mint_hosts[h] += 1

    def _top(counter, n):
        head = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:n]
        rest = sum(v for k, v in counter.items() if k not in dict(head))
        out = [{"k": k, "v": v} for k, v in head]
        if rest:
            out.append({"k": "(other)", "v": rest})
        return out

    # ---- success rate: fresh included reporters' trailing-30d summaries, as reported
    succ_total = succ_failed = 0
    fresh_pks = []
    for pk in included:
        m = report_meta.get(pk)
        if not m or m["last_report_ts"] < NOW - 7 * DAY_S:
            continue
        s = ((m["last_content"].get("windows") or {}).get("30d") or {}).get("summary") or {}
        try:
            tot = int(finite_num(s["total_requests"]))
            fail = int(finite_num(s["failed_requests"]))
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if tot < 0 or fail < 0 or fail > tot:
            continue
        succ_total += tot
        succ_failed += fail
        fresh_pks.append(pk)

    # ---- summary + doc
    tot_msats = sum(v["msats"] for v in inc_ledger.values())
    tot_req = sum(v["req"] for v in inc_ledger.values())
    tot_tok = sum(v["tok"] for v in inc_ledger.values())
    w30 = [r for r in daily
           if int(dt.datetime.strptime(r["d"], "%Y-%m-%d")
                  .replace(tzinfo=dt.timezone.utc).timestamp()) >= win30_lo]
    msats_30d = sum(r["msats"] for r in w30)
    req_30d = sum(r["req"] for r in w30)
    tok_30d = sum(r["tok"] for r in w30)
    active_30d = {k[0] for k, v in inc_ledger.items()
                  if k[1] == "d" and k[2] >= win30_lo
                  and (v["msats"] or v["req"] or v["tok"])}
    models_30d = sum(1 for m in model_names if model_tot[m]["msats_30d"]
                     or model_tot[m]["req_30d"] or model_tot[m]["tok_30d"])
    ledger_days = sorted({_day(ts) for (_, g, ts) in inc_ledger if g == "d"}
                         | {_day(ts + WEEK_S - DAY_S) for (_, g, ts) in inc_ledger if g == "w"})

    data = {
        "generated_utc": dt.datetime.fromtimestamp(NOW, dt.timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%SZ"),
        "summary": {
            "archive_events": {
                "announcements": len(announce_events),
                "analytics_daily": len(analytics_in),
                "catalog": len(catalog_events),
                "reviews": len(review_events),
            },
            "providers_announced": len(ann_pks),
            "providers_reporting": len({pk for (pk, _, _) in inc_ledger}),
            "providers_active_30d": len(active_30d),
            "revenue_30d_sats": msats_to_sats(msats_30d),
            "requests_30d": req_30d,
            "tokens_30d": tok_30d,
            "revenue_alltime_sats": msats_to_sats(tot_msats),
            "requests_alltime": tot_req,
            "tokens_alltime": tot_tok,
            "first_activity": ledger_days[0] if ledger_days else None,
            "last_activity": ledger_days[-1] if ledger_days else None,
            "models_30d": models_30d,
            "catalog_models": len(catalog_models) if catalog_models else None,
            "mints": len(all_mints),
            "success_rate_30d": (round(1.0 - succ_failed / succ_total, 4)
                                 if succ_total else None),
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
             "v": len(squatters),
             "why": "kind-38421 (routstr-provider-announcements) events without the "
                    "u+d tag shape every Routstr consumer requires (live example: "
                    "lnproxy fee quotes) - not Routstr providers at all"},
        ],
        "daily": daily,
        "cum_revenue": cum_revenue,
        "provider_daily_top": provider_daily_top,
        "model_daily_top": model_daily_top,
        "providers": providers_rows,
        "providers_total": len(providers),
        "models": models_rows,
        "catalog": {"updated": catalog_updated, "models": catalog_models,
                    "history": catalog_history},
        # kind-38425 (routstr-provider-reviews) is an open kind: the timeline is
        # newest-first and capped, or one flooder grows the document without bound.
        "reviews": [{"node": sid.get(r["node"], r["node"][:12]), "label": r["label"],
                     "d": _day(r["at"]), "reviewer": r["reviewer"],
                     "name": providers.get(r["node"], {}).get("name") or r["node"][:12]}
                    for r in sorted(review_rows, key=lambda r: (-r["at"], r["node"]))
                    ][:MAX_REVIEW_ROWS],
        "transports": _top(transports, 10),
        "versions": _top(versions, TOP_VERSIONS),
        "mints_top": _top(mint_hosts, TOP_MINTS),
        "success": {"rate_30d": (round(1.0 - succ_failed / succ_total, 4)
                                 if succ_total else None),
                    "requests_reported": succ_total, "failed_reported": succ_failed,
                    "providers_fresh": len(fresh_pks)},
        "coverage": {
            "daily_from": min((_day(ts) for (_, g, ts) in inc_ledger if g == "d"),
                              default=None),
            "weekly_from": min((_day(ts) for (_, g, ts) in inc_ledger if g == "w"),
                               default=None),
            "snapshots_used": len(analytics_in),
        },
        "anomalies": anomalies,
    }

    state = {
        "providers": providers, "ledger": ledger, "prov_tot": prov_tot,
        # Substrate-only material (never in the document): see write_pg + check_invariants.
        "model_days": {m: dict(dm) for m, dm in model_days.items()},
        "catalog_versions": catalog_version_rows,
        # The DOCUMENT's order (-at, node) with a total tie-break; a same-second batch
        # of reviews would otherwise order differently in the two lists and trip the
        # prefix invariant (the document itself is unchanged: its sort is stable over
        # the same (created_at, id)-ordered input, so ties already fall by id there).
        "reviews": sorted(review_rows, key=lambda r: (-r["at"], r["node"], r["id"]))[:MAX_REVIEW_TABLE_ROWS],
    }
    return state, data


# --- integrity ------------------------------------------------------------------------------

def check_invariants(state, data) -> list[str]:
    """Cross-checks between the snapshot's own numbers. Every failure is a wrong-metric
    bug - the caller turns a non-empty list into a blocking asset-check failure."""
    problems: list[str] = []
    s = data["summary"]

    if data["cum_revenue"]:
        series = data["cum_revenue"]
        if any(b["msats"] < a["msats"] for a, b in zip(series, series[1:])):
            problems.append("cum_revenue is not monotone")
        if msats_to_sats(series[-1]["msats"]) != s["revenue_alltime_sats"]:
            problems.append("cum_revenue does not end at revenue_alltime_sats")

    if sum(r["msats"] for r in data["daily"]) > (data["cum_revenue"][-1]["msats"]
                                                 if data["cum_revenue"] else 0):
        problems.append("daily msats exceed the cumulative total")

    day_strs = [r["d"] for r in data["daily"]]
    if day_strs != sorted(day_strs):
        problems.append("daily series is not sorted")
    if len(day_strs) != len(set(day_strs)):
        problems.append("daily series has duplicate days")
    if day_strs:
        d0 = dt.date.fromisoformat(day_strs[0])
        d1 = dt.date.fromisoformat(day_strs[-1])
        if len(day_strs) != (d1 - d0).days + 1:
            problems.append("daily series is not contiguous")
        if s["last_activity"] and day_strs[-1] > s["last_activity"]:
            problems.append("daily series extends past last_activity")

    # Re-derive the included-provider all-time total from STATE (the doc's provider
    # list is capped, so summing its rows would false-positive on a flooded registry).
    included_pks = {pk for pk, p in state["providers"].items() if p["excl"] is None}
    state_alltime = sum(state["prov_tot"][pk]["msats_alltime"] for pk in included_pks)
    if state_alltime != (data["cum_revenue"][-1]["msats"] if data["cum_revenue"] else 0):
        problems.append("included providers' msats_alltime do not sum to the cumulative total")

    ids = [p["id"] for p in data["providers"]]
    if len(ids) != len(set(ids)):
        problems.append("provider display ids collide")

    # OUR model aggregation must re-derive from the ledger. The wire-side identity
    # (named + others == bucket totals) is a THIRD-PARTY property - probed exact on
    # live data but not enforceable, so the models rows are NOT compared against the
    # network totals; they are compared against an independent walk of the ledger.
    NOW = int(dt.datetime.strptime(data["generated_utc"], "%Y-%m-%d %H:%M:%SZ")
              .replace(tzinfo=dt.timezone.utc).timestamp())
    win30 = (NOW // DAY_S - 30) * DAY_S
    exp = {"msats_30d": 0, "req_30d": 0, "tok_30d": 0,
           "msats_alltime": 0, "req_alltime": 0, "tok_alltime": 0}
    for (pk, grain, ts), v in state["ledger"].items():
        if pk not in included_pks:
            continue
        oth = v["others"]
        use_oth = oth["req"] or oth["msats"] or oth["tok"]
        for f, key in (("msats", "msats_alltime"), ("req", "req_alltime"), ("tok", "tok_alltime")):
            exp[key] += sum(m[f] for m in v["models"].values()) + (oth[f] if use_oth else 0)
        if grain == "d" and ts >= win30:
            for f, key in (("msats", "msats_30d"), ("req", "req_30d"), ("tok", "tok_30d")):
                exp[key] += sum(m[f] for m in v["models"].values()) + (oth[f] if use_oth else 0)
    for key, want in exp.items():
        got = sum(m[key] for m in data["models"])
        if got != want:
            problems.append(f"models {key} sum {got} != ledger-derived {want}")
    if any(m["msats_alltime"] < 0 or m["req_alltime"] < 0 or m["tok_alltime"] < 0
           for m in data["models"]):
        problems.append("negative model aggregate")

    hist = data["catalog"]["history"]
    if [h["d"] for h in hist] != sorted(h["d"] for h in hist):
        problems.append("catalog history is not sorted")
    if data["catalog"]["models"] and hist and hist[-1]["n"] != len(data["catalog"]["models"]):
        problems.append("catalog history tail != current catalog size")

    if s["providers_active_30d"] > s["providers_reporting"]:
        problems.append("providers_active_30d exceeds providers_reporting")
    if s["first_activity"] and s["last_activity"] and s["first_activity"] > s["last_activity"]:
        problems.append("first_activity after last_activity")
    # The now_ts coverage clamp in build_ledger guarantees this; a future
    # last_activity means the clamp regressed.
    if s["last_activity"] and s["last_activity"] > data["generated_utc"][:10]:
        problems.append("last_activity is in the future")

    rate = s["success_rate_30d"]
    if rate is not None and not (0.0 <= rate <= 1.0):
        problems.append("success_rate_30d out of [0,1]")

    for grp in ("transports", "versions", "mints_top"):
        if any(row["v"] <= 0 for row in data[grp]):
            problems.append(f"{grp} carries a non-positive count")

    # Partition check against STATE (uncapped); the doc-list version only when the
    # list was not truncated by MAX_PROVIDER_ROWS.
    excl_sum = sum(e["v"] for e in data["exclusions"][:2])
    if len(included_pks) + excl_sum != len(state["providers"]):
        problems.append("included + excluded providers != provider registry size")
    if data["providers_total"] == len(data["providers"]):
        incl_rows = [p for p in data["providers"] if not p["excluded"]]
        if len(incl_rows) + excl_sum != len(data["providers"]):
            problems.append("included + excluded provider rows != provider table size")
    if data["providers_total"] != len(state["providers"]):
        problems.append("providers_total != provider registry size")
    # The per-provider chart series partition the network daily series: the page's
    # Top-N selector folds ranks N+1.. into "(others)" client-side and relies on this.
    top_sum: dict = collections.defaultdict(int)
    for p in data["provider_daily_top"]:
        for pt in p["series"]:
            top_sum[pt["d"]] += pt["msats"]
    net_by_day = {r["d"]: r["msats"] for r in data["daily"]}
    if any(top_sum.get(d, 0) != ms for d, ms in net_by_day.items()) or \
            any(d not in net_by_day for d in top_sum):
        problems.append("provider_daily_top series do not sum to the network daily series")

    problems += _substrate_invariants(state, data, win30)
    return problems


def _substrate_invariants(state, data, win30: int) -> list[str]:
    """The substrate tables the MCP tools read must re-derive the document's numbers -
    this is what lets a tool serve a per-model day series or a catalog delta without
    being a second computation of the methodology. Every check here is a wrong-metric
    bug in the SUBSTRATE (the document is already checked above)."""
    problems: list[str] = []
    model_days = state["model_days"]

    def _day_ts(d: str) -> int:
        return int(dt.datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp())

    # (1) model rows: every named row's six measures + providers_30d re-sum from its
    # model_days; the "(other)" row absorbs the buckets' own residual AND the folded
    # tail (named models beyond MAX_MODEL_ROWS), so its expectation is the remainder.
    named = {m["model"] for m in data["models"] if m["model"] != "(other)"}
    folded = set(model_days) - named - {"(other)"}
    for row in data["models"]:
        members = [row["model"]] if row["model"] != "(other)" else ["(other)", *folded]
        exp = {"msats_alltime": 0, "req_alltime": 0, "tok_alltime": 0,
               "msats_30d": 0, "req_30d": 0, "tok_30d": 0}
        provs30: set = set()
        for m in members:
            for (grain, d), rec in model_days.get(m, {}).items():
                exp["msats_alltime"] += rec["msats"]
                exp["req_alltime"] += rec["req"]
                exp["tok_alltime"] += rec["tok"]
                if grain == "d" and _day_ts(d) >= win30:
                    exp["msats_30d"] += rec["msats"]
                    exp["req_30d"] += rec["req"]
                    exp["tok_30d"] += rec["tok"]
                    provs30 |= rec["provs"]
        for key, want in exp.items():
            if row[key] != want:
                problems.append(f"model_days {key} for {row['model']!r} sums to {want} != row {row[key]}")
        if row["providers_30d"] != len(provs30):
            problems.append(f"model_days providers_30d for {row['model']!r} = {len(provs30)} != row {row['providers_30d']}")
    # (2) the chart series: each named top model's series IS its model_days msats;
    # "(others)" is the per-day sum of everything else (zero days dropped both sides).
    top_named = [s for s in data["model_daily_top"] if s["id"] != "(others)"]
    for s in top_named:
        got = {pt["d"]: pt["msats"] for pt in s["series"]}
        want = {d: rec["msats"] for (grain, d), rec in model_days.get(s["id"], {}).items()
                if grain == "d"}
        if got != want:
            problems.append(f"model_daily_top series for {s['id']!r} != model_days")
    rest = set(model_days) - {s["id"] for s in top_named}
    others_want: dict = collections.defaultdict(int)
    for m in rest:
        for (grain, d), rec in model_days[m].items():
            if grain == "d":
                others_want[d] += rec["msats"]
    others_got: dict = {}
    for s in data["model_daily_top"]:
        if s["id"] == "(others)":
            others_got = {pt["d"]: pt["msats"] for pt in s["series"]}
    if {d: v for d, v in others_want.items() if v} != {d: v for d, v in others_got.items() if v}:
        problems.append("model_daily_top '(others)' series != model_days remainder")
    # (3) catalog versions re-derive the document's size history and current list.
    versions = state["catalog_versions"]
    if [{"d": _day(v["at"]), "n": len(v["models"])} for v in versions] != data["catalog"]["history"]:
        problems.append("catalog_versions do not re-derive catalog.history")
    if versions and versions[-1]["models"] != data["catalog"]["models"]:
        problems.append("newest catalog_versions row != catalog.models")
    for prev, cur in zip([None, *versions], versions):
        before = set(prev["models"]) if prev else set()
        if set(cur["added"]) != set(cur["models"]) - before or set(cur["removed"]) != before - set(cur["models"]):
            problems.append(f"catalog version {cur['id'][:12]} added/removed delta is inconsistent")
    # (4) the persisted review timeline is a superset prefix of the document's.
    doc_reviews = data["reviews"]
    state_reviews = state["reviews"]
    if len(state_reviews) < len(doc_reviews):
        problems.append("routstr_reviews holds fewer rows than the document timeline")
    for a, b in zip(doc_reviews, state_reviews):
        if (a["label"], a["d"], a["reviewer"]) != (b["label"], _day(b["at"]), b["reviewer"]):
            problems.append("document review timeline is not a prefix of routstr_reviews")
            break
    return problems


# --- persistence ----------------------------------------------------------------------------

def strip_nul(v):
    """Strip NUL bytes from strings (recursing into containers) - Postgres TEXT/JSONB
    reject the NUL codepoint. Persistence-side ONLY (same posture as
    pipeline/marketplaces.py). Note for the agreement gate: the comparator reads the
    STRIPPED row back from the DB while the reference JSON is unstripped, so a
    NUL-bearing hostile event surfaces as a diff - by design, that is the signal to
    add a deliberate both-sides rule (the archive held zero NULs when this landed)."""
    if isinstance(v, str):
        return v.replace(chr(0), "")
    if isinstance(v, list):
        return [strip_nul(x) for x in v]
    if isinstance(v, dict):
        # Keys too: a NUL in a JSONB object key fails the INSERT exactly like one in
        # a value (routstr_provider_days.models is keyed by third-party model ids).
        return {strip_nul(k): strip_nul(x) for k, x in v.items()}
    return v


def write_pg(conn, state, data, run_id: str, *,
             min_ledger_rows: int = MIN_LEDGER_ROWS,
             min_providers: int = MIN_PROVIDERS) -> dict:
    """Persist one refresh in ONE transaction: TRUNCATE+INSERT routstr_providers +
    routstr_provider_days (the queryable substrate) and append the routstr_snapshot row
    the page serves. Both anti-shrink gates run BEFORE the TRUNCATE."""
    providers, ledger = state["providers"], state["ledger"]
    announced = sum(1 for p in providers.values() if p["announced"])
    if len(ledger) < min_ledger_rows:
        raise RuntimeError(
            f"anti-shrink: ledger rows {len(ledger)} < floor {min_ledger_rows} - refusing "
            "to TRUNCATE the serving tables (LOWER MIN_LEDGER_ROWS deliberately if the "
            "ledger has legitimately shrunk)")
    if announced < min_providers:
        raise RuntimeError(
            f"anti-shrink: announced providers {announced} < floor {min_providers} - "
            "refusing to TRUNCATE the serving tables")

    from psycopg2.extras import Json, execute_values

    def _ts(v):
        return (dt.datetime.fromtimestamp(v, dt.timezone.utc) if v is not None else None)

    prow = [(
        pk, strip_nul(p["provider_id"]), strip_nul(p["name"]), strip_nul(p["about"]),
        p["class"], strip_nul(p["version"]), Json(strip_nul(p["urls"])),
        Json(strip_nul(p["mints"])), p["announced"], strip_nul(p["review"]),
        _ts(p["first_seen_ts"]), _ts(p["last_announce_ts"]), _ts(p["last_report_ts"]),
        p["excl"] is not None, p["excl"],
        state["prov_tot"][pk]["msats_alltime"], state["prov_tot"][pk]["msats_30d"],
        state["prov_tot"][pk]["req_30d"], state["prov_tot"][pk]["tok_30d"],
        state["prov_tot"][pk]["req_alltime"], state["prov_tot"][pk]["tok_alltime"],
    ) for pk, p in sorted(providers.items())]

    lrow = [(
        pk, grain, dt.datetime.fromtimestamp(ts, dt.timezone.utc).date(),
        v["msats"], v["req"], v["tok"],
        Json(strip_nul(v["models"])), Json(strip_nul(v["others"])),
    ) for (pk, grain, ts), v in sorted(ledger.items())]

    mrow = [(
        strip_nul(m), grain, dt.date.fromisoformat(d),
        rec["msats"], rec["req"], rec["tok"], len(rec["provs"]),
    ) for m, dm in sorted(state["model_days"].items()) for (grain, d), rec in sorted(dm.items())]

    crow = [(
        v["id"], _ts(v["at"]), len(v["models"]), Json(strip_nul(v["models"])),
        Json(strip_nul(v["added"])), Json(strip_nul(v["removed"])),
    ) for v in state["catalog_versions"]]

    rrow = [(
        r["id"], strip_nul(r["node"])[:128], strip_nul(r["label"]), _ts(r["at"]),
        r["reviewer_pubkey"], r["reviewer"] == "curated",
    ) for r in state["reviews"]]

    generated = dt.datetime.strptime(data["generated_utc"], "%Y-%m-%d %H:%M:%SZ").replace(
        tzinfo=dt.timezone.utc)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE routstr_providers")
        execute_values(cur, """
            INSERT INTO routstr_providers (
              pubkey, provider_id, name, about, endpoint_class, version, urls, mints,
              announced, review, first_seen_utc, last_announce_utc, last_report_utc,
              excluded, excl_reason, msats_alltime, msats_30d, requests_30d, tokens_30d,
              requests_alltime, tokens_alltime
            ) VALUES %s""", prow, page_size=500)
        cur.execute("TRUNCATE routstr_provider_days")
        execute_values(cur, """
            INSERT INTO routstr_provider_days (
              pubkey, grain, day, msats, requests, tokens, models, others
            ) VALUES %s""", lrow, page_size=1000)
        cur.execute("TRUNCATE routstr_model_days")
        execute_values(cur, """
            INSERT INTO routstr_model_days (
              model, grain, day, msats, requests, tokens, providers
            ) VALUES %s""", mrow, page_size=1000)
        cur.execute("TRUNCATE routstr_catalog_versions")
        execute_values(cur, """
            INSERT INTO routstr_catalog_versions (
              event_id, version_utc, model_count, models, added, removed
            ) VALUES %s""", crow, page_size=500)
        cur.execute("TRUNCATE routstr_reviews")
        execute_values(cur, """
            INSERT INTO routstr_reviews (
              event_id, node, label, reviewed_utc, reviewer_pubkey, curated
            ) VALUES %s""", rrow, page_size=1000)
        cur.execute(
            "INSERT INTO routstr_snapshot (generated_utc, run_id, data) VALUES (%s, %s, %s)",
            (generated, run_id, Json(strip_nul(data))))
    conn.commit()
    return {"provider_rows": len(prow), "ledger_rows": len(lrow), "model_day_rows": len(mrow),
            "catalog_version_rows": len(crow), "review_rows": len(rrow)}

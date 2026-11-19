"""The independent sides of the nutzaps battery.

Neither side is the MCP server's SQL:

1. **SQL over the RAW tables**, written from `schema/init.sql`: every kind-9321
   (nutzaps) PAYMENT row (`value_source = 'proof_tags'`, all mints, excluded ones
   included), every kind-10019 (nutzap-configs) row, and `nutzap_mint_classes`,
   fetched ONCE and read whole. `Truth` then recomputes every window, rank, sum
   and distinct count in Python - the population rule (payments on mints not
   marked excluded), the latest-config-per-pubkey rule, the listing floor and the
   ordering are applied HERE, from the design record, never by the tool's SQL.
2. **The live website** - the numbers `/nutzaps`, `/nutzaps/mints` (+ the
   `/api/nutzaps?period=` JSON it is built from) and `/nutzaps/mints/<mint>`
   render, read back out of the SvelteKit SSR payload (`node` evaluates the
   `kit.start(...)` options literal). The site reads the same Postgres through its
   OWN SQL (nostrology-web `src/lib/db/postgres.ts`), on ROLLING windows
   (`created_at >= NOW() - N days`); `Truth.web_*` restates those rules exactly,
   evaluated at the instant each page was fetched.

Access is a read-only login on the box (`ssh "$NOSTROLOGY_RO_SSH"` -> `psql` as
`nostrology_ro`), through `truth_sql.run_sections`. Page fetches carry the
User-Agent mark (`USER_AGENT` below) so they read as first-party in Caddy's
access log, the web twin of the MCP `traffic_type=internal` marker.
"""
import datetime as dt
import json
import os
import re
import signal
import subprocess
import tempfile
import time
from collections import defaultdict
from urllib.parse import quote

from . import truth_sql
from .routstr_truth import _balanced_object

BASE = "https://nostrolo.gy"
# The first-party mark for the site: a script's requests carry a `nozzl-verify-<script>`
# User-Agent, which the access log and the traffic reports recognise, so these fetches
# never read as anonymous visitors. (The access log's `operator=1` tag is the operator's
# signed-in browsing alone: nostrology-web deploy/Caddyfile -> (operator-marker).)
USER_AGENT = "nozzl-verify-nutzaps-battery"
UTC = dt.timezone.utc
DAY = 86400
YEAR_SECONDS = 365 * DAY
# The page's All-view listing floor (nostrology-web NUTZAP_ALL_TIME_CONFIG_FLOOR;
# design record sec 3): a mint with no payment is listed at All only when this many
# users' latest kind-10019 (nutzap-configs) names it. A window drops it to 1.
ALL_TIME_CONFIG_FLOOR = 10
ROW_CAP = 2000  # the capped nutzaps metrics (design record sec 3)
RETRY_DELAY = 3  # seconds before retrying an ssh-level failure (times the attempt number)

# ---------------------------------------------------------------------------
# 1. SQL over the raw tables
# ---------------------------------------------------------------------------
# FP is also run ALONE after the MCP and page reads: if the nightly refresh moved
# the tables in between, the comparison is between two datasets and the run aborts.
FINGERPRINT_SQL = """
    select (select count(*) from events_kind_9321_nutzaps) as all_rows,
           (select max(created_at) from events_kind_9321_nutzaps) as max_created_all,
           (select count(*) from events_kind_9321_nutzaps where value_source = 'proof_tags') as pay_rows,
           (select max(created_at) from events_kind_9321_nutzaps where value_source = 'proof_tags') as max_created_pay,
           (select coalesce(sum(amount_sats), 0) from events_kind_9321_nutzaps where value_source = 'proof_tags') as pay_sats,
           (select count(*) from events_kind_10019_nutzap_configs) as cfg_rows,
           (select max(created_at) from events_kind_10019_nutzap_configs) as max_created_cfg,
           (select md5(coalesce(string_agg(mint_url || '=' || excluded::text, ',' order by mint_url), ''))
              from nutzap_mint_classes) as classes_md5,
           extract(epoch from now())::bigint as db_now
"""

SECTIONS = {
    "FP": FINGERPRINT_SQL,
    # Every PAYMENT, on every mint - the excluded-mint rule is applied in Python.
    "PAYMENTS": """
        select created_at, pubkey, mint_url, amount_sats
        from events_kind_9321_nutzaps
        where value_source = 'proof_tags'
        order by created_at, event_id
    """,
    # How much of the table is NOT a payment - context for the population checks.
    "SOURCES": """
        select value_source, count(*) as n, coalesce(sum(amount_sats), 0) as sats
        from events_kind_9321_nutzaps group by 1 order by 1
    """,
    "CLASSES": """
        select mint_url, mint_class, excluded, class_source
        from nutzap_mint_classes order by mint_url
    """,
    # EVERY config row; the latest per pubkey is chosen in Python (ties detected).
    "CONFIGS": """
        select pubkey, created_at, mint_urls
        from events_kind_10019_nutzap_configs
        order by pubkey, created_at desc, event_id
    """,
    # The DATABASE's string order of every mint URL. The leaderboard's last key,
    # "then mint_url", is ORDER BY under the database collation (en_US.UTF-8 on
    # Aurora: punctuation ignored at the first level, so mint1.example.com sorts
    # before mint.21linz.at), not code-point order - fetched as data so the Python
    # sort breaks ties exactly as that collation does, and nothing else.
    "MINT_ORDER": """
        select mint_url, row_number() over (order by mint_url) as rank
        from (select mint_url from events_kind_9321_nutzaps
              union select unnest(mint_urls) from events_kind_10019_nutzap_configs
              union select mint_url from nutzap_mint_classes) u
    """,
    "COLLATION": """
        select datcollate, datctype from pg_database where datname = current_database()
    """,
    # The PAID table - what nostrology_query_nutzaps and the /nutzaps MAU chart read,
    # rebuilt by Dagster from the rows above (schema/nutzap_active_users.sql). Not a
    # truth for the free tool: read to check the paid line, i.e. that the free
    # per-mint counts bracket it in every bucket and that it counts the senders the
    # raw rows hold. A failing check names buckets and the side that broke - never
    # a count - so no paid or network-wide figure reaches the report.
    "PAID_AU": """
        select granularity, (to_timestamp(period_start) at time zone 'UTC')::date as period_start, unique_users
        from active_user_counts_kind_9321_nutzaps
        order by 1, 2
    """,
}


def _ssh_argv():
    remote = (f"PGCONNECT_TIMEOUT=60 psql -h {truth_sql.PGHOST} -U {truth_sql.PGUSER} -d {truth_sql.PGDATABASE} "
              f"-v ON_ERROR_STOP=1 -f -")
    return ["ssh", "-o", "ConnectTimeout=30", truth_sql.ssh_host(), remote]


def run_script(script, timeout=900, argv=None, attempts=3):
    """_run_once, retried when SSH ITSELF failed (exit 255 - e.g. the SSM session
    closing during setup, 'Connection closed by UNKNOWN port 65535', seen
    2026-09-30); a psql error exits 1-3 and is never retried."""
    for n in range(attempts):
        try:
            return _run_once(script, timeout, argv)
        except _SshFailed as e:
            last = e
            time.sleep(RETRY_DELAY * (n + 1))
    raise RuntimeError(f"psql over ssh failed {attempts} times, last: {last}")


class _SshFailed(RuntimeError):
    pass


def _run_once(script, timeout=900, argv=None):
    """truth_sql.run_script without its hang. Over SSH-over-SSM, the ProxyCommand's
    session-manager-plugin inherits ssh's stderr and can outlive ssh (orphaned to
    pid 1); with capture_output that pipe never reaches EOF and subprocess.run
    blocks forever (seen 2026-09-30 on the fingerprint re-read). Here stdout and
    stderr are FILES - only ssh's own exit is awaited - and ssh runs in its own
    session, whose process group (the plugin included) is killed afterwards."""
    with tempfile.TemporaryDirectory() as d:
        src, out, err = (os.path.join(d, n) for n in ("in.sql", "out.txt", "err.txt"))
        with open(src, "w") as fh:
            fh.write(script)
        with open(src) as fin, open(out, "w") as fout, open(err, "w") as ferr:
            proc = subprocess.Popen(argv or _ssh_argv(), stdin=fin, stdout=fout, stderr=ferr, start_new_session=True)
            try:
                rc = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                rc = None
            finally:
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except (ProcessLookupError, PermissionError):
                    pass
                if rc is None:  # reap the ssh the group kill just ended
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
        text, errs = open(out).read(), open(err).read()
    if rc == 255:
        raise _SshFailed(f"ssh failed (255): {errs[-800:]}")
    if rc != 0:
        raise RuntimeError(f"psql over ssh {'timed out' if rc is None else f'failed ({rc})'}: {errs[-800:]}")
    return truth_sql.parse(text)


def run_sql():
    return run_script(truth_sql.build_sections_script(SECTIONS))


def run_fingerprint():
    return run_script(truth_sql.build_sections_script({"FP": FINGERPRINT_SQL}))["FP"][0]


def fingerprint_moved(before, after):
    """The fields that differ between two fingerprints (db_now excluded)."""
    return sorted(k for k in before if k != "db_now" and before.get(k) != after.get(k))


# ---------------------------------------------------------------------------
# Day arithmetic (UTC by construction)
# ---------------------------------------------------------------------------
def day_of(epoch):
    return dt.datetime.fromtimestamp(int(epoch), UTC).date().isoformat()


def epoch_of(day):
    d = dt.date.fromisoformat(day)
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp())


def shift(day, n):
    return (dt.date.fromisoformat(day) + dt.timedelta(days=n)).isoformat()


def days(first, last):
    out, d = [], first
    while d <= last:
        out.append(d)
        d = shift(d, 1)
    return out


def next_month(month):
    """The first of the month after `month`, itself the first of a month."""
    return (dt.date.fromisoformat(month) + dt.timedelta(days=31)).replace(day=1).isoformat()


def bounds(frm=None, to=None):
    """Inclusive UTC calendar days -> half-open epoch bounds [lo, hi); None = open."""
    return (epoch_of(frm) if frm else None, epoch_of(shift(to, 1)) if to else None)


def bucket(day, grain):
    """The UTC bucket start a day falls in: the day, its ISO week's Monday, its month's 1st."""
    d = dt.date.fromisoformat(day)
    if grain == "week":
        d = d - dt.timedelta(days=d.weekday())
    elif grain == "month":
        d = d.replace(day=1)
    return d.isoformat()


# ---------------------------------------------------------------------------
# Views - every method recomputes from the raw rows
# ---------------------------------------------------------------------------
class Truth:
    """Python over the raw sections. `pays` is the counted population (payments on
    mints nutzap_mint_classes does not mark excluded; a mint missing from that table
    counts); `raw` keeps every payment, for the checks that must SEE the excluded
    rows the tool is supposed to leave out."""

    def __init__(self, sql):
        self.fp = sql["FP"][0]
        self.classes = {r["mint_url"]: r for r in sql["CLASSES"]}
        self.excluded = {m for m, r in self.classes.items() if r["excluded"]}
        self.sources = {r["value_source"]: r for r in sql.get("SOURCES", [])}
        self.raw = [(int(r["created_at"]), r["pubkey"], r["mint_url"], int(r["amount_sats"]), day_of(r["created_at"]))
                    for r in sql["PAYMENTS"]]
        self.pays = [p for p in self.raw if p[2] not in self.excluded]
        latest, ties = {}, set()
        for r in sql["CONFIGS"]:
            pk, t, mints = r["pubkey"], int(r["created_at"]), frozenset(r["mint_urls"] or [])
            cur = latest.get(pk)
            if cur is None or t > cur[0]:
                latest[pk] = (t, mints)
                ties.discard(pk)
            elif t == cur[0] and mints != cur[1]:
                ties.add(pk)  # two latest configs disagree: the "latest" is not unique
        self.configs = latest
        self.config_ties = ties
        # the database's string order (MINT_ORDER); code-point order when absent
        self.mint_rank = {r["mint_url"]: int(r["rank"]) for r in sql.get("MINT_ORDER", [])}
        # the PAID table, grain -> {bucket start: unique_users}; empty when a cached
        # raw read predates the section (the paid-line checks then fail, never pass)
        self.paid_au = defaultdict(dict)
        for r in sql.get("PAID_AU", []):
            self.paid_au[r["granularity"]][r["period_start"]] = int(r["unique_users"])
        self.collation = (sql.get("COLLATION") or [{}])[0].get("datcollate", "code point (no MINT_ORDER)")

    def url_key(self, m):
        return (self.mint_rank.get(m, float("inf")), m)

    # -- selection ----------------------------------------------------------
    def select(self, lo=None, hi=None, mint=None, raw=False):
        src = self.raw if raw else self.pays
        return [p for p in src if (lo is None or p[0] >= lo) and (hi is None or p[0] < hi)
                and (mint is None or p[2] == mint)]

    def window(self, frm=None, to=None, mint=None):
        lo, hi = bounds(frm, to)
        return self.select(lo, hi, mint)

    def first_day(self):
        return min(p[4] for p in self.pays) if self.pays else None

    # -- payment_activity ---------------------------------------------------
    def daily(self, frm=None, to=None, mint=None):
        """utc_day -> (payments, sats), sparse, sorted - payment_activity@day."""
        out = {}
        for _, _, _, v, d in self.window(frm, to, mint):
            x = out.setdefault(d, [0, 0])
            x[0] += 1
            x[1] += v
        return {d: tuple(out[d]) for d in sorted(out)}

    def total(self, frm=None, to=None, mint=None):
        """(payments, sats, mints, first_day, last_day) - payment_activity@total."""
        ps = self.window(frm, to, mint)
        ds = [p[4] for p in ps]
        return (len(ps), sum(p[3] for p in ps), len({p[2] for p in ps}),
                min(ds) if ds else None, max(ds) if ds else None)

    # -- mint_leaderboard ---------------------------------------------------
    @staticmethod
    def per_mint(ps):
        """mint -> (payments, sats)."""
        acc = {}
        for _, _, m, v, _ in ps:
            x = acc.setdefault(m, [0, 0])
            x[0] += 1
            x[1] += v
        return {m: (x[0], x[1]) for m, x in acc.items()}

    def configured_by(self, lo=None, hi=None):
        """mint -> distinct pubkeys whose LATEST config lists it, the config's
        created_at inside [lo, hi) (None = unbounded)."""
        out = defaultdict(int)
        for t, mints in self.configs.values():
            if (lo is None or t >= lo) and (hi is None or t < hi):
                for m in mints:
                    out[m] += 1
        return dict(out)

    def _leaderboard(self, lo, hi, floor):
        pm = self.per_mint(self.select(lo, hi))
        cb = self.configured_by(lo, hi)
        rows = []
        for m in set(pm) | set(cb):
            if m in self.excluded:
                continue
            p, s = pm.get(m, (0, 0))
            c = cb.get(m, 0)
            if p >= 1 or c >= floor:
                rows.append((m, p, s, c))
        return rows

    def leaderboard(self, frm=None, to=None):
        """[(mint_url, payments, sats, configured_by)] in the documented order:
        sats, payments, configured_by desc, then mint_url asc. No window = the All
        floor; any from/to = floor 1. (No senders column since the 2026-09-30
        review: over a one-day window it was per-mint DAU, the paid number.)"""
        windowed = frm is not None or to is not None
        lo, hi = bounds(frm, to)
        rows = self._leaderboard(lo, hi, 1 if windowed else ALL_TIME_CONFIG_FLOOR)
        return sorted(rows, key=lambda r: (-r[2], -r[1], -r[3], self.url_key(r[0])))

    # -- mint_activity / mint_active_users -----------------------------------
    def mint_activity(self, frm=None, to=None, mint=None):
        """(utc_day, mint_url) -> (payments, sats)."""
        out = {}
        for _, _, m, v, d in self.window(frm, to, mint):
            x = out.setdefault((d, m), [0, 0])
            x[0] += 1
            x[1] += v
        return {k: tuple(v) for k, v in out.items()}

    def active_users(self, grain, frm=None, to=None, mint=None):
        """(period_start, mint_url) -> distinct senders; a window keeps the buckets
        STARTING inside [frm, to] (inclusive days), each bucket whole."""
        acc = defaultdict(set)
        for _, pk, m, _, d in self.select(None, None, mint):
            b = bucket(d, grain)
            if (frm is None or b >= frm) and (to is None or b <= to):
                acc[(b, m)].add(pk)
        return {k: len(v) for k, v in acc.items()}

    def network_active_users(self, grain):
        """bucket start -> distinct senders ACROSS every counted mint, all history:
        what the paid table holds per bucket, recomputed from the raw rows."""
        acc = defaultdict(set)
        for _, pk, _, _, d in self.pays:
            acc[bucket(d, grain)].add(pk)
        return {b: len(v) for b, v in acc.items()}

    def distinct_senders(self, frm=None, to=None):
        return len({p[1] for p in self.window(frm, to)})

    # -- the website's own rules (rolling, evaluated at the fetch instant) ----
    def config_count(self, mint):
        """Latest configs listing the mint, all time (the page's Configured By)."""
        return sum(1 for _, mints in self.configs.values() if mint in mints)

    def has_page(self, mint):
        """The web's has_page / nutzapMintIsListed: all-time payments (any mint class)
        or the All config floor; the listing's excluded test is separate."""
        return (len(self.select(None, None, mint, raw=True)) >= 1
                or self.config_count(mint) >= ALL_TIME_CONFIG_FLOOR)

    def web_leaderboard(self, cutoff=None):
        """SQL_NUTZAP_MINT_STATS: payments and configs with created_at >= cutoff (NO
        upper bound), the All floor when cutoff is None, excluded never listed.
        mint -> (config_count, payment_count, total_amount_sats, has_page)."""
        floor = ALL_TIME_CONFIG_FLOOR if cutoff is None else 1
        return {m: (c, p, s, self.has_page(m)) for m, p, s, c in self._leaderboard(cutoff, None, floor)}

    def web_daily(self, now, mint=None):
        """SQL_NUTZAPS_DAILY_SERIES (mint None) / SQL_NUTZAP_MINT_DAILY (a mint):
        created_at >= now - 365 d, a calendar from the cutoff's day to today (so the
        OLDEST day is partial and a future-dated row falls off the calendar), zero-
        filled, running sums. The per-mint query applies no excluded filter (its
        page 404s for an excluded mint instead). [(day, n, cum_n, sats, cum_sats)]."""
        cutoff = now - YEAR_SECONDS
        src = self.select(cutoff, None, mint, raw=mint is not None)
        by = {}
        for _, _, _, v, d in src:
            x = by.setdefault(d, [0, 0])
            x[0] += 1
            x[1] += v
        out, cn, cs = [], 0, 0
        for d in days(day_of(cutoff), day_of(now)):
            n, s = by.get(d, (0, 0))
            cn += n
            cs += s
            out.append((d, n, cn, s, cs))
        return out

    def web_mint_mau(self, now, mint):
        """SQL_NUTZAP_MINT_MAU: distinct senders per UTC month over created_at >=
        now - 365 d. Its months come from the clock, not the rows, so a created_at past
        now adds none. Every month has a row, 0 where it had no sender, the current one
        included; the oldest, which the window cuts, has one only where it has a sender."""
        acc = defaultdict(set)
        for _, pk, _, _, d in self.select(now - YEAR_SECONDS, None, mint, raw=True):
            acc[d[:7] + "-01"].add(pk)
        oldest, newest = day_of(now - YEAR_SECONDS)[:7] + "-01", day_of(now)[:7] + "-01"
        out, m = [], oldest
        while m <= newest:
            if acc[m] or m != oldest:
                out.append((m, len(acc[m])))
            m = next_month(m)
        return out

    def web_mint_detail(self, now, mint):
        """getNutzapMintDetail, or None where the page 404s."""
        allp = self.select(None, None, mint, raw=True)
        cfg = self.config_count(mint)
        if mint in self.excluded or not (len(allp) >= 1 or cfg >= ALL_TIME_CONFIG_FLOOR):
            return None
        daily = self.web_daily(now, mint)
        return {"mint_url": mint, "total_payments": daily[-1][2], "total_sats": daily[-1][4],
                "config_count": cfg, "all_time_payments": len(allp), "all_time_sats": sum(p[3] for p in allp),
                "daily": daily, "mau": self.web_mint_mau(now, mint)}

    def web_top_mints(self, now):
        """SQL_NUTZAPS_TOP_MINTS_CUMULATIVE's ranking input: mint -> trailing-365 d
        sats (rolling cutoff, counted population). The page has NO tie-break."""
        acc = defaultdict(int)
        for _, _, m, v, _ in self.select(now - YEAR_SECONDS, None):
            acc[m] += v
        return dict(acc)

    def web_mint_cumulative(self, now, mint):
        return [(d, cs) for d, _, _, _, cs in self.web_daily(now, mint)]


# ---------------------------------------------------------------------------
# Comparison rules (pure; pinned by the unit tests)
# ---------------------------------------------------------------------------
def bracket_violations(per_mint, network):
    """The paid line as a rule: per bucket, no mint's distinct senders exceed the
    count deduplicated across mints, and their sum never falls below it (a sender
    counts once per mint they used). per_mint: {(bucket, mint): n}; network:
    {bucket: n}. Also every bucket on one side must be on the other. Each message
    names the bucket and the side that broke, NEVER a count: `network` is the
    paid series, and these messages land in a committed report."""
    by = defaultdict(list)
    for (b, _), n in per_mint.items():
        by[b].append(n or 0)
    out = [f"{b}: per-mint rows but no network count" for b in sorted(set(by) - set(network))]
    out += [f"{b}: a network count but no per-mint row" for b in sorted(set(network) - set(by))]
    for b in sorted(set(by) & set(network)):
        if max(by[b]) > network[b]:
            out.append(f"{b}: a mint above the network count")
        elif sum(by[b]) < network[b]:
            out.append(f"{b}: the sum over mints below the network count")
    return out


def paid_vs_raw(paid, raw):
    """Buckets where the paid table and the raw re-count disagree, with the side
    that broke and never a count (both are the network-wide series)."""
    def why(b):
        if b not in paid:
            return "missing from the paid table"
        if b not in raw:
            return "in the paid table, not in the raw rows"
        return "paid below raw" if paid[b] < raw[b] else "paid above raw"
    return [f"{b}: {why(b)}" for b in sorted(set(paid) | set(raw)) if paid.get(b) != raw.get(b)]


def sum_matches(per_mint, network):
    """(buckets where the per-mint counts SUM exactly to the network count, buckets):
    the share of the paid series a caller can rebuild from the free rows."""
    by = defaultdict(int)
    for (b, _), n in per_mint.items():
        by[b] += n or 0
    return sum(1 for b, n in network.items() if by.get(b) == n), len(network)


def dense(series, first, last, width=2):
    """A sparse {day: tuple} as a dense list over first..last (zeros filled)."""
    return [(d, *series.get(d, (0,) * width)) for d in days(first, last)]


def running(series, first, last):
    """Running (payments, sats) sums of a sparse {day: (p, s)} over first..last."""
    out, a, b = [], 0, 0
    for d in days(first, last):
        p, s = series.get(d, (0, 0))
        a += p
        b += s
        out.append((d, a, b))
    return out


def bracket_rows(web, inner, outer):
    """A rolling-window table against the two whole-day windows that bracket it.
    Each arg is mint -> tuple of monotone measures. OK when every mint the inner
    window lists is on the page, every page mint is in the outer window, and each
    measure sits between the inner and outer value. Returns the violations
    (empty = inside the bracket)."""
    bad = []
    for m in sorted(set(web) | set(inner) | set(outer)):
        w, i, o = web.get(m), inner.get(m), outer.get(m)
        if i is not None and w is None:
            bad.append(f"{m}: in the inner window, missing on the page")
        elif w is not None and o is None:
            bad.append(f"{m}: on the page, missing from the outer window")
        elif w is not None:
            lo = i if i is not None else (0,) * len(w)
            if not all(a <= b <= c for a, b, c in zip(lo, w, o)):
                bad.append(f"{m}: page {w} outside [{lo}, {o}]")
    return bad


def topn_consistent(web_ranked, truth_totals, n):
    """The page ranks by a sum with NO tie-break, so only the TOTALS of its top N
    are determined. OK when the page's first n (mint, total) pairs carry exactly the
    truth's n largest totals and every total equals the truth's for that mint."""
    head = web_ranked[:n]
    want = sorted(truth_totals.values(), reverse=True)[:n]
    return (sorted((t for _, t in head), reverse=True) == want
            and all(truth_totals.get(m) == t for m, t in head))


def topn_bracket(web_head, inner, outer):
    """The page's top-N set against the whole-day windows around its rolling cutoff:
    each page mint's total within [inner, outer], and no mint left OFF the page could
    have outranked the page's smallest total even with its whole boundary day."""
    if not web_head:
        return False
    for m, t in web_head:
        if not inner.get(m, 0) <= t <= outer.get(m, 0):
            return False
    floor = min(t for _, t in web_head)
    shown = {m for m, _ in web_head}
    return all(inner.get(m, 0) <= floor for m in set(inner) | set(outer) if m not in shown)


# ---------------------------------------------------------------------------
# 2. The live website
# ---------------------------------------------------------------------------
_STATUS_MARK = "__NUTZAPS_BATTERY_STATUS__"


def fetch(path, timeout=180):
    """GET a page with the first-party User-Agent mark; returns the status and the
    [t0, t1] epoch bracket the server's NOW() fell inside."""
    t0 = time.time()
    proc = subprocess.run(
        ["curl", "-sS", "--compressed", "--max-time", str(timeout),
         "-A", USER_AGENT,
         "-w", "\n" + _STATUS_MARK + "%{http_code}", BASE + path],
        capture_output=True, text=True)
    t1 = time.time()
    if proc.returncode != 0:
        raise RuntimeError(f"fetch {path} failed: {proc.stderr[-300:]}")
    body, _, status = proc.stdout.rpartition("\n" + _STATUS_MARK)
    return {"path": path, "status": int(status), "body": body, "t0": int(t0), "t1": int(t1) + 1}


def kit_data(html):
    """The page's own load data (the LEAF node) from the SvelteKit SSR bootstrap:
    the `kit.start(app, element, {...})` options object is a JS literal (unquoted
    keys, devalue output), so `node` evaluates it rather than a regex guessing."""
    m = re.search(r"kit\.start\(\s*app\s*,\s*element\s*,\s*\{", html or "")
    if not m:
        return None
    literal = _balanced_object(html, m.end() - 1)
    proc = subprocess.run(
        ["node", "-e", "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{"
                       "process.stdout.write(JSON.stringify((0,eval)('('+s+')').data))})"],
        input=literal, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"node could not evaluate the page payload: {proc.stderr[-300:]}")
    nodes = [n for n in json.loads(proc.stdout) if n and n.get("type") == "data"]
    return nodes[-1]["data"] if nodes else None


def mint_href(url):
    """nostrology-web mintHref: /nutzaps/mints/<host[:port]><path>, https implied,
    an http mint adds ?scheme=http; %, ? and # are percent-encoded."""
    m = re.match(r"^(https?)://(.*)$", url)
    scheme, rest = (m.group(1), m.group(2)) if m else ("https", url)
    path = re.sub(r"[%?#]", lambda c: quote(c.group(0), safe=""), rest)
    return f"/nutzaps/mints/{path}" + ("?scheme=http" if scheme == "http" else "")


def collect_web(subject_mints, excluded_mint):
    """Every page the battery reads. subject_mints: role -> mint URL (listed mints)."""
    out = {}

    def page(key, path, parse):
        r = fetch(path)
        out[key] = {"path": path, "status": r["status"], "t0": r["t0"], "t1": r["t1"],
                    "data": parse(r["body"]) if r["status"] == 200 else None}

    page("overview", "/nutzaps", kit_data)
    page("mints_page", "/nutzaps/mints", kit_data)
    for period in ("all", "7d", "30d", "1y"):
        page("api_" + period, f"/api/nutzaps?period={period}", json.loads)
    for role, mint in subject_mints.items():
        if mint:
            page("mint_" + role, mint_href(mint), kit_data)
    if excluded_mint:
        page("mint_excluded", mint_href(excluded_mint), lambda body: None)
    return out

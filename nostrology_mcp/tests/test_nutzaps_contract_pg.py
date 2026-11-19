"""Real-SQL tier for the free nutzaps domain - a throwaway local Postgres, no Aurora.

The pure tests (test_nutzaps.py) pin the SQL's SHAPE; this tier runs every nutzaps
metric's REAL statement THROUGH THE WIRED TOOL (server path, window/cap wrappers,
envelope, CSV) over a small hand-built population and compares each answer with an
independent Python recomputation over the same fixture rows (`Ref` below: plain
loops, no SQL). What it catches that nothing else does: a UTC-day boundary cut in
the wrong zone, a population predicate dropped from one statement, the Configured By
"latest config" rule, the listing floors, day/week/month bucket starts (UTC midnight,
Monday, the 1st), the bucket-start window rule, the row-cap plumbing, and that the
free per-mint active users and the PAID table's own rebuild (schema/
nutzap_active_users.sql) count the same senders.

Needs `NOSTROLOGY_CONTRACT_DSN` pointing at a scratch Postgres the test may write to
(e.g. `docker run -d -e POSTGRES_PASSWORD=contract -e POSTGRES_DB=nostrology
-e POSTGRES_INITDB_ARGS='--locale-provider=icu --icu-locale=en-US-u-ka-shifted'
-p 127.0.0.1:15441:5432 postgres:16-alpine` ->
`postgresql://postgres:contract@127.0.0.1:15441/nostrology`, sorting text as the deploy
gate and production do); skips otherwise. The
DDL is schema/init.sql's own nutzaps section (single source). NEVER point this at
the dev cluster.
"""

import csv
import datetime as dt
import functools
import io
import os
from collections import defaultdict

import pytest

from nostrology_mcp import server as srv_mod
from nostrology_mcp.db import fetch_metric

DSN = os.environ.get("NOSTROLOGY_CONTRACT_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="contract tier: set NOSTROLOGY_CONTRACT_DSN")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

A = "https://mint.a.example"  # absent from nutzap_mint_classes: counts
B = "https://mint.b.example/Bitcoin"  # 'unreachable': counted class
C = "https://mint.c.example"  # configs only (>= 10): listed with no window
D = "https://mint.d.example"  # 3 configs, no payment: listed only inside a window
X = "http://127.0.0.1:3338"  # private-host: excluded
T = "https://testnut.example"  # fake-backend: excluded


def ts(y, m, d, hh=12, mm=0, ss=0) -> int:
    return int(dt.datetime(y, m, d, hh, mm, ss, tzinfo=dt.timezone.utc).timestamp())


def pk(n: int) -> str:
    return f"{n:064x}"


# (sender, created_at, mint, sats, value_source) - value_source 'proof_tags' = a payment
PAYMENTS = [
    (pk(1), ts(2026, 8, 31, 23, 59, 59), A, 10, "proof_tags"),
    (pk(1), ts(2026, 9, 1, 0, 0, 0), A, 20, "proof_tags"),  # the UTC-day boundary
    (pk(1), ts(2026, 9, 1, 9), B, 5, "proof_tags"),  # same sender, second mint
    (pk(2), ts(2026, 9, 1, 23, 59, 59), A, 7, "proof_tags"),
    (pk(2), ts(2026, 9, 1, 12), A, 4, "proof_tags"),  # pk(2) twice on A that day: DISTINCT != COUNT
    (pk(2), ts(2026, 9, 7, 0, 0, 1), A, 3, "proof_tags"),  # Monday 2026-09-07
    (pk(3), ts(2026, 9, 6, 23, 0), B, 100, "proof_tags"),  # Sunday: week of 08-31
    (pk(4), ts(2026, 7, 15), A, 1, "proof_tags"),
    (pk(4), ts(2026, 7, 16), A, 1, "proof_tags"),
    (pk(5), ts(2025, 12, 31, 23, 30), B, 42, "proof_tags"),
    (pk(6), ts(2026, 9, 2), X, 999, "proof_tags"),  # excluded mint
    (pk(7), ts(2026, 9, 2), T, 888, "proof_tags"),  # excluded mint
]
NON_PAYMENTS = [
    (pk(8), ts(2026, 9, 1, 10), A, 0, "none"),
    (pk(9), ts(2026, 9, 1, 11), B, 0, "none"),
]

# (pubkey, created_at, [mints]) - several versions per pubkey; only the LATEST counts
CONFIGS = [
    (pk(1), ts(2026, 1, 1), [A, B]),
    (pk(1), ts(2026, 9, 3), [A]),  # latest: A only - B loses pk(1)
    (pk(2), ts(2026, 9, 5), [B]),
    (pk(3), ts(2025, 6, 1), [B, X]),
    *[(pk(100 + i), ts(2026, 3, 1 + i), [C]) for i in range(10)],  # 10 configs for C
    *[(pk(200 + i), ts(2026, 9, 10 + i), [D]) for i in range(3)],  # 3 recent for D
    (pk(300), ts(2026, 9, 20), [T, A]),
]
CLASSES = [
    (B, "unreachable", False, "registry"),
    (X, "private-host", True, "host-rule"),
    (T, "fake-backend", True, "registry"),
]


# -- the independent reference ---------------------------------------------------------

EXCLUDED = {m for m, _, ex, _ in CLASSES if ex}


def day(epoch: int) -> dt.date:
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).date()


def payments(mint=None, lo=None, hi=None):
    """Counted payments; lo/hi = inclusive UTC days."""
    for s, t, m, sats, vs in PAYMENTS + NON_PAYMENTS:
        if vs != "proof_tags" or m in EXCLUDED:
            continue
        if mint is not None and m != mint:
            continue
        if lo is not None and day(t) < lo:
            continue
        if hi is not None and day(t) > hi:
            continue
        yield s, t, m, sats


def latest_configs():
    latest = {}
    for p, t, ms in CONFIGS:
        if p not in latest or t > latest[p][0]:
            latest[p] = (t, ms)
    return latest


def ref_day(mint=None, lo=None, hi=None):
    agg = defaultdict(lambda: [0, 0])
    for _, t, _, sats in payments(mint, lo, hi):
        agg[day(t)][0] += 1
        agg[day(t)][1] += sats
    return [(d.isoformat(), str(c), str(s)) for d, (c, s) in sorted(agg.items())]


def ref_total(mint=None, lo=None, hi=None):
    rows = list(payments(mint, lo, hi))
    days = [day(t) for _, t, _, _ in rows]
    return [(
        str(len(rows)),
        str(sum(r[3] for r in rows)),
        str(len({r[2] for r in rows})),
        min(days).isoformat() if days else "\\N",
        max(days).isoformat() if days else "\\N",
    )]


def ref_mint_activity(mint=None, lo=None, hi=None):
    agg = defaultdict(lambda: [0, 0])
    for _, t, m, sats in payments(mint, lo, hi):
        agg[(day(t), m)][0] += 1
        agg[(day(t), m)][1] += sats
    rows = sorted(agg.items(), key=lambda kv: (kv[0][0], -kv[1][1], kv[0][1]))
    return [(d.isoformat(), m, str(c), str(s)) for (d, m), (c, s) in rows]


def ref_leaderboard(mint=None, lo=None, hi=None):
    windowed = lo is not None or hi is not None
    pay = defaultdict(lambda: [0, 0])
    for _, t, m, sats in payments(None, lo, hi):
        pay[m][0] += 1
        pay[m][1] += sats
    conf = defaultdict(set)
    for p, (t, ms) in latest_configs().items():
        if lo is not None and day(t) < lo:
            continue
        if hi is not None and day(t) > hi:
            continue
        for m in ms:
            conf[m].add(p)
    floor = 1 if windowed else 10
    rows = []
    for m in set(pay) | set(conf):
        c, s = pay[m] if m in pay else (0, 0)
        n_conf = len(conf.get(m, ()))
        if m in EXCLUDED or not (c >= 1 or n_conf >= floor):
            continue
        if mint is not None and m != mint:
            continue
        rows.append((m, c, s, n_conf))
    rows.sort(key=lambda r: (-r[2], -r[1], -r[3], r[0]))
    return [tuple(str(v) for v in r) for r in rows]


def bucket(d: dt.date, grain: str) -> dt.date:
    """The UTC bucket a day falls in: itself, its ISO week's Monday, its month's 1st."""
    if grain == "week":
        return d - dt.timedelta(days=d.weekday())
    return d.replace(day=1) if grain == "month" else d


def ref_active_users(mint=None, lo=None, hi=None, grain="month"):
    agg = defaultdict(set)
    for s, t, m, _ in payments(mint):  # buckets over ALL history, then the window
        agg[(bucket(day(t), grain), m)].add(s)
    rows = [
        (b, m, len(ss)) for (b, m), ss in agg.items()
        if (lo is None or b >= lo) and (hi is None or b <= hi)
    ]
    rows.sort(key=lambda r: (r[0], -r[2], r[1]))
    return [(b.isoformat(), m, str(n)) for b, m, n in rows]


# -- the database side -------------------------------------------------------------------

def _nutzap_ddl() -> str:
    """schema/init.sql's nutzaps section, verbatim (the loader contract tier's cut)."""
    with open(os.path.join(_REPO_ROOT, "schema", "init.sql")) as fh:
        sql = fh.read()
    start = sql.index("-- kind-9321 (nutzaps) (NIP-61 (nutzaps)): one row per nutzap-shaped event.")
    end_marker = "ON active_user_counts_kind_9321_nutzaps (granularity);"
    return sql[start:sql.index(end_marker, start) + len(end_marker)] + "\n"


@pytest.fixture(scope="module")
def db():
    import psycopg2

    if "127.0.0.1" not in DSN and "localhost" not in DSN:
        pytest.fail("the contract tier only ever writes to a LOCAL scratch database")
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS events_kind_9321_nutzaps, events_kind_10019_nutzap_configs, "
                    "nutzap_mint_classes, active_user_counts_kind_9321_nutzaps")
        cur.execute(_nutzap_ddl())
        for i, (s, t, m, sats, vs) in enumerate(PAYMENTS + NON_PAYMENTS):
            cur.execute(
                "INSERT INTO events_kind_9321_nutzaps (event_id, pubkey, created_at, mint_url, "
                "amount_sats, value_source, no_value_reason) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (f"{i:064x}", s, t, m, sats, vs, None if vs == "proof_tags" else "no_proof_tags"),
            )
        for i, (p, t, ms) in enumerate(CONFIGS):
            cur.execute(
                "INSERT INTO events_kind_10019_nutzap_configs (event_id, pubkey, created_at, mint_urls) "
                "VALUES (%s,%s,%s,%s)",
                (f"{i + 5000:064x}", p, t, ms),
            )
        for m, cls, ex, src in CLASSES:
            cur.execute(
                "INSERT INTO nutzap_mint_classes (mint_url, mint_class, excluded, class_source) "
                "VALUES (%s,%s,%s,%s)",
                (m, cls, ex, src),
            )
    conn.close()
    return True


def _skewed_dsn() -> str:
    """The DSN with the session at UTC+14, so a day or bucket cut in the SESSION
    zone instead of 'UTC' moves the fixture's boundary rows and fails - on a UTC
    session (the docker default, and Aurora's) the pin would be untestable."""
    sep = "&" if "?" in DSN else "?"
    return f"{DSN}{sep}options=-c%20TimeZone%3DPacific/Kiritimati"


@pytest.fixture
def tool(db, monkeypatch, tmp_path):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(srv_mod, "fetch_metric", functools.partial(fetch_metric, dsn=_skewed_dsn()))

    def call(metric, grain=None, frm=None, to=None, mint=None):
        out = srv_mod._query_metric_impl(
            metric, "nutzaps", grain, frm, to, None, None, None, None, "csv", None, mint_url=mint
        )
        preamble, table = out.split("\n\n", 1)
        rows = list(csv.reader(io.StringIO(table)))
        return preamble, [tuple(r) for r in rows[1:]]

    return call


WINDOWS = [
    (None, None),
    ("2026-09-01", "2026-09-01"),
    ("2026-08-31", "2026-09-06"),
    ("2026-09-01", None),
    (None, "2026-07-31"),
    ("2020-01-01", "2020-12-31"),  # empty
]


def _d(s):
    return dt.date.fromisoformat(s) if s else None


def _hi(to):
    # an omitted 'to' defaults to today UTC - later than every fixture row
    return _d(to) if to else None


@pytest.mark.parametrize("frm,to", WINDOWS)
@pytest.mark.parametrize("mint", [None, A, B, X])
def test_payment_activity_day(tool, frm, to, mint):
    _, rows = tool("payment_activity", frm=frm, to=to, mint=mint)
    assert rows == ref_day(mint, _d(frm), _hi(to))


@pytest.mark.parametrize("frm,to", WINDOWS)
@pytest.mark.parametrize("mint", [None, A, B, T])
def test_payment_activity_total(tool, frm, to, mint):
    _, rows = tool("payment_activity", "total", frm=frm, to=to, mint=mint)
    assert rows == ref_total(mint, _d(frm), _hi(to))


def test_the_total_equals_the_day_rows_and_the_leaderboard(tool):
    _, days = tool("payment_activity")
    _, total = tool("payment_activity", "total")
    _, board = tool("mint_leaderboard")
    assert int(total[0][0]) == sum(int(r[1]) for r in days) == sum(int(r[1]) for r in board)
    assert int(total[0][1]) == sum(int(r[2]) for r in days) == sum(int(r[2]) for r in board)


@pytest.mark.parametrize("frm,to", WINDOWS)
@pytest.mark.parametrize("mint", [None, A, B, C, D, X])
def test_mint_leaderboard(tool, frm, to, mint):
    _, rows = tool("mint_leaderboard", frm=frm, to=to, mint=mint)
    assert rows == ref_leaderboard(mint, _d(frm), _hi(to))


def test_mint_leaderboard_listing_rules_by_name(tool):
    """The floors and the latest-config rule, spelled out rather than only via Ref."""
    _, all_time = tool("mint_leaderboard")
    by_mint = {r[0]: r for r in all_time}
    assert set(by_mint) == {A, B, C}  # D has 3 configs (< 10), X/T are excluded
    assert by_mint[B][3] == "2"  # pk(2) + pk(3): pk(1)'s LATEST config dropped B
    _, sept = tool("mint_leaderboard", frm="2026-09-01", to="2026-09-30")
    assert D in {r[0] for r in sept}  # a window drops the floor to one config
    assert C not in {r[0] for r in sept}  # C's configs are all from March, no payment


@pytest.mark.parametrize("frm,to", WINDOWS)
@pytest.mark.parametrize("mint", [None, A, B])
def test_mint_activity_day(tool, frm, to, mint):
    _, rows = tool("mint_activity", frm=frm, to=to, mint=mint)
    assert rows == ref_mint_activity(mint, _d(frm), _hi(to))


@pytest.mark.parametrize("frm,to", WINDOWS + [("2026-09-02", "2026-09-30"), ("2026-07-01", "2026-08-31"),
                                     ("2026-09-01", "2026-09-06"), ("2026-09-07", "2026-09-07")])
@pytest.mark.parametrize("mint", [None, A, B])
@pytest.mark.parametrize("grain", [None, "day", "week", "month"])
def test_mint_active_users(tool, grain, frm, to, mint):
    _, rows = tool("mint_active_users", grain, frm=frm, to=to, mint=mint)
    assert rows == ref_active_users(mint, _d(frm), _hi(to), grain or "month")


def test_an_omitted_grain_is_month(tool):
    preamble, rows = tool("mint_active_users")
    assert "grain: month" in preamble and rows == ref_active_users(grain="month")


def test_a_window_keeps_whole_months(tool):
    # from=09-02 excludes September (it STARTED 09-01) even though it overlaps
    _, windowed = tool("mint_active_users", frm="2026-09-02", to="2026-09-30", mint=B)
    assert windowed == []
    _, sept = tool("mint_active_users", frm="2026-09-01", to="2026-09-01", mint=B)
    assert sept == [("2026-09-01", B, "2")]


def test_a_window_keeps_whole_weeks(tool):
    # The week of Monday 08-31 STARTED before from=09-01, so it is excluded whole
    # although five of its days lie inside; a window naming only its Monday keeps
    # it whole, Sunday 09-06 included.
    _, windowed = tool("mint_active_users", "week", frm="2026-09-01", to="2026-09-30")
    assert windowed == [("2026-09-07", A, "1")]
    _, monday = tool("mint_active_users", "week", frm="2026-08-31", to="2026-08-31")
    assert monday == [("2026-08-31", A, "2"), ("2026-08-31", B, "2")]


def test_weeks_start_on_monday_utc(tool):
    """Sunday 09-06 23:00 UTC is in the week of Monday 08-31 - on the UTC+14
    session it is already Monday 09-07 locally, so a bucket cut in the session
    zone moves it; a Sunday-start week would move it too. Monday 09-07 00:00:01
    UTC opens the next week."""
    _, rows = tool("mint_active_users", "week", mint=B)
    assert rows == [("2025-12-29", B, "1"), ("2026-08-31", B, "2")]
    _, rows = tool("mint_active_users", "week", frm="2026-09-07", mint=A)
    assert rows == [("2026-09-07", A, "1")]


def test_days_cut_at_utc_midnight(tool):
    # 08-31 23:59:59 and 09-01 00:00:00 UTC are one second and one day apart
    _, rows = tool("mint_active_users", "day", frm="2026-08-31", to="2026-09-01", mint=A)
    assert rows == [("2026-08-31", A, "1"), ("2026-09-01", A, "2")]


def test_mint_active_users_refuses_a_grain_it_does_not_serve(tool):
    with pytest.raises(ValueError, match=r"serves grain day \| week \| month, not 'total'"):
        tool("mint_active_users", "total")


@pytest.mark.parametrize("mint", [X, T, "https://no-such-mint.invalid"])
def test_a_filter_that_matches_nothing_says_so_over_real_sql(tool, mint):
    for metric, grain in [("payment_activity", "day"), ("payment_activity", "total"),
                          ("mint_leaderboard", "total"), ("mint_activity", "day"),
                          ("mint_active_users", "day"), ("mint_active_users", "week"),
                          ("mint_active_users", "month")]:
        preamble, _ = tool(metric, grain, mint=mint)
        window = [ln for ln in preamble.splitlines() if ln.startswith("window:")][0]
        assert "nothing matches the filter - a mint_url returns nothing" in window, (metric, window)
        assert "no rows in source table" not in window


def test_a_sender_on_two_mints_counts_at_both(tool):
    _, rows = tool("mint_active_users", frm="2026-09-01", to="2026-09-01")
    sept = {r[1]: int(r[2]) for r in rows}
    assert sept[A] == 2 and sept[B] == 2  # pk(1) is in both
    assert sum(sept.values()) > len({s for s, *_ in payments(None, dt.date(2026, 9, 1), dt.date(2026, 9, 30))})
    # the same on one DAY: pk(1) paid through A at 00:00 and through B at 09:00
    _, rows = tool("mint_active_users", "day", frm="2026-09-01", to="2026-09-01")
    assert {r[1]: int(r[2]) for r in rows} == {A: 2, B: 1}  # pk(1)+pk(2) on A, pk(1) on B: 3 > 2 senders


# The PAID table, rebuilt by its own SQL on this fixture. It deduplicates senders
# ACROSS mints; the free per-mint counts must bracket it in every bucket - no mint
# above it, the sum over mints never below it - and equal it wherever each sender
# used one mint. A population drift between the two statements (a predicate dropped
# from one, a different bucket rule) breaks the bracket or the exact count.
@pytest.fixture(scope="module")
def paid_table(db):
    import psycopg2

    conn = psycopg2.connect(_skewed_dsn())  # the rebuild's own UTC pin, at UTC+14 too
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            with open(os.path.join(_REPO_ROOT, "schema", "nutzap_active_users.sql")) as fh:
                cur.execute(fh.read())
            cur.execute("SELECT granularity, period_start, unique_users FROM active_user_counts_kind_9321_nutzaps")
            out = defaultdict(dict)
            for g, start, n in cur.fetchall():
                out[g][dt.datetime.fromtimestamp(start, dt.timezone.utc).date().isoformat()] = n
    finally:
        conn.close()
    return out


@pytest.mark.parametrize("grain", ["day", "week", "month"])
def test_per_mint_counts_bracket_the_paid_network_count(tool, paid_table, grain):
    _, rows = tool("mint_active_users", grain)
    per_bucket = defaultdict(list)
    for b, _, n in rows:
        per_bucket[b].append(int(n))
    network = defaultdict(set)
    for s, t, _, _ in payments():
        network[bucket(day(t), grain).isoformat()].add(s)
    assert paid_table[grain] == {b: len(ss) for b, ss in network.items()}  # the paid rebuild, recomputed
    assert set(per_bucket) == set(paid_table[grain])
    for b, counts in per_bucket.items():
        assert max(counts) <= paid_table[grain][b] <= sum(counts), (grain, b)
    # ...and summing mints over-counts exactly where a sender used two mints in the bucket
    over = {b for b, counts in per_bucket.items() if sum(counts) > paid_table[grain][b]}
    assert over == ({"2026-09-01"} if grain == "day" else {"2026-08-31"} if grain == "week" else {"2026-09-01"})


def test_excluded_mints_never_surface(tool):
    for metric, grain in [("payment_activity", "day"), ("payment_activity", "total"),
                          ("mint_leaderboard", "total"), ("mint_activity", "day"),
                          ("mint_active_users", "day"), ("mint_active_users", "week"),
                          ("mint_active_users", "month")]:
        for mint in (X, T):
            _, rows = tool(metric, grain, mint=mint)
            if metric == "payment_activity" and grain == "total":
                assert rows == [("0", "0", "0", "\\N", "\\N")]
            else:
                assert rows == []
        _, rows = tool(metric, grain)
        assert not any(X in r or T in r for r in rows)


def test_the_envelope_reports_the_row_cap(tool, monkeypatch):
    from nostrology_mcp import metric as metric_mod

    spec = metric_mod.METRICS["nutzaps.mint_activity@day"]
    monkeypatch.setitem(
        metric_mod.METRICS, spec.key,
        metric_mod.MetricSpec(**{**spec.__dict__, "row_cap": 2}),
    )
    preamble, rows = tool("mint_activity")
    assert len(rows) == 2
    total = len(ref_mint_activity())
    assert f"rows: 2 of {total}" in preamble and "truncated: showing the first 2" in preamble


@pytest.mark.parametrize("metric,grain", [
    ("mint_activity", "day"), ("mint_leaderboard", "total"),
    ("mint_active_users", "day"), ("mint_active_users", "week"), ("mint_active_users", "month"),
])
def test_a_capped_window_counts_the_window_not_the_table(tool, monkeypatch, metric, grain):
    """`rows: N of M` must count the WINDOW. A total taken before the window (the
    whole table) printed a truncated: line on windowed calls the cap never touched,
    and nothing failed (review, 2026-09-30). The window below holds fewer rows than
    the whole table, so the two totals differ, and more than the lowered cap."""
    from nostrology_mcp import metric as metric_mod

    frm, to = "2026-07-01", "2026-09-30"
    preamble, full = tool(metric, grain, frm=frm, to=to)
    _, everything = tool(metric, grain, frm="2020-01-01", to=to)
    assert 2 < len(full) < len(everything), (len(full), len(everything))
    assert f"rows: {len(full)}  |" in preamble
    assert not [ln for ln in preamble.splitlines() if ln.startswith("truncated:")]
    key = f"nutzaps.{metric}@{grain}"
    spec = metric_mod.METRICS[key]
    # An in-statement cap (the day_inline / epoch_inline specs) binds its own LIMIT
    # from bind_params, pinned equal to row_cap by test_bind_params_are_not_shared_
    # between_specs; the day_wrap wrapper reads row_cap. Lower both, as one.
    binds = {**spec.bind_params, **({"row_cap": 2} if "row_cap" in spec.bind_params else {})}
    monkeypatch.setitem(metric_mod.METRICS, key,
                        metric_mod.MetricSpec(**{**spec.__dict__, "row_cap": 2, "bind_params": binds}))
    preamble, rows = tool(metric, grain, frm=frm, to=to)
    assert rows == full[:2]
    assert f"rows: 2 of {len(full)}  |" in preamble and "truncated: showing the first 2" in preamble

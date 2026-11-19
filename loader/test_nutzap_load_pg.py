"""kind-9321 (nutzaps) load into a real Postgres: the loader CLI, the schema, the mint
classification and the active-users rebuild, end to end (nostrology-data#8, #9).

Run:  NOSTROLOGY_CONTRACT_DSN=postgresql://postgres:contract@127.0.0.1:15439/nostrology \
        <python> -m pytest loader/test_nutzap_load_pg.py -q
      with a Python that has psycopg2, pytest and PyYAML (the mint registry is YAML; the
      Dagster image has all three, a workstation's .venv-mcp lacks PyYAML)
      against a throwaway local Postgres 16, e.g.
        docker run -d --name nutzap-contract-pg -e POSTGRES_PASSWORD=contract \
          -e POSTGRES_INITDB_ARGS='--locale-provider=icu --icu-locale=en-US-u-ka-shifted' \
          -e POSTGRES_DB=nostrology -p 127.0.0.1:15439:5432 postgres:16-alpine
      (the locale is the deploy gate's, which sorts text as production does)
      Skips without the DSN. Refuses any non-local host. NEVER point it at the dev cluster.

What the pure tests (test_nutzap_value.py) cannot see, this pins:
  - the loader's COPY column list matches the live table (a mismatch fails the whole
    load in production, after the TRUNCATE);
  - each real fixture event lands with the expected amount_sats / value_source /
    no_value_reason, and the non-nutzap is dropped;
  - schema/nutzap_active_users.sql counts only payments, so the draft-era, encrypted,
    token and zero-ecash senders add no active user;
  - the ALTERs in init.sql upgrade a table written before #8 in place, idempotently,
    marking its old rows 'unclassified' rather than passing them off as payments;
  - the CHECK constraints refuse an undefined value_source and any row whose
    amount, source and reason disagree;
  - one hostile or unforeseen event (a huge amount, an oversized or NUL-carrying
    `p`/`e`/`u` tag, an exception in the value rule) degrades to a no-value row, a
    NULL reference or a dropped row, and never aborts the load (which has already
    TRUNCATEd the serving table);
  - --classify-nutzap-mints writes one row per loaded mint URL (both kinds) with the
    class the registry and host rule give it, the MAU rebuild drops excluded mints and
    counts unlisted ones, and a malformed registry changes nothing (#9).
"""

import json
import os
import random
import subprocess
import sys
import tempfile

import pytest

DSN = os.environ.get("NOSTROLOGY_CONTRACT_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="contract tier: set NOSTROLOGY_CONTRACT_DSN")

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
_LOADER = os.path.join(_HERE, "load_jsonl_exports_into_postgres.py")
_REGISTRY = os.path.join(_REPO_ROOT, "data", "nutzaps", "mint_registry.yaml")
with open(os.path.join(_HERE, "fixtures", "nutzap_wire_families.json"), encoding="utf-8") as _fh:
    FIXTURES = json.load(_fh)["fixtures"]


def _read(*parts):
    with open(os.path.join(_REPO_ROOT, *parts), encoding="utf-8") as fh:
        return fh.read()


def _nutzap_ddl() -> str:
    """The kind-9321 (nutzaps) and kind-10019 (nutzap-configs) tables with their ALTERs
    and indexes, the mint classes table, and the active-users table: verbatim from schema/init.sql, from the
    kind-9321 (nutzaps) banner to the active-users index."""
    sql = _read("schema", "init.sql")
    start = sql.index("-- kind-9321 (nutzaps) (NIP-61 (nutzaps)): one row per nutzap-shaped event.")
    end_marker = "ON active_user_counts_kind_9321_nutzaps (granularity);"
    end = sql.index(end_marker, start) + len(end_marker)
    return sql[start:end] + "\n"


@pytest.fixture(scope="module")
def conn():
    import psycopg2

    if "127.0.0.1" not in DSN and "localhost" not in DSN:
        pytest.fail("the contract tier only ever writes to a LOCAL scratch database")
    c = psycopg2.connect(DSN)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS events_kind_9321_nutzaps, events_kind_10019_nutzap_configs, "
                    "nutzap_mint_classes, active_user_counts_kind_9321_nutzaps")
        cur.execute(_nutzap_ddl())
    yield c
    c.close()


def _run_loader(paths, *extra, kind="9321"):
    proc = subprocess.run(
        [sys.executable, _LOADER, "--kind", kind, "--db-url", DSN, "--truncate",
         "--no-blocklist", "--input", *paths, *extra],
        capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return proc.stdout


def _classify(registry=_REGISTRY):
    return subprocess.run(
        [sys.executable, _LOADER, "--classify-nutzap-mints", registry, "--db-url", DSN],
        capture_output=True, text=True, timeout=120,
    )


# The payments the MAU rebuild counts: proof-tag rows not on an excluded mint.
_COUNTED = """FROM events_kind_9321_nutzaps n
              WHERE value_source = 'proof_tags'
                AND NOT EXISTS (SELECT 1 FROM nutzap_mint_classes c
                                 WHERE c.mint_url = n.mint_url AND c.excluded)"""


@pytest.fixture(scope="module")
def loaded(conn):
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "k9321.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            for f in FIXTURES:
                fh.write(json.dumps(f["event"]) + "\n")
        out = _run_loader([path])
    with conn.cursor() as cur:
        cur.execute("SELECT event_id, amount_sats, value_source, no_value_reason FROM events_kind_9321_nutzaps")
        rows = {r[0]: r[1:] for r in cur.fetchall()}
    return rows, out


def test_every_fixture_lands_as_expected(loaded):
    rows, _ = loaded
    for f in FIXTURES:
        want = f["expected"]
        eid = f["event"]["id"]
        if not want["loaded"]:
            assert eid not in rows, f["family"]
            continue
        assert rows[eid] == (want["amount_sats"], want["value_source"], want["no_value_reason"]), f["family"]


def test_the_load_summary_accounts_for_every_row(loaded):
    rows, out = loaded
    payments = sum(1 for r in rows.values() if r[1] == "proof_tags")
    sats = sum(r[0] for r in rows.values())
    assert f"Value: {payments:,} events with proof-tag value ({sats:,} sats), {len(rows) - payments:,} with none" in out
    # The lnuts fixture's amount tag is 1000x its proofs: counted, logged, not used.
    assert "amount tag disagrees with their proof sum (not used for value)" in out


def test_served_value_drops_to_the_proof_sum(loaded):
    rows, _ = loaded
    before = sum(f["served_before_fix"] for f in FIXTURES if f["served_before_fix"] is not None)
    after = sum(r[0] for r in rows.values())
    want = sum(f["expected"]["amount_sats"] for f in FIXTURES if f["expected"]["loaded"])
    assert after == want
    assert before - after == 1000 + 76000 + 5000 + 50000 + 1 + 50 + 1 + 1  # the eight phantom families


def test_active_users_count_payments_only(conn, loaded):
    rows, _ = loaded
    with conn.cursor() as cur:
        cur.execute(_read("schema", "nutzap_active_users.sql"))
        cur.execute("SELECT sum(total_events), count(*) FROM active_user_counts_kind_9321_nutzaps WHERE granularity = 'day'")
        total_events, days = cur.fetchone()
        cur.execute(f"""SELECT count(*), count(DISTINCT date_trunc('day', to_timestamp(created_at) AT TIME ZONE 'UTC'))
                          {_COUNTED}""")
        want_events, want_days = cur.fetchone()
        # A sender whose only events carry no ecash (the encrypted test burst, here)
        # is not an active user in that month.
        enc = next(f["event"] for f in FIXTURES if f["family"] == "encrypted")
        cur.execute("""SELECT unique_users FROM active_user_counts_kind_9321_nutzaps
                        WHERE granularity = 'month'
                          AND period_start = extract(epoch FROM date_trunc('month', to_timestamp(%s) AT TIME ZONE 'UTC'))""",
                    (enc["created_at"],))
        enc_month = cur.fetchone()
    assert total_events == want_events
    assert days == want_days
    assert enc_month is None


@pytest.mark.parametrize("amount,source,reason", [
    (0, "amount_tag", None),            # a source nobody defined
    (0, "proof_tags", None),            # a payment worth nothing
    (5, "proof_tags", "encrypted"),     # a payment with a no-value reason
    (5, "none", "encrypted"),           # a non-payment with value
    (0, "none", None),                  # a non-payment without a reason
])
def test_check_constraints_refuse_an_inconsistent_row(conn, loaded, amount, source, reason):
    import psycopg2

    with conn.cursor() as cur, pytest.raises(psycopg2.errors.CheckViolation):
        cur.execute("""INSERT INTO events_kind_9321_nutzaps
                         (event_id, pubkey, created_at, mint_url, amount_sats, value_source, no_value_reason)
                       VALUES (%s, %s, 1, 'https://m', %s, %s, %s)""",
                    ("e" * 64, "p" * 64, amount, source, reason))


def test_a_hostile_event_cannot_abort_the_load(conn, loaded):
    """The load TRUNCATEs and drops indexes (committed) before the COPY, so an exception
    on one event would leave the serving table EMPTY on every refresh. A 5,000-digit
    string amount used to make int() raise (Python 3.11+ digit limit)."""
    poison = dict(next(f["event"] for f in FIXTURES if f["family"] == "amethyst"))
    poison["id"] = "d" * 64
    poison["tags"] = [["u", "https://mint.example"],
                      ["proof", json.dumps({"amount": "1" * 5000, "id": "k", "secret": "s", "C": "c"})]]
    # Oversized / NUL-carrying references, which a CHAR(64) or text column would refuse.
    bad_refs = dict(poison, id="c" * 64,
                    tags=[["u", "https://mint.example"], ["p", "ab" * 40], ["e", "ab" * 31 + "a\x00"],
                          ["proof", json.dumps({"amount": 2, "id": "k", "secret": "s", "C": "c"})]])
    nul_mint = dict(poison, id="b" * 64, tags=[["u", "https://mint.example\x00"]])
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "k9321.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            for f in FIXTURES:
                fh.write(json.dumps(f["event"]) + "\n")
            for e in (poison, bad_refs, nul_mint):
                fh.write(json.dumps(e) + "\n")
        _run_loader([path])
    with conn.cursor() as cur:
        cur.execute("SELECT amount_sats, value_source, no_value_reason FROM events_kind_9321_nutzaps WHERE event_id = %s",
                    ("d" * 64,))
        assert cur.fetchone() == (0, "none", "invalid-proof-tag")
        cur.execute("SELECT recipient_pubkey, target_event_id, amount_sats FROM events_kind_9321_nutzaps WHERE event_id = %s",
                    ("c" * 64,))
        assert cur.fetchone() == (None, None, 2)
        cur.execute("SELECT count(*) FROM events_kind_9321_nutzaps")
        # poison + bad_refs loaded; nul_mint dropped as no usable mint
        assert cur.fetchone()[0] == sum(f["expected"]["loaded"] for f in FIXTURES) + 2


def test_an_exception_in_the_value_rule_degrades_the_event_not_the_load(conn, monkeypatch):
    """The structural half of the guarantee: whatever _nutzap_value raises, the loader
    records that event as a no-value row and loads the rest."""
    import psycopg2

    sys.path.insert(0, _HERE)
    import load_jsonl_exports_into_postgres as L

    def boom(e):
        raise RuntimeError("unforeseen wire format")

    monkeypatch.setattr(L, "_nutzap_value", boom)
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "k9321.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            for f in FIXTURES:
                fh.write(json.dumps(f["event"]) + "\n")
        c = psycopg2.connect(DSN)
        try:
            loaded_n = L.load_kind_9321_nutzaps([path], c, truncate=True, db_url=DSN)
        finally:
            c.close()
    assert loaded_n == sum(f["expected"]["loaded"] for f in FIXTURES)
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT value_source, no_value_reason FROM events_kind_9321_nutzaps")
        assert cur.fetchall() == [("none", "invalid-proof-tag")]


def test_alters_upgrade_a_pre_fix_table_in_place(conn):
    """The production table predates #8 (nutzaps: value falls back to the amount tag when no
    proof parses). The schema apply must add the columns to it, mark its old rows
    'unclassified' (never 'proof_tags'), and be safe to run again."""
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS pre_fix CASCADE; CREATE SCHEMA pre_fix; SET search_path = pre_fix")
        try:
            cur.execute("""CREATE TABLE events_kind_9321_nutzaps (
                event_id CHAR(64) PRIMARY KEY, pubkey CHAR(64) NOT NULL, created_at BIGINT NOT NULL,
                recipient_pubkey CHAR(64), target_event_id CHAR(64), mint_url TEXT NOT NULL,
                amount_sats BIGINT NOT NULL DEFAULT 0)""")
            cur.execute("INSERT INTO events_kind_9321_nutzaps VALUES (%s, %s, 1, NULL, NULL, 'https://m', 76000)",
                        ("e" * 64, "p" * 64))
            cur.execute(_nutzap_ddl())
            cur.execute(_nutzap_ddl())
            cur.execute("SELECT amount_sats, value_source, no_value_reason FROM events_kind_9321_nutzaps")
            assert cur.fetchall() == [(76000, "unclassified", None)]
            # ...so the payments-only readers drop it until the next refresh rewrites it.
            cur.execute(_read("schema", "nutzap_active_users.sql"))
            cur.execute("SELECT count(*) FROM active_user_counts_kind_9321_nutzaps")
            assert cur.fetchone() == (0,)
        finally:
            cur.execute("SET search_path = public; DROP SCHEMA pre_fix CASCADE")


def _entropy(rng, n):
    """n random alphanumerics: incompressible, so TOAST cannot shrink an index entry."""
    return "".join(rng.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(n))


def _hostile_value(rng):
    """A tag value an adversary could publish. Parquet stores tags as varchar, so every
    element is a string, but its content is arbitrary, including the characters COPY's
    text format must escape (tab, newline, CR, backslash, the literal \\N)."""
    pool = [
        "", "\x00", "\t", "\n", "\r", "\\", "\\N", chr(0xD83C),
        "https://m.example" + chr(0xDC00), "https://m.example\x00", "https://m.example/a\tb\nc\\d\\N\r",
        # Tab, CR and newline make a URL unusable (urlsplit deletes them); a backslash and
        # a literal \N are kept, and COPY's text format must escape both.
        "https://m.example/a\\d\\N",
        "ab" * 32, "AB" * 32, "ab" * 40, "ab" * 31 + "a\x00", "1" * 5000, "-1", "1.5", "1e9",
        "cashuB" + "A" * 40, "[" * 5000, "{" * 5000, "null", "[]", "{}", "[1,2]",
        json.dumps({"amount": "1" * 5000, "id": "k", "secret": "s", "C": "c"}),
        json.dumps({"amount": 2 ** 70, "id": "k", "secret": "s", "C": "c"}),
        json.dumps({"amount": 4, "id": "k", "secret": chr(0xD800), "C": "c"}),
        json.dumps({"amount": 4, "id": "k", "secret": "s\t\n", "C": "c"}),
        json.dumps({"proofs": [{"amount": 1}], "mint": "https://m"}),
        "https://mint.example", "https://mint.example/" + "x" * 10000,
        # Incompressible URLs either side of the 1,024-byte cap: the one above it would
        # fail the index rebuild (btree / GIN entry limit ~2,700 bytes) if it were kept.
        "https://mint.example/" + _entropy(rng, 3000),
        "https://mint.example/" + _entropy(rng, 1024 - len("https://mint.example/")),
    ]
    return rng.choice(pool) if rng.random() < 0.8 else "".join(
        chr(rng.choice([0, 9, 10, 13, 0x5C, 0x41, 0x7B, 0x22, 0xD800, 0xFFFF, 0x1F600, rng.randrange(0x20, 0x250)]))
        for _ in range(rng.randrange(0, 80)))


def _fuzz_events(seed, n, *, kind, url_tag):
    rng = random.Random(seed)
    names = [url_tag, url_tag, "p", "e", "proof", "proof", "amount", "unit", "encryption"]
    for i in range(n):
        tags = [[rng.choice(names), _hostile_value(rng)] + ([_hostile_value(rng)] if rng.random() < 0.1 else [])
                for _ in range(rng.randrange(0, 8))]
        if rng.random() < 0.5:
            tags.insert(0, [url_tag, "https://mint.example"])
        # nozzl clamps a u64 created_at to i64::MAX and keeps future-dated events.
        created_at = rng.choice([1_750_000_000 + i, 1_750_000_000 + i, 4_102_444_800, 2 ** 63 - 1])
        # Note: a lone surrogate cannot actually reach the loader from the archive (Rust
        # String -> Parquet Utf8), so those cases are defence in depth; json.dumps writes
        # them as \ud83c escapes, which the loader's json.loads turns back into one.
        yield {"id": f"{kind:05d}{i:059x}", "pubkey": f"{i:064x}", "created_at": created_at, "kind": kind,
               "tags": tags, "content": _hostile_value(rng), "sig": "0" * 128}


def _write(tmp, events):
    path = os.path.join(tmp, "events.jsonl")
    with open(path, "w", encoding="utf-8") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")
    return path


def test_fuzzed_hostile_nutzaps_never_fail_the_load_or_the_mau(conn):
    """Seeded fuzz, kind-9321 (nutzaps): 2,000 events with adversarial u/p/e/proof/amount/
    unit/encryption tags, content and created_at. The load TRUNCATEs first, so a failure
    in the COPY OR in the index rebuild is an empty (or index-less, pipeline-wedging)
    serving table. Asserts the exact kept count, per-row invariants, and that the MAU
    rebuild (to_timestamp over every created_at) still runs."""
    sys.path.insert(0, _HERE)
    import load_jsonl_exports_into_postgres as L

    events = list(_fuzz_events(8, 2000, kind=9321, url_tag="u"))

    def mint(ev):
        # The loader keeps the first `u` tag that normalizes, not merely the first `u`.
        return next((u for u in (L._normalize_mint_url(t[1]) for t in ev["tags"] if len(t) >= 2 and t[0] == "u")
                     if u), None)

    kept = [ev for ev in events if ev["created_at"] < L._MAX_NUTZAP_CREATED_AT and mint(ev)]
    with tempfile.TemporaryDirectory() as tmp:
        _run_loader([_write(tmp, events)])
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), count(*) FILTER (WHERE value_source = 'proof_tags') FROM events_kind_9321_nutzaps")
        rows, payments = cur.fetchone()
        assert rows == len(kept)
        assert payments == sum(L._nutzap_value(ev).value_source == "proof_tags" for ev in kept) > 0
        cur.execute("""SELECT count(*) FROM events_kind_9321_nutzaps
                        WHERE (recipient_pubkey IS NOT NULL AND recipient_pubkey !~ '^[0-9a-f]{64}$')
                           OR (target_event_id IS NOT NULL AND target_event_id !~ '^[0-9a-f]{64}$')
                           OR octet_length(mint_url) > 1024 OR created_at >= 4102444800""")
        assert cur.fetchone() == (0,)
        # The COPY escaping round-trips: every kept mint URL comes back byte-identical.
        cur.execute("SELECT event_id, mint_url FROM events_kind_9321_nutzaps")
        got = dict(cur.fetchall())
        assert got == {ev["id"]: mint(ev) for ev in kept}
        assert any("\\N" in u for u in got.values())  # the COPY escaping was exercised
        assert any(len(u) > 900 for u in got.values())             # an index entry near the cap
        # Every index came back (a failed rebuild leaves the table index-less).
        cur.execute("SELECT count(*) FROM pg_indexes WHERE tablename = 'events_kind_9321_nutzaps'")
        assert cur.fetchone() == (4,)
        cur.execute(_read("schema", "nutzap_active_users.sql"))
        cur.execute("SELECT sum(total_events) FROM active_user_counts_kind_9321_nutzaps WHERE granularity = 'month'")
        assert cur.fetchone() == (payments,)


def test_fuzzed_hostile_nutzap_configs_never_fail_the_load(conn):
    """Seeded fuzz, kind-10019 (nutzap-configs): the same adversarial values in `mint`
    tags, which land in a GIN-indexed TEXT[] (the live extract already carries probe-like
    junk there: 1,000-char newline runs, HTML entities, NULs, IPv6 literals)."""
    sys.path.insert(0, _HERE)
    import load_jsonl_exports_into_postgres as L

    events = list(_fuzz_events(10019, 2000, kind=10019, url_tag="mint"))
    with tempfile.TemporaryDirectory() as tmp:
        _run_loader([_write(tmp, events)], kind="10019")
    want = {}
    for ev in events:
        urls = []
        for t in ev["tags"]:
            if len(t) >= 2 and t[0] == "mint":
                u = L._normalize_mint_url(t[1])
                if u and u not in urls:
                    urls.append(u)
        want[ev["id"]] = sorted(urls)
    with conn.cursor() as cur:
        cur.execute("SELECT event_id, mint_urls FROM events_kind_10019_nutzap_configs")
        got = {eid: sorted(urls) for eid, urls in cur.fetchall()}
        assert got == want
        assert any(len(u) > 900 for urls in got.values() for u in urls)
        cur.execute("SELECT count(*) FROM pg_indexes WHERE tablename = 'events_kind_10019_nutzap_configs'")
        assert cur.fetchone() == (4,)


# -- mint classification (nostrology-data#9) --

# kind-10019 (nutzap-configs) mint tags as the archive spells them: loopback and probe junk,
# the testnut family, the case-variant host that normalization folds, and the path-case
# variant it must NOT fold.
_CONFIG_MINTS = [
    ["http://127.0.0.1:3338"], ["http://169.254.169.254", "https://mint.minibits.cash/Bitcoin"],
    ["https://nofee.testnut.cashu.space"], ["https://Mint.lnserver.com", "https://mint.lnserver.com/"],
    ["https://mint.minibits.cash/bitcoin"], ["http://[::1]", "https://mint.coinos.io:443"],
    ["https://mint.bitcointxoko.com"],
]


@pytest.fixture(scope="module")
def classified(conn):
    # Loads its own data: the tests above leave the tables holding hostile and fuzzed rows.
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "k9321.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            for f in FIXTURES:
                fh.write(json.dumps(f["event"]) + "\n")
        _run_loader([path])
        path = os.path.join(tmp, "k10019.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            for i, mints in enumerate(_CONFIG_MINTS):
                fh.write(json.dumps({"id": f"{i:064x}", "pubkey": f"{i + 1:064x}", "created_at": 1_780_000_000,
                                     "kind": 10019, "tags": [["mint", m] for m in mints],
                                     "content": "", "sig": "0" * 128}) + "\n")
        _run_loader([path], kind="10019")
    proc = _classify()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    with conn.cursor() as cur:
        cur.execute("SELECT mint_url, mint_class, excluded, class_source FROM nutzap_mint_classes")
        return {r[0]: r[1:] for r in cur.fetchall()}, proc.stdout


def test_every_loaded_mint_url_is_classified_as_its_fixture_says(conn, classified):
    table, _ = classified
    for f in FIXTURES:
        if not f["expected"]["loaded"]:
            continue
        mint = next(t[1] for t in f["event"]["tags"] if t[0] == "u").rstrip("/")
        assert table[mint][0] == f["expected"]["mint_class"], f["family"]


def test_config_mint_urls_are_normalized_and_classified(classified):
    table, _ = classified
    assert table["http://127.0.0.1:3338"] == ("private-host", True, "host-rule")
    assert table["http://169.254.169.254"] == ("private-host", True, "host-rule")
    assert table["http://[::1]"] == ("private-host", True, "host-rule")
    assert table["https://nofee.testnut.cashu.space"] == ("fake-backend", True, "registry")
    # Host case and default port fold into one mint; path case does not.
    assert "https://Mint.lnserver.com" not in table and table["https://mint.lnserver.com"][0] == "unreviewed"
    assert "https://mint.coinos.io:443" not in table and "https://mint.coinos.io" in table
    assert table["https://mint.minibits.cash/bitcoin"] == ("unreviewed", False, "none")
    assert table["https://mint.minibits.cash/Bitcoin"] == ("unreviewed", False, "none")
    # Every class the registry can write passes the table's CHECK (fixtures cover the rest).
    assert table["https://mint.bitcointxoko.com"] == ("unreachable", False, "registry")


def test_the_classification_summary_names_the_mix(classified):
    table, out = classified
    excluded = [u for u, r in table.items() if r[1]]
    assert f"Mint classes: {len(table):,} URLs [" in out
    assert "private-host=" in out and "fake-backend=" in out and "test-mint-gone=1" in out
    # The fixture payments on excluded mints: harness 10 + docker 21 + testnut 32 + macadamia 1.
    assert "excluded: 4 payments, 64 sats" in out
    assert "Review: no unreviewed mint had its first payment in the last 14 days" in out
    assert len(excluded) >= 6


def test_active_users_drop_test_and_private_mints(conn, classified):
    harness = next(f["event"] for f in FIXTURES if f["family"] == "test-harness-loopback")
    with conn.cursor() as cur:
        cur.execute(_read("schema", "nutzap_active_users.sql"))
        cur.execute("SELECT sum(total_events) FROM active_user_counts_kind_9321_nutzaps WHERE granularity = 'month'")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT count(*) {_COUNTED}")
        assert total == cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM events_kind_9321_nutzaps WHERE value_source = 'proof_tags'")
        assert cur.fetchone()[0] - total == 4  # the four fixture payments on excluded mints
        # No excluded sender is an active user in its month: the harness key's month has
        # exactly the senders of counted payments in it (none, here).
        cur.execute(f"""SELECT count(DISTINCT pubkey) {_COUNTED}
                          AND date_trunc('month', to_timestamp(created_at) AT TIME ZONE 'UTC')
                            = date_trunc('month', to_timestamp(%s) AT TIME ZONE 'UTC')""", (harness["created_at"],))
        want = cur.fetchone()[0]
        cur.execute("""SELECT COALESCE(max(unique_users), 0) FROM active_user_counts_kind_9321_nutzaps
                        WHERE granularity = 'month'
                          AND period_start = extract(epoch FROM date_trunc('month', to_timestamp(%s) AT TIME ZONE 'UTC'))""",
                    (harness["created_at"],))
        assert cur.fetchone()[0] == want


def test_a_mint_missing_from_the_classes_table_is_counted(conn, classified):
    """A mint first seen after the last classification is unreviewed: counted, never dropped."""
    with conn.cursor() as cur:
        cur.execute("BEGIN")
        try:
            cur.execute("DELETE FROM nutzap_mint_classes WHERE NOT excluded")
            cur.execute(_read("schema", "nutzap_active_users.sql").replace("BEGIN;", "").replace("COMMIT;", ""))
            cur.execute("SELECT sum(total_events) FROM active_user_counts_kind_9321_nutzaps WHERE granularity = 'month'")
            total = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) {_COUNTED}")
            assert total == cur.fetchone()[0] > 0
        finally:
            cur.execute("ROLLBACK")


def test_a_malformed_registry_changes_nothing(conn, classified, tmp_path):
    table, _ = classified
    bad = tmp_path / "bad.yaml"
    bad.write_text("mints:\n- host: testnut.cashu.space\n  class: fake\n  evidence: x\n"
                   "  as_of: 2026-09-18\n  reviewer: x\n")
    proc = _classify(str(bad))
    assert proc.returncode != 0 and "class 'fake'" in proc.stderr
    with conn.cursor() as cur:
        cur.execute("SELECT mint_url, mint_class, excluded, class_source FROM nutzap_mint_classes")
        assert {r[0]: r[1:] for r in cur.fetchall()} == table


def test_a_failure_mid_rebuild_rolls_back_to_the_previous_classification(conn, classified, monkeypatch):
    """The rebuild is DELETE + INSERT in one transaction: an INSERT that fails (here, a
    class the table's CHECK refuses) leaves yesterday's rows serving, never an empty table."""
    import psycopg2

    sys.path.insert(0, _HERE)
    import load_jsonl_exports_into_postgres as L
    import nutzap_mints as M

    table, _ = classified
    monkeypatch.setattr(M, "classify", lambda url, reg: M.MintClass("bogus", "none", None))
    monkeypatch.setitem(M.CLASSES, "bogus", False)
    c = psycopg2.connect(DSN)
    try:
        with pytest.raises(psycopg2.errors.CheckViolation):
            L.classify_nutzap_mints(_REGISTRY, c)
    finally:
        c.close()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM nutzap_mint_classes")
        assert cur.fetchone()[0] == len(table)


def test_the_review_line_names_a_new_unreviewed_mint(conn, classified, capsys):
    sys.path.insert(0, _HERE)
    import psycopg2
    import load_jsonl_exports_into_postgres as L

    first_payment: dict = {}
    for f in FIXTURES:
        if f["expected"]["mint_class"] == "unreviewed" and f["expected"]["value_source"] == "proof_tags":
            mint = next(t[1] for t in f["event"]["tags"] if t[0] == "u").rstrip("/")
            first_payment[mint] = min(first_payment.get(mint, f["event"]["created_at"]), f["event"]["created_at"])
    newest_mint, newest = max(first_payment.items(), key=lambda kv: kv[1])
    c = psycopg2.connect(DSN)
    try:
        L.classify_nutzap_mints(_REGISTRY, c, now=newest + 86400)
    finally:
        c.close()
    out = capsys.readouterr().out
    assert "Review: 1 unreviewed mint(s) with a first payment in the last 14 days" in out
    assert newest_mint in out and "data/nutzaps/mint_registry.yaml" in out


@pytest.mark.parametrize("statement", [
    "INSERT INTO nutzap_mint_classes VALUES ('https://a', 'private-host', FALSE, 'host-rule', NULL)",
    "INSERT INTO nutzap_mint_classes VALUES ('https://a', 'unreviewed', TRUE, 'none', NULL)",
    "INSERT INTO nutzap_mint_classes VALUES ('https://a', 'fake', TRUE, 'registry', NULL)",
    "INSERT INTO nutzap_mint_classes VALUES ('https://a', 'unreviewed', FALSE, 'guess', NULL)",
])
def test_the_classes_table_refuses_an_inconsistent_row(conn, classified, statement):
    import psycopg2

    with conn.cursor() as cur, pytest.raises(psycopg2.errors.CheckViolation):
        cur.execute(statement)


def test_fuzzed_mint_urls_classify_without_failing(conn):
    """Both fuzz corpora loaded, then classified: every distinct URL gets a row, the run
    exits 0, and the MAU rebuild over the join still runs."""
    events_9321 = list(_fuzz_events(9, 1500, kind=9321, url_tag="u"))
    events_10019 = list(_fuzz_events(10020, 1500, kind=10019, url_tag="mint"))
    with tempfile.TemporaryDirectory() as tmp:
        _run_loader([_write(tmp, events_9321)])
        _run_loader([_write(tmp, events_10019)], kind="10019")
    proc = _classify()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    with conn.cursor() as cur:
        cur.execute("""SELECT count(*) FROM (SELECT mint_url FROM events_kind_9321_nutzaps
                         UNION SELECT UNNEST(mint_urls) FROM events_kind_10019_nutzap_configs) u
                        WHERE NOT EXISTS (SELECT 1 FROM nutzap_mint_classes c WHERE c.mint_url = u.mint_url)""")
        assert cur.fetchone() == (0,)
        cur.execute("SELECT count(*) FROM nutzap_mint_classes")
        assert cur.fetchone()[0] > 1
        cur.execute(_read("schema", "nutzap_active_users.sql"))

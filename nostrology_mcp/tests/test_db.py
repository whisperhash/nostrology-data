"""db.py tests with a stubbed psycopg2 - DSN hygiene and the statement-order
property (as_of probe MUST run before the metric SQL: under READ COMMITTED each
statement gets its own snapshot, and probe-first means a mid-call load commit can
only UNDERSTATE freshness, never overstate it)."""

import sys
import types

import pytest

from nostrology_mcp import db
from nostrology_mcp.metric import METRICS

ALL_SPECS = sorted(METRICS.values(), key=lambda s: s.key)
SPEC_IDS = [s.key for s in ALL_SPECS]


def test_dsn_is_password_free_and_ssl_required():
    dsn = db.build_dsn("localhost", 15432, "nostrology_ro", "nostrology")
    assert "password" not in dsn
    assert dsn.startswith("postgresql://nostrology_ro@localhost:15432/nostrology")
    assert "sslmode=require" in dsn
    assert "connect_timeout=90" in dsn


def test_dsn_from_env_defaults_target_the_tunnel(monkeypatch):
    for var in ("NOSTROLOGY_PGHOST", "NOSTROLOGY_PGPORT", "NOSTROLOGY_PGUSER", "NOSTROLOGY_PGDATABASE"):
        monkeypatch.delenv(var, raising=False)
    assert db.dsn_from_env() == db.build_dsn("localhost", 15432, "nostrology_ro", "nostrology")


class _FakeCursor:
    def __init__(self, executed):
        self._executed = executed
        self._last = None

    def execute(self, sql, params=None):
        self._executed.append((sql, params))
        self._last = sql

    def fetchall(self):
        return [("2026-01-02", 7)]

    def fetchone(self):
        return (1785349327,)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, executed):
        self._executed = executed
        self.closed = False

    def cursor(self):
        return _FakeCursor(self._executed)

    def close(self):
        self.closed = True


def _run(monkeypatch, spec):
    executed = []
    conns = []

    def fake_connect(dsn, password=None):
        assert "password" not in dsn  # the out-of-band rule, checked at the seam
        conn = _FakeConn(executed)
        conns.append(conn)
        return conn

    fake = types.ModuleType("psycopg2")
    fake.connect = fake_connect
    monkeypatch.setitem(sys.modules, "psycopg2", fake)

    result = db.fetch_metric(spec, dsn=db.build_dsn("localhost", 15432, "u", "d"))
    return result, executed, conns


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_fetch_metric_runs_as_of_probe_first_and_closes(monkeypatch, spec):
    result, executed, conns = _run(monkeypatch, spec)
    assert [sql for sql, _ in executed] == [spec.as_of_sql, spec.sql]
    assert result.as_of_epoch == 1785349327
    assert result.rows == [("2026-01-02", 7)]
    assert conns[0].closed


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_fetch_metric_uses_the_specs_own_probe_and_binds(monkeypatch, spec):
    """The probe is per-spec (a global one would report another table's freshness),
    and the bind parameters go to the metric SQL only - the probe takes none, so a
    stray params argument there would mean the two statements were mixed up."""
    _, executed, _ = _run(monkeypatch, spec)
    (probe_sql, probe_params), (metric_sql, metric_params) = executed
    assert probe_sql == spec.as_of_sql
    assert probe_params is None
    assert metric_sql == spec.sql
    # `or None`: fetch_metric deliberately passes None rather than an empty dict for
    # parameter-free specs, because psycopg2 only skips %-interpolation for None (an
    # empty dict still hits the formatter, so a literal % would raise TypeError).
    # Non-empty binds pass through unchanged.
    assert metric_params == (spec.bind_params or None)


# -- connection-identifier scrubbing ---------------------------------------------


def _set_dsn_env(monkeypatch, host, user, database):
    monkeypatch.setenv("NOSTROLOGY_PGHOST", host)
    monkeypatch.setenv("NOSTROLOGY_PGUSER", user)
    monkeypatch.setenv("NOSTROLOGY_PGDATABASE", database)


def test_scrub_removes_host_user_and_database_from_libpq_text(monkeypatch):
    """The real shape: libpq names what it was connecting to, and that text lands in
    a call log shipped to Loki and read by a sandboxed observer."""
    _set_dsn_env(monkeypatch, "nostrology-example.cluster-abc.us-east-1.rds.amazonaws.com", "nostrology_mcp", "nostrology")
    text = (
        'OperationalError: connection to server at '
        '"nostrology-example.cluster-abc.us-east-1.rds.amazonaws.com" (10.0.3.7), port 5432 failed: '
        'FATAL:  password authentication failed for user "nostrology_mcp"\n'
        'FATAL:  database "nostrology" does not exist'
    )
    got = db.scrub_connection_identifiers(text)
    assert "rds.amazonaws.com" not in got
    assert "nostrology_mcp" not in got
    assert got.count("<host>") == 1 and got.count("<user>") == 1 and got.count("<database>") == 1
    # The DIAGNOSIS must survive - scrubbing that ate the error would blind the log.
    assert "password authentication failed" in got
    assert "port 5432 failed" in got


def test_scrub_replaces_longest_first_so_substrings_do_not_mangle(monkeypatch):
    """Load-bearing ordering. `nostrology` (database) is a substring of BOTH
    `nostrology_mcp` (user) and the `nostrology-example...` host, so replacing it first
    would rewrite the middle of the other two - mangling the text AND leaving the
    remainder (`_mcp`, `-example`, the domain) behind."""
    _set_dsn_env(monkeypatch, "nostrology-example.cluster-abc.rds.amazonaws.com", "nostrology_mcp", "nostrology")
    got = db.scrub_connection_identifiers(
        'host "nostrology-example.cluster-abc.rds.amazonaws.com" user "nostrology_mcp" db "nostrology"'
    )
    assert got == 'host "<host>" user "<user>" db "<database>"'
    assert "<database>_mcp" not in got and "<database>-example" not in got


def test_scrub_leaves_unrelated_error_text_untouched(monkeypatch):
    """Everything that is NOT a connection failure - SQL errors, driver type errors -
    carries no identifiers, and is exactly why the text is scrubbed rather than
    dropped."""
    _set_dsn_env(monkeypatch, "somehost", "someuser", "somedb")
    text = 'ProgrammingError: syntax error at or near "SELCT"\nLINE 1: SELCT count(*)'
    assert db.scrub_connection_identifiers(text) == text


def test_scrub_tolerates_unset_env(monkeypatch):
    """Falls back to the local-dev defaults; must not crash or blank the message."""
    for var in ("NOSTROLOGY_PGHOST", "NOSTROLOGY_PGUSER", "NOSTROLOGY_PGDATABASE"):
        monkeypatch.delenv(var, raising=False)
    assert "boom" in db.scrub_connection_identifiers("OperationalError: boom")

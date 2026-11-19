"""Unit tests for pipeline/pg.py — build_dsn (pure) + the MV-refresh / count helpers against a
fake psycopg2 connection (no real DB)."""

from dagster_code.pipeline import pg


def test_build_dsn_is_password_free_and_tls():
    dsn = pg.build_dsn("host.rds.amazonaws.com", 5432, "nostrology", "nostrology")
    assert dsn == "postgresql://nostrology@host.rds.amazonaws.com:5432/nostrology?sslmode=require&connect_timeout=90"
    assert "@" in dsn and ":" + "secret" not in dsn  # no password embedded


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self._conn.calls.append(("execute", sql))

    def fetchone(self):
        return (self._conn.count,)


class _FakeConn:
    def __init__(self, count=0):
        self.calls = []
        self.count = count

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.calls.append(("commit",))


def test_refresh_materialized_views_runs_concurrently_and_commits_each():
    conn = _FakeConn()
    logs: list = []
    timings = pg.refresh_materialized_views(conn, ["mv_a", "mv_b"], log=logs.append)
    # Each MV: a CONCURRENTLY refresh followed by a commit, in order.
    assert conn.calls == [
        ("execute", "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_a"),
        ("commit",),
        ("execute", "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_b"),
        ("commit",),
    ]
    assert set(timings.keys()) == {"mv_a", "mv_b"}
    assert all(isinstance(v, float) for v in timings.values())


def test_count_rows():
    conn = _FakeConn(count=932752)
    assert pg.count_rows(conn, "events_kind_10002_relay_lists") == 932752
    assert conn.calls == [("execute", "SELECT count(*) FROM events_kind_10002_relay_lists")]


def test_apply_schema_executes_file_then_commits(tmp_path):
    sql_file = tmp_path / "init.sql"
    sql_file.write_text("CREATE TABLE IF NOT EXISTS t (id int);\nCREATE INDEX IF NOT EXISTS i ON t(id);\n")
    conn = _FakeConn()
    pg.apply_schema(conn, str(sql_file))
    assert conn.calls == [
        ("execute", "CREATE TABLE IF NOT EXISTS t (id int);\nCREATE INDEX IF NOT EXISTS i ON t(id);\n"),
        ("commit",),
    ]


def test_execute_sql_file_runs_batch_then_commits(tmp_path):
    # A file with its own BEGIN/COMMIT (the nutzap MAU rebuild shape) runs as one batch.
    sql_file = tmp_path / "nutzap_active_users.sql"
    sql_file.write_text("BEGIN;\nTRUNCATE t;\nINSERT INTO t SELECT 1;\nCOMMIT;\n")
    conn = _FakeConn()
    pg.execute_sql_file(conn, str(sql_file))
    assert conn.calls == [
        ("execute", "BEGIN;\nTRUNCATE t;\nINSERT INTO t SELECT 1;\nCOMMIT;\n"),
        ("commit",),
    ]

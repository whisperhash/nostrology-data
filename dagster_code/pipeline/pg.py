"""Postgres helpers for the nativized load: DSN construction, owner connection, row counts, and
CONCURRENTLY materialized-view refresh.

build_dsn is pure (unit-tested); the psycopg2-touching functions import psycopg2 LAZILY so this
module imports without it (the connection paths are integration-tested on the box, not in the
local gate). The password is ALWAYS supplied out-of-band (PGPASSWORD env for the loader subprocess,
the `password=` arg here) — NEVER embedded in a DSN that might get logged.
"""

import time
from typing import Callable, Iterable


def _noop_log(_msg: str) -> None:
    pass


def build_dsn(host: str, port: int, owner: str, database: str) -> str:
    """Password-FREE DSN (libpq/psycopg2 take the password out-of-band). sslmode=require (force_ssl
    refuses plaintext) + connect_timeout=90, the same 90s as run.sh's wait_pg_ready. It was sized for
    the deepest (>24h-idle, reboot-like) resume of the scale-to-zero cluster; the cluster has held a
    0.5 ACU floor since 2026-08-03 and does not pause, so a warm connect is immediate, and the 90s
    stays to cover a restart or a failover."""
    return f"postgresql://{owner}@{host}:{port}/{database}?sslmode=require&connect_timeout=90"


def connect(dsn: str, password: str):
    """psycopg2 connection to `dsn` with the password supplied separately (never in the DSN)."""
    import psycopg2

    return psycopg2.connect(dsn, password=password)


def execute_sql_file(conn, sql_path: str) -> None:
    """Read a .sql file and run it as one statement batch, then commit — the psycopg2 equivalent of
    run.sh's `psql -f <file>`. Used for files with NO psql `\\`-meta-commands and NO CONCURRENTLY /
    dollar-quoted-function bodies (init.sql, nutzap_active_users.sql). A file carrying its own
    BEGIN/COMMIT runs fine; the trailing conn.commit() is then a no-op."""
    with open(sql_path) as fh:
        sql = fh.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def apply_schema(conn, sql_path: str) -> None:
    """Apply the idempotent schema, the boto3-path equivalent of run.sh's `psql -f schema/init.sql`
    run before every load (run.sh:738). init.sql is CREATE … IF NOT EXISTS throughout (tables, MVs
    created WITH DATA, indexes) with NO CONCURRENTLY / transaction-control / dollar-quoted bodies, so
    the whole file runs as a single statement batch. This is what creates a newly-added table / MV /
    index so the TRUNCATE+load and the downstream CONCURRENTLY refresh have their objects present."""
    execute_sql_file(conn, sql_path)


def count_rows(conn, table: str) -> int:
    """SELECT count(*) FROM <table> (the post-load observability stat)."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return int(cur.fetchone()[0])


def refresh_materialized_views(
    conn, mvs: Iterable[str], *, log: Callable[[str], None] = _noop_log
) -> dict:
    """REFRESH MATERIALIZED VIEW CONCURRENTLY each mv, committing after each — the lifted-out
    version of the loader's in-line refresh (loader:2339-2350). CONCURRENTLY relies on each MV's
    unique index (schema/init.sql) and a prior population (CREATE … WITH DATA). Returns {mv: secs}.
    """
    timings: dict[str, float] = {}
    for mv in mvs:
        log(f"[mv] refreshing {mv} (CONCURRENTLY)")
        t0 = time.monotonic()
        with conn.cursor() as cur:
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
        conn.commit()
        secs = round(time.monotonic() - t0, 1)
        timings[mv] = secs
        log(f"[mv] {mv} refreshed in {secs}s")
    return timings

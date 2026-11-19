"""Read-only Postgres access for the MCP server.

Mirrors dagster_code/pipeline/pg.py conventions: build_dsn is pure and password-FREE
(the password travels out-of-band - env var or libpq ~/.pgpass - never in a DSN that
might get logged); psycopg2 imports lazily so the module imports without it.

The env defaults serve LOCAL dev (a workstation reaching the database over an SSH
tunnel on localhost:15432); a deployment overrides all of them (the database endpoint
direct, the dedicated nostrology_mcp role). Any tunnel works locally:
    ssh -f -N -L 127.0.0.1:15432:<database endpoint>:5432 <a host that can reach it>
Only one may hold the port. connect_timeout=90 matches the pipeline's (its pg.py
says why): sized for the scale-to-zero resume, when cold connects measured
14-17s, it now covers a restart or a failover, since the cluster no longer
pauses. Warm connects take ~1.9s through the tunnel, tens of ms on-box (same VPC).
"""

import os
import time
from dataclasses import dataclass

from .metric import MetricSpec


def build_dsn(host: str, port: int, user: str, database: str, connect_timeout: int = 90) -> str:
    """Password-free DSN; sslmode=require (Aurora terminates TLS even through the
    TCP tunnel; `require` does not verify hostname, which the tunnel would break)."""
    return (
        f"postgresql://{user}@{host}:{port}/{database}"
        f"?sslmode=require&connect_timeout={connect_timeout}"
    )


def dsn_from_env() -> str:
    return build_dsn(
        host=os.environ.get("NOSTROLOGY_PGHOST", "localhost"),
        port=int(os.environ.get("NOSTROLOGY_PGPORT", "15432")),
        user=os.environ.get("NOSTROLOGY_PGUSER", "nostrology_ro"),
        database=os.environ.get("NOSTROLOGY_PGDATABASE", "nostrology"),
        connect_timeout=int(os.environ.get("NOSTROLOGY_PGCONNECT_TIMEOUT", "90")),
    )


def scrub_connection_identifiers(text: str) -> str:
    """Replace the DSN's host / user / database literals with placeholders.

    WHY. libpq error text names what it was connecting to - "could not connect to
    server ... host \"nostrology-example.cluster-abc.us-east-1.rds.amazonaws.com\" ...
    user \"nostrology_mcp\"" - and the call log those errors land in is shipped to
    Loki and read by a deliberately sandboxed observer. No password can appear (the
    DSN is password-free by construction, see build_dsn), so this is not a credential
    leak; it is the infrastructure-detail half of the same rule
    (nozzl/CLAUDE.md, "Logs are an observability surface"). The prose that
    crosses to the CLIENT was always sanitized - only our own wording goes out; this
    closes the log copy, which was not.

    Scrubbing rather than dropping the text: the message is the diagnostic for
    everything that ISN'T a connection failure (SQL errors, driver type errors), and
    those carry no identifiers at all. Discarding it to protect the connection case
    would blind the common case.

    LONGEST FIRST, which is load-bearing: these values are substrings of each other.
    The host is `nostrology-example...`, the user `nostrology_mcp`, the database
    `nostrology` - replacing the database first would rewrite the middle of the other
    two into `<database>_mcp` and `<database>-example...`, mangling the text and leaking
    the remainder anyway.

    Best-effort and deliberately NOT a general-purpose sanitizer: it knows only the
    literals this process was configured with. If libpq prints a resolved IP instead
    of the hostname, that is not caught - accepted, since a private VPC address is
    topology, not a secret, and pattern-matching every IP-shaped string would mangle
    legitimate error text."""
    values = [
        (os.environ.get("NOSTROLOGY_PGHOST", "localhost"), "<host>"),
        (os.environ.get("NOSTROLOGY_PGUSER", "nostrology_ro"), "<user>"),
        (os.environ.get("NOSTROLOGY_PGDATABASE", "nostrology"), "<database>"),
    ]
    for value, placeholder in sorted(values, key=lambda kv: len(kv[0] or ""), reverse=True):
        if value:
            text = text.replace(value, placeholder)
    return text


@dataclass(frozen=True)
class MetricResult:
    rows: list  # list[tuple] as fetched, in SQL column order
    as_of_epoch: int | None  # MAX(created_at) over the source table; None if empty
    connect_ms: int
    query_ms: int


def fetch_metric(
    spec: MetricSpec,
    dsn: str | None = None,
    password: str | None = None,
    sql: str | None = None,
    params: dict | None = None,
) -> MetricResult:
    """One connection per call: connect, run the metric SQL + the as_of probe,
    close. Deliberately NO pool: on-box connects are tens of ms, and at
    interactive volumes every call is cold anyway (a held-open connection also
    blocked Aurora's auto-pause, until the cluster stopped pausing on 2026-08-03).
    The role's CONNECTION LIMIT bounds concurrency.

    `spec` is duck-typed on (sql, bind_params, as_of_sql): reference.RecordSpec
    passes through here identically - the annotation names MetricSpec only
    because it was first.

    `sql`/`params` override the spec's own (the windowing path: the server wraps a
    day-grain SQL or extends the bind dict with epoch bounds); None = the spec's
    unmodified statement + binds, which is the byte-identical legacy path."""
    import psycopg2  # lazy: module must import without the driver (pg.py convention)

    dsn = dsn or dsn_from_env()
    password = password or os.environ.get("NOSTROLOGY_PGPASSWORD")  # None -> ~/.pgpass

    effective_sql = sql if sql is not None else spec.sql
    effective_params = params if params is not None else spec.bind_params

    t0 = time.monotonic()
    conn = psycopg2.connect(dsn, password=password)
    connect_ms = int((time.monotonic() - t0) * 1000)
    try:
        t1 = time.monotonic()
        with conn.cursor() as cur:
            # as_of probe FIRST: READ COMMITTED gives each statement its own
            # snapshot, so if a load commits between the two, this order can only
            # UNDERSTATE freshness (which the envelope's disclaimer already covers)
            # - probe-after-rows could claim freshness the rows don't have.
            # The probe comes from the SPEC, not a module global: it must read the
            # metric's own source table, or a future metric on another table would
            # report an unrelated table's freshness as its own.
            cur.execute(spec.as_of_sql)
            as_of_row = cur.fetchone()
            # `or None`, not the dict itself: psycopg2 skips %-interpolation ONLY
            # for params=None. An EMPTY dict still goes through the formatter, so a
            # literal % anywhere in a parameter-free statement (a LIKE '%x', the mod
            # operator) would raise TypeError at execute - in production, for a
            # change that looks parameter-free. Verified against live psycopg2.
            cur.execute(effective_sql, effective_params or None)
            rows = cur.fetchall()
        query_ms = int((time.monotonic() - t1) * 1000)
    finally:
        conn.close()

    as_of_epoch = int(as_of_row[0]) if as_of_row and as_of_row[0] is not None else None
    return MetricResult(rows=rows, as_of_epoch=as_of_epoch, connect_ms=connect_ms, query_ms=query_ms)

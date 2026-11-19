#!/usr/bin/env python3
"""Standalone schema-contract test for the M4 Nutzaps slice (no DB; pure text parse).

Run:  python3 loader/test_nutzap_schema.py      (exits non-zero on failure)

schema/init.sql is the contract nostrology-web reads and the loader TRUNCATEs into.
This locks three invariants for the M4 /nutzaps tables so an accidental deletion,
rename, or drift can't slip through a green build:

  1. Each table's `CREATE TABLE IF NOT EXISTS` + every documented index is in init.sql.
  2. The mint-join key (events_kind_9321_nutzaps.mint_url) is NOT NULL — a row without a
     normalizable mint is dropped by the loader, so a nullable column here would mean a
     silent null-join in the per-mint leaderboard.
  3. Every index init.sql declares for a LOADER-MANAGED table (the two event tables, whose
     indexes the loader drops+recreates) also appears in the loader source
     (SECONDARY_INDEXES) - so the schema apply and the loader's recreate cycle can't disagree
     on index names. active_user_counts_kind_9321_nutzaps is NOT loader-managed (it's
     rebuilt by schema/nutzap_active_users.sql, not the per-kind loader), so its index is
     checked in init.sql only and is deliberately NOT cross-checked against the loader.

Pure stdlib + file reads (no psycopg2 import), so it runs on a workstation during static
checks as well as in-container.
"""
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
_INIT_SQL = os.path.join(_REPO_ROOT, "schema", "init.sql")
_LOADER = os.path.join(_HERE, "load_jsonl_exports_into_postgres.py")

# The contract: table -> indexes that init.sql must declare. These are the three tables
# the /nutzaps pages read (kind 7376 is excluded — no page reads it).
EXPECTED = {
    "events_kind_9321_nutzaps": [
        "idx_k9321_created", "idx_k9321_mint_created", "idx_k9321_recipient",
    ],
    "events_kind_10019_nutzap_configs": [
        "idx_k10019_pubkey_created", "idx_k10019_created", "idx_k10019_mint_urls",
    ],
    "active_user_counts_kind_9321_nutzaps": [
        "idx_auc_k9321nz_granularity",
    ],
}

# Tables whose indexes the loader drops+recreates — cross-check their indexes against the
# loader source. active_user_counts is NOT here (rebuilt by SQL, not the loader).
LOADER_MANAGED = {"events_kind_9321_nutzaps", "events_kind_10019_nutzap_configs"}

# mint_url is the join key for every per-mint query — it must be NOT NULL so a malformed
# row can never silently null-out a mint's payments.
NOT_NULL_COLS = {
    "events_kind_9321_nutzaps": ["mint_url"],
}


def _table_block(sql, table):
    """Return the `CREATE TABLE IF NOT EXISTS <table> ( ... );` body, or None."""
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS\s+" + re.escape(table) + r"\s*\((.*?)\)\s*;",
        sql, re.DOTALL,
    )
    return m.group(1) if m else None


def main():
    failures = []
    with open(_INIT_SQL, "r", encoding="utf-8") as f:
        init_sql = f.read()
    with open(_LOADER, "r", encoding="utf-8") as f:
        loader_src = f.read()

    for table, indexes in EXPECTED.items():
        block = _table_block(init_sql, table)
        if block is None:
            failures.append(f"init.sql: missing `CREATE TABLE IF NOT EXISTS {table} (...)`")
        for idx in indexes:
            if idx not in init_sql:
                failures.append(f"init.sql: missing index {idx} (on {table})")
            # Drift guard: only for loader-managed tables (the loader drops+recreates
            # their indexes, so each name must also live in the loader source).
            if table in LOADER_MANAGED and idx not in loader_src:
                failures.append(f"loader: {idx} (in init.sql for {table}) not found in SECONDARY_INDEXES")

        for col in NOT_NULL_COLS.get(table, []):
            if block is not None and not re.search(
                r"\b" + re.escape(col) + r"\b[^,]*\bNOT NULL\b", block
            ):
                failures.append(f"init.sql: {table}.{col} must be NOT NULL")

    if failures:
        print("FAIL: /nutzaps schema contract:", file=sys.stderr)
        for msg in failures:
            print("  - " + msg, file=sys.stderr)
        sys.exit(1)

    n_tables = len(EXPECTED)
    n_idx = sum(len(v) for v in EXPECTED.values())
    print(f"OK: {n_tables} nutzap tables + {n_idx} indexes present in init.sql "
          f"and consistent with the loader's recreate set")


if __name__ == "__main__":
    main()

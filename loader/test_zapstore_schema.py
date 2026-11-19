#!/usr/bin/env python3
"""Standalone schema-contract test for the M3 /zapstore slice (no DB; pure text parse).

Run:  python3 loader/test_zapstore_schema.py      (exits non-zero on failure)

schema/init.sql is the contract nostrology-web reads and the loader TRUNCATEs into.
This locks three invariants for the four /zapstore tables so an accidental deletion,
rename, or drift can't slip through a green build:

  1. Each table's `CREATE TABLE IF NOT EXISTS` + every documented index is in init.sql.
  2. The app-key columns the trends/leaderboard JOINs depend on are NOT NULL.
  3. Every index init.sql declares for a table whose indexes the loader drops+recreates
     also appears in the loader source (SECONDARY_INDEXES) - so the schema apply and the
     loader's recreate cycle can't disagree on index names. INIT_ONLY_INDEXES lists the
     exceptions: indexes init.sql owns outright, which the loader never drops and so
     must NOT appear in SECONDARY_INDEXES.

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

# The contract: table -> indexes that init.sql must declare. These are the four
# tables the /zapstore pages read (kind-0 profiles are the M2 slice, checked there).
EXPECTED = {
    "events_kind_32267_app_catalog": [
        "idx_k32267_pubkey_d", "idx_k32267_created", "idx_k32267_extra_tags",
    ],
    # The listings the loader sorted OUT of the catalog, with the reason in `excl`.
    # Same columns as the catalog plus that one - the loader COPYs both from the same
    # tuple, so a column added to one and not the other loads silently wrong.
    "events_kind_32267_app_catalog_excluded": [
        "idx_k32267x_pubkey_d", "idx_k32267x_excl",
    ],
    "events_kind_30063_app_releases": [
        "idx_k30063_app", "idx_k30063_created", "idx_k30063_extra_tags",
    ],
    "events_kind_30267_app_stacks": [
        "idx_k30267_pubkey_d", "idx_k30267_created", "idx_k30267_app_refs", "idx_k30267_extra_tags",
    ],
    "events_kind_9735_zaps_zapstore": [
        "idx_k9735zs_app", "idx_k9735zs_created", "idx_k9735zs_target",
    ],
}

# Indexes init.sql owns outright: the loader never drops or recreates them, so
# requiring them in SECONDARY_INDEXES would be requiring a lie.
#   - idx_k9735_signer_created: the /lightning slice, init-only since it was added.
#   - the two on events_kind_32267_app_catalog_excluded: that table carries a couple
#     of hundred rows against the catalog's ~5.5K, far too few for a drop+recreate
#     cycle to pay for itself, so its load keeps every index (and its PRIMARY KEY)
#     in place. Check 1 above still requires init.sql to declare them.
INIT_ONLY_INDEXES = {
    "idx_k9735_signer_created",
    "idx_k32267x_pubkey_d",
    "idx_k32267x_excl",
}

# app_pubkey/app_d_tag are the JOIN keys for every zap-attribution query — they must
# be NOT NULL so a malformed subset row can never silently null-out an app's zaps.
NOT_NULL_COLS = {
    "events_kind_9735_zaps_zapstore": ["app_pubkey", "app_d_tag"],
    # `excl` is the reason the row is here and `loaded_at` is how the three dependent
    # loads tell this refresh's exclusions from a previous one's. A nullable either
    # would be a row that says nothing, or an age that reads as "never".
    "events_kind_32267_app_catalog_excluded": ["excl", "loaded_at"],
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
            # Drift guard: the loader drops+recreates these same indexes, so each must
            # also live in the loader source. A rename on one side only = caught here.
            if idx in INIT_ONLY_INDEXES:
                continue
            if idx not in loader_src:
                failures.append(f"loader: {idx} (in init.sql for {table}) not found in SECONDARY_INDEXES")

        for col in NOT_NULL_COLS.get(table, []):
            if block is not None and not re.search(
                r"\b" + re.escape(col) + r"\b[^,]*\bNOT NULL\b", block
            ):
                failures.append(f"init.sql: {table}.{col} must be NOT NULL")

    if failures:
        print("FAIL: /zapstore schema contract:", file=sys.stderr)
        for msg in failures:
            print("  - " + msg, file=sys.stderr)
        sys.exit(1)

    n_tables = len(EXPECTED)
    n_idx = sum(len(v) for v in EXPECTED.values())
    print(f"OK: {n_tables} /zapstore tables + {n_idx} indexes present in init.sql "
          f"and consistent with the loader's recreate set")


def test_contract():
    """Make the check above visible to `pytest loader/`.

    Both of this directory's standalone contract scripts (this one and loader/test_primary_keys.py)
    were invisible to a pytest run - no `test_*` function, so `pytest loader/ -q`
    collected nothing from them and reported green while they were red. They are
    the two files a schema change touches first, so they get a collectable entry
    point as well as `python3 loader/test_zapstore_schema.py`.
    """
    main()


if __name__ == "__main__":
    main()

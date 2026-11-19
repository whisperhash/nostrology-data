#!/usr/bin/env python3
"""Standalone contract test for the loader's PRIMARY_KEYS fallback map (no DB; text parse).

Run:  python3 loader/test_primary_keys.py      (exits non-zero on failure)

The COPY-based bulk load DROPS the primary key before loading and rebuilds it after
(load_jsonl_exports_into_postgres.py). `_capture_primary_key` reads the live catalog, but
a load that FAILS its PK rebuild leaves the table committed PK-less — so the next run can't
re-capture the PK from the catalog. `PRIMARY_KEYS` is the static fallback that guarantees
the PK is always restored. This test locks PRIMARY_KEYS == schema/init.sql so the fallback
can't silently drift from the real schema (which would mean a recovered table gets the
WRONG PK, or none):

  1. Every table in PRIMARY_KEYS declares that exact column as its inline PRIMARY KEY in
     init.sql, and PRIMARY_KEYS records it as the "(<col>)" SQL the rebuild uses.
  2. Every loader-COPY-managed event table (PK on event_id) declared in init.sql is in
     PRIMARY_KEYS — so a newly-onboarded slice can't be forgotten.
  3. PRIMARY_KEYS lists ONLY event_id PKs — the lightning_provider_signers (PK
     signer_pubkey) + active_user_counts (PK (period_start, granularity)) tables are loaded
     by other paths (--load-lightning-signers / schema/nutzap_active_users.sql), never the COPY
     loader, so their non-event_id PKs must stay out of this map.

Pure stdlib + file reads (no psycopg2), so it runs during workstation static checks too.
"""
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
_INIT_SQL = os.path.join(_REPO_ROOT, "schema", "init.sql")
_LOADER = os.path.join(_HERE, "load_jsonl_exports_into_postgres.py")

# The event tables the loader COPY-loads — each has `event_id CHAR(64) PRIMARY KEY`.
EXPECTED_PK = {
    "events_kind_10002_relay_lists": "event_id",
    "events_kind_0_profiles": "event_id",
    "events_kind_9735_zaps": "event_id",
    "events_kind_32267_app_catalog": "event_id",
    "events_kind_32267_app_catalog_excluded": "event_id",
    "events_kind_30063_app_releases": "event_id",
    "events_kind_30267_app_stacks": "event_id",
    "events_kind_9735_zaps_zapstore": "event_id",
    "events_kind_9321_nutzaps": "event_id",
    "events_kind_10019_nutzap_configs": "event_id",
}


def _table_block(sql, table):
    """Return the `CREATE TABLE IF NOT EXISTS <table> ( ... );` body, or None."""
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS\s+" + re.escape(table) + r"\s*\((.*?)\)\s*;",
        sql, re.DOTALL,
    )
    return m.group(1) if m else None


def _loader_primary_keys(loader_src):
    """Parse the table -> cols_sql entries from the loader's PRIMARY_KEYS literal."""
    m = re.search(r"PRIMARY_KEYS:\s*dict\[str,\s*str\]\s*=\s*\{(.*?)\n\}", loader_src, re.DOTALL)
    if not m:
        return None
    return dict(re.findall(r'"([^"]+)":\s*"(\([^"]*\))"', m.group(1)))


def main():
    failures = []
    with open(_INIT_SQL, encoding="utf-8") as f:
        init_sql = f.read()
    with open(_LOADER, encoding="utf-8") as f:
        loader_src = f.read()

    pk_map = _loader_primary_keys(loader_src)
    if pk_map is None:
        print("FAIL: could not find the PRIMARY_KEYS literal in the loader", file=sys.stderr)
        sys.exit(1)

    # 1 + 2: every expected table is in PRIMARY_KEYS as "(<col>)" AND declared inline in init.sql.
    for table, col in EXPECTED_PK.items():
        if pk_map.get(table) != f"({col})":
            failures.append(f"loader: PRIMARY_KEYS[{table}] must be \"({col})\", got {pk_map.get(table)!r}")
        block = _table_block(init_sql, table)
        if block is None:
            failures.append(f"init.sql: missing CREATE TABLE {table}")
        elif not re.search(r"\b" + re.escape(col) + r"\b[^,]*\bPRIMARY KEY\b", block):
            failures.append(f"init.sql: {table}.{col} must be the inline PRIMARY KEY")

    # 3: no surprise tables — every PRIMARY_KEYS entry is an expected event_id PK.
    for table in pk_map:
        if table not in EXPECTED_PK:
            failures.append(f"loader: PRIMARY_KEYS has unexpected table {table} "
                            f"(only event_id-PK COPY-loaded tables belong here)")

    if failures:
        print("FAIL: PRIMARY_KEYS contract:", file=sys.stderr)
        for msg in failures:
            print("  - " + msg, file=sys.stderr)
        sys.exit(1)

    print(f"OK: {len(EXPECTED_PK)} loader-COPY-managed PKs match init.sql + PRIMARY_KEYS")


def test_contract():
    """Make the check above visible to `pytest loader/`.

    Both of this directory's standalone contract scripts (this one and loader/test_zapstore_schema.py)
    were invisible to a pytest run - no `test_*` function, so `pytest loader/ -q`
    collected nothing from them and reported green while they were red. They are
    the two files a schema change touches first, so they get a collectable entry
    point as well as `python3 loader/test_primary_keys.py`.
    """
    main()


if __name__ == "__main__":
    main()

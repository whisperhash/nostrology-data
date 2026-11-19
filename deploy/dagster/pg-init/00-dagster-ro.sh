#!/bin/bash
# Postgres init hook (runs ONCE, on a fresh dagster-postgres data dir, as POSTGRES_USER).
# Provisions `dagster_ro`, the read-only role on Dagster's run store (as nostrology_ro is on
# the serving database). SELECT-only + default_transaction_read_only; ALTER DEFAULT PRIVILEGES
# so the tables `dagster instance migrate` creates LATER (as POSTGRES_USER) are SELECTable too.
# A deploy re-runs the GRANT after every migrate as a belt-and-suspenders, since this init only
# fires on a fresh data dir.
#
# DAGSTER_RO_PASSWORD is a generated hex secret (no SQL-quoting hazard). If unset, skip cleanly
# (the role just won't exist until a later re-run).
set -euo pipefail

if [ -z "${DAGSTER_RO_PASSWORD:-}" ]; then
  echo "[pg-init] DAGSTER_RO_PASSWORD not set — skipping dagster_ro provisioning (re-run the GRANT later)" >&2
  exit 0
fi

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dagster_ro') THEN
    CREATE ROLE dagster_ro LOGIN PASSWORD '${DAGSTER_RO_PASSWORD}';
  ELSE
    ALTER ROLE dagster_ro LOGIN PASSWORD '${DAGSTER_RO_PASSWORD}';
  END IF;
END \$\$;
GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO dagster_ro;
GRANT USAGE ON SCHEMA public TO dagster_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dagster_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA public GRANT SELECT ON TABLES TO dagster_ro;
ALTER ROLE dagster_ro SET default_transaction_read_only = on;
SQL

echo "[pg-init] dagster_ro provisioned (SELECT-only, default_transaction_read_only=on)"

-- Rebuild active_user_counts_kind_9321_nutzaps from the loaded kind-9321
-- (nutzaps) table.
--
-- DAU/WAU/MAU for the kind-9321 (nutzaps) slice, in pure SQL. Our pipeline loads
-- straight to Postgres (no retained JSONL), so this aggregates the already-loaded
-- base table. Its buckets:
--   * granularity  in  {'day','week','month'}  (same literal strings the getter
--     filters on - getNutzapsUsersMonthly reads granularity='month').
--   * period_start = unix epoch of the UTC bucket start. `AT TIME ZONE 'UTC'`
--     pins the bucketing to UTC regardless of session TZ (dev PG runs UTC
--     anyway); extract(epoch FROM <timestamp w/o tz>) then interprets the naive
--     timestamp as UTC. date_trunc('week') = Monday-of-week = Python
--     fromisocalendar(y,w,1) (matches even at ISO-year boundaries).
-- Population: the loader already applied the global pubkey blocklist when it
-- wrote events_kind_9321_nutzaps, so this aggregates blocklist-cleansed rows.
--
-- Payments only (nostrology-data#8): a sender counts only for rows with
-- value_source = 'proof_tags', i.e. events carrying at least one countable Cashu
-- proof in a `proof` tag. Draft-era, encrypted, content-token and zero-ecash events
-- stay in the base table but are not payments, so they add no active user and no
-- total_events. This table feeds the /nutzaps Overview MAU chart AND the paid
-- `nutzaps.active_users` MCP metric, so both change together.
--
-- Real mints only (nostrology-data#9): a row on an excluded mint (nutzap_mint_classes:
-- private hosts such as the July 2026 test harness on http://127.0.0.1:3338, and reviewed
-- fake-backend or gone test mints such as testnut) adds no active user and no total_events.
-- A mint missing from nutzap_mint_classes counts, like any unreviewed mint.
--
-- Idempotent: TRUNCATE + INSERT in one txn. Reads events_kind_9321_nutzaps and
-- nutzap_mint_classes, so it runs AFTER the kind-9321 (nutzaps) load and the mint
-- classification: the Dagster asset nutzaps_active_users (dagster_code/assets.py) runs it
-- in-process via psycopg2 (pipeline/postwork.rebuild_nutzap_mau). The old run.sh verbs
-- (refresh-nutzaps, rebuild-nutzap-mau) are retired. The Overview MAU
-- chart reads only 'month'; 'day'/'week' are computed for table-contract fidelity
-- (the getNutzapsUsers{Daily,Weekly} getters), at negligible cost.

BEGIN;

TRUNCATE active_user_counts_kind_9321_nutzaps;

INSERT INTO active_user_counts_kind_9321_nutzaps (period_start, granularity, unique_users, total_events)
  SELECT extract(epoch FROM date_trunc('day',   to_timestamp(created_at) AT TIME ZONE 'UTC'))::bigint,
         'day',   count(DISTINCT pubkey)::int, count(*)::bigint
    FROM events_kind_9321_nutzaps n
   WHERE value_source = 'proof_tags'
     AND NOT EXISTS (SELECT 1 FROM nutzap_mint_classes c
                      WHERE c.mint_url = n.mint_url AND c.excluded)
   GROUP BY 1
  UNION ALL
  SELECT extract(epoch FROM date_trunc('week',  to_timestamp(created_at) AT TIME ZONE 'UTC'))::bigint,
         'week',  count(DISTINCT pubkey)::int, count(*)::bigint
    FROM events_kind_9321_nutzaps n
   WHERE value_source = 'proof_tags'
     AND NOT EXISTS (SELECT 1 FROM nutzap_mint_classes c
                      WHERE c.mint_url = n.mint_url AND c.excluded)
   GROUP BY 1
  UNION ALL
  SELECT extract(epoch FROM date_trunc('month', to_timestamp(created_at) AT TIME ZONE 'UTC'))::bigint,
         'month', count(DISTINCT pubkey)::int, count(*)::bigint
    FROM events_kind_9321_nutzaps n
   WHERE value_source = 'proof_tags'
     AND NOT EXISTS (SELECT 1 FROM nutzap_mint_classes c
                      WHERE c.mint_url = n.mint_url AND c.excluded)
   GROUP BY 1;

COMMIT;

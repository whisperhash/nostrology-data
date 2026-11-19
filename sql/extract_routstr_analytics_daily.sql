-- nostrology extract - ROUTSTR ANALYTICS, LAST SNAPSHOT PER (pubkey, UTC day)
--   (routstr: k=38422 routstr-analytics-snapshots)
--
-- kind-38422 (routstr-analytics-snapshots) events are fat (~120 KB of trailing-window
-- JSON each) and providers republish every ~15 minutes, but consecutive snapshots from
-- one provider are near-identical: the ledger reconstruction
-- (dagster_code/pipeline/routstr.py) only needs each provider's LAST snapshot of each
-- UTC day - the daily/weekly window buckets it merges are identical across a day's
-- snapshots except for the still-accumulating leading edge, which the full-coverage
-- rule discards anyway. So the day-level row_number lives HERE and only ~1 row per
-- provider-day leaves Athena.
--
-- Two semantic filters are IN the SQL (mirrored by the reference generator
-- crosscheck/build_routstr_snapshot.py - change BOTH or the agreement gate
-- fails):
--   * the routstr.analytics.snapshot.v1 schema tag - kind-38422
--     (routstr-analytics-snapshots) rows without it are
--     not analytics snapshots (the d-tag '<pfx>:stats' shape is NOT used: the schema
--     tag is the versioned contract routstr-core publishes);
--   * last-per-(pubkey, day) with the NIP-01-style tie-break (created_at DESC, id ASC).
--
-- Trino: element_at(t, 1) / element_at(t, 2) - NOT t[1]/t[2]. from_unixtime is UTC.
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
UNLOAD (
  WITH src AS (
    SELECT id, pubkey, created_at, content, d_tag, ingested_at
    FROM __GLUE_DB__.__GLUE_TABLE__
    WHERE c = 'addressable' AND k = __KIND__ __EXTRA_WHERE__
      AND cardinality(filter(tags, t -> element_at(t, 1) = 'schema'
            AND element_at(t, 2) = 'routstr.analytics.snapshot.v1')) > 0
  ),
  deduped AS (
    SELECT id, pubkey, created_at, content, d_tag,
           row_number() OVER (PARTITION BY id ORDER BY ingested_at) AS rn_id
    FROM src
  ),
  daily AS (
    SELECT id, pubkey, created_at, content, d_tag,
           row_number() OVER (
             PARTITION BY pubkey, date_format(from_unixtime(created_at), '%Y-%m-%d')
             ORDER BY created_at DESC, id ASC) AS rn_day
    FROM deduped
    WHERE rn_id = 1
  )
  SELECT id, pubkey, created_at, content, d_tag
  FROM daily
  WHERE rn_day = 1
)
TO 's3://__BUCKET__/k=__KIND__/__RUN_ID__/'
WITH (format = 'JSON')

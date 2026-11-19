-- nostrology extract — REGULAR + id-dedup only  (e.g. k=9735 zaps, k=3063)
--
-- Faithful Trino/Athena port of nozzl/crates/nozzl-export/src/sql.rs (regular):
-- regular events are NOT collapsed per coordinate — keep ALL distinct events, removing
-- only exact-duplicate ids. Mode is ignored (matches sql.rs::regular_ignores_mode_id_dedup_only).
-- Only the id-level window applies; there is NO latest-per-coord stage.
--
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
UNLOAD (
  WITH src AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, ingested_at
    FROM __GLUE_DB__.__GLUE_TABLE__
    WHERE c = 'regular' AND k = __KIND__ __EXTRA_WHERE__
  ),
  deduped AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig,
           row_number() OVER (PARTITION BY id ORDER BY ingested_at) AS rn_id
    FROM src
  )
  SELECT id, pubkey, kind, created_at, content, tags, sig
  FROM deduped
  WHERE rn_id = 1
)
TO 's3://__BUCKET__/k=__KIND__/__RUN_ID__/'
WITH (format = 'JSON')

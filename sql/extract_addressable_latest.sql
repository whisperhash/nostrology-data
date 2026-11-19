-- nostrology extract — ADDRESSABLE + latest  (e.g. k=30063/30267/32267 zapstore)
--
-- Faithful Trino/Athena port of nozzl/crates/nozzl-export/src/sql.rs
-- (addressable, mode=latest): same as replaceable, but the second window keys on
-- (kind, pubkey, d_tag) — latest per addressable coordinate. d_tag is window-only,
-- never emitted (matches sql.rs::addressable_latest_partitions_by_d_tag).
--
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
UNLOAD (
  WITH src AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, d_tag, ingested_at
    FROM __GLUE_DB__.__GLUE_TABLE__
    WHERE c = 'addressable' AND k = __KIND__ __EXTRA_WHERE__
  ),
  deduped AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, d_tag, ingested_at,
           row_number() OVER (PARTITION BY id ORDER BY ingested_at) AS rn_id
    FROM src
  ),
  latest AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig,
           row_number() OVER (PARTITION BY kind, pubkey, d_tag ORDER BY created_at DESC) AS rn_coord
    FROM deduped
    WHERE rn_id = 1
  )
  SELECT id, pubkey, kind, created_at, content, tags, sig
  FROM latest
  WHERE rn_coord = 1
)
TO 's3://__BUCKET__/k=__KIND__/__RUN_ID__/'
WITH (format = 'JSON')

-- nostrology extract — REPLACEABLE + latest  (e.g. k=10002 NIP-65 relay lists)
--
-- Faithful Trino/Athena port of nozzl/crates/nozzl-export/src/sql.rs
-- (replaceable, mode=latest):
--   id-level dedup    : row_number() OVER (PARTITION BY id ORDER BY ingested_at) = 1
--   latest-per-coord  : row_number() OVER (PARTITION BY kind, pubkey ORDER BY created_at DESC) = 1
-- Athena/Trino has NO QUALIFY (verified), so each window is a subquery + WHERE rn=1.
-- Projects only the 7 bare NIP-01 fields; d_tag/ingested_at are
-- window-only, never emitted (matches sql.rs + the loader's NDJSON contract).
--
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
UNLOAD (
  WITH src AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, d_tag, ingested_at
    FROM __GLUE_DB__.__GLUE_TABLE__
    WHERE c = 'replaceable' AND k = __KIND__ __EXTRA_WHERE__
  ),
  deduped AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, d_tag, ingested_at,
           row_number() OVER (PARTITION BY id ORDER BY ingested_at) AS rn_id
    FROM src
  ),
  latest AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig,
           row_number() OVER (PARTITION BY kind, pubkey ORDER BY created_at DESC) AS rn_coord
    FROM deduped
    WHERE rn_id = 1
  )
  SELECT id, pubkey, kind, created_at, content, tags, sig
  FROM latest
  WHERE rn_coord = 1
)
TO 's3://__BUCKET__/k=__KIND__/__RUN_ID__/'
WITH (format = 'JSON')

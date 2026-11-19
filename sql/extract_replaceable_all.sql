-- nostrology extract - REPLACEABLE + ALL VERSIONS  (ngit: k=10317)
--
-- extract_replaceable_latest.sql WITHOUT its latest-per-pubkey stage: the /git compute
-- (dagster_code/pipeline/ngit.py) needs EVERY archived version of
-- kind-10317 (grasp-server-lists) - a user's adoption month is the month of their EARLIEST
-- archived list, and the current list is chosen in Python with the exact NIP-01 (protocol basics)
-- tie-break (created_at ascending, id descending; the latest template's window orders by
-- created_at alone, which is nondeterministic on ties). Only the id-level dedup applies.
-- Projects the 7 bare NIP-01 (protocol basics) fields (a replaceable kind's coordinate is the pubkey, so
-- d_tag is not emitted).
--
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
UNLOAD (
  WITH src AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, ingested_at
    FROM __GLUE_DB__.__GLUE_TABLE__
    WHERE c = 'replaceable' AND k = __KIND__ __EXTRA_WHERE__
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

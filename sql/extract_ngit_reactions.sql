-- nostrology extract - kind-7 (reactions) to git repositories  (ngit: into=ngit, k=7)
--
-- REGULAR + id-dedup, with the /git subset selected IN Athena: kind-7 (reactions) is a
-- multi-gigabyte kind, and only reactions to a repository announcement leave Athena - a
-- `k` tag valued '30617' (the reacted-to kind, NIP-25 (reactions); gitworkshop and gittr
-- stars) OR an `a` tag whose value names a kind-30617 (repo-announcements) coordinate
-- ('30617:...'). This is the exact input of docs/dataset-reference/ngit-dataset-reference.md
-- section 6.2; which of these are stars ('+' or empty content) and which repository they
-- name is decided in dagster_code/pipeline/ngit.py. The subset may be empty (the
-- extract_nonempty check is a warning for this asset).
--
-- Trino: element_at(t, 1) / element_at(t, 2) - NOT t[1] / t[2].
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
-- (__KIND__ is always 7 - the into=ngit override pins the template; __EXTRA_WHERE__ is empty.)
UNLOAD (
  WITH src AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, ingested_at
    FROM __GLUE_DB__.__GLUE_TABLE__
    WHERE c = 'regular' AND k = __KIND__ __EXTRA_WHERE__
      AND (
        cardinality(filter(tags, t -> element_at(t, 1) = 'k'
              AND element_at(t, 2) = '30617')) > 0
        OR cardinality(filter(tags, t -> element_at(t, 1) = 'a'
              AND element_at(t, 2) LIKE '30617:%')) > 0
      )
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

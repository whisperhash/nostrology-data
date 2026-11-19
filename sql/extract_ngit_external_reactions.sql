-- nostrology extract - kind-17 (external-content-reactions) to git repositories  (ngit: into=ngit, k=17)
--
-- REGULAR + id-dedup, with the /git subset selected IN Athena: only Iris Git's repository
-- stars leave Athena - a `k` tag valued 'git-repo' (Iris Git's own content type, not a NIP-73
-- (external content IDs) one) AND an `i` tag whose value names a
-- kind-30617 (repo-announcements) coordinate ('30617:...'). The rest of the kind is
-- reactions to podcasts, websites and the like. This is the exact input of
-- docs/dataset-reference/ngit-dataset-reference.md section 6.2; each key's current reaction per coordinate,
-- and so which rows are stars and which repository they name, is decided in
-- dagster_code/pipeline/ngit.py. The subset is never empty since nozzl's 2026-10-11
-- backfill (31 rows), so this asset's extract_nonempty check is BLOCKING: on the extract's
-- first run the anti-shrink gate has no served value to compare with, and an empty subset
-- would otherwise write zero Iris Git stars.
--
-- Trino: element_at(t, 1) / element_at(t, 2) - NOT t[1] / t[2].
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
-- (__KIND__ is always 17 - the into=ngit override pins the template; __EXTRA_WHERE__ is empty.)
UNLOAD (
  WITH src AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, ingested_at
    FROM __GLUE_DB__.__GLUE_TABLE__
    WHERE c = 'regular' AND k = __KIND__ __EXTRA_WHERE__
      AND cardinality(filter(tags, t -> element_at(t, 1) = 'k'
            AND element_at(t, 2) = 'git-repo')) > 0
      AND cardinality(filter(tags, t -> element_at(t, 1) = 'i'
            AND element_at(t, 2) LIKE '30617:%')) > 0
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

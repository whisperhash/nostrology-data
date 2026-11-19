-- nostrology extract - kind-1111 (comments) on git objects  (ngit: into=ngit, k=1111)
--
-- REGULAR + id-dedup, with the /git subset selected IN Athena: of ~314,000
-- kind-1111 (comments) only the NIP-34 (git collaboration) discussion (~7,300 on
-- 2026-09-25) leaves Athena. A comment is kept when
--   * a `K` (root kind) tag names an issue, patch, pull request or repository kind:
--     kind-1617 (git-patches), kind-1618 (git-pull-requests),
--     kind-1619 (git-pr-updates), kind-1621 (git-issues),
--     kind-30617 (repo-announcements) or kind-30618 (repo-states); OR
--   * an `A` (root) or `a` (parent) tag's value names a kind-30617 (repo-announcements)
--     coordinate ('30617:...').
-- This is a SUPERSET of the rule in docs/dataset-reference/ngit-dataset-reference.md section 6.1,
-- which reads the FIRST `K` tag only: dagster_code/pipeline/ngit.py re-applies the exact
-- rule (a comment whose first `K` is some other kind and that names no repository is
-- dropped there), so the two never disagree on what the input is.
--
-- Trino: element_at(t, 1) / element_at(t, 2) - NOT t[1] / t[2] (element_at returns NULL
-- past the end; the subscript operator fails).
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
-- (__KIND__ is always 1111 - the into=ngit override pins the template; __EXTRA_WHERE__ is empty.)
UNLOAD (
  WITH src AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, ingested_at
    FROM __GLUE_DB__.__GLUE_TABLE__
    WHERE c = 'regular' AND k = __KIND__ __EXTRA_WHERE__
      AND (
        cardinality(filter(tags, t -> element_at(t, 1) = 'K'
              AND element_at(t, 2) IN ('1617', '1618', '1619', '1621', '30617', '30618'))) > 0
        OR cardinality(filter(tags, t -> element_at(t, 1) IN ('A', 'a')
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

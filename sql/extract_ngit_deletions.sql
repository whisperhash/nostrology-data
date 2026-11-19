-- nostrology extract - kind-5 (event-deletion-requests) SELF-AUTHORED requests naming
-- kind-30617 (repo-announcements) coordinates  (ngit: into=ngit, k=5)
--
-- NOT a per-event extract: kind-5 (event-deletion-requests) is tens of millions of events,
-- so the reduction runs serverless in Athena and only the DISTINCT
-- (author, coordinate, created_at) triples leave Athena - the only facts the /git ledger
-- reads (rule N6 of docs/dataset-reference/ngit-dataset-reference.md section 2.2: a request
-- counts when its created_at is >= the coordinate's current version, and a
-- re-announcement after it restarts the repository). Triples, not the newest created_at
-- per pair: the compute applies the horizon to EACH triple (as_of_day 23:59:59Z and the
-- 2023-01-01 floor), so a future-dated request can never mask an earlier valid one, and
-- the newest surviving triple of a pair decides.
-- The self-authored filter (author = the coordinate's pubkey) is NIP-09 (deletion
-- requests)'s validity rule, exactly as in extract_markets_deletions.sql: a request covers
-- only its OWN author's events, and honouring a cross-author request would delete a
-- repository its maintainer never withdrew. dagster_code/pipeline/ngit.py matches author,
-- pubkey and the full coordinate again, so an extract bug cannot delete another key's
-- repository.
--
-- The id-level dedup window the event extracts run is unnecessary here: DISTINCT already
-- collapses a duplicate-id row's identical triple.
--
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
-- (__KIND__ is always 5 - the into=ngit override pins the template; __EXTRA_WHERE__ is empty.)
UNLOAD (
  SELECT DISTINCT pubkey AS author, element_at(tg, 2) AS coord, created_at
  FROM __GLUE_DB__.__GLUE_TABLE__
  CROSS JOIN UNNEST(tags) AS u(tg)
  WHERE c = 'regular' AND k = __KIND__ __EXTRA_WHERE__
    AND element_at(tg, 1) = 'a'
    AND element_at(tg, 2) LIKE '30617:%'
    AND pubkey = split_part(element_at(tg, 2), ':', 2)
)
TO 's3://__BUCKET__/k=__KIND__/__RUN_ID__/'
WITH (format = 'JSON')

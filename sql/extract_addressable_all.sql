-- nostrology extract - ADDRESSABLE + ALL VERSIONS  (marketplaces: k=30402/30406)
--
-- Unlike extract_addressable_latest.sql there is NO latest-per-coordinate stage: the
-- marketplaces compute (dagster_code/pipeline/marketplaces.py) needs EVERY revision --
-- the farm-title detector counts title/pubkey/price combinations across all versions,
-- and `archive_events` is the all-versions count -- and it performs the current-version
-- selection itself with the generator's exact tie-break (created_at DESC, id ASC; the
-- latest template's window orders by created_at alone, which is nondeterministic on
-- ties). Only the id-level dedup applies, and d_tag IS emitted (the compute's
-- coordinate key + the NIP-09 deletion-coordinate match need it; the latest template
-- keeps it window-only).
--
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
UNLOAD (
  WITH src AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, d_tag, ingested_at
    FROM __GLUE_DB__.__GLUE_TABLE__
    WHERE c = 'addressable' AND k = __KIND__ __EXTRA_WHERE__
  ),
  deduped AS (
    SELECT id, pubkey, kind, created_at, content, tags, sig, d_tag,
           row_number() OVER (PARTITION BY id ORDER BY ingested_at) AS rn_id
    FROM src
  )
  SELECT id, pubkey, kind, created_at, content, tags, sig, d_tag
  FROM deduped
  WHERE rn_id = 1
)
TO 's3://__BUCKET__/k=__KIND__/__RUN_ID__/'
WITH (format = 'JSON')

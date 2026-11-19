-- nostrology extract - kind-5 (event-deletion-requests) SELF-AUTHORED requests naming
-- kind-30402 (markets-product-listings) listings  (marketplaces: into=markets-deletions)
--
-- NOT a per-event extract: kind-5 (event-deletion-requests) is tens of millions of events,
-- so the reduction runs serverless in Athena and only the DISTINCT
-- (author, tag, value, created_at) rows leave it - the only facts the compute reads. NIP-09
-- gives a request two ways to name a listing, and clients use both:
--   tag 'a'  value = the coordinate, `30402:<pubkey>:<d>`. Covers every version up to the
--            request's created_at, so a listing republished after it is live again.
--   tag 'e'  value = the id of ONE version. Covers exactly that event, whatever its date.
-- Until 2026-11-04 this read 'a' tags alone and kept no date, which counted 434 listings their
-- authors had deleted by id as live and excluded 74 republished after their request.
--
-- Rows, not the newest created_at per pair: the compute applies its horizon to EACH row, so a
-- future-dated request can never mask an earlier valid one - as extract_ngit_deletions.sql.
--
-- The self-authored filter is NIP-09's validity rule: a request covers only its OWN author's
-- events; two publishers have issued cross-author requests naming other pubkeys' coordinates,
-- and honouring those would delete live listings their owners never withdrew. An 'a' tag
-- carries its author in the coordinate. An 'e' tag is a bare id, so only joining it to the
-- listing it names can show who wrote that. The compute asserts both held
-- (marketplaces.parse_deletions) rather than re-applying them, so an extract bug fails loud.
--
-- ONE PASS OVER kind-5 (event-deletion-requests), which is why this is a LEFT JOIN and not a
-- UNION ALL of the two forms over a shared WITH: Athena expands a WITH per reference, so that
-- shape scans the kind twice, about 7.2 GiB of the workgroup's enforced 10 GiB per-query cutoff.
-- The inner side is small (kind-30402 (markets-product-listings) scans 21 MiB) and on the right,
-- so it is the hash join's build side.
--
-- THE JOIN READS THE CATALOG'S kind-30402 (markets-product-listings) PARTITIONS, and this
-- extract's own flush and register cover only kind-5 (event-deletion-requests). So this runs
-- after marketplaces_listings_extract, which flushes and registers them (assets.py). A version
-- that lands after that is simply not named today and is picked up tomorrow.
--
-- The id-level dedup window the event extracts run is unnecessary here: DISTINCT already
-- collapses a duplicate-id row's identical rows.
--
-- Placeholders substituted by pipeline/sql.py: __KIND__ __BUCKET__ __RUN_ID__ __EXTRA_WHERE__ __GLUE_DB__ __GLUE_TABLE__
-- (__KIND__ is always 5 -- the into=markets-deletions override pins it; __EXTRA_WHERE__ is empty.)
UNLOAD (
  SELECT DISTINCT r.author, r.tag, r.value, r.created_at
  FROM (
    SELECT pubkey AS author, element_at(tg, 1) AS tag, element_at(tg, 2) AS value, created_at
    FROM __GLUE_DB__.__GLUE_TABLE__
    CROSS JOIN UNNEST(tags) AS u(tg)
    WHERE c = 'regular' AND k = __KIND__ __EXTRA_WHERE__
      AND element_at(tg, 1) IN ('a', 'e')
  ) r
  LEFT JOIN (
    SELECT DISTINCT id, pubkey
    FROM __GLUE_DB__.__GLUE_TABLE__
    WHERE c = 'addressable' AND k = 30402
  ) l ON r.value = l.id AND r.author = l.pubkey
  WHERE (r.tag = 'a' AND r.value LIKE '30402:%' AND r.author = split_part(r.value, ':', 2))
     OR (r.tag = 'e' AND l.id IS NOT NULL)
)
TO 's3://__BUCKET__/k=__KIND__/__RUN_ID__/'
WITH (format = 'JSON')

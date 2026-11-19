-- zapstore_native_releases_daily.sql — daily count of Zapstore-NATIVE app-release
-- events (Nostr kind 30063): the "native" (purple) series behind nostrology-web's
-- "App Releases" chart, published as a Nectar dataset (coordinate
-- 30800:<nostrology>:zapstore/native-app-releases-count-daily).
--
-- Faithful port of the native_count arm of SQL_ZAPSTORE_APP_RELEASES_DAILY
-- (nostrology-web/src/lib/db/postgres.ts), reading the loaded Aurora table
-- events_kind_30063_app_releases. Two deliberate PUBLISHED-DATASET DIVERGENCES from
-- the chart:
--   1. FULL HISTORY, not the chart's trailing-365-day window — a consumer can filter
--      down; they cannot un-trim.
--   2. SPARSE (one row per day that HAS >= 1 native release), not the chart's dense
--      calendar — a consumer can densify; the raw grain stays honest and small.
--
-- Native classification (identical to the chart, postgres.ts:1532-1533): a release is
-- "native" UNLESS signed by the Zapstore CURATOR key AND not one of Zapstore's own
-- first-party apps. Equivalently native = (pubkey <> curator) OR (app_d_tag = ANY(own)).
--   %(curator)s     -> ZAPSTORE_PUBKEY        (the curator/republisher key)
--   %(own_d_tags)s  -> ZAPSTORE_OWN_D_TAGS    (text[]; Zapstore's own first-party apps)
-- Counts release EVENTS, not distinct apps (matches native_count) — 30 patch pushes = 30.
--
-- Day bucket is UTC-explicit: to_timestamp(bigint) yields timestamptz; AT TIME ZONE
-- 'UTC' pins the wall-clock to UTC before truncation, so the boundary does not drift
-- with the DB session TimeZone.

SELECT
    date_trunc('day', to_timestamp(created_at) AT TIME ZONE 'UTC')::date AS day,
    count(*)::bigint AS native_releases
FROM events_kind_30063_app_releases
WHERE pubkey <> %(curator)s
   OR app_d_tag = ANY(%(own_d_tags)s)
GROUP BY 1
ORDER BY 1

-- nostrology-data — serving schema (Postgres 16).
--
-- This file is the schema contract that nostrology-web reads. It grows one
-- slice per milestone:
--   M1 = /relays (kind-10002 (relay-list-metadata); the three relay_* materialized views).
--   M2 = /lightning (kind-9735 (zap-receipts) + kind-0 (profile-metadata) + the
--        provider-signer map; NO MVs - the page queries these base tables live, so the
--        loader issues no REFRESH).
--   M3 = /zapstore (the addressable kind-32267 (zapstore-app-catalog),
--        kind-30063 (zapstore-app-releases) and kind-30267 (zapstore-app-stacks) + the
--        derived events_kind_9735_zaps_zapstore subset; NO MVs - live base-table queries.
--        kind-3063 (zapstore-asset-metadata) is deliberately EXCLUDED: no /zapstore page
--        reads it, so onboarding it would only create a dead table).
--   M4 = Nutzaps / NIP-61 (nutzaps), the /nutzaps surface: kind-9321 (nutzaps)
--        payments + kind-10019 (nutzap-configs) + the precomputed
--        active_user_counts_kind_9321_nutzaps (rebuilt by
--        schema/nutzap_active_users.sql, in pure SQL); NO MVs.
--        kind-7376 (nutzap-redemptions) is EXCLUDED: no /nutzaps page reads it (it
--        backs no transplanted route), so it would only create a dead table.
--
-- The relays slice (table + 3 MVs) is kept exactly as /relays reads it.
--
-- IMPORTANT (cold start): the materialized views below are created WITHOUT
-- `WITH NO DATA`, so each is *populated at creation* (an empty result over the
-- empty base table is still a populated state). Do NOT add `WITH NO DATA` — an
-- unpopulated MV makes the loader's first `REFRESH MATERIALIZED VIEW
-- CONCURRENTLY` fail with "has not been populated".

-- ---------------------------------------------------------------------------
-- Base table: kind-10002 (relay-list-metadata), the NIP-65 (relay list metadata) relay lists.
-- ---------------------------------------------------------------------------
-- Each `r` tag carries a relay URL + optional "read"/"write" marker (no marker
-- = both). The loader expands into three deduplicated arrays so the per-relay
-- leaderboard on /relays computes total / read / write counts via single
-- GIN-indexed ANY() lookups instead of per-row tag inspection. Replaceable
-- kind: one row per pubkey logically; we store everything we receive (PK on
-- event_id) and let queries resolve the latest via DISTINCT ON (pubkey)
-- ORDER BY pubkey, created_at DESC.
CREATE TABLE IF NOT EXISTS events_kind_10002_relay_lists (
    event_id          CHAR(64) PRIMARY KEY,
    pubkey            CHAR(64) NOT NULL,
    created_at        BIGINT   NOT NULL,
    relay_urls        TEXT[]   NOT NULL DEFAULT '{}',
    read_relay_urls   TEXT[]   NOT NULL DEFAULT '{}',
    write_relay_urls  TEXT[]   NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_k10002_pubkey_created    ON events_kind_10002_relay_lists (pubkey, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_k10002_created           ON events_kind_10002_relay_lists (created_at);
CREATE INDEX IF NOT EXISTS idx_k10002_relay_urls        ON events_kind_10002_relay_lists USING GIN (relay_urls);
CREATE INDEX IF NOT EXISTS idx_k10002_read_relay_urls   ON events_kind_10002_relay_lists USING GIN (read_relay_urls);
CREATE INDEX IF NOT EXISTS idx_k10002_write_relay_urls  ON events_kind_10002_relay_lists USING GIN (write_relay_urls);

-- ---------------------------------------------------------------------------
-- Materialized views for the /relays page.
-- ---------------------------------------------------------------------------
-- All three derive from events_kind_10002_relay_lists (replaceable kind:
-- latest-per-pubkey via DISTINCT ON). Each MV is the final shape the frontend's
-- relay queries return — see nostrology-web's lib/db/postgres.ts (getRelayStats
-- / getRelayPerUserDistribution / getRelayTierUserCounts), which SELECT * FROM
-- these. Refresh is triggered by the loader at the end of each
-- kind-10002 (relay-list-metadata) load (REFRESH MATERIALIZED VIEW CONCURRENTLY).

-- Per-relay role counts. Powers the /relays leaderboard table and the
-- bottom-of-page role-mix charts.
CREATE MATERIALIZED VIEW IF NOT EXISTS relay_stats_mv AS
WITH latest_lists AS (
    SELECT DISTINCT ON (pubkey) pubkey, relay_urls, read_relay_urls, write_relay_urls
    FROM events_kind_10002_relay_lists
    ORDER BY pubkey, created_at DESC
),
unrolled AS (
    SELECT
        unnest(relay_urls) AS relay_url,
        read_relay_urls,
        write_relay_urls
    FROM latest_lists
)
SELECT
    relay_url,
    COUNT(*)::int                                                                                AS user_count,
    SUM((relay_url = ANY(read_relay_urls))::int)::int                                            AS read_count,
    SUM((relay_url = ANY(write_relay_urls))::int)::int                                           AS write_count,
    SUM(((relay_url = ANY(read_relay_urls)) AND (relay_url = ANY(write_relay_urls)))::int)::int  AS both_count
FROM unrolled
GROUP BY relay_url;

CREATE UNIQUE INDEX IF NOT EXISTS idx_relay_stats_mv_relay_url
    ON relay_stats_mv (relay_url);
CREATE INDEX IF NOT EXISTS idx_relay_stats_mv_user_count_desc
    ON relay_stats_mv (user_count DESC);

-- Distribution of "how many relays does each user list". Powers the
-- per-user-relay-count histogram.
CREATE MATERIALIZED VIEW IF NOT EXISTS relay_per_user_distribution_mv AS
WITH latest_lists AS (
    SELECT DISTINCT ON (pubkey) pubkey, relay_urls
    FROM events_kind_10002_relay_lists
    ORDER BY pubkey, created_at DESC
),
per_user AS (
    SELECT COALESCE(array_length(relay_urls, 1), 0) AS n FROM latest_lists
),
bucketed AS (
    SELECT
        CASE
            WHEN n = 0 THEN '0'
            WHEN n BETWEEN 1 AND 10 THEN n::text
            WHEN n BETWEEN 11 AND 20 THEN '11–20'
            WHEN n BETWEEN 21 AND 50 THEN '21–50'
            WHEN n BETWEEN 51 AND 100 THEN '51–100'
            ELSE '100+'
        END AS bucket,
        CASE
            WHEN n = 0 THEN 0
            WHEN n BETWEEN 1 AND 10 THEN n
            WHEN n BETWEEN 11 AND 20 THEN 11
            WHEN n BETWEEN 21 AND 50 THEN 21
            WHEN n BETWEEN 51 AND 100 THEN 51
            ELSE 1000
        END AS sort_key
    FROM per_user
)
SELECT bucket, sort_key, COUNT(*)::int AS user_count
FROM bucketed
GROUP BY bucket, sort_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_relay_per_user_dist_mv_sort_key
    ON relay_per_user_distribution_mv (sort_key);

-- Per-tier distinct-user counts. Powers the relay-tier histogram. 7 rows.
-- Tier boundaries MUST stay in sync with the TIERS constant in
-- nostrology-web's lib/components/RelayTierHistogram.svelte.
CREATE MATERIALIZED VIEW IF NOT EXISTS relay_tier_user_counts_mv AS
WITH latest_lists AS (
    SELECT DISTINCT ON (pubkey) pubkey, relay_urls
    FROM events_kind_10002_relay_lists
    ORDER BY pubkey, created_at DESC
),
relay_user_counts AS (
    SELECT relay_url, COUNT(*)::int AS uc
    FROM (SELECT unnest(relay_urls) AS relay_url FROM latest_lists) sub
    GROUP BY relay_url
),
tier_defs(tier_label, sort_key, min_users, max_users) AS (VALUES
    ('1',         1, 1,      1),
    ('2–10',      2, 2,      10),
    ('11–100',    3, 11,     100),
    ('101–1k',    4, 101,    1000),
    ('1k–10k',    5, 1001,   10000),
    ('10k–100k',  6, 10001,  100000),
    ('100k+',     7, 100001, 9999999999)
),
relay_with_tier AS (
    SELECT r.relay_url, t.tier_label, t.sort_key
    FROM relay_user_counts r
    JOIN tier_defs t ON r.uc BETWEEN t.min_users AND t.max_users
),
user_relay AS (
    SELECT pubkey, unnest(relay_urls) AS relay_url FROM latest_lists
    WHERE COALESCE(array_length(relay_urls, 1), 0) > 0
),
total AS (
    SELECT COUNT(*)::int AS total_users
    FROM latest_lists
    WHERE COALESCE(array_length(relay_urls, 1), 0) > 0
)
SELECT
    t.tier_label,
    t.sort_key,
    COUNT(DISTINCT ur.pubkey)::int          AS users_in_tier,
    (SELECT total_users FROM total)::int    AS total_users
FROM tier_defs t
LEFT JOIN relay_with_tier rwt ON rwt.tier_label = t.tier_label
LEFT JOIN user_relay ur ON ur.relay_url = rwt.relay_url
GROUP BY t.tier_label, t.sort_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_relay_tier_user_counts_mv_sort_key
    ON relay_tier_user_counts_mv (sort_key);

-- ===========================================================================
-- M2 - the /lightning slice: kind-9735 (zap-receipts) + kind-0 (profile-metadata) + the
-- provider-signer map.
-- ===========================================================================
-- Base tables below; the two materialized views that the /lightning page actually
-- serves from are defined further down (after lightning_provider_signers). Until
-- 2026-06-30 /lightning queried these base tables live, but the two trend/leaderboard
-- queries each re-scanned the largest tables on every request (kind-9735 (zap-receipts)
-- ~3.5M rows / ~2.8 GB and kind-0 (profile-metadata) ~2.3M rows / ~1.45 GB) - ~5.5 s
-- (trends, x4 concurrent) / ~2 s
-- (leaderboard) warm, I/O-bound at the serving-time 1-ACU ceiling. So they were
-- promoted to MVs (the /relays pattern): a daily provider grain for the trend charts
-- and a domain leaderboard, both refreshed CONCURRENTLY by the Dagster lightning_mv
-- asset after the kind-0 (profile-metadata) / kind-9735 (zap-receipts) / signer loads.
-- See nostrology-web's
-- lib/db/postgres.ts (getLightningProviderStats + getLightningTrends* now read the MVs).

-- kind-0 (profile-metadata): structured metadata fields with FTS on name + about. The
-- /lightning provider leaderboard reads only `lud16` (counts profiles per
-- Lightning-address domain); the full table is lifted faithfully since profiles
-- are foundational and reused by later milestones.
-- NOTE: that leaderboard is COUNT(*) GROUP BY lud16-domain, which equals "distinct
-- users per domain" ONLY because the replaceable extract collapses to
-- latest-per-pubkey (one row per pubkey). The PK is event_id, not pubkey — nothing
-- here enforces one-row-per-pubkey, so a loader that ever wrote multiple
-- kind-0 (profile-metadata) versions per pubkey would inflate the counts.
CREATE TABLE IF NOT EXISTS events_kind_0_profiles (
    event_id     CHAR(64) PRIMARY KEY,
    pubkey       CHAR(64) NOT NULL,
    created_at   BIGINT NOT NULL,
    name         TEXT,
    display_name TEXT,
    about        TEXT,
    nip05        TEXT,
    lud16        TEXT,
    picture      TEXT,
    content_tsv  TSVECTOR GENERATED ALWAYS AS
        (to_tsvector('english', coalesce(name,'') || ' ' || coalesce(display_name,'') || ' ' || coalesce(about,''))) STORED
);
CREATE INDEX IF NOT EXISTS idx_k0_fts ON events_kind_0_profiles USING GIN (content_tsv);
CREATE INDEX IF NOT EXISTS idx_k0_pubkey ON events_kind_0_profiles (pubkey, created_at);
CREATE INDEX IF NOT EXISTS idx_k0_nip05 ON events_kind_0_profiles (left(nip05, 255)) WHERE nip05 IS NOT NULL;

-- kind-9735 (zap-receipts): who zapped what post, how much.
-- Sender parsed from the embedded kind-9734 (NIP-57 zap request) (see src/shared/zap_parser.py).
-- lnurl_signer_pubkey is the raw event.pubkey of the kind-9735 (zap-receipts) - the LNURL
-- service's nostr signer. Per NIP-57 it MUST match the recipient wallet's
-- published `nostrPubkey` at the time of the zap, giving at-the-time provider
-- attribution (see lightning_provider_signers below + the /lightning trends charts).
CREATE TABLE IF NOT EXISTS events_kind_9735_zaps (
    event_id            CHAR(64) PRIMARY KEY,
    sender_pubkey       CHAR(64) NOT NULL,
    recipient_pubkey    CHAR(64),
    created_at          BIGINT NOT NULL,
    target_event_id     CHAR(64),
    amount_msats        BIGINT,
    lnurl_signer_pubkey CHAR(64)
);
ALTER TABLE events_kind_9735_zaps ADD COLUMN IF NOT EXISTS lnurl_signer_pubkey CHAR(64);
CREATE INDEX IF NOT EXISTS idx_k9735_target ON events_kind_9735_zaps (target_event_id) WHERE target_event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_k9735_sender ON events_kind_9735_zaps (sender_pubkey, created_at);
CREATE INDEX IF NOT EXISTS idx_k9735_recipient ON events_kind_9735_zaps (recipient_pubkey, created_at);
-- NOTE: idx_k9735_signer_created is created ONLY here — it is intentionally
-- outside the loader's drop/recreate set (SECONDARY_INDEXES[9735]), so init.sql
-- (the schema apply) is the only thing that ever creates it. It MUST exist
-- before the first kind-9735 (zap-receipts) load - the trends charts JOIN on lnurl_signer_pubkey.
CREATE INDEX IF NOT EXISTS idx_k9735_signer_created ON events_kind_9735_zaps (lnurl_signer_pubkey, created_at) WHERE lnurl_signer_pubkey IS NOT NULL;

-- Mapping: NIP-57 LNURL service signer pubkey -> canonical lud16 domain.
-- Source-of-truth is data/lightning/provider_signers.yaml; the loader's
-- --load-lightning-signers mode truncates + reinserts on each refresh. Powers
-- the /lightning trends charts (at-the-time provider attribution for
-- kind-9735 (zap-receipts) via JOIN on lnurl_signer_pubkey).
CREATE TABLE IF NOT EXISTS lightning_provider_signers (
    signer_pubkey   CHAR(64) PRIMARY KEY,
    provider_domain TEXT     NOT NULL,
    verified_at     DATE,
    verified_via    TEXT,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_lps_domain ON lightning_provider_signers (provider_domain);

-- ---------------------------------------------------------------------------
-- Materialized views for the /lightning page.
-- ---------------------------------------------------------------------------
-- Two MVs, refreshed CONCURRENTLY by the Dagster lightning_mv asset (kinds.py
-- LIGHTNING_MVS) after the kind-0 (profile-metadata) + kind-9735 (zap-receipts) + signer
-- loads. As with the relay MVs above, they are created WITH DATA (do NOT add `WITH NO DATA` - the first
-- CONCURRENTLY refresh would fail "has not been populated"), and each carries a
-- UNIQUE index (CONCURRENTLY's requirement).
--
-- IMPORTANT — these MV bodies are mirrored in nostrology-web's lib/db/postgres.ts
-- as the exported constants SQL_LIGHTNING_PROVIDER_STATS_MV and
-- SQL_LIGHTNING_DAILY_PROVIDER_MV (the web's integration tests build a temp VIEW
-- of the same name from those constants to shadow these MVs over temp fixtures).
-- Keep the two copies byte-faithful — a drift means the tests validate logic the
-- production MV no longer runs.

-- Provider leaderboard: well-formed-lud16 domain counts over kind-0 (profile-metadata) rows.
-- Powers the /lightning provider leaderboard (getLightningProviderStats SELECTs
-- domain + user_count, ordering by user_count DESC on read). COUNT(*) GROUP BY
-- domain equals "distinct users per domain" only because the replaceable
-- kind-0 (profile-metadata) extract collapses to latest-per-pubkey (see the events_kind_0_profiles note).
CREATE MATERIALIZED VIEW IF NOT EXISTS lightning_provider_stats_mv AS
WITH parsed AS (
    SELECT lower(split_part(lud16, '@', 2)) AS domain
    FROM events_kind_0_profiles
    WHERE lud16 ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$'
)
SELECT domain, COUNT(*)::int AS user_count
FROM parsed
GROUP BY domain;

CREATE UNIQUE INDEX IF NOT EXISTS idx_lightning_provider_stats_mv_domain
    ON lightning_provider_stats_mv (domain);
CREATE INDEX IF NOT EXISTS idx_lightning_provider_stats_mv_user_count_desc
    ON lightning_provider_stats_mv (user_count DESC);

-- Daily provider grain: per (day, provider) zap count + capped sats over ALL
-- history. This is the single expensive aggregate (the LEFT JOIN against the
-- signer map + spam filter + sats cap, collapsing the ~3.5M-row
-- kind-9735 (zap-receipts) stream to a few thousand (day, provider) buckets) that all four getLightningTrends*
-- queries used to recompute on every request. They now read this MV and apply
-- only the cheap top-N / calendar-fill / rolling-30d / share math on the small
-- result. The MV is unwindowed (all history); the web query applies its -395d
-- compute window and -365d output trim on read.
--   - provider: the at-the-time signer-mapped lud16 domain, or 'other' when the
--     LNURL signer pubkey is unmapped (zero survivorship bias — see the web
--     getLightningTrends* comment block).
--   - SPAM FILTER (amount_msats <= 21000000000 = 0.21 BTC): drops economically
--     impossible zaps entirely (the 2026-05-19 relay.zapstore.dev spam).
--   - SATS CAP (LEAST(amount_msats, 10000000000) = 0.1 BTC): clamps each zap's
--     contribution to daily_sats so a single large-but-surviving zap can't spike
--     a provider line. daily_zaps (the COUNT) is uncapped.
CREATE MATERIALIZED VIEW IF NOT EXISTS lightning_daily_provider_mv AS
SELECT
    date_trunc('day', to_timestamp(z.created_at))::date AS day,
    COALESCE(m.provider_domain, 'other') AS provider,
    COUNT(*) AS daily_zaps,
    SUM(LEAST(z.amount_msats, 10000000000))::bigint / 1000 AS daily_sats
FROM events_kind_9735_zaps z
LEFT JOIN lightning_provider_signers m ON m.signer_pubkey = z.lnurl_signer_pubkey
WHERE z.amount_msats IS NOT NULL
  AND z.amount_msats > 0
  AND z.amount_msats <= 21000000000
GROUP BY 1, 2;

CREATE UNIQUE INDEX IF NOT EXISTS idx_lightning_daily_provider_mv_day_provider
    ON lightning_daily_provider_mv (day, provider);

-- ===========================================================================
-- M3 - the /zapstore slice: the addressable kind-32267 (zapstore-app-catalog) /
-- kind-30063 (zapstore-app-releases) / kind-30267 (zapstore-app-stacks), plus the derived
-- events_kind_9735_zaps_zapstore subset (kind-9735 (zap-receipts) bearing a top-level
-- ["a","32267:..."] tag).
-- ===========================================================================
-- Like the /lightning slice there are NO materialized views: the /zapstore pages
-- query these base tables live (cumulative-apps / native-share / releases trends,
-- app + publisher leaderboards, per-app/per-publisher detail). So the loader issues
-- no REFRESH for any of those kinds or the zapstore subset of kind-9735 (zap-receipts).
-- See nostrology-web's lib/db/postgres.ts (the getZapstore* getters).
-- kind-0 (profile-metadata) rows (loaded in M2) are
-- reused for publisher names/pictures.

-- kind-32267 (zapstore-app-catalog): parameterized-replaceable app entries from
-- relay.zapstore.dev and the public relays. (pubkey, d_tag) identifies an app,
-- except a napplet, which takes a new d_tag on every publish: the loader keeps
-- only its newest entry, so the table holds one row per app either way (see
-- _napplet_coordinate in the loader). Read by the /zapstore app lists, publisher
-- pages and app counts in nostrology-web's lib/db/postgres.ts.
CREATE TABLE IF NOT EXISTS events_kind_32267_app_catalog (
    event_id    CHAR(64) PRIMARY KEY,
    pubkey      CHAR(64) NOT NULL,
    created_at  BIGINT   NOT NULL,
    d_tag       TEXT     NOT NULL,
    content     TEXT     NOT NULL,
    name        TEXT,
    summary     TEXT,
    icon        TEXT,
    url         TEXT,
    repository  TEXT,
    license     TEXT,
    a_tag       TEXT,
    images      TEXT[],
    topics      TEXT[],
    platforms   TEXT[],
    h_tags      TEXT[],
    extra_tags  JSONB
);
CREATE INDEX IF NOT EXISTS idx_k32267_pubkey_d ON events_kind_32267_app_catalog (pubkey, d_tag);
CREATE INDEX IF NOT EXISTS idx_k32267_created  ON events_kind_32267_app_catalog (created_at);
CREATE INDEX IF NOT EXISTS idx_k32267_extra_tags ON events_kind_32267_app_catalog USING GIN (extra_tags jsonb_path_ops) WHERE extra_tags IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Listings the loader did NOT put in the catalog above, with the reason.
-- ---------------------------------------------------------------------------
-- The catalog table holds kind-32267 (zapstore-app-catalog) events of every
-- publisher our relays carry, and not all of them are Zapstore apps: napplets
-- (NIP-5D web applets, whose publishers take a fresh d-tag per publish and so
-- count as a new app each time), web apps from a localhost build, a business
-- directory, npm libraries, and tests. The operator decided on 2026-10-09 to
-- sort these at LOAD time into their own table rather than flag them in place,
-- so that every reader's existing query is right without changing it.
--
-- THE GATE IS ZAPSTORE'S OWN RULE, not a taxonomy of ours: `zapstore/relay`
-- pkg/events/app.go `Validate()` requires a non-empty `d`, a non-empty `name`,
-- and at least one `f` tag drawn from a closed 14-value platform list
-- (ZAPSTORE_PLATFORMS in the loader). A walk of relay.zapstore.dev on
-- 2026-10-28 returned 4,915 listings and EVERY ONE of them passes this rule;
-- of the 4,810 we also hold, zero fail it, and all 207 that fail are absent
-- from the relay. So the rule never excludes an app Zapstore actually serves.
-- 263 rows land here in total: those 207, plus the 56 below.
--
-- ONE ROW HERE IS NOT ZAPSTORE'S CALL: `ios_only`. Zapstore's list contains
-- `ios-arm64`, but Zapstore does not distribute iOS software - the 2026-10-28 walk
-- found zero iOS listings among the 4,915 it serves, and the client (Flutter) has
-- an `android/` build directory and no `ios/`. The 56 rows we hold are
-- `appstore.<id>` pointers to Apple's App Store from one off-relay publisher, with
-- no releases and no zaps, and they were rank 1 on the Publishers leaderboard. The
-- operator decided on 2026-10-28 to exclude them as policy. `web` is NOT excluded,
-- although the relay serves none of those either: the operator decided the opposite
-- there, because a web listing names something a user can open. See
-- zapstore_policy_exclusion in the loader, kept as a separate function from the
-- Validate() copy so the two rules never blur.
--
-- `excl` labels WHY a row is here. Apart from `ios_only` the label is for analysis
-- and for the future napplet surface (nozzl#44 (capture: the NIP-5D (napplets)
-- kinds)); it is NOT what decides exclusion, because a napplet-specific marker is a
-- moving target - soyLI writes a `latest` tag naming a kind-35129 (NIP-5D
-- named-napplet manifest), while napplet.run writes a `client` tag and no manifest
-- reference at all. The platform rule catches both without knowing either.
CREATE TABLE IF NOT EXISTS events_kind_32267_app_catalog_excluded (
    event_id    CHAR(64) PRIMARY KEY,
    pubkey      CHAR(64) NOT NULL,
    created_at  BIGINT   NOT NULL,
    d_tag       TEXT     NOT NULL,
    content     TEXT     NOT NULL,
    name        TEXT,
    summary     TEXT,
    icon        TEXT,
    url         TEXT,
    repository  TEXT,
    license     TEXT,
    a_tag       TEXT,
    images      TEXT[],
    topics      TEXT[],
    platforms   TEXT[],
    h_tags      TEXT[],
    extra_tags  JSONB,
    excl        TEXT     NOT NULL,
    -- WHICH rule rejected the row. The design turns on keeping Zapstore's own
    -- Validate() and our policy apart, and that separation should survive into the
    -- data rather than living only in the loader: a reader of this table (the future
    -- napplet surface, nozzl#44 (capture: the NIP-5D (napplets) kinds)) can
    -- then tell them apart without knowing the label vocabulary.
    excl_source TEXT     NOT NULL CHECK (excl_source IN ('zapstore', 'policy')),
    -- Wall-clock of the catalog load that wrote this row. NOT a COPY column: the
    -- loader lists its columns explicitly and this one takes the DEFAULT, which is
    -- what keeps the table column-compatible with the catalog plus `excl`
    -- (loader/test_zapstore_conformance.py pins that). It exists because the three
    -- loads that hang off this table - kind-30063 (zapstore-app-releases),
    -- kind-30267 (zapstore-app-stacks) and the kind-9735 (zap-receipts) zapstore
    -- subset - read it to decide what to drop, and a Dagster asset dependency only
    -- orders them WITHIN a run. Re-materialising one of them on its own is the
    -- normal recovery action and reads whatever is here, possibly from a previous
    -- refresh; excluded_listing_coords prints this age so that case is visible
    -- rather than silent.
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_k32267x_pubkey_d ON events_kind_32267_app_catalog_excluded (pubkey, d_tag);
CREATE INDEX IF NOT EXISTS idx_k32267x_excl     ON events_kind_32267_app_catalog_excluded (excl);

-- kind-30063 (zapstore-app-releases): one event per app version, parameterised-
-- replaceable on (pubkey, d_tag) where d_tag is '<package>@<version>'. Each
-- version is a distinct d_tag, so the addressable latest-per-(pubkey,d_tag)
-- extract keeps ALL releases. Powers the "Releases" section on
-- /zapstore/apps/[pubkey]/[d_tag]. Tag taxonomy survey shows ~52% coverage on the bare
-- 'i'/'version' tags; the loader falls back to splitting d_tag on the last '@'.
CREATE TABLE IF NOT EXISTS events_kind_30063_app_releases (
    event_id      CHAR(64) PRIMARY KEY,
    pubkey        CHAR(64) NOT NULL,
    created_at    BIGINT   NOT NULL,
    d_tag         TEXT     NOT NULL,
    content       TEXT     NOT NULL,
    app_d_tag     TEXT,
    version       TEXT,
    channel       TEXT,
    a_tag         TEXT,
    url           TEXT,
    repo_url      TEXT,
    commit_sha    TEXT,
    alt           TEXT,
    release_notes TEXT,
    e_refs        TEXT[],
    platforms     TEXT[],
    extra_tags    JSONB
);
CREATE INDEX IF NOT EXISTS idx_k30063_app        ON events_kind_30063_app_releases (pubkey, app_d_tag, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_k30063_created    ON events_kind_30063_app_releases (created_at);
CREATE INDEX IF NOT EXISTS idx_k30063_extra_tags ON events_kind_30063_app_releases USING GIN (extra_tags jsonb_path_ops) WHERE extra_tags IS NOT NULL;

-- kind-30267 (zapstore-app-stacks): curated app collections, parameterised-
-- replaceable on (pubkey, d_tag). Public stacks list their apps in plaintext
-- ["a","32267:<dev_pubkey>:<app_d_tag>"] tags and have empty content;
-- private/encrypted client features put the app list inside `content` and have
-- no plaintext `a` tags. We mirror Zapstore's public-vs-private filter at query
-- time via `coalesce(array_length(app_refs, 1), 0) > 0`. The loader filters by
-- CURATOR pubkey against the blocklist (see _BLOCKLIST_KINDS in dagster_code/pipeline/load.py).
-- app_refs stores ONLY the kind-32267 (zapstore-app-catalog) a-tag value string
-- ("32267:<pubkey>:<d_tag>") - element[1] of the tag array; the optional NIP-01 relay-hint at element[2] is
-- dropped at load time (it's fetch metadata, not part of the reference identity).
CREATE TABLE IF NOT EXISTS events_kind_30267_app_stacks (
    event_id     CHAR(64) PRIMARY KEY,
    pubkey       CHAR(64) NOT NULL,
    created_at   BIGINT   NOT NULL,
    d_tag        TEXT     NOT NULL,
    content      TEXT     NOT NULL,
    name         TEXT,
    description  TEXT,
    title        TEXT,
    platforms    TEXT[],
    h_tags       TEXT[],
    app_refs     TEXT[],
    extra_tags   JSONB
);
CREATE INDEX IF NOT EXISTS idx_k30267_pubkey_d   ON events_kind_30267_app_stacks (pubkey, d_tag);
CREATE INDEX IF NOT EXISTS idx_k30267_created    ON events_kind_30267_app_stacks (created_at);
CREATE INDEX IF NOT EXISTS idx_k30267_app_refs   ON events_kind_30267_app_stacks USING GIN (app_refs)
    WHERE coalesce(array_length(app_refs, 1), 0) > 0;
CREATE INDEX IF NOT EXISTS idx_k30267_extra_tags ON events_kind_30267_app_stacks USING GIN (extra_tags jsonb_path_ops) WHERE extra_tags IS NOT NULL;

-- kind-9735 (zap-receipts) attributed to a Zapstore app via a top-level
-- ["a","32267:<dev_pubkey>:<app_d_tag>"] tag. Derived subset of
-- events_kind_9735_zaps; populated by the loader's --kind 9735 --into zapstore
-- pass over a kind-9735 (zap-receipts) extract pre-filtered to the
-- kind-32267 (zapstore-app-catalog) a-tag (the zapstore load - see
-- dagster_code/pipeline/load.py). Powers the
-- Overview Zaps chart + the app/publisher zap leaderboards + detail pages.
-- The idx_k9735zs_* indexes ARE in the loader's SECONDARY_INDEXES["9735-zapstore"]
-- drop/recreate set, but the TABLE must exist before the first --into zapstore
-- load's TRUNCATE - the schema apply (run BEFORE every refresh) creates it.
CREATE TABLE IF NOT EXISTS events_kind_9735_zaps_zapstore (
    event_id            CHAR(64) PRIMARY KEY,
    app_pubkey          CHAR(64) NOT NULL,
    app_d_tag           TEXT     NOT NULL,
    sender_pubkey       CHAR(64) NOT NULL,
    recipient_pubkey    CHAR(64),
    created_at          BIGINT   NOT NULL,
    target_event_id     CHAR(64),
    amount_msats        BIGINT,
    lnurl_signer_pubkey CHAR(64)
);
ALTER TABLE events_kind_9735_zaps_zapstore ADD COLUMN IF NOT EXISTS lnurl_signer_pubkey CHAR(64);
CREATE INDEX IF NOT EXISTS idx_k9735zs_app     ON events_kind_9735_zaps_zapstore (app_pubkey, app_d_tag, created_at);
CREATE INDEX IF NOT EXISTS idx_k9735zs_created ON events_kind_9735_zaps_zapstore (created_at);
CREATE INDEX IF NOT EXISTS idx_k9735zs_target  ON events_kind_9735_zaps_zapstore (target_event_id) WHERE target_event_id IS NOT NULL;

-- ===========================================================================
-- M4 = Nutzaps, NIP-61 (nutzaps) - the /nutzaps surface. NO MVs (live base-table
-- queries).
-- kind-7376 (nutzap-redemptions) is EXCLUDED - no /nutzaps page reads it.
-- ===========================================================================

-- kind-9321 (nutzaps) (NIP-61 (nutzaps)): one row per nutzap-shaped event. Sender
-- is the top-level event pubkey; recipient is the lowercase `p` tag; mint is `u`.
-- Mint URL is normalized at load time (loader/nutzap_mints.normalize_mint_url: http(s)
-- only; lowercase scheme and host; no default port or trailing slash; path case kept)
-- so it joins cleanly with the kind-10019 (nutzap-configs) mint_urls array and with
-- nutzap_mint_classes.
--
-- VALUE (nostrology-data#8): amount_sats is the sum of the Cashu proofs carried as
-- JSON in `proof` tags, and nothing else - never the `amount` or `unit` tag. A row
-- whose event carries no countable proof is kept with amount_sats = 0,
-- value_source = 'none' and no_value_reason saying why (the reasons are
-- NUTZAP_NO_VALUE_REASONS in the loader). A PAYMENT is a value_source =
-- 'proof_tags' row: every reader that counts payments, sums sats or counts senders
-- filters on it. 'unclassified' is only the ALTER default for rows a loader older
-- than #8 wrote; the next refresh replaces them all. k9321_value_consistent ties the
-- three columns together. It rides on the no_value_reason ADD COLUMN, so it is added
-- once, with the column, and never re-taken (init.sql runs no DO blocks). ROLLBACK
-- COUPLING: a pre-#8 loader writes every row 'unclassified', which the web and the
-- MAU rebuild treat as not-a-payment, so rolling the loader back empties the nutzap
-- charts until the web is rolled back too.
CREATE TABLE IF NOT EXISTS events_kind_9321_nutzaps (
    event_id          CHAR(64) PRIMARY KEY,
    pubkey            CHAR(64) NOT NULL,
    created_at        BIGINT   NOT NULL,
    recipient_pubkey  CHAR(64),
    target_event_id   CHAR(64),
    mint_url          TEXT     NOT NULL,
    amount_sats       BIGINT   NOT NULL DEFAULT 0,
    value_source      TEXT     NOT NULL DEFAULT 'unclassified'
                      CHECK (value_source IN ('proof_tags', 'none', 'unclassified')),
    no_value_reason   TEXT
                      CONSTRAINT k9321_value_consistent CHECK (
                          (value_source = 'proof_tags' AND amount_sats > 0 AND no_value_reason IS NULL)
                       OR (value_source = 'none' AND amount_sats = 0 AND no_value_reason IS NOT NULL)
                       OR value_source = 'unclassified')
);
ALTER TABLE events_kind_9321_nutzaps ADD COLUMN IF NOT EXISTS value_source TEXT NOT NULL DEFAULT 'unclassified'
    CHECK (value_source IN ('proof_tags', 'none', 'unclassified'));
ALTER TABLE events_kind_9321_nutzaps ADD COLUMN IF NOT EXISTS no_value_reason TEXT
                      CONSTRAINT k9321_value_consistent CHECK (
                          (value_source = 'proof_tags' AND amount_sats > 0 AND no_value_reason IS NULL)
                       OR (value_source = 'none' AND amount_sats = 0 AND no_value_reason IS NOT NULL)
                       OR value_source = 'unclassified');
CREATE INDEX IF NOT EXISTS idx_k9321_created      ON events_kind_9321_nutzaps (created_at);
CREATE INDEX IF NOT EXISTS idx_k9321_mint_created ON events_kind_9321_nutzaps (mint_url, created_at);
CREATE INDEX IF NOT EXISTS idx_k9321_recipient    ON events_kind_9321_nutzaps (recipient_pubkey, created_at);

-- kind-10019 (nutzap-configs), NIP-61 (nutzaps): replaceable per-pubkey config declaring
-- which Cashu mints the user accepts for nutzaps. Each event may list
-- multiple `mint` tags, collected into the `mint_urls` array (normalized,
-- deduplicated). Replaceable kinds keep one row per pubkey logically; we
-- store everything we receive (PK on event_id) and let queries resolve the
-- latest via DISTINCT ON (pubkey) ORDER BY pubkey, created_at DESC.
CREATE TABLE IF NOT EXISTS events_kind_10019_nutzap_configs (
    event_id    CHAR(64) PRIMARY KEY,
    pubkey      CHAR(64) NOT NULL,
    created_at  BIGINT   NOT NULL,
    mint_urls   TEXT[]   NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_k10019_pubkey_created ON events_kind_10019_nutzap_configs (pubkey, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_k10019_created        ON events_kind_10019_nutzap_configs (created_at);
CREATE INDEX IF NOT EXISTS idx_k10019_mint_urls      ON events_kind_10019_nutzap_configs USING GIN (mint_urls);

-- Which mints count (nostrology-data#9): one row per mint URL loaded in either table
-- above, rebuilt after both loads by `loader --classify-nutzap-mints` from the reviewed
-- data/nutzaps/mint_registry.yaml and the automatic private-host rule
-- (loader/nutzap_mints.py). excluded = TRUE removes a mint from value, payments,
-- senders, the leaderboard, "Configured By" and the per-mint page. Readers test
-- `NOT EXISTS (... AND excluded)`, so a URL missing from this table (a mint first seen
-- since the last classification) is counted, like any unreviewed mint.
CREATE TABLE IF NOT EXISTS nutzap_mint_classes (
    mint_url      TEXT    PRIMARY KEY,
    mint_class    TEXT    NOT NULL CHECK (mint_class IN (
                      'private-host', 'fake-backend', 'test-mint-gone',
                      'test-wording-live', 'unreachable', 'unreviewed')),
    excluded      BOOLEAN NOT NULL,
    class_source  TEXT    NOT NULL CHECK (class_source IN ('host-rule', 'registry', 'none')),
    evidence      TEXT,
    CONSTRAINT nutzap_mint_classes_excluded_matches_class CHECK (
        excluded = (mint_class IN ('private-host', 'fake-backend', 'test-mint-gone')))
);

-- Pre-computed active user counts: kind-9321 (nutzaps) / NIP-61 (nutzaps) (DAU/WAU/MAU).
-- Backs the /nutzaps Overview MAU chart (getNutzapsUsersMonthly ->
-- granularity='month'). It is rebuilt by schema/nutzap_active_users.sql - a pure-SQL
-- TRUNCATE+INSERT over events_kind_9321_nutzaps (UTC day/ISO-week/month buckets,
-- period_start = epoch of the bucket start) — run by the Dagster asset nutzaps_active_users,
-- AFTER the kind-9321 (nutzaps) load. The per-mint detail page
-- computes its MAU live, so it does NOT read this (cross-mint) table.
CREATE TABLE IF NOT EXISTS active_user_counts_kind_9321_nutzaps (
    period_start BIGINT NOT NULL,
    granularity TEXT NOT NULL,
    unique_users INT NOT NULL,
    total_events BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (period_start, granularity)
);
CREATE INDEX IF NOT EXISTS idx_auc_k9321nz_granularity
    ON active_user_counts_kind_9321_nutzaps (granularity);

-- ============================================================================
-- /marketplaces - the marketplaces_refresh job's serving tables
-- (dagster_code/pipeline/marketplaces.py; methodology cross-checked against
-- crosscheck/build_markets_snapshot.py - the two must fully agree).
-- ============================================================================

-- One row per CURRENT kind-30402 (markets-product-listings) coordinate - every version
-- collapsed to the newest (created_at DESC, id ASC per NIP-01), classified. `excl` is the
-- single precedence-ordered exclusion bucket (bulk|carrier|unrenderable|synthetic|deleted|
-- withdrawn|option, NULL = live; `option` is a size or colour folded into its product, see
-- pipeline/marketplaces.fold_product_options); the flags to its left are the UNCONDITIONED per-rule verdicts
-- (withdrawn_any/deleted_any style counts read those, since precedence understates every
-- group after the first). TRUNCATE+INSERT by each run inside one transaction, behind the
-- MIN_ACTIVE_LISTINGS anti-shrink gate.
-- No PK on purpose: the coordinate key is (pubkey, d_tag) with NULL d_tag as its own
-- coordinate DISTINCT from d_tag='' (both would collapse to the same coalesced string,
-- so a unique expression index would lie); row identity is enforced upstream by the
-- current-version selection, and check_invariants pins len(recs) == current_coordinates.
CREATE TABLE IF NOT EXISTS marketplaces_listings (
    pubkey        CHAR(64) NOT NULL,
    d_tag         TEXT,
    created_at    BIGINT   NOT NULL,
    published_at  BIGINT,
    -- The listing's effective date: a plausible published_at (1.5e9..now), else created_at,
    -- and never after the run (capped at now: a listing dated ahead is dated at the run).
    -- Relays keep only the current version of an addressable event, so an edited listing
    -- with no published_at is dated by its edit - a floor on age, never an overstatement.
    effective_ts  BIGINT   NOT NULL,
    title         TEXT     NOT NULL,  -- lowercased
    status        TEXT     NOT NULL,  -- lowercased ('' when untagged)
    visibility    TEXT     NOT NULL,  -- lowercased ('' when untagged)
    stock         BIGINT,
    client        TEXT     NOT NULL,  -- '' when untagged
    location      TEXT,               -- raw free-text `location` tag
    geohashes     JSONB,              -- every `g` tag value, in tag order
    categories    JSONB,              -- every `t` tag value, in tag order
    price         JSONB,              -- the first `price` tag, verbatim; on a family's kept row with
                                      -- no price with a unit, its newest priced child's
    unrenderable  BOOLEAN  NOT NULL,
    synthetic     BOOLEAN  NOT NULL,
    nip09_deleted BOOLEAN  NOT NULL,  -- self-authored kind-5 (event-deletion-requests) covers its newest version
    withdrawn     BOOLEAN  NOT NULL,
    excl          TEXT,               -- NULL = live listing (the pool behind every metric)
    cc            TEXT,               -- ISO-3166 alpha-2, or NONGEO/REGION/AMBIG, or NULL
    cc_src        TEXT,               -- geohash|text|country-in-text|gazetteer|risky-name|seller-context
                                      -- |geohash-implausible (the `g` tag decoded somewhere a
                                      -- listing cannot be sold from, and nothing else resolved)
    -- The decode is recorded even when it was refused for attribution, so a row with a
    -- gh_lat and a cc that did not come from it is the record of a rejected geohash.
    gh_lat        DOUBLE PRECISION,
    gh_lon        DOUBLE PRECISION,
    gh_len        INT,
    -- The six below feed NO metric on the page, except `event_id`, which also breaks a tie in
    -- pipeline/marketplaces.fold_product_options. They are written for one reader,
    -- nostrology-web's Recent listings section, which links each row to a marketplace that
    -- can actually render it and leaves out a row none of them can: each column is a
    -- condition one of those marketplaces reads from the listing. Added 2026-10-24,
    -- nostrology-data#29 (/marketplaces: the listings table drops the title as published and
    -- four other per-listing facts the web needs). Nothing is sent anywhere - they are read
    -- on our own server to choose a link, and only the listing's address leaves our side.
    event_id      TEXT,               -- the current version's event id: Plebeian Market
                                      -- addresses a listing by id, not by coordinate. TEXT and
                                      -- not CHAR(64) like `pubkey`: character(64) SPACE-PADS a
                                      -- short value (the web's 64-hex test then rejects the
                                      -- padded string, costing that marketplace a row) and
                                      -- ERRORS on a long one, which would fail the whole
                                      -- INSERT and so the refresh. This column is a fact for
                                      -- one reader, not a key, so a malformed id must cost one
                                      -- link rather than a night's data (review, 2026-10-24)
    title_tag     TEXT,               -- the `title` tag AS PUBLISHED (NULL = untagged);
                                      -- `title` above is lowercased for classification, so
                                      -- the page was drawing the fold
    summary_len   INT,                -- the `summary` tag's length in UTF-16 units (JS
                                      -- String.length, which is what Conduit's 5,000 cap
                                      -- counts); NULL = untagged
    content_warning TEXT,             -- first `content-warning` tag value, RAW: its only
                                      -- reader compares raw and case-sensitively
    expires_at    BIGINT,             -- NIP-40 `expiration`, unix seconds: relays delete an
                                      -- expired event, so nothing can open the listing
    market_image  BOOLEAN             -- an image URL Conduit's own normalizePublicMediaUrl
                                      -- accepts, from the source Conduit would read
                                      -- (pipeline/markets_medialib.py). NULL = the row
                                      -- predates the column, which the web reads as
                                      -- "unknown", never as "yes"
);
-- Added 2026-10-24 (the table predates them in production); every run rewrites every row, so no
-- backfill is needed and no DEFAULT is wanted - a row written before this change reads NULL,
-- which nostrology-web treats as "this fact is absent" rather than as a good answer.
ALTER TABLE marketplaces_listings ADD COLUMN IF NOT EXISTS event_id        TEXT;
ALTER TABLE marketplaces_listings ADD COLUMN IF NOT EXISTS title_tag       TEXT;
ALTER TABLE marketplaces_listings ADD COLUMN IF NOT EXISTS summary_len     INT;
ALTER TABLE marketplaces_listings ADD COLUMN IF NOT EXISTS content_warning TEXT;
ALTER TABLE marketplaces_listings ADD COLUMN IF NOT EXISTS expires_at      BIGINT;
ALTER TABLE marketplaces_listings ADD COLUMN IF NOT EXISTS market_image    BOOLEAN;
CREATE INDEX IF NOT EXISTS idx_mkl_excl    ON marketplaces_listings (excl);
CREATE INDEX IF NOT EXISTS idx_mkl_pubkey  ON marketplaces_listings (pubkey);
CREATE INDEX IF NOT EXISTS idx_mkl_cc      ON marketplaces_listings (cc);
CREATE INDEX IF NOT EXISTS idx_mkl_eff     ON marketplaces_listings (effective_ts);
-- THE INDEX BELOW IS RETAINED, UNUSED, AND DEFERRED ON PURPOSE (the operator's call,
-- 2026-11-04). The planner on this cluster does not choose it and will not until a planner cost
-- parameter changes, which this estate deliberately does not set. It is kept rather than
-- dropped because its shape is right, the cost of keeping it is documentary rather than
-- operational, and the read it serves is not user-facing yet. Every figure below was measured
-- on 2026-11-04 against the statement the web actually ships; they move, so re-measure before
-- trusting any of them, and read the LEVERS paragraph first if you are here because the page
-- got slow.
--
-- WHAT THE QUERY NEEDS. SQL_MARKETPLACES_RECENT (nostrology-web's src/lib/db/postgres.ts) caps
-- rows per seller with a window: row_number() PARTITION BY pubkey ORDER BY effective_ts DESC,
-- coalesce(d_tag,''). Its INPUT must arrive in exactly that order, which is why this index
-- carries the expression and the DESC - a mixed ordering can only be read from an index
-- declared that way round. The statement's own ORDER BY can use no index at all: it sorts the
-- window's OUTPUT, which no index has seen. So this index removes the big sort or nothing does.
--
-- THE PLAN IN FORCE (19,445 rows, 10,653 live, 1,933 live sellers): Seq Scan -> Sort
-- `external merge` of about 3.9 MB -> WindowAgg with `Run Condition: row_number() OVER (?)
-- <= 2` -> top-N heapsort 232 kB -> Limit. Cost 2,797.35; 23.0 ms at best, high twenties
-- typical on a cluster that also carries the nightly refresh.
--   * THE SCAN NODE IS A COIN FLIP, not a decision. The planner compares SCAN PATHS, where
--     Seq Scan is 0.00..1352.67 against a bitmap on idx_mkl_excl at 126.70..1347.50 - 0.38%
--     apart on total cost, inside `compare_path_costs_fuzzily`'s 1% window (STD_FUZZ_FACTOR),
--     where the cheaper STARTUP cost wins and Seq Scan's is 0.00. On 2026-10-25 it was the
--     bitmap. It buys nothing either way (same spill, no measurable time difference), so do
--     not read a change of scan node here as a change in anything.
--   * THE SORT HAS SPILLED TO DISK SINCE ABOUT 2026-10-31. Quoting the spill to the kilobyte
--     asserts precision it does not have: at an unchanged row count it reported 3,792 / 3,800 /
--     3,920 / 3,928 kB across runs on one day.
--
-- THE NEXT CHANGE IS A PLAN FLIP, NOT A CURVE, AND IT IS CLOSE. `cost_tuplesort` decides the
-- sort spills when `tuples * (MAXALIGN(width) + MAXALIGN(SizeofHeapTupleHeader))` exceeds
-- work_mem; SizeofHeapTupleHeader is 23, MAXALIGN makes it 24 on 64-bit, so the per-row figure
-- is MAXALIGN(width) + 24 = 416 at today's width of 392. (Use MAXALIGN(width), not raw width -
-- 392 is already 8-aligned so they coincide today, but a width of 389 would not.) The same
-- arithmetic also decides whether to go PARALLEL, and it runs on the planner's ESTIMATE, not
-- the actual count: once estimated rows x 416 exceeds work_mem the plan becomes Gather Merge +
-- Parallel Seq Scan. Pinned to the kilobyte: parallel at work_mem 4,089 kB, serial at 4,090 kB,
-- which is exactly where 10,066 x 416 = 4,187,456 B lands. So at the 4,096 kB default it tips
-- when the ESTIMATE passes 4,194,304 / 416 = 10,082 rows, and the estimate was 10,066 with
-- `n_mod_since_analyze = 0` after that day's autoanalyze. Sixteen estimated rows, i.e. about 33
-- actual rows at the current 0.518 selectivity. THE EARLIEST FLIP IS THE NEXT NIGHTLY REFRESH
-- (marketplaces_daily, 02:15Z), since only an autoanalyze moves the estimate. Growth
-- averages about 130 live rows a day, but it is lumpy - 2026-11-03 alone added 703 - so do not
-- divide 16 by 130 and conclude it is days away.
--   * AND THE PARALLEL PLAN IS SLOWER, AND MUCH LESS PREDICTABLE: six runs gave 26.5, 27.6,
--     28.1, 32.8, 46.7 and 56.0 ms against this plan's 23.0 best. It barely parallelises - the
--     worker sorts 25 to 159 kB while the leader still does the whole external merge - so it
--     pays for the Gather without splitting the work. This index does NOT prevent that flip;
--     only work_mem does.
--   * DO NOT COMPARE COSTS ACROSS THAT BOUNDARY. At work_mem 4,089 kB the chosen parallel
--     plan's Limit costs 4,501.07, ABOVE this plan's 2,797.35, because crossing the threshold
--     re-prices the serial plan with an external-sort penalty. Both numbers are self-consistent
--     and neither is comparable to the other.
--   * AND NOTE WHAT 2,797.35 RESTS ON: at 4,090 kB the planner BELIEVES the sort fits
--     (10,066 x 416 is under work_mem) while it actually spills, because the real row count is
--     10,653 and needs about 4,374 kB. The plan in force is priced on a sort the planner
--     mis-sizes.
--
-- THE LEVERS, CHEAPEST FIRST, when this stops being operator-only.
--   1. `ALTER ROLE nostrology_web SET work_mem = '8MB'` in schema/roles.sql, which already
--      carries three such lines for this very role (default_transaction_read_only,
--      statement_timeout, idle_in_transaction_session_timeout) and is applied by
--      the deployment's role apply. That is one line, in this repo, scoped to exactly the
--      role the web pool connects as, and it needs no nostrology-web change at all. It removes
--      the spill and pre-empts the parallel flip in one move: 19.3 ms at best, `quicksort
--      Memory: 4466kB`, no temp files.
--      ASK FOR 8 MB, NOT 5 MB. 5 MB does work today (19.98 ms, `quicksort Memory: 4374kB`) but
--      leaves only about 750 kB of headroom - roughly 1,800 more rows, a fortnight at the
--      average growth and far less after a day like 2026-11-03 - so it re-enters spill and then
--      the flip almost immediately. 8 MB holds to roughly 20,000 rows.
--      BE HONEST ABOUT WHAT THIS IS: work_mem changes plan choice - that is exactly how it
--      pre-empts the flip - so it is not plan-neutral. It differs from a cost hint in that
--      random_page_cost = 1.1 asserts something FALSE about the hardware to win an argument,
--      while work_mem states a budget truthfully and the plan follows from it.
--      SCOPE IT TO THE ROLE, never cluster-wide: work_mem is per sort node per backend, this
--      cluster runs between 0.5 and 1.0 ACU (roughly 1 to 2 GiB), max_connections is 194 and
--      the `nostrology` owner role has no connection limit, so a cluster-wide 32 MB is reckless
--      arithmetic. Via the role it is bounded by the web pool's 5 connections (postgres.ts).
--   2. Then, and only together, this index AND a random_page_cost that reaches it. One without
--      the other buys nothing.
--
-- WHY THE PLANNER WILL NOT TAKE IT. random_page_cost is 4, `source=default` - the stock
-- PostgreSQL value, priced for a seeking disk. `pubkey` correlation with physical row order is
-- 0.054, because write_pg inserts in arrival order, so an ordered walk of this index fetches
-- heap pages essentially at random. Lower the parameter and the planner takes it UNPROMPTED,
-- with NO SORT NODE AT ALL: the crossover is between 1.3 and 1.4 (measured: index at 1.0, 1.1,
-- 1.3 and 1.35; bitmap at 1.4), and it is NOT sensitive to effective_cache_size. At 1.0 it
-- costs 2,304.93 against the chosen plan's 2,797.35, and runs 9.97 ms at best with 10.0 to
-- 10.9 ms in 5 of 6 runs - about 2.4x the current plan and the STEADIEST of the four plans
-- here, not the noisiest (warm spread 0.9 ms, against 5.3 for the plan in force and 29.5 for
-- the parallel one).
--   * It touches 10,423 buffers against the seq scan's 1,061, but those land on only 1,061
--     distinct heap pages re-pinned about ten times each, so the exposure is pin traffic, not
--     I/O - bounded at 1,061 heap pages plus the index's 281 if it were ever evicted. It is
--     cached today by being hot, not by right: this database is 6,599 MB against 128 MB of
--     shared_buffers.
--   * SO THE INDEX IS NOT A DUD - IT IS UNREACHABLE, deliberately. The estate sets no planner
--     cost parameter anywhere: the Aurora cluster parameter group carries exactly one,
--     rds.force_ssl=1, and pg_settings confirms every
--     cost parameter at `source=default`.
--   * DO NOT "FORCE IT" WITH enable_seqscan/enable_bitmapscan = off. That does not reach this
--     index at all - it sends the planner to an Index Scan on idx_mkl_excl, 3,970.93 at the
--     scan node and 5,415.60 at the Limit, with the big sort still in place. Use
--     random_page_cost, inside a transaction: SET LOCAL outside one is silently discarded,
--     which has void-ed a measurement here before.
--   * DO NOT TRUST idx_scan ON THIS INDEX. It reads about 36,600 and climbs every time anyone
--     tests against it; it is not production traffic. An ad-hoc query once put this index on
--     the inner side of a nested loop and pg_stat counts each rescan. Prove usage with a DELTA
--     instead - snapshot, let the app serve, re-snapshot. A clean window on 2026-11-04 with the
--     app live moved it ZERO.
--   * DO NOT RE-MEASURE ON A SYNTHETIC TABLE without matching the correlation. A build that
--     inserts in pubkey order gets correlation about 1, where the planner prices the walk
--     near-sequentially and takes the index. That false positive is what the first version of
--     this comment was built on, and a later version then carried a 1x/2x/5x scaling table
--     measured on a 209-byte row that the six columns added on 2026-10-24 had already made
--     wrong - width is now 392, which is why the spill arrived at 1.14x the pool and not 2x.
--
-- NONE OF THIS IS USER-FACING YET, AND THAT IS STRUCTURAL RATHER THAN A MATTER OF VOLUME. The
-- layout load does not run this query at all unless seesSection(...) passes: it is
-- `sees ? getMarketplacesRecentListings(...) : Promise.resolve(null)`, so for everyone else
-- there is no query, not a hidden result. seesSection compares the session pubkey against a
-- hard-coded operator pubkey, the session comes only from an HMAC-verified cookie that fails
-- closed, and the slug table carries `unreleased: true` for recent-listings - so today that is
-- the operator alone, with no env, header or query-param bypass. Releasing that flag is the
-- trigger for lever 1 above, and it also starts adding up to 60 rows to the /marketplaces
-- __data.json that openmarkets.pub polls. Request counts for this page are NOT quoted
-- here on purpose: three instruments gave three answers on the same window, and the gate above
-- is the durable fact. The page's `private, no-cache` is a SITE-WIDE DEFAULT and its
-- `Vary: Cookie` is unconditional for HTML (src/hooks.server.ts) - neither is evidence of this
-- section's gate, and neither reaches the __data.json that consumer fetches. The whole question
-- is tracked in nostrology-web#30 (Recent listings: the read sorts the whole live pool, and its
-- index is unreachable at the default random_page_cost).
CREATE INDEX IF NOT EXISTS idx_mkl_live_seller_eff ON marketplaces_listings
    (pubkey, effective_ts DESC, coalesce(d_tag, ''))
    WHERE excl IS NULL;

-- The page's snapshot: the FULL /marketplaces dataset as one JSONB document per refresh,
-- appended (not overwritten) so refreshes stay comparable over time; the web reads the
-- newest row only. Deliberately a document, not per-metric tables: the page consumes the
-- whole snapshot at once, a single-row read serves it at any plausible traffic, and the
-- shape can evolve without schema migrations - materialize finer-grained serving tables
-- only when a real query-performance need shows up (the listings table above already
-- carries the queryable per-coordinate detail).
CREATE TABLE IF NOT EXISTS marketplaces_snapshot (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    generated_utc TIMESTAMPTZ NOT NULL,
    run_id        TEXT        NOT NULL,
    data          JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mks_generated ON marketplaces_snapshot (generated_utc DESC);

-- ============================================================================
-- /routstr - the routstr_refresh job's serving tables
-- (dagster_code/pipeline/routstr.py; methodology cross-checked against
-- crosscheck/build_routstr_snapshot.py - the two must fully agree).
-- ============================================================================

-- One row per provider (pubkey) that ever genuinely announced on
-- kind-38421 (routstr-provider-announcements) or reported usage on
-- kind-38422 (routstr-analytics-snapshots). TRUNCATE+INSERT by each run inside one transaction,
-- behind the MIN_LEDGER_ROWS / MIN_PROVIDERS anti-shrink gates. All *_msats/req/tok
-- aggregates are the reconstructed-ledger sums (see routstr_provider_days below);
-- name/about/urls are third-party text - escape on render.
CREATE TABLE IF NOT EXISTS routstr_providers (
    pubkey           CHAR(64) PRIMARY KEY,
    provider_id      TEXT,               -- current announcement's d tag (NULL: never announced)
    name             TEXT     NOT NULL,  -- announcement content.name, clipped; pubkey prefix fallback
    about            TEXT     NOT NULL,
    endpoint_class   TEXT     NOT NULL,  -- clearnet|onion|both|private-only|none
    version          TEXT     NOT NULL,  -- announcement `version` tag ('' when untagged)
    urls             JSONB    NOT NULL,  -- every `u` tag value (or the snapshot's endpoint_urls)
    mints            JSONB    NOT NULL,  -- distinct `mint` tag values, sorted
    announced        BOOLEAN  NOT NULL,  -- had a genuine (u+d-tagged) kind-38421 (routstr-provider-announcements) event
    review           TEXT,               -- latest curated kind-38425 (routstr-provider-reviews) label, NULL = none
    first_seen_utc   TIMESTAMPTZ,        -- earliest announcement or earliest ledger activity
    last_announce_utc TIMESTAMPTZ,
    last_report_utc  TIMESTAMPTZ,        -- newest analytics snapshot's generated_at
    excluded         BOOLEAN  NOT NULL,  -- out of the headline aggregates
    excl_reason      TEXT,               -- test|private (NULL = included)
    msats_alltime    BIGINT   NOT NULL,
    msats_30d        BIGINT   NOT NULL,
    requests_30d     BIGINT   NOT NULL,
    tokens_30d       BIGINT   NOT NULL,
    requests_alltime BIGINT   NOT NULL DEFAULT 0,
    tokens_alltime   BIGINT   NOT NULL DEFAULT 0
);
-- Added 2026-08-29 for the MCP provider_leaderboard metric (the table predates them on
-- dev); the DEFAULT only covers the ALTER on an existing table - every run rewrites
-- every row with real values.
ALTER TABLE routstr_providers ADD COLUMN IF NOT EXISTS requests_alltime BIGINT NOT NULL DEFAULT 0;
ALTER TABLE routstr_providers ADD COLUMN IF NOT EXISTS tokens_alltime   BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_rsp_excluded ON routstr_providers (excluded);

-- The reconstructed activity ledger: one row per (provider, grain, bucket day) merged
-- from the analytics snapshots' trailing windows (grain 'd' = daily buckets from the
-- 30d/3m windows, 'w' = weekly buckets from the 1y window, kept only for the
-- provider's pre-daily era so the two grains never overlap; `day` is the bucket's
-- UTC start date). Self-reported by providers; msats are integer millisats. `models`
-- holds the bucket's per-model {req,msats,tok} map (top-N-truncated on the wire),
-- `others` the provider's own residual aggregate.
CREATE TABLE IF NOT EXISTS routstr_provider_days (
    pubkey   CHAR(64) NOT NULL,
    grain    CHAR(1)  NOT NULL,  -- 'd' | 'w'
    day      DATE     NOT NULL,
    msats    BIGINT   NOT NULL,
    requests BIGINT   NOT NULL,
    tokens   BIGINT   NOT NULL,
    models   JSONB    NOT NULL,
    others   JSONB    NOT NULL,
    PRIMARY KEY (pubkey, grain, day)
);
CREATE INDEX IF NOT EXISTS idx_rpd_day ON routstr_provider_days (day);

-- Per-model bucket activity over INCLUDED providers only (exclusions already applied,
-- unlike routstr_provider_days): one row per (model, grain, bucket start day) with the
-- three measures and the count of included providers reporting that model in that
-- bucket. Same grain rule as routstr_provider_days ('w' = a provider's pre-daily era
-- only), so summing every row re-derives the document's all-time model figures
-- exactly. `model` is the provider-reported identifier verbatim (clipped at 60 chars);
-- "(other)" is the providers' own residual for models they truncated off the wire.
-- Substrate for the MCP model_activity metrics; check_invariants proves it re-sums to
-- the document's model rows and chart series. TRUNCATE+INSERT, same transaction.
CREATE TABLE IF NOT EXISTS routstr_model_days (
    model     TEXT    NOT NULL,
    grain     CHAR(1) NOT NULL,  -- 'd' | 'w'
    day       DATE    NOT NULL,
    msats     BIGINT  NOT NULL,
    requests  BIGINT  NOT NULL,
    tokens    BIGINT  NOT NULL,
    providers INTEGER NOT NULL,
    PRIMARY KEY (model, grain, day)
);
CREATE INDEX IF NOT EXISTS idx_rmd_day ON routstr_model_days (day);

-- Every archived version of the curated kind-38423 (routstr-model-catalog)
-- "routstr-21-models" list, oldest first by version_utc, with the membership delta
-- against the previous version. The document keeps only the size history; this is the
-- substrate for the MCP catalog_versions / model_catalog records.
CREATE TABLE IF NOT EXISTS routstr_catalog_versions (
    event_id    CHAR(64)    PRIMARY KEY,
    version_utc TIMESTAMPTZ NOT NULL,
    model_count INTEGER     NOT NULL,
    models      JSONB       NOT NULL,  -- sorted, deduped, clipped model ids
    added       JSONB       NOT NULL,  -- vs the previous version ([] for the first)
    removed     JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rcv_version ON routstr_catalog_versions (version_utc DESC);

-- The kind-38425 (routstr-provider-reviews) timeline, newest MAX_REVIEW_TABLE_ROWS
-- (5,000) valued review events - the document truncates to 200 and carries 12-hex node
-- ids; this table carries the full node key and the reviewer key so a per-provider
-- timeline is complete and relay-verifiable. `curated` = signed by the curated key
-- (the only reviewer that sets a provider's status).
CREATE TABLE IF NOT EXISTS routstr_reviews (
    event_id        CHAR(64)    PRIMARY KEY,
    node            TEXT        NOT NULL,  -- the reviewed provider key (node/p tag, third-party text)
    label           TEXT        NOT NULL,  -- lowercased, clipped
    reviewed_utc    TIMESTAMPTZ NOT NULL,
    reviewer_pubkey CHAR(64)    NOT NULL,
    curated         BOOLEAN     NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rrv_node ON routstr_reviews (node, reviewed_utc DESC);

-- The page's snapshot: the FULL /routstr dataset as one JSONB document per refresh,
-- appended (not overwritten); the web reads the newest row only. Same serving shape
-- and rationale as marketplaces_snapshot.
CREATE TABLE IF NOT EXISTS routstr_snapshot (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    generated_utc TIMESTAMPTZ NOT NULL,
    run_id        TEXT        NOT NULL,
    data          JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rss_generated ON routstr_snapshot (generated_utc DESC);

-- ============================================================================
-- /git - the ngit_refresh job's serving tables
-- (dagster_code/pipeline/ngit.py; the rule text is
-- docs/dataset-reference/ngit-dataset-reference.md, and the independent cross-check crosscheck/ngit_check.py
-- re-derives ngit_repos / ngit_pushes row by row from that text alone).
-- ============================================================================

-- One row per kind-30617 (repo-announcements) coordinate (pubkey, d) - kept OR excluded,
-- except the N0 rule's oversized identifiers (counted in the ledger, written to no row):
-- `family` is the noise-ledger rule that matched (N1..N7, N8a, N8b, N9, N10) or 'repo'. The
-- repository-level columns (repo_key, cls, grasp, last_state, last_push, pushes_*) are
-- filled on kept rows only and repeat the REPOSITORY's values on every member row; the
-- rest describe this coordinate. `d` and `name` are third-party text (NUL-stripped; `name`
-- clipped to 80, `d` never clipped - N0 keeps it within 200 characters) - escape on render.
-- TRUNCATE+INSERT by each run inside one transaction, behind the relative anti-shrink
-- gates (pipeline/ngit.py write_pg).
CREATE TABLE IF NOT EXISTS ngit_repos (
    pubkey           CHAR(64) NOT NULL,
    d                TEXT     NOT NULL,             -- never clipped (N0 excludes > 200)
    family           TEXT     NOT NULL,             -- 'repo' | 'N1'..'N7' | 'N8a' | 'N8b' | 'N9' | 'N10'
    repo_key         TEXT,                          -- '<lead pubkey>:<d>' (kept rows)
    is_lead          BOOLEAN  NOT NULL DEFAULT false,
    name             TEXT,                          -- current version, clipped 80
    first_seen       BIGINT   NOT NULL,             -- earliest archived version's created_at
    last_seen        BIGINT   NOT NULL,             -- current version's created_at
    versions         INTEGER  NOT NULL,
    cls              TEXT,                          -- REPOSITORY hosting class (kept rows)
    grasp            BOOLEAN,                       -- repository is GRASP-hosted (cls grasp_only / grasp_mixed)
    hosts            JSONB,                         -- [{host, cls}] of this coordinate, <= 20
    relays           JSONB,                         -- normalized relays of this coordinate, <= 20
    euc              CHAR(40),
    tool             TEXT,
    has_state        BOOLEAN  NOT NULL,
    last_state       BIGINT,                        -- REPOSITORY level (kept rows)
    last_push        BIGINT,
    pushes_total     INTEGER  NOT NULL DEFAULT 0,   -- REPOSITORY level, pushes
    pushes_30d       INTEGER  NOT NULL DEFAULT 0,
    pushes_90d       INTEGER  NOT NULL DEFAULT 0,
    PRIMARY KEY (pubkey, d)
);

-- The push ledger: one row per kind-30618 (repo-states) version of a KEPT repository's
-- member coordinates (state republish loops excluded), with the flags of the reference's
-- section 3 - `changed` (the ref map differs from the previous version's) and `push`
-- (changed, on or after the 2026-06-14T03:22:39Z capture wall) - and the per-version change
-- counts; and since 2026-10-12 one row per hashtree push (section 3: a group of a
-- hashtree-class repository's kind-30617 (repo-announcements) republishes), told apart by
-- `source`. Bounded by the noise ledger, never by a flooder. Same transaction as ngit_repos.
CREATE TABLE IF NOT EXISTS ngit_pushes (
    event_id         CHAR(64) PRIMARY KEY,
    pubkey           CHAR(64) NOT NULL,
    d                TEXT     NOT NULL,
    repo_key         TEXT     NOT NULL,
    created_at       BIGINT   NOT NULL,
    changed          BOOLEAN  NOT NULL,
    push             BOOLEAN  NOT NULL,
    head_changed     BOOLEAN  NOT NULL,
    heads_moved      INTEGER  NOT NULL,
    default_moved    BOOLEAN  NOT NULL,
    branches_added   INTEGER  NOT NULL,
    branches_removed INTEGER  NOT NULL,
    tags_added       INTEGER  NOT NULL,
    tags_removed     INTEGER  NOT NULL,
    tags_moved       INTEGER  NOT NULL,
    branches         INTEGER,
    tags             INTEGER,
    source           TEXT     NOT NULL DEFAULT 'state'
                              CHECK (source IN ('state', 'htree_announcement'))
);
-- `counted` (push AND among the repository's first 50 pushes of its UTC day) went with the
-- daily cap on 2026-10-02: every push counts. A live database's column is dropped by the
-- ngit job's own write (pipeline/ngit.py write_pg, under its TRUNCATE lock), never here: every
-- job applies this file, and an ALTER here would queue behind a reader of ngit_pushes.
-- `source` and nullable `branches` / `tags` came with hashtree pushes on 2026-10-12 (a hashtree
-- push has no branch or tag count); a live table gets them the same way, from write_pg. The
-- default is what the pipeline before that change writes without naming the column, so a
-- rollback still inserts.
CREATE INDEX IF NOT EXISTS idx_ngp_repo ON ngit_pushes (repo_key, created_at);
CREATE INDEX IF NOT EXISTS idx_ngr_family ON ngit_repos (family);

-- The per-repository figures: one row per KEPT repository (ngit_repos' lead rows), on the
-- lead's coordinate, with the document's display name and the repository's hosting class.
-- The page lists Stars and Zaps from it since 2026-10-18 (the operator's choice: the ranking
-- and length of a list are the page's to choose, and the coming MCP domain reads the same
-- rows). `run_id` is the ngit_snapshot row written with it: the page uses the rows only
-- beside that row, and the document's own lists otherwise. Rewritten by each run with DELETE
-- (not TRUNCATE, so a page read never waits on the run's lock) in the same transaction as
-- ngit_repos, ngit_pushes and the snapshot; check_invariants ties every figure to the
-- document. `d` and `name` are third-party text - escape on render.
CREATE TABLE IF NOT EXISTS ngit_repo_stats (
    repo_key         TEXT     PRIMARY KEY,          -- '<lead pubkey>:<d>' (ngit_repos.repo_key)
    run_id           TEXT     NOT NULL,             -- the ngit_snapshot row written with it
    pubkey           CHAR(64) NOT NULL,             -- the lead coordinate
    d                TEXT     NOT NULL,
    name             TEXT     NOT NULL,             -- the lead's name, else d; cleaned, clipped 80
    cls              TEXT     NOT NULL,             -- the repository's hosting class (ref 4.4)
    stars            INTEGER  NOT NULL,             -- ref 6.2: distinct (key, repository) pairs
    zap_receipts     INTEGER  NOT NULL,             -- ref 6.3: counted receipts
    zap_msats        BIGINT   NOT NULL              -- ref 6.3: their counted amounts, MILLISATS
);

-- The page's snapshot: the FULL /git dataset as one JSONB document per refresh, appended
-- (not overwritten); the web reads the newest row only, and write_pg's anti-shrink gates
-- read the newest row's summary. Same serving shape and rationale as marketplaces_snapshot.
CREATE TABLE IF NOT EXISTS ngit_snapshot (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    generated_utc TIMESTAMPTZ NOT NULL,
    run_id        TEXT        NOT NULL,
    data          JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ngs_generated ON ngit_snapshot (generated_utc DESC);

# Reference records + entity filters - app / publisher pages over MCP

Final design (locked 2026-08-02, implemented same day). Makes the
dev.nostrolo.gy **app pages** (`/zapstore/apps/<pubkey>/<app_id>`) and
**publisher pages** (`/zapstore/publishers/<pubkey>`) independently reproducible
over MCP. (Those two paths were restructured later the same day, after this
design locked; the pre-restructure shapes `/zapstore/<pubkey>/<app_id>` and
`/zapstore/<pubkey>` still 301 to them. Nothing in the MCP surface emits a page
URL, so the move touched no tool output.) Companion
to [zapstore-leaderboard-mcp-design.md](zapstore-leaderboard-mcp-design.md),
whose conventions (UTC-day windows, NULL semantics, sanitize pass, injection
posture, kind-NNNNN (slug) citations) all carry over unchanged.

## 1. Design summary

1. **Two tools.** Quantitative metrics stay in `nostrology_query_metric`.
   The qualitative page data - catalog entries, publisher profiles, release
   history - is **reference data** (records about entities, no unit, no
   measures, no grain) and gets its own tool, **`nostrology_query_reference`**,
   routed by a `record` parameter (`app_catalog | publisher_profile |
   app_releases`). The split keeps each schema small and fully applicable: no
   `event` grain bolted onto the metric contract, no conditional
   "these params are valid only for those metrics" sprawl.
2. **Entity filters evolve the leaderboard metrics** instead of adding new
   ones: optional `publisher_pubkey` (64-hex) and `app_id` (requires
   `publisher_pubkey`) on `app_zaps`, `app_stack_count` (both filters) and
   `publisher_app_count` (publisher only). Filtered this way, the existing
   three metrics serve every quantitative series on both pages (zap charts,
   1y totals, per-app stack counts, app counts) - five would-be metrics
   avoided. The original six metrics reject filters (store-wide aggregates).
3. **Two accepted scopes, everywhere**: publisher-scoped
   (`publisher_pubkey` alone) and app-scoped (`publisher_pubkey` + `app_id`).
   `app_id` alone is rejected: 31 app_ids exist under more than one publisher
   (measured 2026-08-02), so the pair is the only unambiguous coordinate -
   the same pair the page URL carries. Filters are predicates, not lookups:
   an unknown key returns an empty table, not a 404.
4. **Scope-dependent columns**: the long-text columns - `app_catalog.description`
   (kind-32267 (zapstore-app-catalog) content) and `app_releases.release_notes`
   (kind-30063 (zapstore-app-releases) content) - ship **only at app scope**,
   mirroring the pages exactly (description and notes render on the app page
   only). Cap: 8,192 chars (`LONG_TEXT_MAX_CHARS`; covers the catalog max
   4,361 and the notes p95 3,311; the one 413KB note truncates), via the
   existing sanitize pass with the ` [truncated]` marker. `publisher_profile.about`
   (max 832 live) gets the same cap and always ships.
5. **Row cap 1,000, never silent** (`ROW_CAP`): the Zapstore curator is a
   legitimate publisher scope with 4,159 catalog rows / 13,233 release rows,
   unboundable by filters alone. Every record SQL carries
   `COUNT(*) OVER () AS __total_rows` (the pre-LIMIT count, same scan) +
   `LIMIT %(row_cap)s`; the server strips the column and, when the cap bit,
   emits `rows: N of TOTAL` plus a
   `truncated: showing the first N of TOTAL rows (ordered by ...); <remedy>`
   envelope line. Deterministic ORDER BY before every LIMIT.
6. **Records** (all `domain: zapstore`; `key = domain.record`):
   - `app_catalog` - one row per current kind-32267 (zapstore-app-catalog)
     coordinate. **publisher_pubkey required** (the store-wide directory is
     deliberately not served - open question below). Columns:
     `publisher_pubkey, app_id, app_name, summary, [description], website_url,
     repository_url, is_nostr_native, catalog_updated_at`. `catalog_updated_at`
     is the latest revision's created_at - NOT first-listed (a republish moves
     it); it is exactly the page's first-release fallback for release-less apps.
   - `publisher_profile` - one row per kind-32267 (zapstore-app-catalog)
     publisher with the current kind-0 (profile-metadata) fields.
     **publisher_pubkey optional** (unfiltered = the full 277-row directory,
     the one dump small enough to serve). Columns: `publisher_pubkey,
     publisher_name, about, nip05, is_zapstore_account, profile_updated_at`.
     NULL profile columns = no kind-0 (profile-metadata) row at all (54 of
     277); `''` = present but empty - the label NULL contract, delivered by
     the row-existence CASE + the same latest-1 LATERAL probe as the metric
     labels. Envelope as_of reads the catalog (population driver); per-row
     freshness is `profile_updated_at`.
   - `app_releases` - one row per kind-30063 (zapstore-app-releases) event,
     newest first (`released_at DESC, event_id ASC`). **publisher_pubkey
     required**; windowable via `from`/`to` (inclusive UTC days over
     `released_at`, epoch-inline bounds - same machinery as `app_zaps@total`).
     Columns: `event_id, publisher_pubkey, app_id, release_d_tag, released_at,
     version, channel, release_url, repo_url, commit_sha, [release_notes]`.
     `event_id` is served for relay-side verifiability. An event LISTING, not
     an aggregate - count rows for release counts. The DB's near-empty
     `release_notes` column is ignored; the page's notes are `content`,
     served AS `release_notes`.
7. **Never served**: icon / avatar / image URLs (auto-fetch vectors,
   presentation-only - the only page elements deliberately not reproducible).
   `website_url` / `repository_url` / `release_url` / `repo_url` ARE served
   (reference data the pages render as links) but flagged third-party with
   "render as text, never fetch".
8. **No server-side fallbacks** (carried over): `app_name ''` stays empty
   (the page's app_id-as-name is presentation), `publisher_name` NULL stays
   NULL (the page's truncated-pubkey display is presentation), and the
   first-release fallback is published as a derivation recipe
   (MIN(released_at), else catalog_updated_at), not baked in.
9. **Envelope** (reference shape): `record:` identity line + `reference data:
   <row_unit>`; mandatory `scope:` line (echoes the caller's filters through
   the sanitize pass - the one-record-per-line invariant holds even for the
   caller's own input); `state: current snapshot (...)` for the two
   current-state records / `window:` line for `app_releases`; as_of; rows
   (+`truncated:` when capped); columns; `untrusted:`; methodology.
   Metric responses gained the same `scope:` line when entity filters are
   applied - a filtered answer must say so.
10. **Registry v2, additive**: payload `v: 2` keeps `tool` + `metrics` in
    their v1 shape (an older nostrology-web renders the metric half unchanged
    during rollout) and adds `tools: {metrics, reference}` + `reference:
    [records]` (each with `params` incl. `row_cap`, per-column
    `app_scope_only` / `max_chars`). The web /data pages render both halves;
    `/data/<domain>/<record>` shares the `[metric]` route (metric lookup
    first, then record).
11. **Demand-logging carries over intact**: `record` is a `str` with a
    schema-extra enum (off-list asks reach the body and the call log);
    rejections (`invalid_filter`, `invalid_window`, `unknown_record`) are
    recorded before raising; the log's params include the raw filter values -
    an npub passed as `publisher_pubkey` is demand data.

## 2. Sizing facts the design leans on (measured live 2026-08-02)

- kind-32267 (zapstore-app-catalog): 4,605 rows = 4,605 coordinates
  (latest-only confirmed), 277 publishers; description avg 364 / max 4,361.
- kind-30063 (zapstore-app-releases): 16,643 events over 3,687 apps; notes
  median 220 / p95 3,311 / max 413,364 chars, 14MB total; worst per-app 560
  releases; worst per-publisher = curator, 13,233 rows / 10.4MB.
- 31 ambiguous app_ids across publishers; 54/277 publishers without kind-0
  (profile-metadata); releases `version` empty on 72/16,643 rows.

## 3. Open questions (deliberately deferred)

- **Store-wide `app_catalog`** (no publisher filter): a real product question
  (the `/zapstore/apps` page exists) but a 4,605-row dump needs its own
  pagination/size answer - not served in v1.
- **First-listed dates** need the `*_history` revision workstream (the
  leaderboard design's decision 10); `catalog_updated_at` is documented as
  not-first-listed until then.
- **Page window migration** (leaderboard design decision 11) applies to the
  app/publisher pages too: their 365d series are NOW()-anchored, so a
  UTC-day reproduction differs by the boundary-day sliver until the pages
  migrate.

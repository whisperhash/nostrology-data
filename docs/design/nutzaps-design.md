# /nutzaps: what is counted, and how (living design doc)

This doc describes how the pipeline turns kind-9321 (nutzaps) events into:

- the numbers on nostrolo.gy/nutzaps and /nutzaps/mints;
- the paid MCP metric `nutzaps.active_users`.

It describes the current state, what the code does now, and is updated when the rules change.

Related kinds:

- kind-10019 (nutzap-configs): feeds "Configured By" on /nutzaps/mints.
- kind-7376 (nutzap-redemptions): the loader supports it, but no refresh loads it and nothing reads it (`dagster_code/assets.py`).

## Pipeline

1. **Extract.** `nutzaps_refresh` (Dagster, daily 01:45 UTC) extracts every kind-9321 (nutzaps) event from the nozzl archive through Athena. It keeps the full event (`content`, `tags`), deduplicated by id.
2. **Load.** The loader writes `events_kind_9321_nutzaps` with a whole-table TRUNCATE + COPY (`loader/load_jsonl_exports_into_postgres.py`, `load_kind_9321_nutzaps`). It drops:
   - blocklisted senders;
   - events without a normalizable `u` (mint) tag.

   kind-10019 (nutzap-configs) loads the same way into `events_kind_10019_nutzap_configs`.
3. **Mint classification.** After both loads, `loader --classify-nutzap-mints` rebuilds `nutzap_mint_classes`: one row per mint URL in either table, with its class (asset `nutzaps_mint_classes`). See "Test and private mints" below.
4. **MAU rebuild.** `schema/nutzap_active_users.sql` rebuilds `active_user_counts_kind_9321_nutzaps` (day / week / month) from the loaded table minus excluded mints, in the same run.
5. **Serve.**
   - nostrology-web reads all four tables (`src/lib/db/postgres.ts`).
   - The MCP server reads the active-users table for `nutzaps.active_users`.

Force a refresh with the deployment's re-run action for `nutzaps_refresh`. The run is queued and serialized against the schedules.

## Value: the proofs in `proof` tags, nothing else

Since nostrology-data#8 (2026-09-18), `amount_sats` is the sum of `amount` over the Cashu proofs carried as JSON objects in `proof` tags. The code is `_nutzap_value`.

Nothing else is ever read for value:

- **The `amount` tag.** NIP-61 (nutzaps) removed it on 2025-02-04, because it "has to be checked against the list of proofs anyway". It was frequently in millisats, and no current receiver reads it.
- **The `unit` tag.** It described the amount tag from 2024-08-07 to 2025-02-04, and the proofs from 2025-10-31.
- **Ecash anywhere other than a `proof` tag:** 2024 draft proofs in content, cashuA/cashuB tokens, encrypted payloads.

**What counts as a proof.** A proof is a JSON object that carries all four NUT-00 (Cashu notation) fields: `amount`, `id`, `secret`, `C`.

- `secret` and `C` must be non-empty strings.
- `amount` must be a positive integer, or a string of ASCII digits. cashu-ts 4 serializes amounts as strings.
- These are rejected:
  - floats, including `1.0`;
  - booleans, signs, exponents and whitespace;
  - zero and negatives;
  - any amount above 21M BTC in sats.
- Rejected proofs are skipped and counted, and they never raise. A bad proof beside good ones only lowers the value.

**Each proof counts once per event,** keyed by (`secret`, `C`). Counting a proof once across events is not done yet.

**Sum cap.** An event whose sum exceeds 21M BTC in sats is worth 0, with reason `implausible-amount`. That keeps a hostile event from overflowing the BIGINT column and failing the whole COPY.

**No single public event may break the load.** The load TRUNCATEs the table and drops its indexes, committed, before the COPY. There are two ways one event can do damage:

- **A failure during the COPY** leaves /nutzaps empty, and it stays empty on every refresh while the event remains in the archive.
- **A failure during the index rebuild** is worse. The rows stay committed without their indexes, and `init.sql`, which every surface applies before loading, then fails to recreate the index. The whole nightly pipeline stops until the row is removed by hand.

Both loaders of this surface, kind-9321 (nutzaps) and kind-10019 (nutzap-configs), guard every value that reaches the table:

| value | guard | failure it prevents |
|---|---|---|
| proof amounts | `_nutzap_value` never raises. It checks a digit string's length before `int()`, which refuses more than 4,300 digits. Sums above all bitcoin are refused. | a Python exception mid-load; a BIGINT overflow in COPY |
| the whole value rule | the loader wraps `_nutzap_value`. Any exception becomes a `none` / `invalid-proof-tag` row and a `WARN` with the event id. | an unforeseen wire format aborting the load |
| `p`, `e` | kept only when 64-hex (lowercased), else NULL | `CHAR(64)` overflow or a NUL in COPY |
| `u`, `mint` URLs | `_normalize_mint_url` returns None on a NUL, a lone surrogate, or more than 1,024 UTF-8 bytes (`_MAX_URL_BYTES`). The kind-10002 (relay-list-metadata) `_normalize_relay_url` shares the rule. | COPY or psycopg2 encoding errors; a btree/GIN entry over ~2,700 bytes failing the index rebuild |
| `created_at` (kind-9321 (nutzaps)) | an event at or after 2100-01-01 is dropped and counted (`created_at out of range`). nozzl keeps future-dated events and clamps u64 to i64::MAX. | `to_timestamp()` overflow in the MAU rebuild and the per-mint page |
| text in general | COPY escaping of tab, newline, CR and backslash (`_pg_copy_field`) | a malformed COPY row |

- **Trusted as archived:** `id` and `pubkey` (signature-verified hex) and the event's shape. Every tag element is a string, and Athena emits valid UTF-8.
- **Where the longest real values sit:** the longest archived mint URL is 380 bytes and the longest relay URL 985, so the 1,024-byte cap drops no real data.
- **How these were found:** each guard came from a review that reproduced the failure. They were a 5,000-digit amount, an oversized `p` tag, an incompressible 3 KB mint URL and a far-future `created_at`.
- **Tests:** a seeded fuzz tier in `loader/test_nutzap_load_pg.py` feeds 2,000 adversarial events per kind through the real loader into Postgres. It asserts the exact kept rows, byte-identical URLs, rebuilt indexes and a working MAU rebuild. Removing any guard above fails it.
- **The structural fix is not done.** It would load into a staging table and swap it in, so that any failure leaves yesterday's data served. It would change every loader, so it is tracked separately as [nostrology-data#10](https://github.com/whisperhash/nostrology-data/issues/10).

**Units.** Proofs are treated as sat-denominated. Every keyset that an archived nutzap uses, and that could be resolved, is sat. Nothing guards against a proof in another unit yet.

## A payment is a `value_source = 'proof_tags'` row

Every nutzap-shaped event keeps its row, so counts of what was published stay available. Each row carries two classification columns:

| column | values |
|---|---|
| `value_source` | `proof_tags`: at least one countable proof; `amount_sats` > 0. `none`: no countable proof; `amount_sats` = 0. `unclassified`: the ALTER default for rows written by a pre-#8 loader, gone after the next refresh. |
| `no_value_reason` | Set when `value_source = 'none'`. Checked first match wins, in the order below (`NUTZAP_NO_VALUE_REASONS`). |

The CHECK constraint `k9321_value_consistent` enforces the pairing in the database:

- `proof_tags` requires `amount_sats > 0` and no reason;
- `none` requires `amount_sats = 0` and a reason.

It rides on the `no_value_reason` ADD COLUMN, so it is created once with the column. The set of reason strings is not constrained in SQL.

**Rollback coupling.** A loader older than #8 writes every row `unclassified`, and every reader treats that as not-a-payment. Rolling the Dagster image back past #8 therefore has two effects:

- It empties the web's nutzap charts until nostrology-web is rolled back too.
- The Overview MAU and the paid `nutzaps.active_users` go the other way. The old image's MAU SQL has no filter, so they count non-payments again, while the MCP text still says "payments only".

Roll back all three together (Dagster image, web, MCP). After any refresh, `unclassified` must be 0: `SELECT count(*) FROM events_kind_9321_nutzaps WHERE value_source = 'unclassified'`.

| reason | what the event carries | producer | events | sats served before #8 |
|---|---|---|---:|---:|
| `implausible-amount` | proofs whose sum exceeds all bitcoin | none seen | 0 | 0 |
| `encrypted` | an `encryption` tag, NIP-44 (encrypted payloads) content | NostrPulse "Encrypted NutZap" (2026-09-08, testnut) | 13 | 350,000 |
| `cashu-token-in-proof-tag` | a cashuA/B string, or a wrapped `{proofs, mint}` object, in a `proof` tag | RUNSTR-style carrier; Robots Building Education's older builder | 13 | 55,090 |
| `invalid-proof-tag` | `proof` tags, none a valid proof | none seen | 0 | 0 |
| `draft-content` | a JSON proof list in content, unit `msat(s)` in `amount[2]` | 2024-07 NDK `wallet` branch + nutsack | 521 | 3,601,000 |
| `content-proofs` | a JSON proof list in content (plain-secret bearer ecash) | WesternBTC bridge, 2026 | 63 | 28,267 |
| `content-token` | a cashuA/B token in content | PWANS, DENOS | 128 | 510 |
| `zero-ecash` | no ecash, `amount "0"` | NDK failed-mint bug (Olas) | 20 | 0 |
| `no-proof` | nothing that looks like ecash | none seen among loaded rows | 0 | 0 |

Counts are over the 5,418 loaded rows of the archive to 2026-09-17. The 71 archived events with no usable mint and the 4 from blocklisted senders are not loaded.

**Every reader that counts payments, sums sats or counts senders filters on `value_source = 'proof_tags'`,** and drops excluded mints (next section):

| reader | where |
|---|---|
| Overview "Nutzap Payments" / "Nutzap Sats" | `SQL_NUTZAPS_DAILY_SERIES` |
| /nutzaps/mints leaderboard (Payments, Total Amount) | `SQL_NUTZAP_MINT_STATS` |
| "Cumulative sats by mint" chart | `SQL_NUTZAPS_TOP_MINTS_CUMULATIVE` |
| per-mint page: series, totals, MAU | `SQL_NUTZAP_MINT_DAILY`, `SQL_NUTZAP_MINT_MAU`, `SQL_NUTZAP_MINT_LISTING` (all-time totals) |
| Overview MAU chart, and the paid `nutzaps.active_users` | `schema/nutzap_active_users.sql` |

"Configured By" reads kind-10019 (nutzap-configs); it is not affected by the payment filter, but excluded mints lose their row, "Configured By" included.

- **What "Configured By" means.** For each mint, it is the number of pubkeys that list that mint in their latest kind-10019 (nutzap-configs). It counts announcements, not usage, and that is by design: the operator decided it on 2026-09-19.
- **Keys that are never used still count.** On 2026-09-19 that was 57% of the keys on counted mints, most of them Minibits-only.
- **Do not add a liveness rule.** Usage is the Payments column.

- **A new reader must filter too.** The nostrology-web test `src/lib/db/nutzaps.test.ts` seeds a phantom `unclassified` row and a `none` row next to real payments, so any existing query that loses the filter fails.
  - That holds only when the test runs against a database. The deploy build has no Postgres, so it skips these tests, and nostrology-data has no CI. Run them locally before changing a nutzap query.
- **A mint whose events all lack proofs** no longer appears on the leaderboard (unless enough users configure it), and so has no per-mint page.

## What #8 changed in the served numbers

Loaded rows only: served before the fix vs the same events under the rule.

| window | served before | proof-tag value | ratio |
|---|---:|---:|---:|
| all time | 4,296,997 | 262,130 | 16.4x |
| trailing 365 days | 511,320 | 77,456 | 6.6x |
| trailing 90 days | 408,019 | 52,484 | 7.8x |
| 2024 | 3,603,593 | 2,593 | |
| 2025 | 189,601 | 189,184 | |
| 2026 to 09-17 | 503,803 | 70,353 | |

- **No payment changed value.** On all 4,660 rows with proof-tag value, the proofs already won before #8. The whole drop is the 758 rows that had no countable proof, which the old loader valued from the `amount` tag.
- **Payments vs rows.** Payments are 4,660 of 5,418 rows. The monthly active users for 2026-05 drop from 7 to 5.
- **Independent check.** The totals were recomputed straight from the Parquet archive with an independent DuckDB implementation of the rule, not the loader. It agreed with the loader on every event: 0 disagreements.

That check covers value and payment status, not the reason labels, which only the fixtures pin. It also reads the raw Parquet rather than the Athena extract; the matching row count (5,418) covers that link.

**What these numbers still included.** The 90-day window held 30,963 sats on a loopback mint (`http://127.0.0.1:3338`, the July 2026 test harness) plus testnut. #9 removed them; see the next section.

## Test and private mints (nostrology-data#9)

Since 2026-09-18 some mints are **excluded**: their payments, sats and senders count nowhere, and they have no row on /nutzaps/mints and no per-mint page. Every other mint counts, including one nobody has reviewed. Unreviewed never means fake.

**Two sources decide a mint's class** (`loader/nutzap_mints.py`, `classify`; first match wins):

1. **The private-host rule, automatic.** A mint on a host that a recipient elsewhere cannot reach is `private-host`, excluded:
   - loopback, RFC 1918, carrier-grade NAT (Tailscale), link-local (including 169.254.169.254), unspecified, documentation, multicast and reserved addresses, IPv4 and IPv6 (`_NON_PUBLIC_NETS`, written out rather than taken from `ipaddress.is_private`, whose ranges differ between Python versions);
   - special-use and private names: `localhost`, `.local`, `.internal`, `.lan`, `.home`, `.home.arpa`, `.corp`, `.test`, `.example`, `.invalid`, `.localdomain`;
   - single-label hosts (`http://umbrel:3338`), and a numeric last label (`127.1`, which a browser reads as an address).

   Why: the recipient cannot redeem ecash from someone else's machine, so it is not a payment to anyone. The URL is also not one mint: every developer's `127.0.0.1:3338` is a different server. .onion and plain-http public hosts are reachable and stay counted.
2. **The reviewed registry**, `data/nutzaps/mint_registry.yaml`. Each entry names a `url` (one mint) or a `host` (every URL on it), a `class`, `evidence` (a URL or a quote), `as_of` and `reviewer`. The loader refuses a malformed file, so a typo fails the refresh instead of counting a test mint.

| class | excluded | meaning | today |
|---|---|---|---|
| `private-host` | yes | the rule above | 20 URLs, incl. the July 2026 harness and six throwaway Docker mints of 2026-07-30 |
| `fake-backend` | yes | the operator says invoices are marked paid without payment | the testnut family (testnut, nofee., nofees.), plus published fake mints not yet seen in nutzaps |
| `test-mint-gone` | yes | published as a test mint, no longer answers | testmint.macadamia.cash |
| `test-wording-live` | no | calls itself test/demo but settles real Lightning | mint.routstr.com, mint.lnpay.cz, mint.unclerics.cash, 8333.space:3338 |
| `unreachable` | no | no DNS today; past payments were real | mint.bitcointxoko.com, mint.0xchat.com, stablenut.umint.cash |
| `unreviewed` | no | everything else | |

- **Wording alone is not grounds for exclusion:** 8333.space:3338 calls itself a test mint and settles real Lightning payments.
- **A mint named "testnut" is not excluded without evidence:** testnut.era21.space (18 configs, no payments) stays unreviewed.
- **Not built:** keysets derived from public seeds (mint.0xchat.com, cashu.puhcho.me, mint.lnfast.com, mint.lnserver.com, mint.solife.me). The problem is per keyset, not per mint, and needs keyset ids per proof in the table. Its effect is almost all before 2026.
- **The web page names testnut** in its method line; the paid MCP tool does not, because testnut is still accepted, unadvertised, as payment on that tool (#5), and the paid envelope must never name an unadvertised mint (`test_the_paid_SUCCESS_envelope_never_names_the_hidden_mint`).

**The table.** `nutzap_mint_classes` (`mint_url` PK, `mint_class`, `excluded`, `class_source`, `evidence`), with a CHECK that `excluded` matches the class. The classification rebuilds it with DELETE + INSERT in one transaction, after validating the registry, so a failure leaves the previous rows serving. Readers test `NOT EXISTS (... AND excluded)`: a URL missing from the table, a mint first seen since the last classification, counts.

**Deploy order.** The web's nutzap queries read `nutzap_mint_classes`, which only `init.sql` creates. Deploy nostrology-data first (the Dagster stack, then a run of `nutzaps_refresh`, which applies `init.sql` and fills the table), and nostrology-web after. The other order serves errors on /nutzaps until a Dagster run creates the table.

**Failure coupling.** The MAU rebuild now waits for the classification, and the classification waits for BOTH loads. A failed kind-10019 (nutzap-configs) load, or a registry the loader refuses, therefore leaves `active_user_counts_kind_9321_nutzaps` at the previous run's values while the kind-9321 (nutzaps) table is fresh. The paid metric's `as_of` (the newest event) then still looks fresh. The run fails loudly (Discord), and the registry is parsed by the tests, but nothing marks the metric stale: that is the load watermark of nostrology-data#10.

**Rollback coupling.** Rolling the Dagster image back past #9 while the web stays: the old MAU SQL counts every mint again (the paid metric's 2026-07 goes back to 182), the web keeps excluding by a classes table that no longer updates, and the old normalizer's spellings (host case, default ports) are missing from that table and so count. Roll back the Dagster image, the web and the MCP together, as for #8.

**New mints get looked at.** Each classification prints, to the Dagster compute log:

```
  Mint classes: 163 URLs [fake-backend=7, private-host=20, test-mint-gone=1, test-wording-live=4, unreachable=3, unreviewed=128]; excluded: 262 payments, 35,886 sats
  Review: no unreviewed mint had its first payment in the last 14 days
```

When the `Review:` line names a mint, check its NUT-06 (mint info) text and add it to the registry if it is a test mint, then deploy the Dagster stack and re-run `nutzaps_refresh`. Nothing alerts on it yet.

**Changing the classes.** `nutzap_mint_classes` is `CREATE TABLE IF NOT EXISTS`, so its two CHECK constraints (the class list, and which classes are excluded) never change on their own. Adding or re-scoping a class needs a hand-run ALTER of both in production before the deploy; `test_the_sql_check_constraints_list_the_same_classes_as_python` keeps `init.sql` and `nutzap_mints.CLASSES` in step.

**Where it is applied:**

| reader | how |
|---|---|
| Overview payments and sats, cumulative sats by mint | `notExcludedMint(...)` in `SQL_NUTZAPS_DAILY_SERIES`, `SQL_NUTZAPS_TOP_MINTS_CUMULATIVE` |
| /nutzaps/mints leaderboard | `SQL_NUTZAP_MINT_STATS` drops excluded mints entirely, "Configured By" included |
| per-mint page | exists exactly when the 'All' leaderboard lists the mint (`nutzapMintIsListed`: not excluded, and one payment ever or 10 configs); a listed mint with no activity in the past year renders its charts at 0 |
| Overview MAU and the paid `nutzaps.active_users` | `schema/nutzap_active_users.sql`; the metric's `partition` line says so |

**Effect, measured on the archive to 2026-09-17** (loaded rows, payments only):

| | before #9 | after |
|---|---:|---:|
| sats, all time | 262,130 | 226,244 |
| MAU 2026-06 | 32 | 25 |
| MAU 2026-07 | 182 | 18 |
| MAU 2026-08 / 2026-09 (to 09-17) | 32 / 13 | 31 / 13 |
| /nutzaps/mints 'All' rows / links that 404ed | 60 / 41 | 49 / 0 |

The 262 excluded payments are 211 on the loopback harness, 6 on the Docker mints, 43 on testnut and nofees.testnut, and 2 on testmint.macadamia.cash. None of the 165 senders on private hosts ever paid on a real mint, and none of their 61 recipients ever received on one.

## Mint URLs: one spelling

Both loaders store every mint URL through `nutzap_mints.normalize_mint_url`, so `u` tags, `mint` tags, the classes table and the registry join on one spelling. It folds only what cannot change which server answers:

- surrounding whitespace and trailing slashes;
- the case of the scheme and the host;
- a default port (`:443` on https, `:80` on http).

**The path keeps its case.** `https://mint.minibits.cash/Bitcoin` answers; `/bitcoin` returns 404 (checked 2026-09-18). Folding them would credit Minibits with 23 users whose config points at a dead URL, so they stay two mints. Query and fragment are kept too. Not usable, so the event or config entry has no mint:
- a tab, CR or newline, or a port that is not a number in 0..65535;
- a backslash before the path (Python and browsers disagree on the host);
- a host part that does not rebuild to itself (`[v1.fe]`, `[::1]x`);
- a `.` or `..` path segment, which a browser resolves away.

The private-host rule also judges IPv6 spellings of an IPv4 address (mapped, 6to4, NAT64, IPv4-compatible) as that address.

On the archive this changed two spellings, `https://Mint.lnserver.com` and `HTTPS://mint.minibits.cash/Bitcoin`, and dropped one junk config entry, `https://example.com\..\`.

**Links.** The per-mint route is `/nutzaps/mints/<host[:port]><path>`, https implied; an http mint adds `?scheme=http`, and `%`, `?` and `#` in the path are percent-encoded so the page gets the URL whole (nostrology-web `src/lib/utils/mintUrl.ts`). The label keeps the port, so mints that differ only by port no longer read the same. A windowed leaderboard can list a mint on one config, which has no page; the query's `has_page` column (judged on all-time counts) leaves that row unlinked.

## Observability

Each load prints, to the Dagster compute log:

```
  Value: 4,660 events with proof-tag value (262,130 sats), 758 with none [encrypted=13, ...]; 13 proof tags rejected
  Info: 24 events' amount tag disagrees with their proof sum (not used for value), e.g. ...
```

- **The `Info` line** replaces the old per-event "canary" WARN. It compares the unit-aware `amount` tag with the proof sum, and nothing reads it for value.
- **A jump** in the `none` count or in `proof tags rejected` means a new wire format has appeared. Classify it before trusting its value.
- **Nothing alerts on these lines yet.** An asset check that fails on `unclassified > 0` and warns on a shift in the reason mix would close that gap. It is not built.

## Tests

- **`loader/test_nutzap_value.py`** (pure, fast).
  - One real archived event per wire family, from `loader/fixtures/nutzap_wire_families.json`. The test recomputes each NIP-01 (basic protocol) id to prove the fixture is the event as archived. It pins each fixture's value, source and reason next to its pre-#8 served value.
  - A property test: rewriting or deleting the `amount`/`unit` tags of any fixture never moves its value.
  - Parser edge cases, and the contract with `schema/init.sql` and the MAU SQL.
- **`loader/test_nutzap_load_pg.py`** (real Postgres; `NOSTROLOGY_CONTRACT_DSN`, local only).
  - Runs the loader CLI on the fixtures, then the MAU rebuild.
  - Checks the upgrade of a pre-#8 table (old rows become `unclassified` and are excluded) and the CHECK constraint.
- **nostrology-web `src/lib/db/nutzaps.test.ts`** runs the five production queries verbatim against temp tables (`TEST_DATABASE_URL`).
- **`nostrology_mcp/tests/test_server_nutzaps.py`** pins the paid metric's population text.
- **`loader/test_nutzap_mints.py`** (pure): URL normalization, the private-host rule at the edges of every range, registry validation, classification precedence, and the class of every fixture's mint under the shipped registry. The fixture file carries four real payments on excluded mints (the loopback harness, a Docker mint, testnut, testmint.macadamia.cash) and each fixture's expected `mint_class`.
- **`loader/test_nutzap_load_pg.py`** also covers the classification: the table's rows, the MAU exclusion, an unlisted mint counting, a malformed registry changing nothing, a mid-rebuild failure rolling back, the `Review:` line, and the fuzz corpora through classification.
- **nostrology-web `src/lib/db/nutzaps.test.ts`** seeds excluded, unlisted and config-only mints; `src/lib/utils/mintUrl.test.ts` round-trips every link.

The tests that read the YAML registry need PyYAML: it is in the Dagster image, not in a workstation's `.venv-mcp`, where they skip.

When a new wire format appears:

1. Add its real event to the fixture file, with its expected classification.
2. Extend `_nutzap_value` if needed; a new reason goes at the right place in `NUTZAP_NO_VALUE_REASONS`.

## MCP methodology

`nutzaps.active_users` states its population in its `partition` line (`nostrology_mcp/metric.py`). It counts events that carry at least one Cashu proof in a `proof` tag, a resolvable mint that is not excluded, and a sender that is not blocklisted. The draft format, encrypted payloads, bare tokens, events without ecash, and test and private mints are named as excluded, and it says that any other mint counts, reviewed or not. The paid tool serves no value metric, so no value rule appears in its text.

- **Freshness.** Its `as_of` is still the newest kind-9321 (nutzaps) event of any kind, payment or not. That means "archive read through", not "latest payment".
- **Schema locks.** Every Dagster load applies `init.sql`, so the `ADD COLUMN IF NOT EXISTS` statements take a brief exclusive lock on this table on every refresh, even when the columns already exist. The zaps and routstr tables already do the same, and the table is small.

## Still open

- **Public-seed keysets:** per keyset, needs keyset ids per proof.
- **Count a proof once across events.**
- **Receivable value:** exclude x-only and unlocked proofs.
- **Verify DLEQ; archive keyset public keys.**
- **kind-7376 (nutzap-redemptions) value** is the redeemer's self-reported `amount` tag, because its content is encrypted. Nothing shows it. If anything ever does, label it self-reported.

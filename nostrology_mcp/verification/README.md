# MCP verification battery

**None of these batteries is in CI.** They are run by hand at each MCP launch or release, and their
reports read. They need read access to the production database, and the Routstr one to the S3 archive
too, which CI does not hold. They reach the database over `ssh "$NOSTROLOGY_RO_SSH"`, the ssh
destination of a read-only login on the deployment's box, and stop, naming the variable,
without it; the Routstr one also reads `AWS_PROFILE` and `NOZZL_ARCHIVE_BUCKET`.

Also in this package, unrelated to the battery below: **`cashu_probe.py`**, the
committed six-probe fixture that checks which tool-result
channels reach the MODEL in Claude Code. It is a manual procedure with its
measured baseline in its own docstring; re-run it after any Python MCP SDK or
Claude Code upgrade.

An end-to-end battery that asks the **deployed** `nostrology` MCP server a few
hundred questions about Zapstore and checks every answer against an independent
path. It is the live-data counterpart of `nostrology_mcp/tests/` - those are unit
tests over mocked rows, this is "does the thing actually deployed tell the
truth about the data actually in Aurora".

```sh
cd nostrology-data
python3 -m nostrology_mcp.verification.run              # full run
python3 -m nostrology_mcp.verification.run --reuse      # re-verify cached fetches
python3 -m nostrology_mcp.verification.run --self-test  # prove the harness can fail
```

Reports land in **`nostrology_mcp/verification/battery-artifacts/<date>-zapstore-mcp-verification-battery.html`**
with the raw comparison data beside them as `<date>-results.json`. Per-run raw
fetches (MCP payloads, the SQL dump, the scraped values) go to
`nostrology_mcp/verification/battery-artifacts/raw/<date>/` - a few MB per run, fully
regenerable. `--reuse` reads back today's (UTC), which is how you re-examine a failing
check without hitting the deployment again. With no cache for today a run goes live, `--reuse` and
`--self-test` included, so neither is an offline check: `nostrology_mcp/tests/` is. `battery-artifacts/` is gitignored: these are test artifacts,
kept only on the machine that ran the battery. `--out <dir>` sends a report and its raw cache somewhere else.

Exit status is non-zero if anything FAILs, so it can gate a workflow later.

## The Routstr battery (2026-08-30)

The second domain has its own battery, organised by TOOL and METRIC rather than
by chart - one section per Routstr metric and record, the contract a client
relies on, the whole-document agreement with the reference implementation and the
live page:

```sh
python3 -m nostrology_mcp.verification.routstr_run              # full run (minutes; needs S3 creds + node)
python3 -m nostrology_mcp.verification.routstr_run --reuse      # re-verify cached fetches
python3 -m nostrology_mcp.verification.routstr_run --self-test  # prove the harness can fail
python3 -m nostrology_mcp.verification.run --domain routstr     # the same, via the shared entry point
```

Reports (about 220 checks) land in `nostrology_mcp/verification/battery-artifacts/<date>-routstr-mcp-verification-battery.html`
(+ `<date>-routstr-results.json`). Its independent sides differ from Zapstore's
because the Routstr numbers are not one SQL statement away from raw events:

| Path | Module | Why it is independent |
|---|---|---|
| The reference re-derivation | `routstr_truth.build_reference` -> `crosscheck/build_routstr_snapshot.py` | DuckDB straight over the raw S3 archive of the four kinds, pinned to the served document's `generated_utc` twice over - as the created_at horizon AND as the archive-state cutoff (`--ingested-before`, so rows the archive received after the run are dropped): a SECOND IMPLEMENTATION of the whole methodology. Needs `AWS_PROFILE` (a profile that can read the archive) and `NOZZL_ARCHIVE_BUCKET` set, and `.venv-crosscheck` (`crosscheck/requirements.in`). Slow; cached as `raw/<date>/reference.json`. If the nightly moved between the two reads the run aborts - rerun without `--reuse`. |
| SQL over the substrate | `routstr_truth.SECTIONS` | The raw ledger buckets and registry/catalog/review tables fetched whole and sliced in Python (`Truth`) - every window, rank and sum is recomputed here, never by the tool's SQL. A bound window counts daily buckets only, the page's rule. |
| The live page | `routstr_truth.page_document` | The document `/routstr` server-renders, evaluated back from the SvelteKit payload by `node`. |

Subjects (top provider, an excluded provider with ledger rows, the top model, a
model folded out of the leaderboard, a reviewed node) and windows ("last 7 days"
and "last 30 days" from today, the refresh-anchored 30 days that reproduce the
page, "this month") are all derived at run time in `routstr_run.build_context` -
nothing is hard-coded. Plan ids are `RS..` so they never collide with the
Zapstore plan's in the shared `raw/<date>/` cache. `nostrology_mcp/tests/test_verification_routstr.py`
pins the deterministic plumbing (truth views, context derivation, page parser,
diff rule).

## The nutzaps battery (2026-09-30)

The free `nutzaps` domain of `nostrology_query_metric` (design record:
[docs/design/nutzaps-mcp-design.md](../../docs/design/nutzaps-mcp-design.md)) has
its own battery, organised by metric like the Routstr one - `payment_activity`,
`mint_leaderboard`, `mint_activity`, `mint_active_users` (day, week and month) -
plus cross-call reconciliation, the `mint_url` filter (normalization, the
excluded-mint rule), the rejections, the envelope, the PAID tool's unchanged free
path, the paid line (the free per-mint counts held against the paid table) and the
three live pages:

```sh
python3 -m nostrology_mcp.verification.nutzaps_run              # full run (a minute or two)
python3 -m nostrology_mcp.verification.nutzaps_run --reuse      # re-verify cached fetches
python3 -m nostrology_mcp.verification.nutzaps_run --self-test  # prove the harness can fail
python3 -m nostrology_mcp.verification.run --domain nutzaps     # the same, via the shared entry point
```

Reports land in `nostrology_mcp/verification/battery-artifacts/<date>T<HHMM>Z-nutzaps-mcp-verification-battery.html`
(+ `-nutzaps-results.json`); `--tag local` suffixes both (`...-battery-local.html`) and
keeps its raw cache apart (`raw/<date>-local/`), so a deployed run's `--reuse` never
reads a local server's answers. Its independent sides:

| Path | Module | Why it is independent |
|---|---|---|
| SQL over the raw tables | `nutzaps_truth.SECTIONS` + `Truth` | Every kind-9321 (nutzaps) PAYMENT row (`value_source = 'proof_tags'`, on every mint), every kind-10019 (nutzap-configs) row and `nutzap_mint_classes`, fetched once and recomputed in Python: the excluded-mint rule, the latest-config-per-pubkey rule (ties detected), the listing floor (10 at All, 1 with a window), the ordering, every window, rank, sum and distinct count. Never the tool's SQL. One thing comes from the database on purpose: its string ORDER of the mint URLs (`MINT_ORDER`), because the leaderboard's last key, "then mint_url", is sorted by the database collation (`en_US.UTF-8`, which ignores punctuation: `mint1.example.com` before `mint.21linz.at`), not by code point. |
| The live pages | `nutzaps_truth.collect_web` | `/nutzaps`, `/nutzaps/mints` (+ `/api/nutzaps?period=all/7d/30d/1y`) and `/nutzaps/mints/<mint>`, read out of the SvelteKit `kit.start(...)` payload by `node`, fetched with the first-party User-Agent mark (`nutzaps_truth.USER_AGENT`). Each is compared with `Truth.web_*` - the site's ROLLING rules restated, evaluated at the fetch instant (exact) - and with the tool's two whole-day windows around its cutoff (BOUNDED). |
| Cross-call + contract | `nutzaps_questions` | Day rows summing to `payment_activity@total`, leaderboards summing to it, `mint_activity` == `payment_activity` per day and per mint, filtered == client-sliced, grain defaults (day, total, day, month), `mint_active_users` at day, week and month (weeks start on Monday, a window from a Wednesday leaves that week out whole, every (day, mint) nests inside its week and month, the day buckets are exactly `mint_activity`'s (day, mint) keys, `grain=total` rejected) and no `senders` column on the leaderboard, the "nothing matches the filter - ..." note on every filtered call that matched nothing (an excluded mint, `https://no-such-mint.invalid`) and on no other call, the longest live mint URL (380 chars) round-tripping whole, the rejections and their remedies (since 0.5.1 a refused grain must name the call that answers it - `grain=total` for a week of `payment_activity`, `mint_leaderboard` for `mint_activity` totals, `mint_activity` for the leaderboard's days, and why no sum of buckets replaces `mint_active_users` total), `additive_over_time` true on exactly `payment_activity@day` and `mint_activity@day`, `active_users` on the free tool naming `nostrology_query_nutzaps`, normalization (host and scheme case, trailing slash, no scheme, default port; the PATH keeps its case), an excluded mint returning nothing, envelope lines, and the paid tool still answering with its terms without a token (never paid). |
| The paid line | `nutzaps_truth.SECTIONS["PAID_AU"]` + `nutzaps_questions` (`s_paid_line`) | Not a truth for the free tool: the PAID table (`active_user_counts_kind_9321_nutzaps`), read with the raw rows. It must count the senders the raw rows hold, deduplicated across mints, and in every bucket the tool's per-mint counts must bracket it - no mint above it, the sum over mints never below it. The report's findings measure how much of the paid series the free rows reproduce (the design record's 87% of days); it lists disagreements only, never the paid series. |

Subjects (the top mint by all-time sats, a mint with configs but few payments, a
config-only mint listed by the 10-config floor, the excluded mint with the most raw
payments, a mint whose path has upper case) and windows (last 7 / 30 days, this
month, the past full month, the web's trailing year from its partial oldest day D0
and from D0 + 1, the rolling 7d / 30d brackets) are derived at run time in
`nutzaps_run.build_context`. Plan ids are `NZ..`. After the MCP calls the table
fingerprint is read again: if the nightly refresh (01:45 UTC) moved the data
mid-run, the run aborts with exit status 2 rather than compare two datasets.
The SQL goes over SSH-over-SSM with stdout and stderr in FILES, not pipes: the
ProxyCommand's `session-manager-plugin` inherits ssh's stderr and can outlive ssh, and
a pipe-based `subprocess.run` then waits forever for EOF (it hung a run on
2026-09-30); an ssh-level failure (exit 255) is retried twice.
`nostrology_mcp/tests/test_verification_nutzaps.py` pins the plumbing and drives
the whole question list against a fake server answering from a fixture.

To try it against a LOCAL server before a deploy, open a tunnel over the read-only login
(`ssh -f -N -L 127.0.0.1:15432:<aurora-endpoint>:5432 "${NOSTROLOGY_RO_SSH:?}"`), start
`NOSTROLOGY_MCP_TRANSPORT=streamable-http NOSTROLOGY_MCP_LOG_DIR=<scratch> python -m nostrology_mcp.server`
from the repo root (db.py's defaults are that tunnel: `localhost:15432`, `nostrology_ro`,
password from `~/.pgpass`; the port is `NOSTROLOGY_MCP_PORT`, default 8080), and pass
`--url http://127.0.0.1:8080/mcp --tag local`. The public endpoint is only ever
called as `https://nostrolo.gy/mcp?traffic_type=internal`; `--url` refuses any other
spelling of it.

## The three independent paths (Zapstore)

Nothing is scored against the MCP server's own reasoning, and **no expected value
is stored in this harness** - both sides are recomputed on every run.

| Path | Module | Why it is independent |
|---|---|---|
| SQL over the raw event tables | `truth_sql.py` | Written against `schema/init.sql`, run read-only on Aurora as `nostrology_ro` via `ssh "$NOSTROLOGY_RO_SSH"`. The native/republished partition comes from `subjects.is_native` - the domain rule - not from the server's SQL. |
| The live website | `web_truth.py` | Chart series and card rows scraped from the rendered `nostrolo.gy` pages. The site reads the same Postgres through its *own* SQL in `nostrology-web`, so agreement is a second implementation agreeing. |
| Cross-call + contract checks | `questions.py` | Internal consistency the server must hold regardless of the data (grain=day summing to grain=total, entity filters matching client-side filtering, partitions summing to the whole) plus error handling, row caps, column contracts, freshness. |

## Verdicts

- **PASS** - the MCP answer equals the independent answer.
- **BOUNDED** - MCP brackets the target and the gap is a declared limitation.
  Today this is exactly one situation: **the site cuts trailing windows on a
  mid-day timestamp (`NOW() - N days`) while MCP windows on whole UTC calendar
  days.** The boundary day can only be taken whole or dropped whole, so when its
  events straddle the cutoff the live value lands strictly between the two
  windows MCP can express. Affected checks fetch both windows and assert the live
  value sits between them.
- **FAIL** - the answers disagree. **Confirm the independent side first**, then
  the question, then the server. On the first run of this battery six checks
  failed and all six were wrong expectations, not server bugs.

A run where everything is exact is luck, not a stronger guarantee: whether a
given trailing-window card lands exact or bounded depends on that day's traffic.

## Adding questions

Everything lives in `questions.py`. Two rules:

1. **Never hard-code an expected value.** `want` must be recomputed from `T`
   (the SQL views) or `W` (the live site) on every run. A literal will rot the
   next time Dagster refreshes the serving tables - the first version of this
   harness hard-coded them and they went stale within a day.
2. **Fill in `how`.** It is what makes the report readable as evidence, and what
   a future reader uses to judge whether the check is genuinely independent.

```python
ask("Chart 4 - Zaps", "How many zaps landed last month?",
    ["M18"],                                  # the MCP call ids this rests on
    sum(M["M18"].series().values()),          # got  - from MCP
    wsum(T.zap_count(), MF, MT),              # want - recomputed independently
    "windowed zap_count vs the SQL month count")
```

If the question needs an MCP call that does not exist yet, add it to `plan.py`
with a new stable id and reference that id. If it needs ground truth that is not
in `truth_sql.py` yet, add a section there - prefer a **generic** table the
questions slice in Python over a question-specific aggregate, so the next
question gets it for free.

`subjects.py` holds the entities the battery asks about. They are subjects, not
answers: both sides recompute for whichever entity is named, so a subject can be
swapped freely. Prefer stable, structurally interesting subjects (a developer
with several apps and a profile; the curator's own first-party app, which is
native despite the curator key; a publisher with no kind-0 (profile-metadata) event at all) over
whatever currently tops a leaderboard.

## Modules

| File | Role |
|---|---|
| `run.py` | Orchestrator. Reads the independent sides **first** (two MCP calls are parameterised from them), then runs the plan, verifies, writes the report. |
| `plan.py` | The MCP calls. Smaller than the question list - one call backs many questions. |
| `mcp_client.py` | Streamable-HTTP MCP client. Transport is `curl` because a workstation's python may lack a usable CA bundle. |
| `mcp_parse.py` | Splits a response into envelope, column contract and CSV table. |
| `truth_sql.py` | The SQL ground truth. Each section is emitted as one line of `json_agg` - third-party names and release notes contain delimiters and newlines, and JSON escaping is the only framing that survives them. |
| `web_truth.py` | Live-page extraction. Layout-coupled by nature; `run.py` aborts if a selector yields nothing rather than scoring empty against empty. |
| `questions.py` | The battery. |
| `report.py` | HTML rendering. |

## Known operational quirks

- **The first SQL run is slow** (several minutes): the script pulls the full
  catalog; `--reuse` avoids paying it twice.
- **The leaderboards page is ~1.8 MB** of server-rendered rows, hence the
  generous fetch timeout.
- **An empty HTTP body has been seen once** from the MCP endpoint (2026-08-02,
  `zap_count` with an open-ended `from`), answering correctly on retry with no
  server-side error logged. `mcp_client.call()` retries transport-level
  emptiness and `run.py` reports which calls needed a retry, so a recurrence
  shows up in the run output rather than as a mystery failure.

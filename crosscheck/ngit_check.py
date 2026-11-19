#!/usr/bin/env python3
"""
ngit_check.py - the INDEPENDENT cross-check of the /git dataset (Git-on-Nostr, NIP-34 (git
collaboration)). It re-derives the `ngit_repos` / `ngit_pushes` substrate, the snapshot's
`summary` block and its four weekly series from the raw archive and diffs them against the
pipeline's export.

WRITTEN FROM THE REFERENCE, NEVER FROM THE PIPELINE. Every rule here comes from
docs/dataset-reference/ngit-dataset-reference.md (authoritative; "section N" means its sections), the substrate
columns of its section 9, and the DDL + summary field names of docs/design/ngit-design.md section 8.
Whoever changes this file must not open dagster_code/pipeline/ngit.py, its tests, or a pipeline
dry run, although they sit in the same repository: a check that copies the
production compute cannot catch the production compute's mistakes. A difference is a bug in
one implementation or an ambiguity in the reference, and an ambiguity is fixed in the
REFERENCE first. The construction is deliberately different (SQL for dedup, windows, joins
and aggregates; Python for the procedural parts), not a port.

COMMANDS (python = .venv-crosscheck/bin/python, which has duckdb 1.5.3)
  python crosscheck/ngit_check.py --self-test
  python crosscheck/ngit_check.py --duckdb <scratchpad>/ngit.duckdb --now 2026-09-25T07:41:00Z --out <dir>
  python crosscheck/ngit_check.py --s3 --now <generated_utc> \
      --ingested-before <run start> --heavy-from-extracts <run_id> \
      --out <dir> --compare <exported substrate dir>        # pinned production comparison
  python crosscheck/ngit_check.py --out <dir> --compare <other dir>        # diff two existing dirs
  THE PINS (reference section 9.1 has the whole recipe): <run_id> and <generated_utc> are the
  served ngit_snapshot row's (the newest); <run start> is THAT run's start, from Dagster by that
  run id (runOrError startTime; drop its fraction), never from last-run-ngit_refresh.json, which
  names the newest run even when it failed, and never <generated_utc>, which can only add false
  differences from rows ingested during the run. --ingested-before is inclusive: rows with
  ingested_at equal to the cutoff are kept. It is one instant for every kind, while each of the
  pipeline's eight extracts flushes its kind's WAL at its own moment, seconds to a couple of
  minutes after the run start, so a difference confined to rows ingested between the run start
  and that kind's extract flush (its `[extract] flushing` log line) is the pin's imprecision,
  not a bug.

INPUTS  --duckdb PATH: a local copy opened READ-ONLY (e30617, e30618, e10317 = id-deduped
  events; git1111 = the git comment subset; e7, e9735, e5 when present, else empty - the
  2026-09-25 copy has no kind-7 (reactions), kind-9735 (zap-receipts) or kind-5
  (event-deletion-requests) rows, the self-test supplies them; e17 = kind-17
  (external-content-reactions), which the two-kind stars rule (STARS) needs: a copy without
  it, or whose e17 holds no row ingested by the pin, stops the check with exit 2, naming the
  table, while the kind-7 (reactions) rule alone reads no e17 and needs none).
  --s3: s3://$NOZZL_ARCHIVE_BUCKET/c=<class>/k=<kind>/p=*/dt=*/*.parquet via httpfs + the
  credential_chain secret (AWS_PROFILE; http timeout 180 s, 8 retries, 16 threads); the comment, reaction,
  external reaction, zap and deletion scans carry the section 6 filters (deletions: `a` values
  naming a 30617: coordinate whose pubkey is the author) in their WHERE clause, only the needed
  columns are read, and the three heavy kinds (kind-7 (reactions) 6 GiB, kind-9735
  (zap-receipts) 2.4 GB, kind-5 (event-deletion-requests) 10 GiB) are scanned one `dt`
  partition at a time (a failed GET retries its partition). ONE instant, this checker's own
  start, taken before any read, is now_ts and every kind's --ingested-before unless given.
  Under --s3, kind-17 (external-content-reactions) is read from the archive, --heavy-from-extracts
  or not, in one pass (about 7,100 events, 31 in scope, on 2026-10-11), never from an extract, so
  the scan also checks the pipeline's Athena admission filter for it. The two-kind stars rule
  (STARS) lists its Parquet first, before any other archive read, and when it holds no row
  ingested by the pin the check stops with exit 2, naming the prefix: the kind reaches S3 only
  when the producer flushes its write-ahead log (each pipeline run's extract does; ingested_at
  stays the producer's observation time), so before the first flush there is none, and read as
  empty it would put stars_iris at 0 and blame it, stars_total and stars_repos on the pipeline.
  The kind-7 (reactions) rule alone reads no kind-17 (external-content-reactions) and needs none.
  --heavy-from-extracts RUN_ID: those three kinds come instead from the pipeline run's Athena
  UNLOAD parts, <extracts-base>/k=<kind>/<RUN_ID>-k<kind>-ngit/*.gz (gz NDJSON: events for
  kind-7 (reactions) and kind-9735 (zap-receipts), (author, coord, created_at) triples for
  kind-5 (event-deletion-requests)). INDEPENDENCE: this mode reuses the pipeline's Athena
  admission filter for those kinds (sql/extract_ngit_{reactions,zaps,deletions}.sql), so it
  is independent only downstream of admission; the parts carry no ingested_at, so they are
  pinned by the pipeline run, not by --ingested-before. An S3 lifecycle rule deletes the parts
  7 days after the run, and a kind read as empty would blame N6 and every social figure on the
  pipeline, so the parts are listed first, before any archive read, and a kind with ZERO parts
  stops the check with exit 2, naming the kind, the run id and the prefix; --allow-empty-heavy
  loads such a kind as empty, deliberately.
  --signers PATH: section 6.3's verified LNURL provider signer keys (default:
  data/lightning/provider_signers.yaml, the list /lightning uses); a missing file, a
  `signers:` list this reader cannot parse, or no key at all stops the check with exit 2.
STARS  Section 6.2's rule changed with the nostrology-data deploy of 2026-10-11: kind-17
  (external-content-reactions) stars (Iris Git's) join the kind-7 (reactions) ones, and the
  document gains stars_iris, archive_events.external_reactions and the anomaly slots
  future_dated.external_reactions and past_dated.external_reactions (THE FOUR KEYS). That day's
  03:18:08 UTC document predates the deploy and the first one holding the keys is generated later
  the same day, so no date tells them apart; the reference's own rule does: a document that
  lacks stars_iris counts kind-7 (reactions) stars alone (section 6.2), and only documents
  generated before 2026-10-12 may lack the keys (sections 1 and 6.2). A derivation run with
  --compare DIR reads DIR's summary.json first, before any other read: one carrying stars_iris
  is derived by the two-kind rule; one without it generated before 2026-10-12 by the kind-7
  (reactions) rule alone, without the four keys and without reading kind-17
  (external-content-reactions), so such a row re-checks with zero differences; one without it
  whose generated_utc is missing or unreadable stops the run (exit 2). Without --compare the
  two-kind rule applies. A `[stars]` line names the rule and why. Two choices are this
  checker's, for documents that break the reference's rule: one without stars_iris generated on
  or after 2026-10-12 is derived by the two-kind rule it had to follow, so the compare counts
  every key it lacks and holds its stars to both kinds; and as the deploy's first run writes the
  four keys together ("first written by the first run after the nostrology-data deploy of
  2026-10-11", sections 1 and 6.2), a document carrying some of them but not all differs once per
  key it lacks, even one generated before 2026-10-12.
RULE CHANGES OF 2026-10-12  Three rules apply from the first run after the nostrology-data deploy
  that brought each, and this checker applies all three to every row, so an older row re-derives
  with differences:
  - section 4.3's third denylist, the hosts shown not to be GRASP servers (five until
    git.gittr.space left it for the forge list, the third rule), which no document marks:
    wherever one of them decided a class (cls, grasp, grasp_repos, grasp_servers, hosting,
    no_state, grasp_per_repo, by_host);
  - section 3's hashtree pushes, which a document marks by carrying pushes_hashtree_30d (this
    checker does not choose its rule by that, as it does for STARS): every push figure of a
    document with a hashtree push, and the export's ngit_pushes, which before that deploy has no
    `source` column (one difference per row) nor the hashtree rows;
  - section 4.2's forge list gaining git.gittr.space, gittr's hosted git server, which no document
    marks either: a repository on it and on no host but forges is forge_only, no longer
    other_hosts (cls, hosting, no_state), and by_host counts it under forge, not self_hosted.
  A fourth, section 2.2's N10 (the nostr.net catalog key, the nostrology-data deploy of 18:27
  UTC), changes no older row: no announcement by that key was archived before that deploy, and
  the N10 ledger row an older document lacks reads as 0.
  The three nostrology-data deploys were at 09:28 UTC (the denylist), 11:02 UTC (the hashtree
  pushes) and 13:01 UTC (git.gittr.space a forge). Check an older row with the checker as it was
  before the change, saved inside crosscheck/ as ngit_check_<date>.py (it finds the zap parser
  relative to its own path, in a checkout named nostrology-data). Those versions predate this
  directory: for a row written before the denylist (03:18:33 UTC and earlier), the checker as of
  2026-10-11 14:50:14 UTC; for one written between 09:28 and 11:02 UTC, as of 2026-10-12
  09:23:55 UTC; for one written between 11:02 and 13:01 UTC, as of 2026-10-12 10:56:19 UTC.
OUTPUTS (--out DIR)  ngit_repos.csv (every announcement coordinate, by pubkey, d),
  ngit_pushes.csv (every ledger row, state versions and hashtree pushes, by repo_key, created_at,
  event_id), and
  summary.json (the contract's summary fields, no names, no concentration, plus exclusions
  [{k, unit, v}], hosting, recency, no_state, grasp_per_repo, anomalies {future_dated,
  past_dated}, and the weekly series pushes_weekly, new_repos_weekly, comments_weekly and
  grasp_lists.per_week, rows {w, ...}; a new_repos_weekly row also has by_host [{k, v}], its
  repositories once under every kind of public host they are on, grasp, hashtree, forge,
  self_hosted, then none); under the two-kind stars rule also stars_iris, and
  external_reactions in archive_events and in both anomaly blocks. --compare DIR diffs
  every repos column but name/hosts/relays, every pushes column and every summary field (per
  column a count and up to 10 examples) and exits 1 on any difference; a column only DIR's CSV
  has is one difference (the reference does not define it: an export from before the
  nostrology-data deploy of 2026-10-02, that day's nightly run included, still has
  ngit_pushes.counted); a partition or ledger row absent on one side reads as 0; a weekly
  series compares row by row (a week absent on one side does not read as 0: the series are
  zero-filled) and as its sequence of weeks, `<series>.@weeks`, so a duplicated, missing or
  misordered week is a difference, and a row's by_host likewise entry by entry (every entry
  is present, zeros included, so an entry absent on one side does not read as 0) and as its
  sequence of keys, `new_repos_weekly.<w>.by_host.@keys`; anomalies, grasp_per_repo,
  as_of_day, comments_90d, new_repos_weekly, its by_host, comments_weekly and
  grasp_lists.per_week compare only when both sides carry them, and a line names each one
  carried by one side only (by_host as new_repos_weekly.*.by_host, `*` any week, and only
  when both sides carry new_repos_weekly: a side without the series is judged on the series);
  comments_90d is first written by the first run after the nostrology-data deploy of
  2026-10-01, and a document generated on or after 2026-10-02 without it is a difference, and
  so is one generated on or after 2026-10-03 without the three weekly series of the deploy of
  2026-10-02, or on or after 2026-10-09 without the by_host of the deploy of 2026-10-08
  (OPTIONAL_UNTIL; a side lacking such a key whose generated_utc is missing or
  unreadable stops the compare). The four keys of STARS likewise compare only when both sides
  carry them, and a document generated on or after 2026-10-12 without one is a difference
  (sections 1 and 6.2: documents generated before 2026-10-12 may lack them); a side carrying
  some of them but not all differs once per key it lacks, whatever its date (STARS). When one
  side carries stars_iris and the other lacks it and was generated before 2026-10-12, the two
  count different stars (section 6.2: a document that lacks it counts kind-7 (reactions) stars
  alone), so their stars_total and stars_repos are not compared, a line naming each (a
  derivation run with --compare that side derives them by its rule). Of a served document,
  only the summary block, the blocks
  named here and grasp_lists.per_week are read: the coverage block (partial_first_week, the
  first `w` of pushes_weekly, among it), the monthly series, cum_repos, the top lists and the
  rest of grasp_lists are outside the comparison.
EXIT  0 no difference (or a derivation without --compare); 1 differences, from --compare only;
  2 could not run: a missing input (MissingInput, a [FATAL] line), a bad argument, or any
  crash, whose traceback is printed (reference section 9.1).

READINGS  Taken here where the reference (revision of 2026-09-25 22:27 UTC) leaves room:
  - a deletion `a` value is self-authored when its second `:` field equals the author (the
    literal "author equals the coordinate's pubkey", and the extract template's split_part
    test), so a value without a `d` part still forms an archive_events.deletions pair (as
    section 1 says), though it can never match a coordinate for N6;
  - N8b classifies clone values with an EMPTY GRASP set: the set is built from kept
    repositories, so that is the only acyclic reading ("reachability never depends on the
    GRASP set" fails only for a non-network scheme, such as nostr://, on a GRASP host);
  - a nip.io / sslip.io IPv4 starts the name or follows a `.` (app-10-0-0-1.nip.io is not
    private), and each octet is at most 255, as in a dotted IPv4;
  - zaps: the signer keys are the `signers` lists of provider_signers.yaml (it has no
    `nostrPubkeys` field; each listed key was verified as a provider's NIP-57 (lightning
    zaps) nostrPubkey); the first `p` value is compared as is (no case folding); "the
    request parses" is the zapper's parse (a JSON object with a non-empty string pubkey);
    the requested amount is the FIRST `amount` tag's value, equal to the bolt11 msats as text.
  And where its revision of 2026-10-02 (no daily cap; the weekly series) does:
  - a comment's root reads the FIRST `K` value ("when `K` is `1621`"), as the section 6.1 input
    filter does; a first `K` of any other value, or no `K`, is `other`;
  - comment_threads ("distinct `E` overall") counts distinct FIRST-`E` values, as a week's
    threads ("distinct first-`E` values") do; the self-test's two-`E` comment adds no thread
    to it under either reading.
  That revision's other readings are pinned in it since: the series' place (top level,
  per_week inside grasp_lists) and rows ({w, pushes, repos, maintainers}; {w, issues, prs,
  patches, repo, other, authors, threads}), pushes_weekly's first `w` = the coverage key
  partial_first_week (outside the comparison), and a GRASP user dated by its earliest version,
  whatever that version lists.
  Its revision of 2026-10-08 (new_repos_weekly's by_host) pins its own: the first day a document
  must carry by_host (2026-10-09), and its comparison, entry by entry and as its sequence of keys.
  Its revision of 2026-10-11 (kind-17 (external-content-reactions) stars) pins its own, so they
  are its rules, not readings here: the content trimmed with Python's str.strip() (as section
  4.1 pins for clone values; four deliberate differences from Iris Git's own parser, the trim's
  among them, are listed in section 6.2), both anomaly slots and archive_events over the
  admitted subset (section 1 step 2), a target without a second colon dropped from the stars
  with no fall-through but kept in the input, `d` everything after the second colon, the first
  day a document must carry the four keys (2026-10-12), and the stars of a document without
  stars_iris (kind-7 (reactions) alone). One reading remains: a NULL content (no Nostr event
  has one) is not a star, as no kind-7 (reactions) row with one is. The checker's two choices
  for documents that break the rule are in STARS.
"""
import argparse
import csv
import datetime as dt
import importlib.util
import json
import os
import re
import ssl
import sys
import tempfile
import time
import traceback
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import duckdb

# DuckDB's httpfs reads none of Python's TLS settings: it verifies against the first existing path
# in its own short list of well-known bundles, which on macOS is only the OS's static bundle (still
# holding two roots compromised in 2025), and it ignores SSL_CERT_FILE. On macOS, point it at the venv
# interpreter's own default store instead, and refuse an interpreter whose default is missing or is
# that same static file: a bare python3 from PATH may be such a build. Elsewhere the list finds the
# distribution's maintained bundle, and an empty ca_cert_file keeps that default.
CA_FILE = ssl.get_default_verify_paths().openssl_cafile if sys.platform == "darwin" else ""
APPLE_STATIC = os.path.realpath(CA_FILE).endswith("/etc/ssl/cert.pem") if CA_FILE else False  # tls-posture-ok: the refusal names the file it refuses
if CA_FILE and (not os.path.isfile(CA_FILE) or APPLE_STATIC):
    sys.exit(f"{sys.executable}: its default CA bundle {CA_FILE!r} is missing or is the OS's static one; "
             "run under .venv-crosscheck (crosscheck/requirements.in)")

DAY = 86400
def _required_env(name, what):
    value = os.environ.get(name, "").strip()
    if not value:
        sys.exit(f"set {name} to {what} (the pipeline reads the same variable)")
    return value


def archive_root(archive=None):
    """The nozzl archive's root: `archive` when given (a local tree), else s3://$NOZZL_ARCHIVE_BUCKET,
    read only when an --s3 read needs it."""
    return archive or "s3://" + _required_env("NOZZL_ARCHIVE_BUCKET", "the nozzl archive's S3 bucket")


def extracts_root():
    """The pipeline's Athena UNLOAD parts (--heavy-from-extracts): s3://$NOSTROLOGY_EXTRACTS_BUCKET."""
    return "s3://" + _required_env("NOSTROLOGY_EXTRACTS_BUCKET", "the pipeline's extracts S3 bucket")
# the heavy kinds by table, named for messages: scanned per `dt` partition, or read from the UNLOAD parts
HEAVY = {"rx": "kind-7 (reactions)", "zp": "kind-9735 (zap-receipts)", "dl": "kind-5 (event-deletion-requests)"}
ZAP_PARSER = Path(__file__).resolve().parents[1] / "src" / "shared" / "zap_parser.py"
SIGNERS = Path(__file__).resolve().parents[1] / "data" / "lightning" / "provider_signers.yaml"


def instant(s):
    """ISO-8601 (Z, offset, or naive = UTC) or integer epoch seconds -> epoch seconds."""
    s = str(s).strip()
    if re.fullmatch(r"[0-9]+", s):
        return int(s)
    t = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return int((t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)).timestamp())


def day_iso(day):
    return None if day is None else (dt.date(1970, 1, 1) + dt.timedelta(days=int(day))).isoformat()


def week_of(day):
    """Section 1's week: the ISO week (Monday start) holding UTC day number `day`, as its Monday's
    day number (day 0, 1970-01-01, was a Thursday)."""
    return day - (day + 3) % 7


WEEK = "day - (day + 3) % 7"  # week_of() over a `day` column, in SQL
# section 1's capture walls: STATE_WALL gates a push (section 3); each wall's week starts a weekly series
ANN_WALL = instant("2026-05-02T16:50:26Z")
STATE_WALL = instant("2026-06-14T03:22:39Z")
COMMENT_WALL = instant("2026-06-18T02:59:29Z")
PAST_CUT = instant("2023-01-01T00:00:00Z")  # earlier created_at: a broken clock, counted as past_dated
G_KEY = "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
# section 2.2's N10 since 2026-10-12: the nostr.net catalog, the awesome-nostr list's entries re-published as
# announcements of other people's projects
CATALOG_KEY = "62ec33a4090f0959e5c547a33f22a3253688bba121667a69e64a2dc758f8baaf"
# section 4.2's forge hosts; git.gittr.space, gittr's hosted git server, since 2026-10-12
FORGES = {"github.com", "gitlab.com", "codeberg.org", "bitbucket.org", "git.sr.ht", "tangled.org",
          "gitea.com", "gitee.com", "framagit.org", "notabug.org", "git.gittr.space"}
# section 4.3's three hand-kept denylists: web viewers and documentation sites; general relays serving no git;
# hosts shown not to be GRASP servers (the probe of 2026-10-12)
GRASP_DENY = {"gitworkshop.dev", "nostrhub.io", "gittr.space", "ngit.dev",
              "relay.damus.io", "nos.lol", "relay.primal.net", "relay.nostr.band", "relay.nostrcheck.me",
              "git.jb55.com", "relay.vanderwarker.family", "gitrepublic.imwald.eu", "denimroad.feeds.relay.tools"}
NET_SCHEMES = {"http", "https", "ssh", "git", "ws", "wss"}
REACHABLE = {"hashtree", "grasp", "forge", "self_hosted"}
FAMILIES = ["N0", "N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8a", "N8b", "N9", "N10"]
HOSTING = ["grasp_only", "grasp_mixed", "hashtree", "forge_only", "other_hosts", "none"]
HOST_KINDS = ["grasp", "hashtree", "forge", "self_hosted", "none"]  # by_host: REACHABLE in 4.4's order, then none
RECENCY = [("30d", 0, 29), ("90d", 30, 89), ("180d", 90, 179), ("365d", 180, 364), ("older", 365, 10 ** 9)]
ROOTS = ["issues", "prs", "patches", "repo", "other"]  # section 6.1's comment roots, in the series' field order
MAX_ZAP, CAP_ZAP = 21_000_000_000, 10_000_000_000

# ---- inputs (SQL) ------------------------------------------------------------------------------
# table -> (kind, archive class, local table, content needed)
KINDS = {
    "ann": (30617, "addressable", "e30617", False),  # kind-30617 (repo-announcements)
    "st": (30618, "addressable", "e30618", False),   # kind-30618 (repo-states)
    "gl": (10317, "replaceable", "e10317", False),   # kind-10317 (grasp-server-lists)
    "cm": (1111, "regular", "git1111", True),        # kind-1111 (comments)
    "rx": (7, "regular", "e7", True),                # kind-7 (reactions)
    "xr": (17, "regular", "e17", True),              # kind-17 (external-content-reactions); two-kind stars only
    "zp": (9735, "regular", "e9735", False),         # kind-9735 (zap-receipts)
    "dl": (5, "regular", "e5", False),               # kind-5 (event-deletion-requests)
}
ADMIT = {  # the section 6 input filters (and the deletion reduction), in the scan's WHERE clause
    "cm": "(list_filter(tags, t -> t[1] = 'K')[1][2] IN ('1617', '1618', '1619', '1621', '30617', '30618')"
          " OR len(list_filter(tags, t -> t[1] IN ('A', 'a') AND starts_with(t[2], '30617:'))) > 0)",
    "rx": "len(list_filter(tags, t -> (t[1] = 'k' AND t[2] = '30617')"
          " OR (t[1] = 'a' AND starts_with(t[2], '30617:')))) > 0",
    # section 6.2: a `k` tag valued git-repo AND an `i` tag whose value starts with 30617: (both)
    "xr": "(len(list_filter(tags, t -> t[1] = 'k' AND t[2] = 'git-repo')) > 0"
          " AND len(list_filter(tags, t -> t[1] = 'i' AND starts_with(t[2], '30617:'))) > 0)",
    "zp": "len(list_filter(tags, t -> t[1] = 'a' AND starts_with(t[2], '30617:'))) > 0",
    "dl": "len(list_filter(tags, t -> t[1] = 'a' AND starts_with(t[2], '30617:')"
          " AND split_part(t[2], ':', 2) = pubkey)) > 0",
}
# `{x}.coord` (a 30617:<pubkey>:<d> string, d = everything after the second colon) -> coords c
COORD_JOIN = ("LEFT JOIN coords c ON {x}.coord LIKE '30617:%:%' AND c.pubkey = split_part({x}.coord, ':', 2)"
              " AND c.d = substr({x}.coord, 8 + length(split_part({x}.coord, ':', 2)))")
FIRST_A = "list_filter(tags, t -> t[1] = '{n}' AND starts_with(t[2], '30617:'))[1][2]"


def connect(src, path, remote):
    """A read-only connection to the local copy, or an in-memory one; `remote` (any input read
    from S3) adds httpfs, the credential chain and patient http settings (a 6-10 GiB scan from a
    workstation otherwise dies on one GET timeout)."""
    con = duckdb.connect(path, read_only=True) if src == "duckdb" else duckdb.connect()
    con.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW'")
    if remote:
        if not (os.environ.get("AWS_PROFILE") or os.environ.get("AWS_ACCESS_KEY_ID")):
            sys.exit("set AWS_PROFILE to a profile that can read the archive (or export its credentials)")
        for stmt in ("INSTALL httpfs", "LOAD httpfs", f"SET ca_cert_file = '{CA_FILE}'",
                     "SET http_timeout = 180", "SET http_retries = 8",
                     "SET http_retry_wait_ms = 1000", "SET http_retry_backoff = 2", "SET threads = 16",
                     "CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1')"):
            con.execute(stmt)
    return con


class MissingInput(Exception):
    """An input the comparison needs is absent. The run stops: read as empty, the input would
    surface as differences blamed on the pipeline, never as the gap it is."""


def extract_parts(con, heavy, log):
    """--heavy-from-extracts: each heavy kind's UNLOAD parts, listed ONCE, before any archive read.
    An S3 lifecycle rule deletes them 7 days after the run, and a kind loaded with no parts is an
    empty input that blames N6 and every social figure on the pipeline, a false alarm that looks
    like a finding. So a kind with ZERO parts raises MissingInput, unless allow_empty
    (--allow-empty-heavy) loads it empty on purpose, with a warning."""
    base, run_id, allow_empty = heavy
    parts, missing = {}, []
    for name, label in HEAVY.items():
        kind = KINDS[name][0]
        prefix = f"{base}/k={kind}/{run_id}-k{kind}-ngit/"
        parts[name] = [f for (f,) in con.execute(f"SELECT file FROM glob('{prefix}*.gz')").fetchall()]
        log(f"[extracts] {name} {label}: {len(parts[name])} UNLOAD parts under {prefix}")
        if not parts[name]:
            missing.append(f"  {label} under {prefix}")
    if missing and not allow_empty:
        raise MissingInput("\n".join(
            [f"--heavy-from-extracts {run_id}: no UNLOAD parts (*.gz) for"] + missing +
            ["An S3 lifecycle rule deletes a run's parts 7 days after the run (a wrong run id finds none either).",
             "Read as empty, these kinds would blame N6 and every social figure on the pipeline, so the check stops.",
             "Re-run the pipeline and check its new snapshot row, or drop --heavy-from-extracts to scan these",
             "kinds from the S3 archive (slower; pinned by --ingested-before). --allow-empty-heavy loads them as",
             "empty instead, deliberately."]))
    if missing:
        log("\n".join(["[extracts] WARNING --allow-empty-heavy: loading as EMPTY"] + missing +
                      ["N6 and every social figure will differ from the pipeline's for that reason alone."]))
    return parts


SIGNER_ITEM = re.compile(r"-\s+(['\"]?)([^'\"\s]*)\1")  # `- key`, bare or quoted


def load_signers(path):
    """Section 6.3's verified LNURL provider signer keys: every key in a `signers:` list of
    provider_signers.yaml (the file has no `nostrPubkeys` field; each provider's `signers` were
    verified against its NIP-57 (lightning zaps) nostrPubkey). The nzl venv has no PyYAML, so
    this reads the file's one shape, a block list of bare or quoted 64-hex keys under
    `signers:`, and stops the run on anything else in such a list, or on a file with no key: a
    misread list would drop zaps and read as differences blamed on the pipeline."""
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError as e:
        raise MissingInput(f"--signers {path}: {e}") from None
    keys, block = set(), None  # block: the indent of the open `signers:` key
    for n, line in enumerate(lines, 1):
        body = re.sub(r"(^|\s)#.*", "", line).rstrip()  # a YAML comment starts a line or follows a space
        if not body:
            continue
        indent = len(body) - len(body.lstrip())
        if block is not None and indent >= block and body.lstrip().startswith("-"):
            m = SIGNER_ITEM.fullmatch(body.lstrip())
            if not m or not re.fullmatch(r"[0-9a-f]{64}", m.group(2)):
                raise MissingInput(f"--signers {path}:{n}: not a 64-hex signer key: {body.strip()!r}")
            keys.add(m.group(2))
            continue
        block = None
        m = re.fullmatch(r"(\s*(?:-\s+)?)signers:(.*)", body)
        if m and m.group(2).strip():
            raise MissingInput(f"--signers {path}:{n}: a `signers:` value this reader cannot parse: "
                               f"{body.strip()!r}")
        if m:
            block = len(m.group(1))
    if not keys:
        raise MissingInput(f"--signers {path}: no signer key found")
    return keys


def iris_input(con, src, archive, cutoff, log):
    """Under the two-kind stars rule (STARS): kind-17 (external-content-reactions) must hold a row
    ingested by the pin, in the archive (--s3) or in the local copy's table e17 (--duckdb). The
    kind reaches S3 only when the producer flushes its write-ahead log, which each pipeline run's
    extract does, so before its first flush there is no Parquet at all, and a local copy made
    without the kind has no e17; read as empty, the kind would put stars_iris at 0 and blame it,
    stars_total and stars_repos on the pipeline, a false alarm that looks like a finding. So
    load_inputs calls this before any other read, and it raises MissingInput when there is no
    Parquet or no e17, or none of its rows was ingested by --ingested-before. Cheap: the kind is
    tiny, and only ingested_at is read. The kind-7 (reactions) rule alone never calls it."""
    kind, cls, local, _ = KINDS["xr"]
    if src == "s3":
        prefix = f"{archive_root(archive)}/c={cls}/k={kind}/"
        files = [f for (f,) in con.execute(f"SELECT file FROM glob('{prefix}p=*/dt=*/*.parquet')").fetchall()]
        found, source = bool(files), f"read_parquet({files})"
        what, absent = f"{len(files)} Parquet file(s) under {prefix}", f"no Parquet under {prefix}"
        hint = ["The kind reaches S3 when the producer flushes its write-ahead log, which each pipeline run's",
                "extract does: check a document generated after that flush."]
    else:
        tables = {t for (t,) in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        found, source = local in tables, local
        what, absent = f"table {local} of the local copy", f"no table {local} in the local copy (--duckdb)"
        hint = [f"Give the local copy the kind's events as table {local} (id, pubkey, created_at, content, tags,",
                "ingested_at), or derive with --s3, which reads them from the archive."]
    pin = f" WHERE ingested_at <= {cutoff}" if cutoff is not None else ""
    rows = con.execute(f"SELECT count(*) FROM {source}{pin}").fetchone()[0] if found else 0
    log(f"[input] xr kind-17 (external-content-reactions): {what}, {rows} rows ingested by the pin")
    if not rows:
        raise MissingInput("\n".join(
            ["kind-17 (external-content-reactions): " + (f"no row ingested by --ingested-before {cutoff} in {what}"
                                                          if found else absent),
             "The two-kind stars rule (section 6.2: kind-7 (reactions) and kind-17 (external-content-reactions)",
             "stars) needs the kind: read as empty it would put stars_iris at 0 and blame it, stars_total and",
             "stars_repos on the pipeline, so the check stops."] + hint +
            ["A document without stars_iris generated before 2026-10-12, given as --compare, is derived by the",
             "kind-7 (reactions) rule alone, which reads no kind-17 (external-content-reactions)."]))
    return rows


def admitted_rows(con, src, name, cutoff, parts, log, archive=None):
    """One kind's admitted rows (before dedup) as temp table {name}_raw: from the local copy, from
    the archive (a heavy kind one `dt` partition at a time, so a failed GET retries one
    partition), or from the UNLOAD parts extract_parts listed (heavy kinds under
    --heavy-from-extracts; `parts` is None without it). `archive` is the archive's root: the
    bucket, or a local tree of the same layout in the self-test."""
    kind, cls, local, content = KINDS[name]
    cols = f"id, pubkey, created_at, {'content' if content else 'NULL::VARCHAR'} AS content, tags, ingested_at"
    where = " AND ".join(([f"ingested_at <= {cutoff}"] if cutoff is not None else []) +
                         ([ADMIT[name]] if name in ADMIT else [])) or "true"
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name}_raw (id VARCHAR, pubkey VARCHAR, created_at BIGINT, "
                "content VARCHAR, tags VARCHAR[][], ingested_at BIGINT)")
    if parts is not None and name in HEAVY:  # no ingested_at in the parts: pinned by the pipeline run itself
        files = parts[name]
        spec = ({"author": "VARCHAR", "coord": "VARCHAR", "created_at": "BIGINT"} if name == "dl" else
                {"id": "VARCHAR", "pubkey": "VARCHAR", "created_at": "BIGINT", "content": "VARCHAR", "tags": "VARCHAR[][]"})
        sel = ("author || ' ' || coord || ' ' || created_at, author, created_at, NULL, [['a', coord]], NULL"
               if name == "dl" else f"id, pubkey, created_at, {'content' if content else 'NULL'}, tags, NULL")
        if files:  # empty only under --allow-empty-heavy; extract_parts stops the run otherwise
            con.execute(f"INSERT INTO {name}_raw SELECT * FROM (SELECT {sel} FROM read_json({files}, "
                        f"format = 'newline_delimited', columns = {spec})) t(id, pubkey, created_at, content, "
                        f"tags, ingested_at) WHERE {ADMIT[name]}")
    elif src == "s3" and name in HEAVY:
        by_dt = defaultdict(list)
        for (f,) in con.execute(f"SELECT file FROM glob('{archive_root(archive)}/c={cls}/k={kind}/p=*/dt=*/*.parquet')").fetchall():
            by_dt[re.search(r"/dt=([^/]+)/", f).group(1)].append(f)
        for i, dt_ in enumerate(sorted(by_dt), 1):
            for attempt in range(1, 6):
                try:  # a failed INSERT inserts nothing, so a retry cannot double-count
                    con.execute(f"INSERT INTO {name}_raw SELECT {cols} FROM read_parquet({by_dt[dt_]}) WHERE {where}")
                    break
                except duckdb.Error as e:
                    if attempt == 5:
                        raise
                    log(f"[load] {name} dt={dt_} attempt {attempt} failed ({str(e)[:100]}); retrying")
                    time.sleep(20 * attempt)
            if i % 20 == 0 or i == len(by_dt):
                log(f"[load] {name} k={kind}: {i}/{len(by_dt)} dt partitions scanned")
    elif src == "s3":
        con.execute(f"INSERT INTO {name}_raw SELECT {cols} FROM "
                    f"read_parquet('{archive_root(archive)}/c={cls}/k={kind}/p=*/dt=*/*.parquet') WHERE {where}")
    elif local in {t for (t,) in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}:
        con.execute(f"INSERT INTO {name}_raw SELECT {cols} FROM {local} WHERE {where}")


def load_inputs(con, src, now_ts, as_of_end, cutoff, log, heavy=None, iris=True, archive=None):
    """Section 1 steps 1-3 per kind: id-dedup (earliest ingested_at), the horizon (keep
    PAST_CUT <= created_at <= as_of_end; count > now_ts and < PAST_CUT), and the coordinate. Temp tables ann/st (with d), gl, cm, rx, zp, del (deletion triples), coords
    (an integer cid per coordinate), ann_c/st_c (per-coordinate version statistics), and xr under
    the two-kind stars rule (`iris`, STARS) alone: the kind-7 (reactions) rule reads no kind-17
    (external-content-reactions) and writes no external_reactions key."""
    future, past = {}, {}
    parts = extract_parts(con, heavy, log) if heavy else None  # first: a missing kind stops the run at once
    if iris:
        iris_input(con, src, archive, cutoff, log)  # first too: kind-17 (external-content-reactions) with no row
    for name, (kind, *_) in KINDS.items():
        if name == "xr" and not iris:
            continue
        t0 = time.time()
        admitted_rows(con, src, name, cutoff, parts, log, archive)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {name}_in AS SELECT * EXCLUDE (ingested_at) FROM {name}_raw
            QUALIFY row_number() OVER (PARTITION BY id ORDER BY ingested_at) = 1""")
        future[name], past[name] = con.execute(f"SELECT count(*) FILTER (WHERE created_at > {now_ts}), count(*) "
                                               f"FILTER (WHERE created_at < {PAST_CUT}) FROM {name}_in").fetchone()
        con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM {name}_in "
                    f"WHERE created_at BETWEEN {PAST_CUT} AND {as_of_end}")  # deletions: per triple
        con.execute(f"DROP TABLE {name}_in; DROP TABLE {name}_raw")
        n = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        log(f"[load] {name} k={kind}: {n} rows kept, {future[name]} future-dated, {past[name]} past-dated, "
            f"{time.time() - t0:.1f}s")
    for name in ("ann", "st"):
        con.execute(f"ALTER TABLE {name} ADD COLUMN d VARCHAR")
        con.execute(f"UPDATE {name} SET d = coalesce(list_filter(tags, t -> t[1] = 'd')[1][2], '')")
    con.execute("""CREATE OR REPLACE TEMP TABLE del AS
        SELECT pubkey, t[2] AS coord, max(created_at) AS last_req
        FROM (SELECT pubkey, created_at, unnest(tags) AS t FROM dl)
        WHERE t[1] = 'a' AND starts_with(t[2], '30617:') AND split_part(t[2], ':', 2) = pubkey GROUP BY ALL""")
    con.execute("""CREATE OR REPLACE TEMP TABLE coords AS
        SELECT row_number() OVER (ORDER BY pubkey, d) AS cid, pubkey, d
        FROM (SELECT pubkey, d FROM ann UNION SELECT pubkey, d FROM st)""")
    # per coordinate, in version order (created_at asc, id DESC): versions, first/last seen,
    # the median consecutive gap, and the current version (the last row in that order)
    for name in ("ann", "st"):
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {name}_c AS
            WITH v AS (
              SELECT c.cid, x.created_at, x.tags,
                     x.created_at - lag(x.created_at) OVER (PARTITION BY c.cid ORDER BY x.created_at, x.id DESC) AS gap,
                     row_number() OVER (PARTITION BY c.cid ORDER BY x.created_at DESC, x.id) AS newest
              FROM {name} x JOIN coords c USING (pubkey, d))
            SELECT cid, count(*) AS versions, min(created_at) AS first_seen, max(created_at) AS last_seen,
                   median(gap) AS med_gap, any_value(tags) FILTER (WHERE newest = 1) AS cur_tags
            FROM v GROUP BY cid""")
    # the kinds with anomaly slots (section 1 step 2), external_reactions under the two-kind stars rule alone
    slots = ((("announcements", "ann"), ("states", "st"), ("grasp_lists", "gl"), ("comments", "cm"),
              ("reactions", "rx")) + ((("external_reactions", "xr"),) if iris else ()) + (("zaps", "zp"),))
    events = {k: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for k, t in slots + (("deletions", "del"),)}
    return events, {"future_dated": {k: future[t] for k, t in slots}, "past_dated": {k: past[t] for k, t in slots}}


def hand_off(con, table, columns, rows, workdir):
    """Python rows -> a DuckDB temp table through a CSV file: executemany and list parameters
    cost ~30 s per 90k rows here, a CSV round trip 0.1 s. Only CSV-safe values travel (hex ids,
    integers, booleans), which is why coordinates cross as integer cids, never as d strings."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE {table} ({', '.join(f'{n} {t}' for n, t in columns)})")
    if not rows:
        return
    path = os.path.join(workdir, f".handoff-{table}-{os.getpid()}.csv")
    try:
        with open(path, "w", newline="") as f:
            csv.writer(f, lineterminator="\n").writerows(rows)
        cols = ", ".join(f"'{n}': '{t}'" for n, t in columns)
        con.execute(f"INSERT INTO {table} SELECT * FROM read_csv('{path}', header = false, "
                    f"auto_detect = false, columns = {{{cols}}})")
    finally:
        os.unlink(path)


# ---- tags, identities, URLs (Python) -----------------------------------------------------------
def tags_named(tags, name):
    return [t for t in tags if t and t[0] == name]


def first_value(tags, name):
    """tag[1] of the FIRST `name` tag; None when there is none or it carries no value."""
    for t in tags:
        if t and t[0] == name:
            return t[1] if len(t) > 1 else None
    return None


def all_values(tags, name):
    """Section 2.1: values of ALL `name` tags, trimmed, empties dropped, in order, deduplicated."""
    out = []
    for t in tags_named(tags, name):
        for v in t[1:]:
            v = (v or "").strip()
            if v and v not in out:
                out.append(v)
    return out


B32 = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"


def npub_hex(s):
    """BIP-173 bech32 decode of an npub to 64 lowercase hex; None on any failure."""
    data = s[5:]
    if s != s.lower() or not s.startswith("npub1") or len(data) < 6 or any(c not in B32 for c in data):
        return None
    vals = [B32.index(c) for c in data]
    chk = 1
    for v in [ord(c) >> 5 for c in "npub"] + [0] + [ord(c) & 31 for c in "npub"] + vals:
        top, chk = chk >> 25, (chk & 0x1ffffff) << 5 ^ v
        for i, g in enumerate((0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3)):
            chk ^= g if (top >> i) & 1 else 0
    if chk != 1:
        return None
    acc = bits = 0
    out = []
    for v in vals[:-6]:
        acc, bits = (acc << 5) | v, bits + 5
        if bits >= 8:
            bits -= 8
            out.append((acc >> bits) & 0xff)
    if bits >= 5 or (acc << (8 - bits)) & 0xff:
        return None
    return bytes(out).hex() if len(out) == 32 else None


HEX64 = re.compile(r"[0-9a-fA-F]{64}")
HEX40 = re.compile(r"[0-9a-f]{40}")


def norm_pubkey(v):
    """Section 2.1: 64-hex lowercased, npub1... decoded, anything else dropped (None)."""
    if v and HEX64.fullmatch(v):
        return v.lower()
    return npub_hex(v) if v and v.startswith("npub1") else None


def role_active(t):
    return len(t) < 4 or len(t) % 2 == 1


def maintainer_listing(tags, own):
    """Section 2.1: active M/m values when any M/m/o tag exists, else all maintainers values;
    normalized; the announcement's own pubkey removed."""
    if any(t and t[0] in ("M", "m", "o") for t in tags):
        raw = [v for t in tags if t and t[0] in ("M", "m") and role_active(t) for v in t[1:]]
    else:
        raw = [v for t in tags_named(tags, "maintainers") for v in t[1:]]
    return {p for p in map(norm_pubkey, raw) if p} - {own}


SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://")
SCP = re.compile(r"[^@/\s]+@([^:/\s]+):")
PORT = re.compile(r":[0-9]*\Z")
GRASP_PATH = re.compile(r"/npub1[02-9ac-hj-np-z]{58}/")
IPV4 = re.compile(r"([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})")
NIP_IO = re.compile(r"(?:.*\.)?([0-9]{1,3})[.-]([0-9]{1,3})[.-]([0-9]{1,3})[.-]([0-9]{1,3})\.(?:nip|sslip)\.io")


def parse_url(u):
    """Section 4.1: (scheme or None, scp_like, host or None, grasp_shaped)."""
    m = SCHEME.match(u)
    if m:
        scheme, rest = u[:m.end() - 3].lower(), u[m.end():]
        cut = re.search(r"[/?#]", rest)
        auth, tail = (rest[:cut.start()], rest[cut.start():]) if cut else (rest, "")
        hostport = auth.rpartition("@")[2]
        pm = PORT.search(hostport)
        host = hostport[:pm.start()] if pm else hostport
        shaped = scheme in ("http", "https", "ws", "wss") and GRASP_PATH.match(tail) is not None
        return scheme, False, host.lower() or None, shaped
    m = SCP.match(u)
    return (None, True, m.group(1).lower(), False) if m else (None, False, None, False)


def is_private(h):
    """Section 4.2's private-host test: localhost, 0.0.0.0, a .local name, or an IPv4 in a private
    range, dotted, or embedded in a nip.io / sslip.io name (the four octets right before the
    suffix, `.` or `-` separated, starting the name or after a `.`)."""
    if h in ("localhost", "0.0.0.0") or h.endswith(".local"):
        return True
    m = IPV4.fullmatch(h) or NIP_IO.fullmatch(h)
    if not m or any(int(x) > 255 for x in m.groups()):
        return False
    a, b = int(m.group(1)), int(m.group(2))
    return a in (10, 127) or (a == 172 and 16 <= b <= 31) or (a == 192 and b == 168) or (a == 100 and 64 <= b <= 127)


def host_class(parsed, grasp_hosts):
    """Section 4.2, in its precedence."""
    scheme, scp, host, _ = parsed
    if scheme == "htree":
        return "hashtree"
    if host is not None and is_private(host):
        return "private"
    if host is not None and host in grasp_hosts:
        return "grasp"
    if host in FORGES:
        return "forge"
    if host is None or (scheme not in NET_SCHEMES and not scp):
        return "non_network"
    return "self_hosted"


def hosting_class(classes):
    """Section 4.4 over the reachable host classes R."""
    r = set(classes) & REACHABLE
    if "grasp" in r:
        return "grasp_only" if r == {"grasp"} else "grasp_mixed"
    if "hashtree" in r:
        return "hashtree"
    if r == {"forge"}:
        return "forge_only"
    return "other_hosts" if "self_hosted" in r else "none"


def norm_relay(v):
    """Section 4.5 (and 5.2): lowercase, a leading ws:// or wss:// stripped, trailing / stripped."""
    v = v.lower()
    for p in ("ws://", "wss://"):
        if v.startswith(p):
            v = v[len(p):]
            break
    return v.rstrip("/")


# ---- announcements, the noise ledger, repositories ---------------------------------------------
class Coord:
    """One kind-30617 (repo-announcements) coordinate, attributes read from its current version."""

    def __init__(self, cid, pk, d, versions, first_seen, last_seen, med_gap, tags):
        tags = [list(t) for t in (tags or []) if t]
        self.cid, self.pk, self.d, self.tags = cid, pk, d, tags
        self.versions, self.first_seen, self.last_seen, self.med_gap = versions, first_seen, last_seen, med_gap
        self.name = first_value(tags, "name") or ""
        self.clone, self.web, self.relays = (all_values(tags, n) for n in ("clone", "web", "relays"))
        self.t = [(v or "").strip().lower() for t in tags_named(tags, "t") for v in t[1:]]
        self.client, self.alt = first_value(tags, "client"), first_value(tags, "alt")
        rs = tags_named(tags, "r")
        self.euc = next((t[1] for t in rs if len(t) >= 3 and t[2] == "euc" and t[1] and HEX40.fullmatch(t[1])),
                        next((t[1] for t in rs if len(t) == 2 and t[1] and HEX40.fullmatch(t[1])), None))
        self.listing = maintainer_listing(tags, pk)
        self.self_lead = any(t[0] == "M" and role_active(t) and len(t) > 1 and norm_pubkey(t[1]) == pk
                             for t in tags)
        self.parsed = [parse_url(v) for v in self.clone]
        self.family = self.repo = None

    def ledger_family(self, has_state, deletion_at):
        """Section 2.2, first match wins."""
        if len(self.d) > 200 or re.search(r"[\x00-\x1f\x7f]", self.d):
            return "N0"
        if any(v.startswith("https://nostr.life/") for v in self.clone + self.web):
            return "N1"
        if any("@example.com:" in v for v in self.clone) or tags_named(self.tags, "euc"):
            return "N2"
        if self.name == "<human-readable project name>":
            return "N3"
        if re.fullmatch(r"20[0-9]{6}", self.d):
            return "N4"
        if self.versions >= 1000 and self.med_gap is not None and self.med_gap < 300:
            return "N5"
        if (any(t[:2] in (["deleted", "true"], ["status", "deleted"]) for t in self.tags)
                or self.name.lower() == "deleted" or (deletion_at is not None and deletion_at >= self.last_seen)):
            return "N6"
        if self.pk == G_KEY:
            return "N7"
        hosts = [p[2] for p in self.parsed]
        if hosts and all(h is not None and is_private(h) for h in hosts):
            return "N8a"
        if not has_state and not any(host_class(p, frozenset()) in REACHABLE for p in self.parsed):
            return "N8b"  # no reachable clone value, or none at all; classified without the GRASP set
        if "personal-fork" in self.t and not has_state:
            return "N9"
        if self.pk == CATALOG_KEY:
            return "N10"
        return "repo"

    def tool_rule(self):
        """Section 4.5 tools, first match: (label, None) or (None, raw client value)."""
        if self.client is not None and self.client.strip():  # a client blank after trimming is no client
            low = self.client.lower()
            if "shakespeare" in low:
                return "Shakespeare", None
            return ("NostrHub", None) if "nostrhub" in low else (None, low)
        if "shakespeare" in self.t:
            return "Shakespeare", None
        if any(p[0] == "htree" for p in self.parsed):
            return "hashtree", None
        if self.alt is not None and self.alt.startswith("git repository:"):
            return "ngit / GitWorkshop (inferred)", None
        if any(t[0] and t[0].startswith("refs/heads/") for t in self.tags):
            return "git-sync mirror (refs in announcement)", None
        return "unknown", None


def components(nodes, edges):
    """Connected components by depth-first search; `nodes` fixes the discovery order."""
    seen, out = set(), []
    for n in nodes:
        if n in seen:
            continue
        seen.add(n)
        stack, comp = [n], []
        while stack:
            x = stack.pop()
            comp.append(x)
            for y in sorted(edges.get(x, ())):
                if y not in seen:
                    seen.add(y)
                    stack.append(y)
        out.append(comp)
    return out


class Repo:
    """Section 2.3: a component of reciprocally joined kept announcements, and its lead."""

    def __init__(self, rk, members):
        leads = [m for m in members if m.self_lead]
        self.rk, self.lead = rk, leads[0] if len(leads) == 1 else min(members, key=lambda m: (m.first_seen, m.pk))
        self.members = [self.lead] + sorted((m for m in members if m is not self.lead), key=lambda m: m.pk)
        self.key = f"{self.lead.pk}:{self.lead.d}"
        self.first_seen = min(m.first_seen for m in members)
        self.parsed = list(dict.fromkeys(p for m in self.members for p in m.parsed))
        self.euc = self.lead.euc or next((m.euc for m in self.members[1:] if m.euc), None)
        self.last_state = self.last_push = None
        self.pushes = {"total": 0, "30d": 0, "90d": 0}


# ---- the push ledger walk (Python) -------------------------------------------------------------
def ref_map(tags):
    """Section 3: refs/heads/ and refs/tags/ tags with a value and no ^{} suffix -> value; first wins."""
    m = {}
    for t in tags:
        n = t[0] if len(t) > 1 else None
        if n and n.startswith(("refs/heads/", "refs/tags/")) and not n.endswith("^{}") and n not in m:
            m[n] = t[1]
    return m


def walk_coordinate(rows):
    """One member coordinate's state rows in version order -> the section 3 flags and counts."""
    out, prev = [], None
    for event_id, created_at, tags in rows:
        tags = [list(t) for t in (tags or []) if t]
        refs, head = ref_map(tags), first_value(tags, "HEAD")
        heads = {n for n in refs if n.startswith("refs/heads/")}
        tagn = {n for n in refs if n.startswith("refs/tags/")}
        f = {"event_id": event_id, "created_at": created_at, "branches": len(heads), "tags": len(tagn)}
        if prev is None:
            f.update(changed=True, head_changed=False, heads_moved=0, default_moved=False,
                     branches_added=len(heads), branches_removed=0, tags_added=len(tagn), tags_removed=0, tags_moved=0)
        else:
            p_refs, p_head, p_heads, p_tagn = prev
            moved = {n for n in refs.keys() & p_refs.keys() if refs[n] != p_refs[n]}
            f.update(changed=refs != p_refs, head_changed=head != p_head, heads_moved=len(moved & heads),
                     default_moved=bool(head and head.startswith("ref: refs/heads/") and head[5:] in moved),
                     branches_added=len(heads - p_heads), branches_removed=len(p_heads - heads),
                     tags_added=len(tagn - p_tagn), tags_removed=len(p_tagn - tagn), tags_moved=len(moved & tagn))
        f["push"] = f["changed"] and created_at >= STATE_WALL
        out.append(f)
        prev = (refs, head, heads, tagn)
    return out


PUSH_FLAGS = ["changed", "push", "head_changed", "heads_moved", "default_moved", "branches_added",
              "branches_removed", "tags_added", "tags_removed", "tags_moved", "branches", "tags"]
# section 3 (2026-10-12): a hashtree push's flags, in PUSH_FLAGS order: a push that moves no ref the
# ledger can read, with no branch or tag count; and the two values of a ledger row's source
HTREE_FLAGS = [True, True, False, 0, False, 0, 0, 0, 0, 0, None, None]
SOURCES = ("state", "htree_announcement")
# section 3: a hashtree-class coordinate's announcement version less than this many seconds after
# the version before it belongs to that version's push (hashtree republishes once per changed ref)
HTREE_PUSH_GAP_S = 120


# ---- the derivation ----------------------------------------------------------------------------
def weekly(wall, asof, by_week, fields):
    """The weekly series' shape (sections 3, 5.2, 6.1, 7): one row {w, <fields>} per ISO week
    from the week holding the `wall` instant to the week of as_of_day (`asof`, a day number),
    labelled by its Monday's date and zero-filled. `by_week` maps a week's Monday (a day number)
    to its values: a row dated in the start week counts on either side of the wall instant, one
    dated before that week's Monday is in no week."""
    return [{"w": day_iso(m), **dict(zip(fields, by_week.get(m, [0] * len(fields))))}
            for m in range(week_of(wall // DAY), week_of(asof) + 1, 7)]


def star_pairs(con, iris):
    """Section 6.2 over load_inputs' tables (rx, xr, ann, coords) and the member table mem: the
    distinct (pubkey, rk) pairs (rk: the repository's index) of the kind-7 (reactions) stars and
    of the kind-17 (external-content-reactions) stars, each on a member coordinate. The second
    set is empty under the kind-7 (reactions) rule alone (iris False), which reads no xr."""
    # kind-7 (reactions): a `+` or "" row (untrimmed); its target a 30617: `a` value, else the
    # first `e` looked up among the ids of ALL surviving announcement versions
    rx_pairs = set(con.execute(f"""
        WITH s AS (SELECT pubkey, {FIRST_A.format(n='a')} AS coord, list_filter(tags, t -> t[1] = 'e')[1][2] AS e
                   FROM rx WHERE content IN ('+', '')),
        tgt AS (SELECT s.pubkey, CASE WHEN s.coord IS NOT NULL THEN c.cid ELSE byid.cid END AS cid
                FROM s {COORD_JOIN.format(x='s')}
                LEFT JOIN (SELECT a.id, k.cid FROM ann a JOIN coords k USING (pubkey, d)) byid
                       ON s.coord IS NULL AND byid.id = s.e)
        SELECT DISTINCT tgt.pubkey, m.rk FROM tgt JOIN mem m USING (cid)""").fetchall())
    if not iris:
        return rx_pairs, set()
    # kind-17 (external-content-reactions), section 6.2: the target is the FIRST `i` value starting
    # 30617:, `d` everything after its second colon (colons kept); a target without a second colon
    # drops the row from the stars, with no fall-through to a later `i` value (it stays in the
    # input: load_inputs counted it); per (pubkey, target) the LAST row in section 1's version
    # order (created_at ascending, id descending: among equal created_at the lowest id) is the
    # key's current reaction, whatever its content, so a later `-` withdraws; each coordinate maps
    # to its repository, so one key's stars of two member coordinates of a repository are one pair
    current = con.execute(f"""
        WITH x AS (SELECT pubkey, id, created_at, content, {FIRST_A.format(n='i')} AS coord FROM xr),
        cur AS (SELECT * FROM x WHERE coord LIKE '30617:%:%'
                QUALIFY row_number() OVER (PARTITION BY pubkey, coord ORDER BY created_at DESC, id) = 1)
        SELECT cur.pubkey, m.rk, cur.content FROM cur {COORD_JOIN.format(x='cur')} JOIN mem m ON m.cid = c.cid
        """).fetchall()
    # a star when that reaction's content, trimmed with Python's str.strip() (section 6.2, as 4.1 pins
    # for clone values), is `+` or empty; anything else is not, Iris Git's unstar `-` included
    return rx_pairs, {(pk, rk) for pk, rk, content in current if content is not None and content.strip() in ("+", "")}


def derive(con, src, now_ts, cutoff, workdir, log=lambda s: print(s, file=sys.stderr), heavy=None, *, signers,
           iris=True, archive=None):
    """The whole derivation; `iris` picks section 6.2's stars rule (STARS: True for kind-7
    (reactions) and kind-17 (external-content-reactions) stars and the four keys, False for
    kind-7 (reactions) stars alone), `archive` is the archive's root for --s3."""
    t0 = time.time()
    asof = (now_ts - DAY) // DAY  # as_of_day as a UTC day number; an "N d" window is [asof-N+1, asof]
    w = lambda n: f"day BETWEEN {asof - n + 1} AND {asof}"  # noqa: E731
    events, anomalies = load_inputs(con, src, now_ts, (asof + 1) * DAY - 1, cutoff, log, heavy, iris, archive)

    # section 2: attributes, the noise ledger, components and leads
    coords = [Coord(*r) for r in con.execute(
        "SELECT a.cid, c.pubkey, c.d, a.versions, a.first_seen, a.last_seen, a.med_gap, a.cur_tags "
        "FROM ann_c a JOIN coords c USING (cid) ORDER BY c.cid").fetchall()]
    st_stats = {cid: (n, med) for cid, n, med in con.execute("SELECT cid, versions, med_gap FROM st_c").fetchall()}
    deletions = {(pk, coord): at for pk, coord, at in con.execute("SELECT * FROM del").fetchall()}
    for c in coords:
        c.family = c.ledger_family(c.cid in st_stats, deletions.get((c.pk, f"30617:{c.pk}:{c.d}")))
    kept = {(c.pk, c.d): c for c in coords if c.family == "repo"}
    fam = Counter(c.family for c in coords)
    log(f"[ledger] {len(coords)} coordinates, {len(kept)} kept: " + " ".join(f"{k}={fam[k]}" for k in FAMILIES))
    recip, oneway = defaultdict(set), defaultdict(set)  # reciprocal = repositories; one-way = shared_listed_repos
    for (pk, d), a in kept.items():
        for other in a.listing:
            b = kept.get((other, d))
            if b is None:
                continue
            for graph in (oneway, recip) if pk in b.listing else (oneway,):
                graph[a.cid].add(b.cid)
                graph[b.cid].add(a.cid)
    by_cid = {c.cid: c for c in kept.values()}
    repos = [Repo(i, [by_cid[x] for x in comp]) for i, comp in enumerate(components(sorted(by_cid), recip))]
    for r in repos:
        for m in r.members:
            m.repo = r

    # section 4: the GRASP host set, then repository classes
    grasp_hosts = {p[2] for c in kept.values() for p in c.parsed if p[3] and p[2] and "." in p[2]
                   and not is_private(p[2]) and p[2] not in FORGES and p[2] not in GRASP_DENY}
    per_repo_hosts = Counter()
    for r in repos:
        r.cls = hosting_class(host_class(p, grasp_hosts) for p in r.parsed)
        r.grasp = r.cls in ("grasp_only", "grasp_mixed")
        if r.grasp:
            n = len({p[2] for p in r.parsed if p[2] in grasp_hosts})  # any URL shape
            per_repo_hosts["4+" if n >= 4 else str(n)] += 1
    client_freq = Counter(r.lead.tool_rule()[1] for r in repos if r.lead.tool_rule()[1] is not None)
    top_clients = {v for v, _ in sorted(client_freq.items(), key=lambda kv: (-kv[1], kv[0]))[:5]}

    def tool(c):
        label, client = c.tool_rule()
        return label if label is not None else (client if client in top_clients else "other client")

    # section 3: the push ledger over member coordinates, minus republish loops
    hand_off(con, "mem", [("cid", "BIGINT"), ("rk", "BIGINT")], [(m.cid, r.rk) for r in repos for m in r.members],
             workdir)
    loops = {cid for cid, (n, med) in st_stats.items() if n >= 1000 and med is not None and med < 120}
    n_loops, n_orphans = len(loops & set(by_cid)), len(set(st_stats) - set(by_cid))
    rows = con.execute("SELECT c.cid, s.id, s.created_at, s.tags FROM st s JOIN coords c USING (pubkey, d) "
                       "JOIN mem USING (cid) ORDER BY c.cid, s.created_at, s.id DESC").fetchall()
    ledger, run = [], []
    for i, row in enumerate(rows):
        run.append(row[1:])
        if i + 1 < len(rows) and rows[i + 1][0] == row[0]:
            continue
        if row[0] not in loops:
            ledger += [[f["event_id"], row[0], by_cid[row[0]].repo.rk, f["created_at"]] + [f[k] for k in PUSH_FLAGS]
                       + ["state"] for f in walk_coordinate(run)]
        run = []
    # section 3's hashtree pushes: a member coordinate's kind-30617 (repo-announcements) versions, in
    # version order (created_at ascending, id descending), in groups - a version less than
    # HTREE_PUSH_GAP_S after the one before joins its group - one push per group, at its first
    # version, but the coordinate's first group and any group starting before STATE_WALL; at a
    # repository whose class is hashtree
    htree = {r.rk for r in repos if r.cls == "hashtree"}
    for cid, rk, eid, at in con.execute(f"""
            SELECT cid, rk, id, created_at FROM (
                SELECT cid, rk, id, created_at, starts,
                       sum(starts) OVER (PARTITION BY cid ORDER BY created_at, id DESC
                                         ROWS UNBOUNDED PRECEDING) AS grp
                FROM (
                    SELECT m.cid, m.rk, a.id, a.created_at,
                           CASE WHEN lag(a.created_at) OVER w IS NULL
                                  OR a.created_at - lag(a.created_at) OVER w >= {HTREE_PUSH_GAP_S}
                                THEN 1 ELSE 0 END AS starts
                    FROM ann a JOIN coords c USING (pubkey, d) JOIN mem m USING (cid)
                    WINDOW w AS (PARTITION BY m.cid ORDER BY a.created_at, a.id DESC)))
            WHERE starts = 1 AND grp > 1 AND created_at >= {STATE_WALL}
            ORDER BY cid, created_at, id DESC""").fetchall():
        if rk in htree:
            ledger.append([eid, cid, rk, at] + HTREE_FLAGS + ["htree_announcement"])
    hand_off(con, "led", [("event_id", "VARCHAR"), ("cid", "BIGINT"), ("rk", "BIGINT"), ("created_at", "BIGINT")] +
             [(k, "BOOLEAN" if k in ("changed", "push", "head_changed", "default_moved") else "INTEGER")
              for k in PUSH_FLAGS] + [("source", "VARCHAR")], ledger, workdir)
    # every push row counts: no daily cap (and no `counted`) since the nostrology-data deploy of 2026-10-02
    con.execute(f"""CREATE OR REPLACE TEMP TABLE led2 AS
        SELECT l.*, c.pubkey, c.d, l.created_at // {DAY} AS day FROM led l JOIN coords c USING (cid)""")
    # last_state is the newest STATE row's (a hashtree push is no state); the pushes count both sources
    for rk, last_state, last_push, total, p30, p90 in con.execute(f"""
            SELECT rk, max(created_at) FILTER (WHERE source = 'state'), max(created_at) FILTER (WHERE push),
                   count(*) FILTER (WHERE push),
                   count(*) FILTER (WHERE push AND {w(30)}), count(*) FILTER (WHERE push AND {w(90)})
            FROM led2 GROUP BY rk""").fetchall():
        repos[rk].last_state, repos[rk].last_push = last_state, last_push
        repos[rk].pushes = {"total": total, "30d": p30, "90d": p90}
    ps = con.execute(f"""SELECT count(*) FILTER (WHERE push), count(*) FILTER (WHERE push AND {w(7)}),
            count(*) FILTER (WHERE push AND {w(30)}), count(DISTINCT rk) FILTER (WHERE push AND {w(30)}),
            count(DISTINCT rk) FILTER (WHERE push AND {w(90)}), count(DISTINCT pubkey) FILTER (WHERE push AND {w(30)}),
            max(created_at) FILTER (WHERE source = 'state'), max(created_at) FILTER (WHERE push),
            count(*) FILTER (WHERE push AND source = 'htree_announcement' AND {w(30)})
        FROM led2""").fetchone()
    # pushes_weekly from STATE_WALL's week: a push is dated on or after the wall instant, so every one is in a week
    pushes_weekly = weekly(STATE_WALL, asof, {wk: n for wk, *n in con.execute(f"""
        SELECT {WEEK} AS wk, count(*), count(DISTINCT rk), count(DISTINCT pubkey) FROM led2 WHERE push GROUP BY wk
        """).fetchall()}, ["pushes", "repos", "maintainers"])
    log(f"[pushes] {len(ledger)} ledger rows, {ps[0]} pushes, {n_loops} loops, {n_orphans} orphan state coordinates")

    # section 5.2: current kind-10317 (grasp-server-lists) lists with a valid `g` value; such a
    # user's week (grasp_lists.per_week) is the week of its list's earliest surviving version
    grasp_users, gl_weeks = 0, Counter()
    lists = con.execute("SELECT tags, min(created_at) OVER (PARTITION BY pubkey) FROM gl QUALIFY row_number() "
                        "OVER (PARTITION BY pubkey ORDER BY created_at DESC, id) = 1").fetchall()
    for tags, first in lists:  # the current version's tags; the created_at of the user's earliest version
        hosts = [norm_relay(t[1]).split("/")[0] for t in (tags or []) if t and t[0] == "g" and len(t) > 1 and t[1]]
        if any("." in h and not h.endswith("example.com") for h in hosts):
            grasp_users += 1
            gl_weeks[week_of(first // DAY)] += 1

    # section 6.1: the comment ledger and attribution; the root reads the first `K` value
    con.execute(r"""CREATE OR REPLACE TEMP TABLE cm_x AS
        SELECT id, pubkey, created_at // 86400 AS day, list_filter(tags, t -> t[1] = 'E')[1][2] AS thread,
               coalesce(""" + FIRST_A.format(n="A") + ", " + FIRST_A.format(n="a") + r""") AS coord,
               CASE list_filter(tags, t -> t[1] = 'K')[1][2] WHEN '1621' THEN 'issues' WHEN '1618' THEN 'prs'
                    WHEN '1619' THEN 'prs' WHEN '1617' THEN 'patches' WHEN '30617' THEN 'repo'
                    WHEN '30618' THEN 'repo' ELSE 'other' END AS root,
               len(list_filter(tags, t -> t[1] = 'expiration')) > 0 AS eph,
               len(list_filter(tags, t -> t[1] = 'imported')) > 0 AS imp,
               coalesce(regexp_matches(content, '^\*\*@[^*]+\*\*'), false) AS foreign_line
        FROM cm""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cm_y AS
        SELECT x.*, m.rk, CASE WHEN x.eph THEN 'ephemeral' WHEN x.imp THEN 'imported'
                               WHEN mk.pubkey IS NOT NULL THEN 'mirrored' ELSE 'counted' END AS ledger
        FROM cm_x x
        LEFT JOIN (SELECT pubkey FROM cm_x GROUP BY pubkey HAVING count(*) >= 50
                   AND 10 * count(*) FILTER (WHERE foreign_line) > 9 * count(*)) mk ON mk.pubkey = x.pubkey
        {COORD_JOIN.format(x='x')} LEFT JOIN mem m ON m.cid = c.cid""")
    cm = dict(zip(("total", "30d", "90d", "threads", "people", "attributable", "ephemeral", "imported",
                   "mirrored"),
                  con.execute(f"""SELECT count(*) FILTER (WHERE ledger = 'counted'),
            count(*) FILTER (WHERE ledger = 'counted' AND {w(30)}),
            count(*) FILTER (WHERE ledger = 'counted' AND {w(90)}),
            count(DISTINCT thread) FILTER (WHERE ledger = 'counted'),
            count(DISTINCT pubkey) FILTER (WHERE ledger = 'counted'),
            count(*) FILTER (WHERE ledger = 'counted' AND rk IS NOT NULL),
            count(*) FILTER (WHERE ledger = 'ephemeral'), count(*) FILTER (WHERE ledger = 'imported'),
            count(*) FILTER (WHERE ledger = 'mirrored') FROM cm_y""").fetchone()))
    # comments_weekly from COMMENT_WALL's week, counted comments only; authors / threads distinct within the week
    comments_weekly = weekly(COMMENT_WALL, asof, {wk: n for wk, *n in con.execute(f"""
        SELECT {WEEK} AS wk, {', '.join(f"count(*) FILTER (WHERE root = '{r}')" for r in ROOTS)},
               count(DISTINCT pubkey), count(DISTINCT thread)
        FROM cm_y WHERE ledger = 'counted' GROUP BY wk""").fetchall()}, ROOTS + ["authors", "threads"])

    # section 6.2: the stars of both kinds as (pubkey, repository) pairs, so a key that starred one
    # repository from both clients is one star; a kind-17 (external-content-reactions) `-` leaves
    # that key's kind-7 (reactions) star on the repository standing (it is in the other set)
    rx_pairs, xr_pairs = star_pairs(con, iris)
    starred = rx_pairs | xr_pairs
    stars_total, stars_repos = len(starred), len({rk for _, rk in starred})
    log(f"[stars] {len(rx_pairs)} kind-7 (reactions) pairs, " + (
        f"{len(xr_pairs)} kind-17 (external-content-reactions) pairs, {stars_total} distinct" if iris else
        "the kind-7 (reactions) rule alone") + f", {stars_repos} repositories")

    # section 6.3: zaps (amounts through the shared zap_parser)
    spec = importlib.util.spec_from_file_location("zap_parser", ZAP_PARSER)
    zap_parser = sys.modules.setdefault("zap_parser", importlib.util.module_from_spec(spec))
    spec.loader.exec_module(zap_parser)  # registered first: its @dataclass resolves the module by name
    members = {r.rk: {m.pk for m in r.members} for r in repos}
    zap = {"receipts": 0, "msats": 0, "zappers": set(), "repos": set()}
    for rk, signer, tags in con.execute(f"""SELECT m.rk, z.pubkey, z.tags
            FROM (SELECT pubkey, tags, {FIRST_A.format(n='a')} AS coord FROM zp) z
            {COORD_JOIN.format(x='z')} JOIN mem m ON m.cid = c.cid""").fetchall():
        tags = [t for t in tags or [] if t]
        if signer not in signers or first_value(tags, "p") not in members[rk]:
            continue  # not signed by a listed LNURL provider, or its FIRST `p` is not a member pubkey
        last = {t[0]: t[1] for t in tags if len(t) > 1}  # the LAST tag of a name that has a value
        try:  # any error while reading the amount: no readable amount
            bolt11 = last.get("bolt11")
            amount = zap_parser._parse_bolt11_amount(bolt11) if bolt11 and len(bolt11) <= 2048 else None
        except Exception:
            amount = None
        if amount is None or amount > MAX_ZAP:
            continue
        request = None
        try:  # any error while parsing: the request does not parse, the receipt still counts
            desc = last.get("description") or ""
            parsed = json.loads(desc) if len(desc) <= 65536 else None
            if isinstance(parsed, dict) and isinstance(parsed.get("pubkey"), str) and parsed["pubkey"]:
                request = parsed
        except Exception:
            pass
        rtags = request.get("tags") if request is not None else None
        asked = first_value([t for t in rtags if isinstance(t, list)], "amount") if isinstance(rtags, list) else None
        if asked not in (None, "") and asked != str(amount):
            continue  # NIP-57 (lightning zaps) appendix F: the invoice must be for the requested amount
        zap["receipts"], zap["msats"] = zap["receipts"] + 1, zap["msats"] + min(amount, CAP_ZAP)
        zap["repos"].add(rk)
        if request is not None:
            zap["zappers"].add(request["pubkey"])

    # the summary block, the ledger and the partitions
    recency = Counter()
    for r in repos:
        if r.last_state is not None:
            age = max(0, asof - r.last_state // DAY)  # as stated; the as_of_end horizon keeps age >= 0
            recency[next(k for k, lo, hi in RECENCY if lo <= age <= hi)] += 1
    hosting = Counter(r.cls for r in repos)
    no_state = Counter(r.cls for r in repos if r.last_state is None)
    grasp_repos = hosting["grasp_only"] + hosting["grasp_mixed"]
    # section 7's new_repos_weekly: repositories by the week of first_seen, split by today's class;
    # s = maintainers by the week of their earliest KEPT announcement's first-seen
    first_ann, new_repos = {}, defaultdict(lambda: [0, 0, 0])  # week -> [grasp, other, s]
    # by_host: week label -> its repositories once under every kind in their R (section 4.4: the
    # reachable host classes over the clone values), `none` when R is empty; R is read here, apart
    # from r.cls and new_repos, so the identities the self-test holds (grasp's v = grasp; the
    # entries sum to at least v) are a check
    new_hosts = defaultdict(Counter)
    for c in kept.values():
        first_ann[c.pk] = min(first_ann.get(c.pk, c.first_seen), c.first_seen)
    for r in repos:
        wk = week_of(r.first_seen // DAY)
        new_repos[wk][0 if r.grasp else 1] += 1
        new_hosts[day_iso(wk)].update({host_class(p, grasp_hosts) for p in r.parsed} & REACHABLE or {"none"})
    for first in first_ann.values():
        new_repos[week_of(first // DAY)][2] += 1
    new_repos_weekly = weekly(ANN_WALL, asof, {wk: [g + o, g, o, s] for wk, (g, o, s) in new_repos.items()},
                              ["v", "grasp", "other", "s"])
    for row in new_repos_weekly:  # every kind, in section 7's order, zeros included
        row["by_host"] = [{"k": k, "v": new_hosts[row["w"]][k]} for k in HOST_KINDS]
    summary = {
        "archive_events": events, "announcements_total": len(coords), "announcements_kept": len(kept),
        "repos": len(repos), "forks": sum(1 for r in repos if tags_named(r.lead.tags, "u") or "personal-fork" in r.lead.t),
        "repos_with_state": sum(1 for r in repos if r.last_state is not None), "repos_pushed_30d": ps[3],
        "repos_pushed_90d": ps[4], "pushes_7d": ps[1], "pushes_30d": ps[2], "pushes_total": ps[0],
        "pushes_hashtree_30d": ps[8],
        "maintainers": len({c.pk for c in kept.values()}), "maintainers_active_30d": ps[5],
        "multi_maintainer_repos": sum(1 for r in repos if len(r.members) > 1),
        "shared_listed_repos": sum(1 for comp in components(sorted(by_cid), oneway) if len(comp) > 1),
        "fork_families": sum(1 for n in Counter(r.euc for r in repos if r.euc).values() if n > 1),
        "grasp_repos": grasp_repos, "grasp_share": grasp_repos / len(repos) if repos else 0,
        "grasp_servers": len(grasp_hosts), "grasp_list_users": grasp_users,
        "comments_total": cm["total"], "comments_30d": cm["30d"], "comments_90d": cm["90d"],
        "comment_threads": cm["threads"], "comment_participants": cm["people"],
        "comments_attributable_share": cm["attributable"] / cm["total"] if cm["total"] else 0,
        "stars_total": stars_total, "stars_repos": stars_repos, **({"stars_iris": len(xr_pairs)} if iris else {}),
        "zap_receipts": zap["receipts"],
        "zap_msats": zap["msats"], "zappers": len(zap["zappers"]), "zaps_repos": len(zap["repos"]),
        "first_announcement": day_iso(min(r.first_seen for r in repos) // DAY) if repos else None,
        "newest_state": day_iso(ps[6] // DAY) if ps[6] is not None else None,
        "newest_push": day_iso(ps[7] // DAY) if ps[7] is not None else None}
    doc = dict(summary, generated_utc=dt.datetime.fromtimestamp(now_ts, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"),
               as_of_day=day_iso(asof),
               exclusions=[{"k": f, "unit": "announcements", "v": fam[f]} for f in FAMILIES] + [
                   {"k": "state_republish_loop", "unit": "state coordinates", "v": n_loops},
                   {"k": "orphan_states", "unit": "state coordinates", "v": n_orphans}] + [
                   {"k": k, "unit": "comments", "v": cm[k]} for k in ("ephemeral", "imported", "mirrored")],
               hosting=[{"k": k, "v": hosting[k]} for k in HOSTING],
               recency=[{"k": k, "v": recency[k]} for k, _, _ in RECENCY],
               no_state={"total": sum(no_state.values()), "by_class": [{"k": k, "v": no_state[k]} for k in HOSTING]},
               grasp_per_repo=[{"k": k, "v": per_repo_hosts[k]} for k in ("1", "2", "3", "4+")],
               anomalies=anomalies, pushes_weekly=pushes_weekly, new_repos_weekly=new_repos_weekly,
               comments_weekly=comments_weekly,
               grasp_lists={"per_week": weekly(STATE_WALL, asof, {wk: [n] for wk, n in gl_weeks.items()}, ["v"])})

    # the substrate rows (section 9)
    def clip(s, n):  # section 9: control and format characters (Unicode Cc, Cf) removed, then clipped
        return "".join(ch for ch in s if unicodedata.category(ch) not in ("Cc", "Cf"))[:n]

    repo_rows = []
    for c in (c for c in coords if c.family != "N0"):  # N0 coordinates have no substrate row
        r = c.repo
        hosts = list(dict.fromkeys((p[2], host_class(p, grasp_hosts)) for p in c.parsed if p[2]))[:20]
        repo_rows.append({
            "pubkey": c.pk, "d": c.d, "family": c.family, "repo_key": r and r.key,
            "is_lead": r is not None and r.lead is c, "name": clip(c.name, 80), "first_seen": c.first_seen,
            "last_seen": c.last_seen, "versions": c.versions, "cls": r and r.cls, "grasp": r and r.grasp,
            "hosts": json.dumps([{"host": h, "cls": k} for h, k in hosts]),
            "relays": json.dumps(list(dict.fromkeys(x for x in map(norm_relay, c.relays) if "." in x))[:20]),
            "euc": c.euc, "tool": tool(c), "has_state": c.cid in st_stats,
            "last_state": r and r.last_state, "last_push": r and r.last_push,
            **{f"pushes_{k}": r.pushes[k] if r else 0 for k in ("total", "30d", "90d")}})
    repo_rows.sort(key=lambda x: (x["pubkey"], x["d"]))
    push_rows = [dict(zip(["event_id", "pubkey", "d", "rk", "created_at"] + PUSH_FLAGS + ["source"], row))
                 for row in con.execute("SELECT event_id, pubkey, d, rk, created_at, "
                                        + ", ".join(PUSH_FLAGS) + ", source FROM led2").fetchall()]
    for p in push_rows:
        p["repo_key"] = repos[p.pop("rk")].key
    push_rows.sort(key=lambda p: (p["repo_key"], p["created_at"], p["event_id"]))
    log(f"[done] derivation {time.time() - t0:.1f}s")
    return repo_rows, push_rows, doc


REPO_COLS = ["pubkey", "d", "family", "repo_key", "is_lead", "name", "first_seen", "last_seen", "versions", "cls",
             "grasp", "hosts", "relays", "euc", "tool", "has_state", "last_state", "last_push", "pushes_total",
             "pushes_30d", "pushes_90d"]
PUSH_COLS = ["event_id", "pubkey", "d", "repo_key", "created_at", "changed", "push", "head_changed",
             "heads_moved", "default_moved", "branches_added", "branches_removed", "tags_added", "tags_removed",
             "tags_moved", "branches", "tags", "source"]


def write_outputs(out, repo_rows, push_rows, doc):
    def cell(v):
        return "" if v is None else ("true" if v is True else "false" if v is False else v)
    os.makedirs(out, exist_ok=True)
    for name, cols, rows in (("ngit_repos.csv", REPO_COLS, repo_rows), ("ngit_pushes.csv", PUSH_COLS, push_rows)):
        with open(os.path.join(out, name), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(cols)
            w.writerows([cell(r[c]) for c in cols] for r in rows)
    with open(os.path.join(out, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")


# ---- compare -----------------------------------------------------------------------------------
BOOL_COLS = {"is_lead", "grasp", "has_state", "changed", "push", "head_changed", "default_moved"}
# a column the reference dropped, which an export from before the change still carries
DROPPED_COLS = {"counted": "dropped with the daily cap by the nostrology-data deploy of 2026-10-02: an export "
                           "from before that deploy, that day's nightly run included, or a column the pipeline kept"}
SKIP_FIELDS = {"concentration", "name", "names", "lead_name", "why", "generated_utc"}
SERIES = ("pushes_weekly", "new_repos_weekly", "comments_weekly")  # weekly series at a document's top level
# STARS: the four keys of the nostrology-data deploy of 2026-10-11, which writes them together;
# stars_iris also marks a document whose stars are of both kinds, and STARS_RULED are the fields
# whose rule changed with it
IRIS_KEYS = ("stars_iris", "archive_events.external_reactions", "anomalies.future_dated.external_reactions",
             "anomalies.past_dated.external_reactions")
STARS_RULED = ("stars_total", "stars_repos")
OPTIONAL_BLOCKS = {"anomalies", "grasp_per_repo", "as_of_day", "comments_90d", "new_repos_weekly", "comments_weekly",
                   "grasp_lists.per_week", "new_repos_weekly.*.by_host", *IRIS_KEYS}
# An optional field that was ADDED to the document is optional only for documents generated
# before its first run (the day here, UTC); a later document without it is a pipeline
# regression and counts as a difference. comments_90d: the first nightly run after the
# nostrology-data deploy of 2026-10-01; the three weekly series: after the deploy of 2026-10-02;
# new_repos_weekly's by_host: after the deploy of 2026-10-08 (the reference names 2026-10-09);
# the four keys of STARS: after the deploy of 2026-10-11, whose first document comes the same day
# (the reference names 2026-10-12).
# A key may be a dotted path (grasp_lists.per_week): it covers every path under it (block()), and
# a `*` segment matches any one segment: new_repos_weekly.*.by_host is every week's by_host, a
# row field, judged only when both sides carry new_repos_weekly itself (diff_summary()).
OPTIONAL_UNTIL = {"comments_90d": "2026-10-02", "new_repos_weekly": "2026-10-03", "comments_weekly": "2026-10-03",
                  "grasp_lists.per_week": "2026-10-03", "new_repos_weekly.*.by_host": "2026-10-09",
                  **dict.fromkeys(IRIS_KEYS, "2026-10-12")}
ZERO_IF_ABSENT = ("exclusions.", "hosting.", "recency.", "no_state.by_class.", "grasp_per_repo.")


def norm_cell(col, v):
    v = (v or "").strip()
    if v == "":
        return None
    if col in BOOL_COLS and v.lower() in ("true", "t", "1", "false", "f", "0"):
        return "true" if v.lower() in ("true", "t", "1") else "false"
    return str(int(v.split(".")[0])) if re.fullmatch(r"-?[0-9]+(\.0*)?", v) else v


def diff_csv(name, mine, other, key, skip):
    def load(d):
        with open(os.path.join(d, name), newline="", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            return rd.fieldnames or [], {tuple(r[k] for k in key): r for r in rd}
    (cols_a, a), (cols_b, b) = load(mine), load(other)
    bad = 0
    extra = [c for c in cols_b if c not in cols_a]  # a column the reference does not define: one difference each
    if extra:
        bad += len(extra)
        print(f"  {name}: {len(extra)} column(s) only in {other}, which the reference does not define: "
              + ", ".join(f"{c} ({DROPPED_COLS[c]})" if c in DROPPED_COLS else c for c in extra))
    for side, only in ((mine, a.keys() - b.keys()), (other, b.keys() - a.keys())):
        if only:
            bad += len(only)
            print(f"  {name}: {len(only)} rows only in {side}; e.g. {sorted(only)[:10]}")
    common = sorted(a.keys() & b.keys())
    for col in [c for c in cols_a if c not in skip and c not in key]:  # a missing column differs on every row
        diffs = [(k, a[k][col], b[k].get(col)) for k in common
                 if norm_cell(col, a[k][col]) != norm_cell(col, b[k].get(col))]
        if diffs:
            bad += len(diffs)
            print(f"  {name}.{col}: {len(diffs)} rows differ (mine | other)")
            for k, x, y in diffs[:10]:
                print(f"      {':'.join(k)[:90]}: {x!r} | {y!r}")
    print(f"  {name}: {len(common)} rows compared, {bad or 'no'} differences")
    return bad


def flatten(doc):
    """summary.json (flat) or a whole NgitSnapshot document -> {dotted path: value}; lists of
    {k, ...} are keyed by k, so order and absent zero rows do not matter, and the weekly series'
    {w, ...} rows by w (zero-filled, so a week absent on one side is a difference), plus the
    series' weeks in order as ONE value, `<series>.@weeks`: keyed rows alone would let a
    duplicated week collapse (the last row wins) and ignore order. A {k, ...} list inside a
    series row (new_repos_weekly's by_host) is keyed by k too, but holds every entry, zeros
    included (ZERO_IF_ABSENT does not cover it), in a fixed order, so its keys in order are ONE
    value as well, `<path>.@keys`. Of a whole document: the
    summary block, the blocks and SERIES named here, and grasp_lists.per_week alone of
    grasp_lists (its other keys are not re-derived)."""
    if isinstance(doc.get("summary"), dict):
        whole = doc
        doc = dict(whole["summary"], **{k: whole[k] for k in ("exclusions", "hosting", "recency", "no_state",
                                                                "grasp_per_repo", "anomalies", "as_of_day") + SERIES
                                         if k in whole})
        if isinstance(whole.get("grasp_lists"), dict) and "per_week" in whole["grasp_lists"]:
            doc["grasp_lists"] = {"per_week": whole["grasp_lists"]["per_week"]}
    out = {}

    def walk(prefix, v, in_row=False):  # in_row: v lies inside a weekly series' row
        by = next((n for n in ("k", "w") if isinstance(v, list) and v
                   and all(isinstance(x, dict) and n in x for x in v)), None)
        if isinstance(v, dict):
            for k, x in v.items():
                if k not in SKIP_FIELDS:
                    walk(f"{prefix}.{k}" if prefix else k, x, in_row)
        elif by:
            if by == "w":  # the sequence of weeks: a duplicated, missing or misordered week differs here
                out[f"{prefix}.@weeks"] = [x["w"] for x in v]
            elif in_row:  # a row's by_host: a duplicated, missing or misordered entry differs here
                out[f"{prefix}.@keys"] = [x["k"] for x in v]
            for x in v:
                for kk, xx in x.items():
                    if kk != by and kk not in SKIP_FIELDS:
                        walk(f"{prefix}.{x[by]}" + ("" if kk == "v" else f".{kk}"), xx, in_row or by == "w")
        else:
            out[prefix] = v
    walk("", doc)
    return out


def covers(key, path):
    """A dotted key covers the path it names and every path under it; a `*` segment of the key
    matches any one segment of the path (new_repos_weekly.*.by_host: every week's by_host)."""
    k, p = key.split("."), path.split(".")
    return len(k) <= len(p) and all(x in ("*", y) for x, y in zip(k, p))


def block(path, keys=OPTIONAL_BLOCKS):
    """The longest of `keys` covering a flattened path (covers(); the longest decides, so
    grasp_lists.per_week is never left to a set's order against grasp_lists, nor a week's
    by_host against new_repos_weekly), else the path's first component."""
    return max((k for k in keys if covers(k, path)), key=len, default=path.split(".")[0])


def read_summary(d):
    """DIR/summary.json -> (its flattened fields, generated_utc in epoch seconds, or None when it is
    missing or unreadable, and generated_utc as written)."""
    with open(os.path.join(d, "summary.json"), encoding="utf-8") as f:
        doc = json.load(f)
    try:
        gen = instant(doc["generated_utc"])
    except (KeyError, TypeError, ValueError, OverflowError):
        gen = None  # judged by the caller, and only where a dated optional key is absent
    return flatten(doc), gen, doc.get("generated_utc")


def stars_rule(other):
    """STARS: the stars rule of a derivation compared against the export in `other`, as (iris,
    why): iris True for kind-7 (reactions) and kind-17 (external-content-reactions) stars and the
    four keys, False for kind-7 (reactions) stars alone. Section 6.2's own rule decides, as no
    date can (the deploy's first document comes the same day as the last one from before it): a
    document that lacks stars_iris counts kind-7 (reactions) stars alone, and only documents
    generated before 2026-10-12 may lack it. So one carrying stars_iris is of the two-kind rule
    and one without it generated before that day of the kind-7 (reactions) rule; one without it
    generated on or after that day breaks the rule, and this checker derives it by the two-kind
    rule it had to follow (the compare counts every key it lacks); one without it whose
    generated_utc is missing or unreadable cannot be judged (MissingInput, as in diff_summary).
    Called before any archive read."""
    flat, gen, raw = read_summary(other)
    until = OPTIONAL_UNTIL["stars_iris"]
    if "stars_iris" in flat:
        return True, f"{other} carries stars_iris"
    if gen is None:
        raise MissingInput(f"summary.stars_iris: absent from {other}, whose generated_utc ({raw!r}) is missing or "
                           f"unreadable, so whether its stars are kind-7 (reactions) stars alone (a document from "
                           f"before {until}) cannot be judged")
    if gen < instant(until):
        return False, f"{other} lacks stars_iris and was generated {day_iso(gen // DAY)}, before {until}"
    return True, (f"{other} lacks stars_iris but was generated {day_iso(gen // DAY)}, on or after {until}: a "
                  f"regression, judged by the two-kind rule it had to follow")


def diff_summary(mine, other):
    """Returns (differences, fields not compared). A dated optional key (OPTIONAL_UNTIL) absent
    from a side whose generated_utc is missing or unreadable raises MissingInput: whether its
    absence is a regression cannot be judged, so the compare could not run (exit 2). A key inside
    another optional key that a side lacks altogether (a week's by_host, the series absent) is
    judged with that key: neither named again nor counted twice. A side carrying some of the four
    keys of STARS but not all differs once per key it lacks, whatever its date; and when one side
    carries stars_iris while the other lacks it and was generated before 2026-10-12, the two count
    different stars (section 6.2: a document that lacks it counts kind-7 (reactions) stars alone),
    so STARS_RULED are not compared, a line naming each."""
    (a, gen_a, raw_a), (b, gen_b, raw_b) = read_summary(mine), read_summary(other)
    tops_a, tops_b = {block(p) for p in a}, {block(p) for p in b}
    one_side = OPTIONAL_BLOCKS & (tops_a ^ tops_b)  # carried by one side only
    bad = compared = skipped = 0
    carry = [[k for k in IRIS_KEYS if k in tops] for tops in (tops_a, tops_b)]  # each side's keys of STARS
    torn = set()  # (side index, key): a key of IRIS_KEYS a side lacks while carrying another
    for i, side in enumerate((mine, other)):
        for k in (k for k in IRIS_KEYS if carry[i] and k not in carry[i]):
            bad += 1
            torn.add((i, k))
            print(f"  summary.{k}: absent from {side}, which carries {', '.join(carry[i])}: the deploy of 2026-10-11 "
                  f"writes the four together")
    for top in sorted(one_side):
        if any(k != top and covers(k, top) for k in one_side):
            continue  # inside a key that side lacks altogether: judged with it
        if (1 if top in tops_a else 0, top) in torn:
            continue  # counted above
        lacking, gen, raw = (other, gen_b, raw_b) if top in tops_a else (mine, gen_a, raw_a)
        until = OPTIONAL_UNTIL.get(top)
        if until and gen is None:
            raise MissingInput(f"summary.{top}: absent from {lacking}, whose generated_utc ({raw!r}) is missing or "
                               f"unreadable, so whether it predates the key's first run ({until}) cannot be judged")
        if until and gen >= instant(until):
            bad += 1
            print(f"  summary.{top}: absent from {lacking}, generated {day_iso(gen // DAY)}, "
                  f"on or after its first run ({until})")
        else:
            skipped += 1
            print(f"  summary.{top}: only in {mine if top in tops_a else other}, not compared")
    # STARS: a side without stars_iris counts kind-7 (reactions) stars alone (section 6.2), so against a
    # side with it STARS_RULED count different stars; from 2026-10-12 a side without it breaks the rule
    # (its absence is counted above) and is held to both kinds, like the other side
    split, until = None, OPTIONAL_UNTIL["stars_iris"]
    if ("stars_iris" in a) != ("stars_iris" in b):
        lacking, gen = (other, gen_b) if "stars_iris" in a else (mine, gen_a)
        split = lacking if gen is not None and gen < instant(until) else None
    for path in sorted(a.keys() | b.keys()):
        top = block(path)
        if top in OPTIONAL_BLOCKS and not (top in tops_a and top in tops_b):
            continue
        if split and path in STARS_RULED:
            skipped += 1
            print(f"  summary.{path}: not compared: {split} lacks stars_iris and predates {until}, so it counts "
                  f"kind-7 (reactions) stars alone (section 6.2), the other side kind-17 (external-content-reactions) "
                  f"stars too (a derivation run with --compare {split} derives them by its rule)")
            continue
        zero = path.startswith(ZERO_IF_ABSENT) and not path.endswith(".unit")
        x, y = a.get(path, 0 if zero else None), b.get(path, 0 if zero else None)
        compared += 1
        numeric = all(isinstance(z, (int, float)) and not isinstance(z, bool) for z in (x, y))
        if not (abs(x - y) <= 1e-9 if numeric else x == y):
            bad += 1
            print(f"  summary.{path}: {x!r} | {y!r}" + ("   (absent on one side)" if path not in a or path not in b else ""))
    print(f"  summary: {compared} fields compared, {bad or 'no'} differences, {skipped} not compared")
    return bad, skipped


def compare(mine, other):
    print(f"[compare] mine={mine} other={other}")
    bad = diff_csv("ngit_repos.csv", mine, other, ("pubkey", "d"), {"name", "hosts", "relays"})
    bad += diff_csv("ngit_pushes.csv", mine, other, ("event_id",), set())
    summary_bad, skipped = diff_summary(mine, other)
    bad += summary_bad
    # The skipped count rides on the verdict, so "ZERO differences" is never read without it.
    print(f"[compare] {bad or 'ZERO'} differences ({skipped} summary fields not compared)")
    return 1 if bad else 0


# ---- self-test ---------------------------------------------------------------------------------
def self_test():
    """A tiny synthetic archive through derive() on the --duckdb path, e7 / e9735 / e5 included:
    reciprocal vs one-way pairs, self-lead and npub, a peeled-tag republish, no daily cap (51
    pushes of one repository on one UTC day all count), a pre-wall pair, the comment windows,
    the four weekly series (the start week from its Monday, on either side of the wall instant;
    zero-filled weeks; a row the second before the start week in no week; the as_of week last,
    on a Friday run and on a Monday run whose own-day row is in no week; authors, threads (the
    first `E`) and s per week; grasp_mixed as grasp; imported and mirrored comments out;
    new_repos_weekly's by_host: one week whose five kinds all differ in count, from repositories
    on several kinds at once, every row's kinds in order with zeros, grasp's v the row's grasp
    and the entries summing to at least v), stars of both kinds (the kind-7 (reactions) targets;
    each key's current kind-17 (external-content-reactions) reaction per coordinate, its content
    trimmed, a same-second tie the lowest id's, the first 30617: `i` value the target, both tags
    needed, the union of the kinds, the horizon and id-dedup, the kind-7 (reactions) rule alone),
    zaps (hostile ones, and the signer / member / amount conditions), N6 deletion triples, the
    horizon, N0, N8a / N8b reachability, private nip.io names, the GRASP host set's exclusions,
    pinned readings, the provider_signers.yaml reader, the --heavy-from-extracts reader over the
    same events as gz NDJSON parts (and its stop when a kind has none), the --s3 path over a local
    tree of the bucket's layout (and its stop when kind-17 (external-content-reactions) has no row
    by the pin, or no Parquet at all), the same stop on --duckdb (no e17, or none of its rows by the
    pin) and that path's kind-7 (reactions) rule without e17, the stars rule an export picks
    (STARS), also through main(), and the compare:
    documents older than comments_90d, the weekly series or by_host (either side of by_host's
    first day), or without a readable generated_utc; a served-shape document; a series
    duplicated, reversed, short or long a week; a by_host off by one in one kind, reversed, or
    short of a zero entry; the four keys of STARS against the kind-7 (reactions) row of
    2026-10-11, a regression of 2026-10-12, an undated document, each key off by one and a document
    carrying some of them; an export that still has the counted column; and the exit status
    through main()."""
    import contextlib
    import gzip
    import hashlib
    import io
    import shutil
    con = duckdb.connect()
    con.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW'")
    for t in ("e30617", "e30618", "e10317", "git1111", "e7", "e17", "e9735", "e5"):
        con.execute(f"CREATE TABLE {t} (id VARCHAR, pubkey VARCHAR, created_at BIGINT, content VARCHAR, "
                    "tags VARCHAR[][], ingested_at BIGINT)")
    seq = iter(range(10 ** 6))

    def add(table, pk, at, tags, content="", eid=None, ingested=None):  # ingested_at is `at` unless given
        eid = eid or hashlib.sha256(str(next(seq)).encode()).hexdigest()
        con.execute(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?)",
                    [eid, pk, at, content, tags, at if ingested is None else ingested])
        return eid
    day = lambda s: instant(s + "T00:00:00Z")  # noqa: E731
    refs = lambda main, *extra: [["HEAD", "ref: refs/heads/main"], ["refs/heads/main", main]] + list(extra)  # noqa: E731
    # F and its npub are a published pair (fiatjaf's key): the decoder is checked against a fact
    F, F_NPUB = ("3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d",
                 "npub180cvv07tjdrrgpa0j7j7tmnyl2yr6yr7l8j4s3evf6u64th6gkwsyjh6w6")
    A, B, C, D, E, X, Y, P, Q = (ch * 64 for ch in "abcde1234")
    ann = {}
    for pk, d_, at, extra in ((A, "r1", "2026-07-01", [["maintainers", B], ["client", "GitPlaza"]]),
                              (B, "r1", "2026-07-02", [["maintainers", A.upper()]]),  # hex is case-insensitive
                              (C, "r2", "2026-07-01", [["maintainers", D], ["client", "gitplaza"]]),  # one-way
                              (D, "r2", "2026-07-01", []),
                              (E, "r3", "2026-07-06", [["M", E], ["m", F_NPUB], ["client", "A NostrHub app"]]),
                              # E's own week (s) is 2026-07-06, its repository's (F's first-seen) 2026-06-29
                              (F, "r3", "2026-07-01", [["maintainers", E]]),
                              (X, "gone", "2026-07-01", []), (X, "back", "2026-07-01", []),
                              (X, "tie", "2026-07-01", []), (X, "other", "2026-07-01", []),
                              (X, "back", "2026-07-03", [])):  # re-announced after its deletion
        ann[pk, d_] = add("e30617", pk, day(at), [["d", d_], ["clone", f"https://github.com/{pk[:4]}/{d_}"]] + extra)
    year1, xid = -62135596800, {}  # 0001-01-01T00:00:00Z: dropped and past_dated
    for d_, at in (("fut", day("2026-07-01")), ("fut", day("2026-07-03")), ("late", day("2026-07-01")),
                   ("old", year1), ("old", day("2026-07-01")),
                   ("today", instant("2026-09-25T02:00:00Z")), ("today", day("2026-10-01")),  # own day; future
                   ("n" * 201, day("2026-07-01")), ("m" * 200, day("2026-07-01")),  # N0; the longest kept d
                   ("bad\x1fid", day("2026-07-01")), ("ok id", day("2026-07-01"))):  # N0: a control char; a space
        xid[d_, at] = add("e30617", X, at, [["d", d_], ["clone", "https://github.com/x/" + d_[:8]]])
    for pk, coord, at in ((X, "gone", "2026-07-02"), (X, "back", "2026-07-02"), (X, "tie", "2026-07-01"),
                          (Y, "other", "2026-07-02"),  # not self-authored
                          (X, "fut", "2026-07-02"), (X, "fut", "2026-10-01"),  # the newer triple is future-dated
                          (X, "late", "2026-10-01")):  # no surviving triple
        add("e5", pk, day(at), [["a", f"30617:{X}:{coord}"]])
    shaped = "https://grasp.example.org:7334/npub1" + "q" * 58 + "/p1.git"  # a :port keeps the shape
    add("e30617", P, day("2026-07-01"), [["d", "p1"], ["clone", shaped], ["clone", "git@grasp.example.org:p/p1"]])
    add("e30617", Q, day("2026-07-01"), [["d", "q1"], ["clone", "git@grasp.example.org:q/q1"]])  # scp only
    W = "6" * 64  # sections 2.2 and 4.1 to 4.5: one coordinate, so one repository, per d
    grasp_path = lambda host, scheme="https": f"{scheme}://{host}/npub1{'q' * 58}/w.git"  # noqa: E731
    # a forge never enters the GRASP set, gittr's git server among them (section 4.2), in its HTTPS and its
    # scp-like SSH form: on the denylist instead it would make this repository other_hosts, on neither list
    # grasp_servers 2 and this repository grasp_mixed
    for d_, tags in (("gh-shaped", [["clone", grasp_path("github.com")], ["clone", grasp_path("git.gittr.space")],
                                    ["clone", f"git@git.gittr.space:npub1{'q' * 58}/w.git"]]),
                     # nor does a general relay, or a host shown not to be a GRASP server (any one of them
                     # in the set would make grasp_servers 2 and this repository grasp_mixed)
                     ("relay-shaped", [["clone", grasp_path("relay.damus.io", "wss")]]
                      + [["clone", grasp_path(h)] for h in ("git.jb55.com", "relay.vanderwarker.family",
                                                            "gitrepublic.imwald.eu",
                                                            "denimroad.feeds.relay.tools")]),
                     ("nip-private", [["clone", grasp_path("grasp.127.0.0.1.nip.io")]]),  # private: N8a, not GRASP
                     ("sslip-private", [["clone", "https://x.10-0-0-1.sslip.io/w.git"]]),  # dashed, private: N8a
                     ("nip-public", [["clone", "https://1.2.3.4.nip.io/w.git"]]),  # a public embedded IPv4: kept
                     ("unreachable", [["clone", "http://localhost/w"], ["clone", "file:///srv/w"]]),  # N8b, not N8a
                     ("no-clone", []),  # N8b
                     ("ctl-trim", [["clone", "\x1c\x1fhttps://codeberg.org/w/t.git\x1e "]]),  # str.strip() trims
                     ("blank-client", [["clone", "https://github.com/w/b"], ["client", " \t "],
                                       ["t", "shakespeare"], ["name", "a\u202eb\x00c\u200bd"]])):  # no client
        add("e30617", W, day("2026-07-01"), [["d", d_]] + tags)
    # N10, the nostr.net catalog key (section 2.2): excluded with a public clone value, while one with no clone
    # value at all is N8b first and a stateless personal fork N9 first, the last rule losing to every other; no
    # client tag, so the rule cannot be the tag
    for d_, tags in (("cat-forge", [["clone", "https://github.com/nostr-protocol/nips.git"]]),
                     ("cat-web-only", [["web", "https://code.pobblelabs.org/fossil/nostr_relay/"]]),
                     ("cat-fork", [["clone", "https://github.com/o/fork.git"], ["t", "personal-fork"]])):
        add("e30617", CATALOG_KEY, day("2026-07-01"), [["d", d_]] + tags)
    M1, M2 = "8" * 64, "f" * 64  # new_repos_weekly starts with ANN_WALL's week, 2026-04-27
    for pk, d_, at in ((M1, "m-early", "2026-04-26T23:59:59Z"),  # the second before: M1 and this repository in no week
                       (M1, "m-late", "2026-05-12T00:00:00Z"),   # in 2026-05-11, while M1 stays in no week
                       (M2, "m-wall", "2026-04-27T00:00:00Z"),   # its first second, days before the wall instant
                       (M2, "20260420", "2026-04-20T00:00:00Z")):  # N4: an excluded announcement dates no maintainer
        add("e30617", pk, instant(at), [["d", d_], ["clone", f"https://github.com/{pk[:4]}/{d_}"]])
    M3 = "9a" * 32  # a GRASP clone and a forge clone: grasp_mixed, which new_repos_weekly counts as grasp
    add("e30617", M3, instant("2026-05-05T00:00:00Z"), [["d", "m3"], ["clone", shaped.replace("/p1.git", "/m3.git")],
                                                       ["clone", "https://github.com/m3/m3"]])
    K = "c7" * 32  # by_host, the week of 2026-08-03: repositories on several kinds at once, each counted under every
    # kind in its R, so the five kinds' counts all differ: grasp 4, hashtree 2, forge 5, self_hosted 3, none 1
    url = {"grasp": grasp_path("grasp.example.org"), "hashtree": f"htree://npub1{'q' * 58}/k",
           "self": "https://git.example.net/k.git", "forge": "https://gitlab.com/k/k"}
    for kinds, n in ((("grasp", "forge"), 2), (("grasp", "hashtree"), 1), (("hashtree", "self"), 1),
                     (("self", "forge"), 2)):
        for i in range(n):
            add("e30617", K, day("2026-08-05"), [["d", f"{'-'.join(kinds)}-{i}"]] + [["clone", url[k]] for k in kinds])
    K2 = "c8" * 32  # the third GRASP + forge repository has two members, the lead (K) on the forge, K2 on GRASP:
    for pk, other, kind in ((K, K2, "forge"), (K2, K, "grasp")):  # R is over the union of the members' clones
        add("e30617", pk, day("2026-08-05"), [["d", "grasp-forge-2"], ["maintainers", other], ["clone", url[kind]]])
    # nothing reachable (a private and a non_network value): kept by its state, not N8b; the state predates
    # STATE_WALL, so it is no push
    add("e30617", K, day("2026-08-05"), [["d", "none-0"], ["clone", "http://localhost/k"], ["clone", "file:///srv/k"]])
    add("e30618", K, day("2026-03-02"), [["d", "none-0"]] + refs("k0"))
    for at, sha in (("2026-06-14T03:00:00Z", "w0"), ("2026-06-14T05:00:00Z", "w1")):  # STATE_WALL's week, either
        add("e30618", M2, instant(at), [["d", "m-wall"]] + refs(sha))                  # side of the instant: one push
    add("e30618", P, instant("2026-09-25T01:00:00Z"), [["d", "p1"]] + refs("p0"))  # own day: waits a run
    t0 = instant("2026-09-20T01:00:00Z")  # 51 pushes of repository {A, B} on one UTC day: no cap, all count
    for pk, n, offset in ((A, 30, 0), (B, 21, 50)):  # interleaved: A, B, A, B, ..., A
        for i in range(n):
            add("e30618", pk, t0 + 100 * i + offset, [["d", "r1"]] + refs(f"{pk[0]}{i}"))
    v1 = refs("aaa", ["refs/tags/v1", "bbb"])
    add("e30618", C, day("2026-09-01"), [["d", "r2"]] + v1 + [["refs/heads/no-value"]])  # 1-element: ignored
    add("e30618", C, day("2026-09-02"), [["d", "r2"]] + v1 + [["refs/tags/v1^{}", "ccc"]])  # peeled republish
    add("e30618", C, day("2026-09-03"), [["d", "r2"]] + refs("ddd", ["refs/tags/v1", "bbb"]))  # main moves
    for at, sha in (("2026-06-01", "p1"), ("2026-06-10", "p2"), ("2026-07-01", "p3")):  # a pre-wall pair
        add("e30618", D, day(at), [["d", "r2"]] + refs(sha))
    for at, extra in (("2026-09-24T23:59:59Z", []), ("2026-08-26T00:00:00Z", []),  # as_of_day; 30 d's first second
                      ("2026-08-25T23:59:59Z", []), ("2026-06-27T00:00:00Z", []),  # 90 d only; 90 d's first second
                      ("2026-06-26T23:59:59Z", []),                     # before 90 d: in comments_total only
                      ("2026-09-10T00:00:00Z", [["expiration", "1"]]),  # ephemeral: in no comment figure
                      ("2026-09-25T02:00:00Z", [])):                    # the run's own day: waits for the next run
        add("git1111", Y, instant(at), [["K", "1621"]] + extra, "hi")
    Y2, Y3, T1, T2 = "0" * 64, "c1" * 32, "1e" * 32, "2e" * 32  # comments_weekly starts with COMMENT_WALL's week
    for pk, at, tags in ((Y2, "2026-06-14T23:59:59Z", [["K", "1617"], ["E", T1]]),   # the second before: in no week
                         (Y2, "2026-06-15T00:00:00Z", [["K", "1617"], ["E", T1]]),   # its first second: patches
                         (Y3, "2026-06-16T12:00:00Z", [["K", "1618"], ["E", T1]]),   # before the wall instant: prs
                         (Y2, "2026-06-20T00:00:00Z", [["K", "1619"], ["E", T2]]),   # prs too, a second thread
                         (Y2, "2026-06-21T23:59:59Z", [["K", "30617"]]),             # repo; no E: no thread
                         (Y2, "2026-06-22T00:00:00Z", [["K", "30618"], ["E", T1]]),  # the next week: Y2, T1 again
                         # admitted by its A tag; the FIRST `K` decides the root: other, not issues
                         (Y3, "2026-06-24T00:00:00Z", [["K", "1"], ["K", "1621"], ["A", f"30617:{C}:r2"]]),
                         # the FIRST `E` is its thread: T2, new to this week (T1, its last `E`, is not); both
                         # are threads already, so comment_threads reads the same under either reading
                         (Y3, "2026-06-23T00:00:00Z", [["K", "1621"], ["E", T2], ["E", T1]])):
        add("git1111", pk, instant(at), tags, "hi")
    add("git1111", Y2, instant("2026-07-09T00:00:00Z"), [["K", "1621"], ["imported", "github"]], "hi")  # imported
    MK = "d1" * 32  # a mirror key: 50 comments, every one a re-posted foreign author line, so all 50 mirrored
    for i in range(50):
        add("git1111", MK, instant("2026-07-08T00:00:00Z") + i, [["K", "1617"]], f"**@alice{i}** wrote: hi")
    add("git1111", Y, instant("2026-09-28T00:30:00Z"), [["K", "1621"]], "hi")  # the Monday run's own day
    for pk, versions in (("e1" * 32, [("2026-06-07T23:59:59Z", "wss://grasp.one.org"),   # the second before
                                      ("2026-07-01T00:00:00Z", "grasp.one.org/")]),      # 2026-06-08: in no week
                         ("e2" * 32, [("2026-06-10T00:00:00Z", "grasp.two.org")]),       # before the wall instant
                         ("e3" * 32, [("2026-07-01T00:00:00Z", "localhost"),             # an empty first list
                                      ("2026-08-01T00:00:00Z", "grasp.two.org")]),       # still dates the user
                         ("e4" * 32, [("2026-07-15T00:00:00Z", "grasp.two.org"),         # an empty CURRENT list:
                                      ("2026-08-15T00:00:00Z", "wss://grasp.example.com")]),  # no user, no week
                         ("e5" * 32, [("2026-09-24T23:59:59Z", "grasp.two.org")])):      # the as_of week, last
        for at, g in versions:  # kind-10317 (grasp-server-lists): grasp_lists.per_week starts with 2026-06-08
            add("e10317", pk, instant(at), [["g", g]])
    for pk, tags, content in ((X, [["a", f"30617:{C}:r2"]], "+"), (X, [["a", f"30617:{C}:r2"]], "+"),  # one pair
                              (X, [["a", f"30617:{B}:r1"]], ""),         # member B -> repository A:r1
                              (Y, [["k", "30617"], ["e", ann[D, "r2"]]], "+"),  # through `e` -> D:r2
                              (Y, [["k", "30617"], ["e", xid["old", year1]]], "+"),  # a dropped version: no target
                              (P, [["a", f"30617:{C}:r2"]], "-"),         # not a star
                              (Q, [["a", "30617:" + "9" * 64 + ":none"]], "+"),  # no such coordinate
                              ("7" * 64, [["a", "30617:" + C], ["e", ann[D, "r2"]]], "+")):  # malformed `a`: no `e`
        add("e7", pk, day("2026-09-10"), tags, content)
    # section 6.2's kind-17 (external-content-reactions) stars, Iris Git's: a reactor key per case (IR); each
    # key's CURRENT reaction per (pubkey, coordinate) decides, its content trimmed; on 2026-09-10 unless noted
    IR = {n: hashlib.sha256(f"iris {n}".encode()).hexdigest() for n in (
        "pmp", "pm", "trim", "empty", "strip", "bom", "other", "tie-plus", "tie-minus", "member", "two-members",
        "first", "url", "nocolon", "colon", "k-only", "i-only", "k-30617", "own-day", "future", "past", "excluded")}
    co = lambda pk, d_: f"30617:{pk}:{d_}"  # noqa: E731
    git = lambda *i: [["k", "git-repo"]] + [["i", v] for v in i]  # noqa: E731
    t9 = day("2026-09-10")
    for pk, at, tags, content in (
            (IR["pmp"], t9, git(co(C, "r2")), "+"), (IR["pmp"], t9 + 1, git(co(C, "r2")), "-"),
            (IR["pmp"], t9 + 2, git(co(C, "r2")), "+"),                          # + - +: a star
            (IR["pm"], t9, git(co(C, "r2")), "+"), (IR["pm"], t9 + 1, git(co(C, "r2")), "-"),  # + -: none
            (IR["empty"], t9, git(co(D, "r2")), ""),                             # empty: a star
            (IR["strip"], t9, git(co(D, "r2")), "\x1f\x85+\u3000"),              # str.strip() trims all three: a star
            (IR["bom"], t9, git(co(D, "r2")), "\ufeff+"),                         # str.strip() keeps U+FEFF: none
            (IR["other"], t9, git(co(C, "r2")), "++"),                           # other content: none
            (IR["member"], t9, git(co(B, "r1")), "+"),                           # a star on member B's coordinate
            (IR["member"], t9 + 1, git(co(A, "r1")), "-"),                       # stands: this `-` is on another one
            (IR["two-members"], t9, git(co(A, "r1")), "+"),                      # two member coordinates of one
            (IR["two-members"], t9, git(co(B, "r1")), "+"),                      # repository: one pair
            (IR["first"], t9, git(co("9" * 64, "none"), co(C, "r2")), "+"),      # the FIRST 30617: value: no target
            (IR["url"], t9, git("https://git.iris.to/x", co(D, "r2")), "+"),     # a value not starting 30617: skipped
            (IR["nocolon"], t9, git(f"30617:{C}", co(C, "r2")), "+"),            # dropped; no later value stands in
            (IR["nocolon"], day("2026-10-01"), git(f"30617:{C}"), "+"),          # in the input still: future_dated
            (IR["colon"], t9, git(co(C, "r2:x")), "+"),                          # `d` is r2:x (colons kept): no star
            (IR["k-only"], t9, [["k", "git-repo"], ["a", co(C, "r2")]], "+"),    # no `i`: not admitted
            (IR["i-only"], day("2026-10-01"), [["i", co(C, "r2")]], "+"),        # no `k`: not admitted, so not future
            (IR["k-30617"], t9, [["k", "30617"], ["i", co(C, "r2")]], "+"),      # `k` not git-repo: not admitted
            (X, t9, git(co(C, "r2")), "+"),                                      # X's kind-7 (reactions) pair as well
            (X, t9, git(co(A, "r1")), "+"), (X, t9 + 1, git(co(A, "r1")), "-"),  # its kind-7 (reactions) star stays
            (IR["own-day"], t9, git(co(E, "r3")), "+"),                          # a star until the next run, as its
            (IR["own-day"], instant("2026-09-25T02:00:00Z"), git(co(E, "r3")), "-"),  # `-` of the run's own day waits
            (IR["future"], t9, git(co(E, "r3")), "+"),                           # a star: its future `-` is dropped
            (IR["future"], day("2026-10-01"), git(co(E, "r3")), "-"),
            (C, t9, git(co(C, "r2")), "+"),                                      # the owner's own: a star
            (IR["excluded"], t9, git(co(M2, "20260420")), "+")):                 # an N4 coordinate: no star
        add("e17", pk, at, tags, content)
    trim = add("e17", IR["trim"], t9, git(co(D, "r2")), " +\n")                  # trimmed: a star; its copy, ingested
    add("e17", IR["trim"], t9, git(co(D, "r2")), " +\n", eid=trim, ingested=t9 + 3600)  # an hour later: one row
    add("e17", IR["past"], PAST_CUT - 1, git(co(E, "r3")), "+", ingested=t9)     # past_dated: dropped, no star
    for pk, rows in ((IR["tie-plus"], (("f" * 64, "-"), ("0" * 64, "+"))),      # one second: the lowest id's is
                     (IR["tie-minus"], (("1" * 64, "-"), ("e" * 64, "+")))):     # current, inserted last or first
        for eid, content in rows:
            add("e17", pk, t9, git(co(C, "r2")), content, eid=eid)
    S1, S2 = "5" * 64, "ab" * 32  # the fixture's listed LNURL provider signer keys
    zr = lambda z, **pad: json.dumps({"pubkey": z * 32, **pad})  # noqa: E731
    for coord, bolts, descs in ((C, ["garbage", "lnbc10u1x"], [zr("z1")]),  # the LAST bolt11: 1,000,000 msats
                                (C, ["lnbc150m1x"], [zr("z2")]),              # 1.5e10 -> capped at 1e10
                                (C, ["lnbc300m1x"], [zr("z3")]),              # 3e10 > 2.1e10: dropped
                                (D, ["lnbcx"], [zr("z4")]),                   # unreadable amount: dropped
                                (D, ["lnbc20u1x"], [zr("za"), "{not json"]),  # the LAST description does not parse
                                (C, ["lnbc0n1x"], [zr("z5")]),                # 0 msats is a readable amount: kept
                                (C, ["lnbc" + "9" * 320 + "p1x"], [zr("h1")]),  # unreadable to the parser: dropped
                                (C, ["lnbc10u1" + "q" * 3000], [zr("h2")]),   # longer than 2,048: dropped
                                (D, ["lnbc1u1x"], ["[" * 12000]),             # does not parse: counts, no zapper
                                (D, ["lnbc2u1x"], [zr("h4", pad="x" * 70000)])):  # longer than 65,536: no zapper
        add("e9735", S1, day("2026-09-10"), [["a", f"30617:{coord}:r2"], ["p", coord]]
            + [["bolt11", b] for b in bolts] + [["description", z] for z in descs])
    for signer, coord, ps, bolt, desc in (  # section 6.3's signer, member and amount conditions
            ("9" * 64, f"{C}:r2", [C], "lnbc3u1x", zr("n1")),                         # an unlisted signer: dropped
            (S1, f"{C}:r2", [D], "lnbc4u1x", zr("n2")),                               # pays a non-member: dropped
            (S1, f"{C}:r2", [D, C], "lnbc4u1x", zr("n3")),                            # only the FIRST `p` counts
            (S1, f"{C}:r2", [C], "lnbc4u1x", zr("n4", tags=[["amount", "400001"]])),  # 400,001 != 400,000
            (S2, f"{A}:r1", [B], "lnbc5u1x", zr("n5", tags=[["amount", "500000"]])),  # B, not the lead: counted
            (S1, f"{D}:r2", [D], "lnbc6u1x", zr("n6", tags=[["amount", ""]]))):       # empty amount: counted
        add("e9735", signer, day("2026-09-10"), [["a", f"30617:{coord}"]] + [["p", p] for p in ps]
            + [["bolt11", bolt], ["description", desc]])
    with tempfile.TemporaryDirectory() as td:
        def signer_file(name, text):
            with open(os.path.join(td, name), "w", encoding="utf-8") as f:
                f.write(text)
            return os.path.join(td, name)

        def stops(path):
            try:
                load_signers(path)
            except MissingInput:
                return True
            return False
        signers = load_signers(signer_file("signers.yaml", f"# fixture\nproviders:\n- domain: one.example\n"
                                           f"  signers:\n  - {S1}\n  verified_at: '2026-05-09'\n- domain: two.example\n"
                                           f"  signers:\n  # {S1}: a comment, not a key\n  - '{S2}'  # quoted\n"))
        stopped = [stops(signer_file("flow.yaml", f"providers:\n- domain: x\n  signers: [{S1}]\n")),
                   stops(signer_file("short.yaml", f"providers:\n- domain: x\n  signers:\n  - {S1[1:]}\n")),
                   stops(signer_file("none.yaml", "providers: []\n")), stops(os.path.join(td, "absent.yaml"))]
        real_signers = load_signers(SIGNERS)
        now = instant("2026-09-25T07:41:00Z")
        repos, pushes, doc = derive(con, "duckdb", now, None, td, log=lambda s: None, signers=signers)
        # section 6.2's two star sets, read back over this derivation's tables as (pubkey, repository key) pairs
        key_of = {(r["pubkey"], r["d"]): r["repo_key"] for r in repos}
        rkey = {rk: key_of[pk, d_] for rk, pk, d_ in con.execute(
            "SELECT m.rk, c.pubkey, c.d FROM mem m JOIN coords c USING (cid)").fetchall()}
        pairs7, pairs17 = ({(pk, rkey.get(rk, rk)) for pk, rk in pairs}  # an unknown rk stays: checks fail
                           for pairs in star_pairs(con, True))
        docK7 = derive(con, "duckdb", now, None, td, log=lambda s: None, signers=signers, iris=False)[2]
        # section 3's hashtree pushes, on an archive of their own (the main one has none, so its figures
        # stand): H is hashtree-only, with a version before the wall, a group starting exactly one gap
        # before it (so the version at the wall starts a group of its own), one 60 days back (in 90 d,
        # not 30 d) and two in 30 d; M is GRASP and hashtree (grasp_mixed), whose republish is no push
        # but whose state is; T is hashtree with a state of its own, so both count; O is announced
        # once; Z, HG and HF are below
        hcon = duckdb.connect()
        hcon.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW'")
        for t in ("e30617", "e30618", "e10317", "git1111", "e7", "e17", "e9735", "e5"):
            hcon.execute(f"CREATE TABLE {t} (id VARCHAR, pubkey VARCHAR, created_at BIGINT, content VARCHAR, "
                         "tags VARCHAR[][], ingested_at BIGINT)")
        hseq = iter(range(10 ** 6))

        def hadd(table, pk, at, tags, eid=None):
            eid = eid or hashlib.sha256(f"htree-{next(hseq)}".encode()).hexdigest()
            hcon.execute(f"INSERT INTO {table} VALUES (?, ?, ?, '', ?, ?)", [eid, pk, at, tags, at])
        H, M, T, O, Z = ("d1" * 32, "d2" * 32, "d3" * 32, "d4" * 32, "d5" * 32)
        HG, HF = ("d6" * 32, "d7" * 32)
        GAP = HTREE_PUSH_GAP_S
        # Z: created, then two versions in one second start a push; in version order (id descending
        # among equals) the higher id is the group's first, so its row is the HIGHER id
        hadd("e30617", Z, day("2026-09-01"), [["d", "z"], ["clone", f"htree://npub1{'q' * 58}/z"]])
        for eid in ("0" * 64, "f" * 64):
            hadd("e30617", Z, day("2026-09-15"), [["d", "z"], ["clone", f"htree://npub1{'q' * 58}/z"]], eid)
        # HG: created; a push of three refs 100 s apart (one push, 200 s from first to last); then a
        # version GAP - 1 after the one before (the same push) and one exactly GAP after (a new one)
        # HF: a first push of two refs (both left out), then one push
        # MA and MB: one repository of two member coordinates (each lists the other); each member's
        # first group is left out and each later group is a push, grouped per coordinate: MB's push
        # 30 s after MA's is a push of its own
        g18 = day("2026-09-18")
        for pk, d_, at in ((HG, "g", day("2026-09-01")), (HG, "g", day("2026-09-16")),
                           (HG, "g", day("2026-09-16") + 100), (HG, "g", day("2026-09-16") + 200),
                           (HG, "g", g18), (HG, "g", g18 + GAP - 1), (HG, "g", g18 + 2 * GAP - 1),
                           (HF, "f", day("2026-09-02")), (HF, "f", day("2026-09-02") + 50),
                           (HF, "f", day("2026-09-19"))):
            hadd("e30617", pk, at, [["d", d_], ["clone", f"htree://npub1{'q' * 58}/{d_}"]])
        for pk, d_, at, extra in ((H, "h", STATE_WALL - 30 * DAY, []), (H, "h", STATE_WALL - GAP, []),
                                  (H, "h", STATE_WALL, []), (H, "h", day("2026-07-26"), []),
                                  (H, "h", day("2026-09-10"), []), (H, "h", day("2026-09-20"), []),
                                  (M, "m", day("2026-09-01"), [["clone", grasp_path("grasp.example.org")]]),
                                  (M, "m", day("2026-09-11"), [["clone", grasp_path("grasp.example.org")]]),
                                  (T, "t", day("2026-09-01"), []), (T, "t", day("2026-09-12"), []),
                                  (O, "o", day("2026-09-01"), [])):
            hadd("e30617", pk, at, [["d", d_], ["clone", f"htree://npub1{'q' * 58}/{d_}"]] + extra)
        MA, MB = ("d8" * 32, "d9" * 32)
        for pk, other, at in ((MA, MB, day("2026-09-01")), (MB, MA, day("2026-09-02")),
                              (MA, MB, day("2026-09-14")), (MB, MA, day("2026-09-14") + 30)):
            hadd("e30617", pk, at, [["d", "mm"], ["clone", f"htree://npub1{'q' * 58}/mm"], ["maintainers", other]])
        hadd("e30618", M, day("2026-09-12"), [["d", "m"]] + refs("m0"))
        hadd("e30618", T, day("2026-09-05"), [["d", "t"]] + refs("t0"))
        os.makedirs(f"{td}/hashtree")
        hrepos, hpushes, hdoc = derive(hcon, "duckdb", now, None, f"{td}/hashtree", log=lambda s: None,
                                       signers=signers, iris=False)
        hrow = {(r["pubkey"], r["d"]): r for r in hrepos}
        hsrc = defaultdict(list)
        for p in hpushes:
            hsrc[p["pubkey"], p["d"]].append((p["source"], p["created_at"]))
        for table, kind in (("e7", 7), ("e9735", 9735), ("e5", 5)):  # the same events as UNLOAD parts
            os.makedirs(f"{td}/k={kind}/run1-k{kind}-ngit")
            with gzip.open(f"{td}/k={kind}/run1-k{kind}-ngit/part-0.gz", "wt") as f:
                for eid, pk, at, content, tags in con.execute(f"SELECT id, pubkey, created_at, content, tags "
                                                              f"FROM {table}").fetchall():
                    if kind == 5:  # the template's output: one (author, coord, created_at) per `a` tag
                        f.writelines(json.dumps({"author": pk, "coord": t[1], "created_at": at}) + "\n"
                                     for t in tags if t[0] == "a")
                    else:
                        f.write(json.dumps({"id": eid, "pubkey": pk, "kind": kind, "created_at": at,
                                            "content": content, "tags": tags, "sig": ""}) + "\n")
        doc2 = derive(con, "duckdb", instant("2026-09-25T07:41:00Z"), None, td, log=lambda s: None,
                      heavy=(td, "run1", False), signers=signers)[2]
        shutil.copytree(f"{td}/k=7/run1-k7-ngit", f"{td}/k=7/run2-k7-ngit")  # run2: two kinds' parts expired
        gap = None
        try:
            derive(con, "duckdb", instant("2026-09-25T07:41:00Z"), None, td, log=lambda s: None,
                   heavy=(td, "run2", False), signers=signers)
        except MissingInput as e:
            gap = str(e)
        doc3 = derive(con, "duckdb", instant("2026-09-25T07:41:00Z"), None, td, log=lambda s: None,
                      heavy=(td, "run2", True), signers=signers)[2]
        doc4 = derive(con, "duckdb", instant("2026-09-28T03:18:07Z"), None, td, log=lambda s: None,
                      signers=signers)[2]  # a Monday run: as_of_day is the Sunday 2026-09-27
        # --s3 over a local tree of the bucket's layout, a `dt` partition per kind: the same documents; under the
        # two-kind stars rule a kind-17 (external-content-reactions) archive with no row ingested by the pin (each is
        # ingested after 2026-06-01) or no Parquet at all (before the kind's first flush) stops the run
        tree = f"{td}/archive"
        for kind, cls, local, _ in KINDS.values():
            os.makedirs(f"{tree}/c={cls}/k={kind}/p=0/dt=2026-09-10")
            con.execute(f"COPY {local} TO '{tree}/c={cls}/k={kind}/p=0/dt=2026-09-10/events.parquet' (FORMAT parquet)")

        def s3_run(cutoff, iris=True):  # an --s3 derivation's document over the tree, or the MissingInput it raised
            try:
                return derive(con, "s3", now, cutoff, td, log=lambda s: None, signers=signers, iris=iris,
                              archive=tree)[2]
            except MissingInput as e:
                return str(e)
            except duckdb.Error as e:  # read_parquet over no file, say: this case fails, the self-test goes on
                return f"raised {type(e).__name__}"
        doc_s3, early = s3_run(None), s3_run(instant("2026-06-01T00:00:00Z"))
        shutil.rmtree(f"{tree}/c=regular/k=17")
        absent, doc_s3_7 = s3_run(None), s3_run(None, iris=False)
        local_early = None  # --duckdb under the two-kind rule: an e17 with no row ingested by the pin stops it too
        try:
            derive(con, "duckdb", now, instant("2026-06-01T00:00:00Z"), td, log=lambda s: None, signers=signers)
        except MissingInput as e:
            local_early = str(e)
        write_outputs(f"{td}/mine", repos, pushes, doc)
        # a document from before the first runs of comments_90d (2026-10-02) and of the three weekly series
        # (2026-10-03); "late" / "later" lack them yet were generated on those days
        old = {k: v for k, v in doc.items() if k not in ("comments_90d", "new_repos_weekly", "comments_weekly",
                                                          "grasp_lists")}
        # a series on both sides compares cell by cell and as its sequence of weeks: a changed cell, a week
        # lacking, zero rows ahead of the real ones (keyed by w alone, the last row would win), the weeks
        # reversed, a week more on the served side
        offw, hole, dup, rev, extra = (json.loads(json.dumps(doc)) for _ in range(5))
        offw["grasp_lists"]["per_week"][0]["v"] += 1
        hole["pushes_weekly"] = [r for r in doc["pushes_weekly"] if r["w"] != "2026-06-15"]
        dup["pushes_weekly"] = ([dict(r, pushes=0, repos=0, maintainers=0) for r in doc["pushes_weekly"]]
                                + doc["pushes_weekly"])
        rev["comments_weekly"] = doc["comments_weekly"][::-1]
        extra["pushes_weekly"] = doc["pushes_weekly"] + [{"w": "2026-09-28", "pushes": 0, "repos": 0, "maintainers": 0}]
        # by_host (first day 2026-10-09) lacking from every row: generated the last second before that day (not
        # compared), its first second (a difference) or never (the compare stops); on both sides it compares entry
        # by entry and as its sequence of keys: one kind off by one, one week's entries reversed, a zero entry
        # lacking (every entry is present, zeros included)
        hostold = dict(doc, generated_utc="2026-10-08 23:59:59Z", new_repos_weekly=[
            {f: x for f, x in r.items() if f != "by_host"} for r in doc["new_repos_weekly"]])
        hostoff, hostrev, hostgap = (json.loads(json.dumps(doc)) for _ in range(3))
        for side, w, edit in ((hostoff, "2026-08-03", lambda c: [dict(e, v=e.get("v", 0) + 1)
                                                                 if e.get("k") == "hashtree" else e for e in c]),
                              (hostrev, "2026-08-03", lambda c: c[::-1]),
                              (hostgap, "2026-05-04", lambda c: [e for e in c if e.get("k") != "none"])):
            for r in side["new_repos_weekly"]:
                if r["w"] == w:  # a derivation without that week leaves the side as is: checks fail, nothing raises
                    r["by_host"] = edit(r.get("by_host") or [])
        hostrev["hosting"].reverse()  # top-level lists of classes are keyed by k: their order is no difference
        hostrev["no_state"]["by_class"].reverse()
        nogen = {k: v for k, v in old.items() if k != "generated_utc"}  # no date to judge its absent keys by
        top = ("exclusions", "hosting", "recency", "no_state", "grasp_per_repo", "anomalies", "as_of_day",
               "generated_utc") + SERIES  # a served document: these beside its summary block
        served = dict({k: doc[k] for k in top}, summary={k: v for k, v in doc.items()
                                                         if k not in top + ("grasp_lists",)},
                      new_repos_monthly=[{"w": "2026-07", "v": 99}],  # a series this checker does not derive
                      grasp_lists=dict(doc["grasp_lists"], per_month=[{"w": "2026-07", "v": 99}],
                                       length=[{"k": "1", "v": 99}], first_choice=[{"host": "x.org", "users": 99}]))
        for name, side in (("old", old), ("off", dict(doc, comments_90d=doc["comments_90d"] + 1)),
                           ("late", dict(old, generated_utc="2026-10-02 03:18:07Z")),
                           ("later", dict(old, generated_utc="2026-10-03 03:18:07Z")), ("offw", offw), ("hole", hole),
                           ("served", served), ("dup", dup), ("rev", rev), ("extra", extra), ("nogen", nogen),
                           ("badgen", dict(old, generated_utc="2 Oct 2026")),
                           ("fullnogen", {k: v for k, v in doc.items() if k != "generated_utc"}),
                           ("hostold", hostold), ("hostlate", dict(hostold, generated_utc="2026-10-09 00:00:00Z")),
                           ("hostnogen", {k: v for k, v in hostold.items() if k != "generated_utc"}),
                           ("hostoff", hostoff), ("hostrev", hostrev), ("hostgap", hostgap)):
            os.makedirs(f"{td}/{name}")
            with open(f"{td}/{name}/summary.json", "w", encoding="utf-8") as f:
                json.dump(side, f)
        # STARS: the four keys and the rule, against the kind-7 (reactions) row of 2026-10-11 03:18:08 UTC, a
        # regression of 2026-10-12, an undated document, each key off by one, and documents carrying some of the
        # four (no archive_events.external_reactions; no stars_iris); the substrate CSVs come along, for main()
        ioff, itorn, itorn2 = (json.loads(json.dumps(doc)) for _ in range(3))
        ioff["stars_iris"] = ioff.get("stars_iris", 0) + 1  # a derivation lacking a key: checks fail, nothing raises
        ioff["archive_events"]["external_reactions"] = ioff["archive_events"].get("external_reactions", 0) + 1
        for slot in ("future_dated", "past_dated"):
            ioff["anomalies"][slot]["external_reactions"] = ioff["anomalies"][slot].get("external_reactions", 0) + 1
        itorn["archive_events"].pop("external_reactions", None)
        itorn2.pop("stars_iris", None)
        for name, side in (("iold", dict(docK7, generated_utc="2026-10-11 03:18:08Z")),
                           ("ilate", dict(docK7, generated_utc="2026-10-12 03:18:08Z")),
                           ("inogen", {k: v for k, v in docK7.items() if k != "generated_utc"}), ("ioff", ioff),
                           ("itorn", dict(itorn, generated_utc="2026-10-11 07:30:00Z")),
                           ("itorn2", dict(itorn2, generated_utc="2026-10-11 07:30:00Z")), ("mine7", docK7)):
            shutil.copytree(f"{td}/mine", f"{td}/{name}")
            with open(f"{td}/{name}/summary.json", "w", encoding="utf-8") as f:
                json.dump(side, f)
        rules = [stars_rule(f"{td}/{n}")[0] for n in ("mine", "served", "iold", "ilate", "itorn2")]
        rule_halt = None
        try:
            stars_rule(f"{td}/inogen")
        except MissingInput as e:
            rule_halt = str(e)
        with open(f"{td}/mine/ngit_pushes.csv", newline="", encoding="utf-8") as f:
            lines = list(csv.reader(f))
        with open(f"{td}/old/ngit_pushes.csv", "w", newline="", encoding="utf-8") as f:  # an export from before
            csv.writer(f, lineterminator="\n").writerows([lines[0] + ["counted"]] + [r + ["t"] for r in lines[1:]])

        def halted(name):  # the MissingInput a compare against `name` stops with, or None when it ran
            try:
                diff_summary(f"{td}/mine", f"{td}/{name}")
            except MissingInput as e:
                return str(e)
            return None
        with contextlib.redirect_stdout(io.StringIO()) as said:
            diffs = [diff_summary(f"{td}/mine", f"{td}/{n}") for n in ("old", "off", "late", "later", "offw", "hole",
                                                                       "served", "dup", "rev", "extra", "fullnogen",
                                                                       "hostold", "hostlate", "hostoff", "hostrev",
                                                                       "hostgap")]
            halts = [halted(n) for n in ("nogen", "badgen", "hostnogen")]
            idiffs = [diff_summary(f"{td}/{m}", f"{td}/{n}") for m, n in (
                ("mine", "iold"), ("mine7", "iold"), ("mine", "ilate"), ("mine", "ioff"), ("mine", "itorn"),
                ("mine", "itorn2"))]
            ihalt = halted("inogen")
            csv_diffs = [diff_csv("ngit_pushes.csv", f"{td}/mine", f"{td}/{n}", ("event_id",), set())
                         for n in ("mine", "old")]
        # the exit status through main(): only differences exit 1; a crash or a MissingInput exits 2
        for name in ("same", "differ", "array", "noid", "undated"):
            shutil.copytree(f"{td}/mine", f"{td}/{name}")
        for name, side in (("differ", dict(doc, comments_90d=doc["comments_90d"] + 1)), ("array", []),
                           ("undated", nogen)):
            with open(f"{td}/{name}/summary.json", "w", encoding="utf-8") as f:
                json.dump(side, f)
        with open(f"{td}/noid/ngit_pushes.csv", "w", newline="", encoding="utf-8") as f:  # no event_id column
            csv.writer(f, lineterminator="\n").writerows([r[1:] for r in lines])

        def exit_status(name):
            err = io.StringIO()
            try:
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
                    code = main(["--out", f"{td}/mine", "--compare", f"{td}/{name}"])
            except BaseException as e:  # an escaped exception fails this case; it must not end the self-test
                code = f"raised {type(e).__name__}"
            return code, err.getvalue()
        codes = [exit_status(n) for n in ("same", "differ", "absent", "array", "noid", "undated")]
        # STARS through main(): derivations from the fixture as a local copy, each compared against an export;
        # the second copy has no e17, no kind-17 (external-content-reactions)
        fx, fx7 = f"{td}/fixture.duckdb", f"{td}/fixture-no-e17.duckdb"
        for path, tables in ((fx, list(KINDS.values())), (fx7, [k for k in KINDS.values() if k[2] != "e17"])):
            con.execute(f"ATTACH '{path}' AS fx")
            for _, _, local, _ in tables:
                con.execute(f"CREATE TABLE fx.{local} AS SELECT * FROM {local}")
            con.execute("DETACH fx")

        def via(name, db=fx, tag="via"):  # (exit status, stdout, stderr) of a derivation compared against `name`
            out, err = io.StringIO(), io.StringIO()
            try:
                with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                    code = main(["--duckdb", db, "--now", "2026-09-25T07:41:00Z", "--signers", f"{td}/signers.yaml",
                                 "--out", f"{td}/{tag}-{name}", "--compare", f"{td}/{name}"])
            except BaseException as e:  # an escaped exception fails this case; it must not end the self-test
                code = f"raised {type(e).__name__}"
            return code, out.getvalue(), err.getvalue()
        vias = {n: via(n) for n in ("iold", "same", "ilate", "inogen")}
        via_wrote = os.path.exists(f"{td}/via-inogen")  # the rule is picked before anything is read or written
        vias7 = {n: via(n, fx7, "via7") for n in ("same", "iold")}
    row = {(r["pubkey"], r["d"]): r for r in repos}
    by = defaultdict(list)
    for p in pushes:
        by[p["pubkey"]].append(p)
    ab = sorted(by[A] + by[B], key=lambda p: (p["created_at"], p["event_id"]))
    part = {k: {x["k"]: x["v"] for x in doc[k]} for k in ("recency", "grasp_per_repo")}

    def without_iris(d):  # a document minus STARS_RULED and the four keys of STARS
        d = json.loads(json.dumps(d))
        for k in STARS_RULED + ("stars_iris",):
            d.pop(k, None)
        d["archive_events"].pop("external_reactions", None)
        for slot in ("future_dated", "past_dated"):
            d["anomalies"][slot].pop("external_reactions", None)
        return d

    # the weekly series: their labels in order, and each week's fields by label (a missing week reads {}, so a
    # check fails instead of raising); the expected labels are counted here with dt.date, not with week_of
    lab = lambda rows: [r["w"] for r in rows]  # noqa: E731
    weeks = lambda w0, n: [str(dt.date.fromisoformat(w0) + dt.timedelta(weeks=i)) for i in range(n)]  # noqa: E731
    zero = lambda *fields: dict.fromkeys(fields, 0)  # noqa: E731
    sw = {k: defaultdict(dict, {r["w"]: {f: x for f, x in r.items() if f not in ("w", "by_host")} for r in rows})
          for k, rows in (("pushes", doc["pushes_weekly"]), ("repos", doc["new_repos_weekly"]),
                          ("comments", doc["comments_weekly"]), ("grasp", doc["grasp_lists"]["per_week"]))}
    # by_host per week (a missing week or field reads []); section 7's order spelled out, not read from HOST_KINDS
    bh = defaultdict(list, {r["w"]: r.get("by_host") or [] for r in doc["new_repos_weekly"]})
    kinds = ["grasp", "hashtree", "forge", "self_hosted", "none"]
    hosts = lambda *n: [{"k": k, "v": v} for k, v in zip(kinds, n)]  # noqa: E731
    checks = [
        ("hashtree pushes (section 3): one per group of a hashtree-class repository's member coordinate's "
         "versions less than HTREE_PUSH_GAP_S apart, chained, but its first group, from STATE_WALL on (a "
         "group starting before it is not one, the wall's own is); a group starting on a tie is its higher "
         "id; a grasp_mixed republish is no push, its state is; a hashtree repository with a state counts "
         "both, and its last_state is the state's",
         hsrc[H, "h"] == [("htree_announcement", t) for t in (STATE_WALL, day("2026-07-26"), day("2026-09-10"),
                                                             day("2026-09-20"))]
         and hsrc[HG, "g"] == [("htree_announcement", t) for t in (day("2026-09-16"), g18, g18 + 2 * GAP - 1)]
         and hsrc[HF, "f"] == [("htree_announcement", day("2026-09-19"))]
         and hsrc[MA, "mm"] == [("htree_announcement", day("2026-09-14"))]
         and hsrc[MB, "mm"] == [("htree_announcement", day("2026-09-14") + 30)]
         and hrow[MA, "mm"]["repo_key"] == hrow[MB, "mm"]["repo_key"]
         and hsrc[M, "m"] == [("state", day("2026-09-12"))]
         and sorted(hsrc[T, "t"]) == [("htree_announcement", day("2026-09-12")), ("state", day("2026-09-05"))]
         and hsrc[O, "o"] == []
         and [(p["source"], p["event_id"]) for p in hpushes if p["pubkey"] == Z] == [("htree_announcement", "f" * 64)]
         and [hrow[k]["cls"] for k in ((H, "h"), (M, "m"), (T, "t"))] == ["hashtree", "grasp_mixed", "hashtree"]
         and (hrow[H, "h"]["last_state"], hrow[H, "h"]["last_push"], hrow[H, "h"]["pushes_total"])
         == (None, day("2026-09-20"), 4)
         and (hrow[T, "t"]["last_state"], hrow[T, "t"]["last_push"]) == (day("2026-09-05"), day("2026-09-12"))),
        ("a hashtree push row: push and changed, every other flag false or 0, no branch or tag count; the "
         "push figures count it, pushes_hashtree_30d counts it alone, newest_state reads states only",
         all(p["push"] and p["changed"] and not p["head_changed"] and not p["default_moved"]
             and p["branches"] is None and p["tags"] is None and p["heads_moved"] == p["tags_moved"] == 0
             for p in hpushes if p["source"] == "htree_announcement")
         and {p["source"] for p in hpushes} == set(SOURCES)
         and (hdoc["pushes_hashtree_30d"], hdoc["pushes_30d"], hdoc["pushes_7d"], hdoc["pushes_total"])
         == (10, 12, 4, 14)
         and (hdoc["repos_with_state"], hdoc["repos_pushed_30d"], hdoc["newest_state"], hdoc["newest_push"])
         == (2, 7, "2026-09-12", "2026-09-20")
         and HTREE_PUSH_GAP_S == 120
         and doc["pushes_hashtree_30d"] == 0 and {p["source"] for p in pushes} == {"state"}),
        ("npub decoding (two published pairs, one corrupted)",
         npub_hex(F_NPUB) == F and npub_hex(F_NPUB[:-1] + "q") is None and
         npub_hex("npub10xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqpkge6d") == G_KEY),
        ("reciprocal pair -> one repository, earliest first-seen leads",
         row[(A, "r1")]["repo_key"] == row[(B, "r1")]["repo_key"] == f"{A}:r1" and row[(A, "r1")]["is_lead"]),
        ("one-way pair -> two repositories", row[(C, "r2")]["repo_key"] != row[(D, "r2")]["repo_key"]),
        ("unique self-lead beats the earlier member (npub-listed)", row[(F, "r3")]["repo_key"] == f"{E}:r3"),
        ("repos / multi_maintainer_repos / shared_listed_repos = 30 / 3 / 4",
         (doc["repos"], doc["multi_maintainer_repos"], doc["shared_listed_repos"]) == (30, 3, 4)),
        ("peeled-tag republish is not a change", by[C][1]["changed"] is False and by[C][1]["push"] is False),
        ("first version counts its own refs; a 1-element ref tag is ignored",
         (by[C][0]["branches_added"], by[C][0]["tags_added"], by[C][0]["branches"]) == (1, 1, 1)),
        ("moved default branch", by[C][2]["heads_moved"] == 1 and by[C][2]["default_moved"] is True),
        ("no daily cap: all 51 pushes of repository {A, B} on one UTC day count, across both members, in the "
         "ledger (no counted column), the repository's row and its week",
         len(ab) == 51 and all(p["push"] for p in ab) and not any("counted" in p for p in pushes)
         and "counted" not in PUSH_COLS and row[(A, "r1")]["pushes_total"] == row[(B, "r1")]["pushes_30d"] == 51
         and sw["pushes"]["2026-09-14"] == {"pushes": 51, "repos": 1, "maintainers": 2}),
        ("the ledger has exactly the reference's keys: no daily_cap row",
         [e["k"] for e in doc["exclusions"]]
         == FAMILIES + ["state_republish_loop", "orphan_states", "ephemeral", "imported", "mirrored"]),
        ("pre-wall pair: changed but not a push",
         [(p["changed"], p["push"]) for p in by[D]] == [(True, False), (True, False), (True, True)]),
        ("pushes_total 55, pushes_30d 53, pushes_7d 51: every push counts; a state on the run's own day waits for "
         "the next run", (doc["pushes_total"], doc["pushes_30d"], doc["pushes_7d"]) == (55, 53, 51) and not by[P]),
        ("pushes_weekly: ISO weeks from STATE_WALL's (2026-06-08: its state before the wall instant is no push, the "
         "one after is) to the as_of week (2026-09-21), labelled by the Monday, zero-filled; repos and maintainers "
         "distinct within the week; the pushes sum to pushes_total",
         lab(doc["pushes_weekly"]) == weeks("2026-06-08", 16)
         and sw["pushes"]["2026-06-08"] == sw["pushes"]["2026-06-29"] == {"pushes": 1, "repos": 1, "maintainers": 1}
         and sw["pushes"]["2026-08-31"] == {"pushes": 2, "repos": 1, "maintainers": 1}
         and sw["pushes"]["2026-06-15"] == sw["pushes"]["2026-09-21"] == zero("pushes", "repos", "maintainers")
         and sum(r["pushes"] for r in doc["pushes_weekly"]) == doc["pushes_total"]),
        ("new_repos_weekly: ISO weeks of first_seen from ANN_WALL's (2026-04-27, from its first second, days before "
         "the wall instant) to the as_of week, zero-filled; the second before it is in no week, for a repository "
         "and for a maintainer; grasp by today's class, grasp_mixed included; s = each maintainer's earliest KEPT "
         "announcement's week",
         lab(doc["new_repos_weekly"]) == weeks("2026-04-27", 22)
         and sw["repos"]["2026-04-27"] == {"v": 1, "grasp": 0, "other": 1, "s": 1}
         and sw["repos"]["2026-05-04"] == {"v": 1, "grasp": 1, "other": 0, "s": 1}
         and row[(M3, "m3")]["cls"] == "grasp_mixed"
         and sw["repos"]["2026-05-11"] == {"v": 1, "grasp": 0, "other": 1, "s": 0}
         and sw["repos"]["2026-06-29"] == {"v": 18, "grasp": 2, "other": 16, "s": 9}
         and sw["repos"]["2026-07-06"] == {"v": 0, "grasp": 0, "other": 0, "s": 1}
         and sw["repos"]["2026-05-18"] == sw["repos"]["2026-09-21"] == zero("v", "grasp", "other", "s")
         and (sum(r["v"] for r in doc["new_repos_weekly"]), sum(r["s"] for r in doc["new_repos_weekly"]))
         == (doc["repos"] - 1, doc["maintainers"] - 1)),
        ("new_repos_weekly.by_host: every row has grasp, hashtree, forge, self_hosted and none in that order, zeros "
         "included; 2026-08-03 counts each repository once under every kind in its R (3 GRASP + forge, one of them "
         "the union of two members' clones, 1 GRASP + hashtree, 1 hashtree + self-hosted, 2 self-hosted + forge: "
         "grasp 4, hashtree 2, forge 5, self_hosted 3; 1 with nothing reachable, kept by its state: none 1); "
         "2026-06-29 single kinds, 2026-05-04 one repository under both grasp and forge; a zero-filled week five "
         "zeros",
         all([e.get("k") for e in bh[r["w"]]] == kinds for r in doc["new_repos_weekly"])
         and bh["2026-08-03"] == hosts(4, 2, 5, 3, 1)
         and sw["repos"]["2026-08-03"] == {"v": 8, "grasp": 4, "other": 4, "s": 2}
         and row.get((K2, "grasp-forge-2"), {}).get("repo_key") == f"{K}:grasp-forge-2"
         and bh["2026-06-29"] == hosts(2, 0, 14, 2, 0) and bh["2026-05-04"] == hosts(1, 0, 1, 0, 0)
         and bh["2026-05-18"] == hosts(0, 0, 0, 0, 0) and row.get((K, "none-0"), {}).get("has_state") is True),
        ("new_repos_weekly.by_host: in every row, on a Friday and on a Monday run, grasp's v = the row's grasp and "
         "the entries sum to at least the row's v (R is read apart from the row's tally, so this checks one against "
         "the other)",
         all(sum(e.get("v", 0) for e in r.get("by_host") or []) >= r["v"]
             and sum(e.get("v", 0) for e in r.get("by_host") or [] if e.get("k") == "grasp") == r["grasp"]
             for rows in (doc["new_repos_weekly"], doc4["new_repos_weekly"]) for r in rows)),
        ("comments_total 13, comments_90d 4, comments_30d 2: whole UTC days ending on as_of_day, counted comments "
         "only (1 ephemeral, 1 imported and a mirror key's 50 are not); a comment on the run's own day waits for "
         "the next run",
         (doc["comments_total"], doc["comments_90d"], doc["comments_30d"]) == (13, 4, 2)
         and [next(e["v"] for e in doc["exclusions"] if e["k"] == k) for k in ("ephemeral", "imported", "mirrored")]
         == [1, 1, 50]),
        ("comments_weekly: ISO weeks from COMMENT_WALL's (2026-06-15, from its first second; a comment of that week "
         "before the wall instant counts) to the as_of week, zero-filled; counted comments only (2026-07-06's "
         "imported and mirrored ones are not); roots by the FIRST K, threads by the FIRST E; authors and threads "
         "distinct within the week (Y2 and T1 count again the next week); the second before the start week is in "
         "no week",
         lab(doc["comments_weekly"]) == weeks("2026-06-15", 15)
         and sw["comments"]["2026-06-15"] == dict(zero(*ROOTS), prs=2, patches=1, repo=1, authors=2, threads=2)
         and sw["comments"]["2026-06-22"] == dict(zero(*ROOTS), issues=3, repo=1, other=1, authors=3, threads=2)
         and sw["comments"]["2026-07-06"] == zero(*ROOTS, "authors", "threads")
         and sw["comments"]["2026-08-24"] == dict(zero(*ROOTS), issues=2, authors=1, threads=0)
         and sw["comments"]["2026-09-07"] == zero(*ROOTS, "authors", "threads")  # its one comment is ephemeral
         and sw["comments"]["2026-09-21"] == dict(zero(*ROOTS), issues=1, authors=1, threads=0)
         and sum(r[k] for r in doc["comments_weekly"] for k in ROOTS) == doc["comments_total"] - 1),
        ("grasp_lists.per_week: users (a non-empty CURRENT list) by the ISO week of their list's earliest version, "
         "empty or not, from STATE_WALL's week (2026-06-08; days before the wall instant count) to the as_of week, "
         "zero-filled; the second before it is in no week",
         lab(doc["grasp_lists"]["per_week"]) == weeks("2026-06-08", 16) and doc["grasp_list_users"] == 4
         and {k: x.get("v") for k, x in sw["grasp"].items() if x.get("v")} == {"2026-06-08": 1, "2026-06-29": 1,
                                                                                "2026-09-21": 1}),
        ("recency buckets sum to repos_with_state",
         doc["repos_with_state"] == 5 and part["recency"] == {"30d": 2, "90d": 1, "180d": 1, "365d": 1, "older": 0}),
        ("horizon: own-day and future versions dropped, only > now_ts future_dated; year 1 past_dated",
         (X, "today") not in row and doc["anomalies"]["future_dated"]["announcements"] == 1
         and doc["anomalies"]["past_dated"]["announcements"] == 1
         and (row[(X, "old")]["first_seen"], row[(X, "old")]["versions"]) == (day("2026-07-01"), 1)),
        ("N0: a 201-character d or a control character is counted and has no row; 200 and a space are kept",
         not any(r["d"].startswith("n" * 200) or "\x1f" in r["d"] for r in repos)
         and row.get((X, "m" * 200), {}).get("family") == row.get((X, "ok id"), {}).get("family") == "repo"
         and doc["announcements_total"] == 45 and next(e["v"] for e in doc["exclusions"] if e["k"] == "N0") == 2),
        ("GRASP: a :port keeps the shape; an scp URL on a GRASP host is grasp and counts per repo",
         doc["grasp_servers"] == 1 and row[(Q, "q1")]["cls"] == row[(P, "p1")]["cls"] == "grasp_only"
         and row[(Q, "q1")]["grasp"] is True and part["grasp_per_repo"]["1"] == 7),
        ("tools: contains and labels on the lowercased client",
         (row[(A, "r1")]["tool"], row[(E, "r3")]["tool"]) == ("gitplaza", "NostrHub")),
        ("stars, kind-7 (reactions): by `a`, through `e` (surviving versions only), member -> repository; `-`, "
         "unknown and a malformed `a` (no `e` fallback) dropped; the kind-7 (reactions) rule alone counts them alone",
         pairs7 == {(X, f"{C}:r2"), (X, f"{A}:r1"), (Y, f"{D}:r2")}
         and (docK7["stars_total"], docK7["stars_repos"]) == (3, 3)),
        ("stars, kind-17 (external-content-reactions): admitted by a `k` git-repo AND a 30617: `i` (either alone, or "
         "a `k` of 30617, is not); the target the FIRST 30617: `i` value (a URL `i` ahead of it skipped; one naming "
         "no announcement is the target over a later member's; one without a second colon drops the row, no later "
         "value standing in); each key's CURRENT reaction per coordinate in version order (+ - + a star, + - none; "
         "a same-second tie the lowest id's, inserted last or first); content trimmed by str.strip() (` +\\n`, "
         "empty, U+001F, U+0085 and U+3000 around `+`: stars; U+FEFF ahead of `+`, kept, and `++`: none); `d` "
         "everything after the second colon, so r2:x is not r2; per coordinate, so a `-` on another member "
         "coordinate of the repository withdraws nothing, and per repository, so stars of two member coordinates "
         "are one pair; the owner's own counts, an N4 coordinate's does not",
         pairs17 == {(IR["pmp"], f"{C}:r2"), (IR["trim"], f"{D}:r2"), (IR["empty"], f"{D}:r2"),
                     (IR["strip"], f"{D}:r2"), (IR["tie-plus"], f"{C}:r2"), (IR["member"], f"{A}:r1"),
                     (IR["two-members"], f"{A}:r1"), (IR["url"], f"{D}:r2"), (X, f"{C}:r2"),
                     (IR["own-day"], f"{E}:r3"), (IR["future"], f"{E}:r3"), (C, f"{C}:r2")}),
        ("stars of both kinds: stars_total the distinct (pubkey, repository) pairs over both (X's kind-7 (reactions) "
         "and kind-17 (external-content-reactions) stars of C:r2 are one; X's kind-17 (external-content-reactions) "
         "`-` on A:r1 leaves its kind-7 (reactions) star of A:r1); stars_iris the "
         "kind-17 (external-content-reactions) pairs alone, that shared pair included; stars_repos over both",
         (doc["stars_total"], doc.get("stars_iris"), doc["stars_repos"]) == (14, 12, 4)),
        ("kind-17 (external-content-reactions) preprocessing: archive_events counts the admitted rows that survive "
         "id-dedup (a copy ingested an hour later is one row) and the horizon, 29, a row whose target has no "
         "second colon included (it stays in the input); a `-` on the run's own day waits (a star today, none on "
         "the Monday run); in the external_reactions anomaly slots the future `-`, a future row whose target has "
         "no second colon, and the past-dated `+`, the unadmitted future row in neither",
         doc["archive_events"].get("external_reactions") == 29
         and (doc["anomalies"]["future_dated"].get("external_reactions"),
              doc["anomalies"]["past_dated"].get("external_reactions")) == (2, 1)
         and (doc4.get("stars_iris"), doc4["stars_total"], doc4["archive_events"].get("external_reactions"))
         == (11, 13, 30)),
        ("the kind-7 (reactions) rule alone: no stars_iris, no external_reactions in archive_events or in either "
         "anomaly block, and nothing else differs from the two-kind document but stars_total and stars_repos",
         "stars_iris" not in docK7 and "external_reactions" not in docK7["archive_events"]
         and not any("external_reactions" in docK7["anomalies"][s] for s in ("future_dated", "past_dated"))
         and without_iris(docK7) == without_iris(doc)),
        ("--s3 over a local tree of the bucket's layout: the same documents under either rule; under the two-kind "
         "rule a kind-17 (external-content-reactions) archive with no row ingested by the pin, or without Parquet "
         "(before its first flush), stops the run naming the prefix; the kind-7 (reactions) rule alone needs none",
         doc_s3 == doc and doc_s3_7 == docK7
         and isinstance(early, str) and f"no row ingested by --ingested-before {instant('2026-06-01T00:00:00Z')} "
                                        f"in 1 Parquet file(s) under {tree}/c=regular/k=17/" in early
         and isinstance(absent, str) and f"no Parquet under {tree}/c=regular/k=17/" in absent
         and all("kind-17 (external-content-reactions): " in x for x in (early, absent))),
        ("zaps: LAST bolt11 / description, 1e10 cap, > 2.1e10 and unreadable dropped, 0 msats kept; an unlisted "
         "signer, a non-member FIRST `p` and a request amount mismatch dropped; another member, an empty amount kept",
         (doc["zap_receipts"], doc["zap_msats"], doc["zappers"], doc["zaps_repos"]) == (8, 10_004_400_000, 5, 3)),
        ("hostile zaps: an overflowing or over-long bolt11 is dropped, a 12,000-deep or over-long "
         "description counts without a zapper, and nothing raises", doc["zap_receipts"] == 8),
        ("N6: at/after the current version; re-announcement restarts; others' requests ignored",
         [row[(X, d)]["family"] for d in ("gone", "back", "tie", "other")] == ["N6", "repo", "N6", "repo"]),
        ("N6 triples: a future-dated newer request is dropped, the surviving older one decides",
         [row[(X, d)]["family"] for d in ("fut", "late")] == ["repo", "repo"]
         and doc["archive_events"]["deletions"] == 4),
        ("--heavy-from-extracts: the same stars, zaps, deletions and ledger from gz NDJSON parts",
         all(doc2[k] == doc[k] for k in ("archive_events", "exclusions", "stars_total", "stars_repos", "stars_iris",
                                         "zap_receipts", "zap_msats", "zappers", "zaps_repos"))),
        ("--heavy-from-extracts: a kind with no parts under the run id stops the run, naming the kind, the run "
         "id and the prefix; --allow-empty-heavy loads it as empty",
         gap is not None and "run2" in gap and "7 days" in gap and HEAVY["rx"] not in gap
         and all(f"{HEAVY[n]} under {td}/k={KINDS[n][0]}/run2-k{KINDS[n][0]}-ngit/" in gap for n in ("zp", "dl"))
         and (doc3["stars_total"], doc3["zap_receipts"], doc3["archive_events"]["deletions"])
         == (doc["stars_total"], 0, 0)),
        ("GRASP host set: a GRASP-shaped path on a forge, on a general relay or on any of the four hosts shown not "
         "to be GRASP servers adds no host; the forge stays one, the others read self_hosted; git.gittr.space "
         "is a forge (section 4.2), and so is a non-network value on it, by the same precedence",
         doc["grasp_servers"] == 1
         and (row[(W, "gh-shaped")]["cls"], row[(W, "relay-shaped")]["cls"]) == ("forge_only", "other_hosts")
         and sorted((e["host"], e["cls"]) for e in json.loads(row[(W, "gh-shaped")]["hosts"]))
         == [("git.gittr.space", "forge"), ("github.com", "forge")]
         and host_class(parse_url(f"nostr://git.gittr.space/npub1{'q' * 58}/w"), set()) == "forge"),
        ("private: an IPv4 in a nip.io / sslip.io name, dotted or dashed, is private (N8a); a public one is not",
         [row[(W, d)]["family"] for d in ("nip-private", "sslip-private", "nip-public")] == ["N8a", "N8a", "repo"]
         and next(e["v"] for e in doc["exclusions"] if e["k"] == "N8a") == 2
         and not is_private("10.0.0.256.nip.io")),
        ("N8b: no REACHABLE clone value (a private and a non_network one, or none at all) and no state",
         [row[(W, d)]["family"] for d in ("unreachable", "no-clone")] == ["N8b", "N8b"]
         and next(e["v"] for e in doc["exclusions"] if e["k"] == "N8b") == 3),
        ("N10: the nostr.net catalog key's announcement is excluded, keyed on the pubkey; one of its announcements "
         "with no clone value is N8b first, and a stateless personal fork of it N9 (N10 is the last rule)",
         [row[(CATALOG_KEY, d)]["family"] for d in ("cat-forge", "cat-web-only", "cat-fork")] == ["N10", "N8b", "N9"]
         and next(e["v"] for e in doc["exclusions"] if e["k"] == "N10") == 1),
        ("tools: a client blank after trimming counts as no client",
         row[(W, "blank-client")]["tool"] == "Shakespeare"),
        ("substrate name: control and format characters (Cc: NUL; Cf: a bidi override, a zero-width space) removed",
         row[(W, "blank-client")]["name"] == "abcd"),
        ("trimming is str.strip(): U+001C to U+001F go too, so the clone value still parses",
         row[(W, "ctl-trim")]["cls"] == "forge_only"
         and json.loads(row[(W, "ctl-trim")]["hosts"]) == [{"host": "codeberg.org", "cls": "forge"}]),
        ("provider_signers.yaml: every key of the real file, and the fixture's quoted and commented ones, are "
         "read; an unparsable list, a bad key, no key or no file stops the run",
         signers == {S1, S2} and all(stopped)
         and real_signers == set(re.findall(r"\b[0-9a-f]{64}\b", SIGNERS.read_text(encoding="utf-8")))),
        ("--compare: comments_90d and the three weekly series are not compared against a document from before "
         "their first runs, and a line names each; on both sides they are compared, a series cell by cell, and a "
         "week one side lacks differs in each field and in the sequence; a document generated on or after a first "
         "run without its key is a difference (comments_90d 2026-10-02, the series 2026-10-03)",
         diffs[:6] == [(0, 4), (1, 0), (1, 3), (4, 0), (1, 0), (4, 0)]
         and all(f"summary.{k}: only in {td}/mine, not compared" in said.getvalue()
                 for k in OPTIONAL_UNTIL if k not in IRIS_KEYS)
         and f"summary.comments_90d: absent from {td}/late, generated 2026-10-02" in said.getvalue()
         and f"summary.grasp_lists.per_week: absent from {td}/later, generated 2026-10-03" in said.getvalue()
         and "summary.grasp_lists.per_week.2026-06-08: 1 | 2" in said.getvalue()
         and "summary.pushes_weekly.2026-06-15.pushes: 0 | None   (absent on one side)" in said.getvalue()),
        ("--compare: a served document's weekly series come from its top level, grasp_lists.per_week alone of its "
         "grasp_lists; the monthly series and the rest of grasp_lists are not compared", diffs[6] == (0, 0)),
        ("--compare: each series is also ONE sequence of weeks: zero rows ahead of the real ones (keyed rows alone "
         "let the last row win: no difference), the weeks reversed, and a week only the served side has (paths "
         "only on that side, in each field and in the sequence) are differences",
         diffs[7:10] == [(1, 0), (1, 0), (4, 0)] and "summary.comments_weekly.@weeks: " in said.getvalue()
         and "summary.pushes_weekly.2026-09-28.pushes: None | 0   (absent on one side)" in said.getvalue()),
        ("--compare: new_repos_weekly's by_host is not compared against a document lacking it generated the last "
         "second before its first day (2026-10-09), and a line names it; one generated that day's first second "
         "without it differs; on both sides one kind off by one differs, naming the week and the kind, and so do "
         "a week's entries reversed (the sequence of keys; the top-level hosting and no_state.by_class reversed do "
         "not) and a zero entry lacking (in the entry, as no 0, and in the sequence); a document without the whole "
         "series is judged on the series alone",
         diffs[11:16] == [(0, 1), (1, 0), (1, 0), (1, 0), (2, 0)]
         and said.getvalue().count(f"summary.new_repos_weekly.*.by_host: only in {td}/mine, not compared") == 1
         and f"summary.new_repos_weekly.*.by_host: absent from {td}/hostlate, generated 2026-10-09, on or after its "
             f"first run (2026-10-09)" in said.getvalue()
         and "summary.new_repos_weekly.2026-08-03.by_host.hashtree: 2 | 3\n" in said.getvalue()
         and "summary.new_repos_weekly.2026-08-03.by_host.@keys: " in said.getvalue()
         and "summary.new_repos_weekly.2026-05-04.by_host.none: 0 | None   (absent on one side)" in said.getvalue()
         and "summary.new_repos_weekly.2026-05-04.by_host.@keys: " in said.getvalue()),
        ("--compare: a dated optional key absent from a document whose generated_utc is missing or unreadable "
         "stops the compare (MissingInput) instead of being skipped, a week's by_host too; with every key present "
         "no date is needed",
         len(halts) == 3 and all(h is not None and "is missing or unreadable" in h for h in halts)
         and "summary.new_repos_weekly.*.by_host: absent from" in (halts[2] or "")
         and diffs[10] == (0, 0)),
        ("--compare, STARS: against the kind-7 (reactions) row of 2026-10-11 03:18:08 UTC the four keys are not "
         "compared, a line naming each, nor are stars_total and stars_repos (other stars), a line naming each; a "
         "derivation by that rule re-checks it with zero differences and nothing skipped; from 2026-10-12 a "
         "document lacking them differs in each key and its stars are compared; each key off by one differs; a "
         "document carrying some of the four differs once per key it lacks, on 2026-10-11 too, its stars compared "
         "when it carries stars_iris and not when it lacks it (kind-7 (reactions) stars alone); one lacking them "
         "without a readable generated_utc stops the compare",
         idiffs == [(0, 6), (0, 0), (6, 0), (4, 0), (1, 0), (1, 2)]
         and all(f"summary.{k}: only in {td}/mine, not compared" in said.getvalue() for k in IRIS_KEYS)
         and all(f"summary.{k}: not compared: {td}/iold lacks stars_iris and predates 2026-10-12"
                 in said.getvalue() for k in STARS_RULED)
         and all(f"summary.{k}: absent from {td}/ilate, generated 2026-10-12, on or after its first run (2026-10-12)"
                 in said.getvalue() for k in IRIS_KEYS)
         and "summary.stars_total: 14 | 3\n" in said.getvalue() and "summary.stars_repos: 4 | 3\n" in said.getvalue()
         and "summary.stars_iris: 12 | 13\n" in said.getvalue()
         and "summary.anomalies.past_dated.external_reactions: 1 | 2\n" in said.getvalue()
         and f"summary.archive_events.external_reactions: absent from {td}/itorn, which carries stars_iris, "
             f"anomalies.future_dated.external_reactions" in said.getvalue()
         and f"summary.stars_iris: absent from {td}/itorn2, which carries archive_events" in said.getvalue()
         and isinstance(ihalt, str) and "is missing or unreadable" in ihalt),
        ("STARS: the export picks the rule (section 6.2: a document without stars_iris counts kind-7 (reactions) "
         "stars alone): carrying stars_iris (flat or served), both kinds; without it before 2026-10-12, the "
         "kind-7 (reactions) rule, even while it carries another of the four keys; without it from 2026-10-12 (a "
         "regression), both kinds; without it and undated, the run stops (MissingInput)",
         rules == [True, True, False, True, False] and isinstance(rule_halt, str)
         and "summary.stars_iris: absent from" in rule_halt and "is missing or unreadable" in rule_halt),
        ("STARS through main(): compared against the kind-7 (reactions) row of 2026-10-11 the derivation is by that "
         "rule and reads ZERO differences with nothing skipped; against a document carrying stars_iris, by both "
         "kinds, likewise; against a 2026-10-12 one lacking the keys, by both kinds, 6 differences (exit 1); against "
         "an undated one it stops (exit 2) before writing anything",
         [v[0] for v in vias.values()] == [0, 0, 1, 2]
         and "[stars] kind-7 (reactions) stars alone, without the four keys" in vias["iold"][2]
         and "[stars] kind-7 (reactions) and kind-17 (external-content-reactions) stars" in vias["same"][2]
         and "a regression" in vias["ilate"][2]
         and all("[compare] ZERO differences (0 summary fields not compared)" in vias[n][1] for n in ("iold", "same"))
         and "[compare] 6 differences (0 summary fields not compared)" in vias["ilate"][1]
         and "[FATAL] summary.stars_iris: absent from" in vias["inogen"][2] and not via_wrote),
        ("--duckdb under the two-kind rule: a local copy without e17, compared against a document carrying "
         "stars_iris, stops (exit 2) naming the table, and an e17 with no row ingested by the pin stops too; the "
         "same copy without e17, compared against the kind-7 (reactions) row of 2026-10-11, is derived by that rule "
         "and reads ZERO differences",
         [vias7[n][0] for n in ("same", "iold")] == [2, 0]
         and "[FATAL] kind-17 (external-content-reactions): no table e17 in the local copy (--duckdb)"
             in vias7["same"][2]
         and "[stars] kind-7 (reactions) stars alone, without the four keys" in vias7["iold"][2]
         and "[compare] ZERO differences (0 summary fields not compared)" in vias7["iold"][1]
         and isinstance(local_early, str)
         and f"no row ingested by --ingested-before {instant('2026-06-01T00:00:00Z')} in table e17 of the local copy"
             in local_early),
        ("--compare: an export that still has the counted column (dropped by the deploy of 2026-10-02) differs "
         "once, naming it",
         csv_diffs == [0, 1] and f"ngit_pushes.csv: 1 column(s) only in {td}/old, which the reference does not "
                                 f"define: counted (dropped with the daily cap by the nostrology-data deploy of "
                                 f"2026-10-02" in said.getvalue()),
        ("exit status through main(): 0 no difference, 1 differences (compare alone), 2 could not run: a missing "
         "directory, a summary.json holding a JSON array and a pushes CSV without event_id (each with its "
         "traceback), and a document lacking a dated key without a readable generated_utc ([FATAL])",
         [c for c, _ in codes] == [0, 1, 2, 2, 2, 2] and all("Traceback" in e for _, e in codes[2:5])
         and "[FATAL] summary.comments_90d: absent from" in codes[5][1]),
        ("as_of end: on a Monday run (as_of_day the Sunday 2026-09-27) every series ends with the week of "
         "2026-09-21, and a comment of the run's own Monday (00:30) is in no week and in no total",
         all(lab(rows)[-1:] == ["2026-09-21"] for rows in (doc4["pushes_weekly"], doc4["new_repos_weekly"],
                                                           doc4["comments_weekly"], doc4["grasp_lists"]["per_week"]))
         and next((r for r in doc4["comments_weekly"] if r["w"] == "2026-09-21"), {}).get("issues") == 2
         and sum(r[k] for r in doc4["comments_weekly"] for k in ROOTS) == doc4["comments_total"] - 1 == 13),
        ("block(): the longest optional key covering a path decides, whatever order the keys come in; a `*` "
         "segment matches exactly one segment, a week",
         block("grasp_lists.per_week.2026-06-08", ("grasp_lists", "grasp_lists.per_week")) == "grasp_lists.per_week"
         and block("grasp_lists.length.1") == "grasp_lists" and block("comments_90d") == "comments_90d"
         and block("new_repos_weekly.2026-08-03.by_host.hashtree") == "new_repos_weekly.*.by_host"
         and block("new_repos_weekly.2026-08-03.by_host.@keys") == "new_repos_weekly.*.by_host"
         and block("new_repos_weekly.2026-08-03.grasp") == block("new_repos_weekly.by_host.none") == "new_repos_weekly"
         and block("new_repos_weekly.@weeks") == "new_repos_weekly"),
    ]
    for label, ok in checks:
        print(f"  {'ok  ' if ok else 'FAIL'} {label}")
    print(f"[self-test] {sum(ok for _, ok in checks)}/{len(checks)} passed")
    return 0 if all(ok for _, ok in checks) else 1


def main(argv=None):
    """The command line; returns the exit status. 1 means differences and comes from compare()
    alone (section 9.1), and an uncaught exception would also exit 1, so a crash is caught here:
    its traceback is printed and it exits 2, "could not run", like a MissingInput ([FATAL]).
    argparse exits 2 on a bad argument by itself."""
    try:
        return run(argv)
    except MissingInput as e:
        print(f"[FATAL] {e}", file=sys.stderr)
    except Exception:  # any crash: a missing directory, a summary.json that is no object, a CSV without its key
        traceback.print_exc()
    return 2


def run(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--duckdb", metavar="PATH", help="local DuckDB copy (opened read-only)")
    src.add_argument("--s3", action="store_true", help="read the raw archive on S3")
    ap.add_argument("--now", help="now_ts (ISO-8601 or epoch seconds; the pinned comparison passes the snapshot "
                    "row's generated_utc; default: this checker's own start)")
    ap.add_argument("--ingested-before", help="the ingest cutoff, inclusive: rows with ingested_at equal to the "
                    "cutoff are kept, later rows dropped (ISO-8601 or whole epoch seconds; one instant for every "
                    "kind; the pinned comparison passes the Dagster run's START; --s3 default: this checker's "
                    "own start)")
    ap.add_argument("--heavy-from-extracts", metavar="RUN_ID", help="read kind-7 (reactions), "
                    "kind-9735 (zap-receipts) and kind-5 (event-deletion-requests) from the run's UNLOAD parts "
                    "(an S3 lifecycle rule deletes them 7 days after the run; a kind with none stops the check "
                    "with exit 2)")
    ap.add_argument("--allow-empty-heavy", action="store_true", help="with --heavy-from-extracts: load a kind "
                    "with no UNLOAD parts as EMPTY instead of stopping (N6 and every social figure then differ)")
    ap.add_argument("--extracts-base", default=None, help="where the UNLOAD parts live "
                    "(default: s3://$NOSTROLOGY_EXTRACTS_BUCKET)")
    ap.add_argument("--signers", metavar="PATH", default=str(SIGNERS), help="section 6.3's verified LNURL "
                    "provider signer keys, a provider_signers.yaml (default: %(default)s)")
    ap.add_argument("--out", help="output directory (derivation target, or the left side of --compare)")
    ap.add_argument("--compare", metavar="DIR", help="diff --out against DIR; exit 1 on any difference (with "
                    "--duckdb or --s3, DIR's summary.json first picks the stars rule: STARS)")
    ap.add_argument("--self-test", action="store_true", help="run the synthetic cases and exit")
    args = ap.parse_args(argv)
    pin = int(time.time())  # ONE run-start instant, taken before any extract
    if args.self_test:
        return self_test()
    if not args.out:
        ap.error("--out is required")
    if args.allow_empty_heavy and not args.heavy_from_extracts:
        ap.error("--allow-empty-heavy only applies with --heavy-from-extracts")
    if args.duckdb or args.s3:
        heavy = (((args.extracts_base or extracts_root()).rstrip("/"), args.heavy_from_extracts, args.allow_empty_heavy)
                 if args.heavy_from_extracts else None)
        mode = "s3" if args.s3 else "duckdb"
        now_ts = instant(args.now) if args.now else pin
        cutoff = instant(args.ingested_before) if args.ingested_before else (pin if mode == "s3" else None)
        print(f"[run] {mode} now={dt.datetime.fromtimestamp(now_ts, dt.timezone.utc).isoformat()} "
              f"ingested_before={cutoff} heavy_from_extracts={args.heavy_from_extracts}", file=sys.stderr)
        signers = load_signers(args.signers)  # before any read: a bad list stops the run at once (main: exit 2)
        print(f"[signers] {len(signers)} LNURL provider signer keys from {args.signers}", file=sys.stderr)
        # STARS: the export picks section 6.2's stars rule, before any archive read (main: exit 2 if it cannot)
        iris, why = stars_rule(args.compare) if args.compare else (True, "no --compare, the current rule")
        print("[stars] " + ("kind-7 (reactions) and kind-17 (external-content-reactions) stars, with the four keys"
                            if iris else "kind-7 (reactions) stars alone, without the four keys") + f": {why}",
              file=sys.stderr)
        os.makedirs(args.out, exist_ok=True)
        con = connect(mode, args.duckdb, remote=mode == "s3" or bool(heavy and heavy[0].startswith("s3://")))
        repo_rows, push_rows, doc = derive(con, mode, now_ts, cutoff, args.out, heavy=heavy, signers=signers,
                                           iris=iris)
        write_outputs(args.out, repo_rows, push_rows, doc)
        series = SERIES + ("grasp_lists",)  # one line each: indented, the weekly rows run to hundreds of lines
        print(json.dumps({k: v for k, v in doc.items() if k != "exclusions" and k not in series}, indent=1))
        print("exclusions: " + ", ".join(f"{e['k']}={e['v']} ({e['unit']})" for e in doc["exclusions"]))
        for k in series:
            print(f"{k}: {json.dumps(doc[k], separators=(',', ':'))}")
    elif not args.compare:
        ap.error("give --duckdb PATH, --s3, --compare DIR or --self-test")
    return compare(args.out, args.compare) if args.compare else 0


if __name__ == "__main__":
    sys.exit(main())

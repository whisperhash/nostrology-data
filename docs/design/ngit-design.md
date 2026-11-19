# The /git surface - design

**Living design doc** for the Git-on-Nostr (NIP-34 (git collaboration)) data surface end to
end: from the raw events in the nozzl S3 archive, through the nightly Dagster compute, to the
serving contract the page reads and the page itself. Built 2026-09-25. Revised in place after
each review; it describes the current state, not its history. The page is "Git on Nostr" at
`/git` (it was `/ngit` until 2026-09-29, fork 21); the job, the tables, the documents and the
code keep the name ngit.

Companions, each owning a different layer:

- `docs/dataset-reference/ngit-dataset-reference.md` - the METRIC SEMANTICS, rule by rule: the
  text the pipeline implements AND the text the independent cross-check is written from
  (section 11). Where this doc and the reference disagree, the reference wins and this doc
  is wrong.
- [dagster-orchestration-design.md](dagster-orchestration-design.md) ("The /git surface") - how
  this job fits the whole Dagster orchestration.

## 1. What this surface is, in one paragraph

NIP-34 (git collaboration) puts the *coordination layer* of a git repository on Nostr: a
maintainer announces a repository - kind-30617 (repo-announcements) - with its clone URLs,
collaboration relays and maintainers; every push republishes the repository's branch and
tag tips - kind-30618 (repo-states); users declare which GRASP servers (git hosts that are
also Nostr relays) they want their repositories on - kind-10317 (grasp-server-lists).
Issues, patches and pull requests are regular events, archived since 2026-10-09 but not read
yet (section 13.1); their replies - kind-1111 (comments) - are read, as are stars - kind-7
(reactions) and, since 2026-10-11, Iris Git's kind-17 (external-content-reactions) - and zaps -
kind-9735 (zap-receipts). Relays keep only the newest version of an addressable event, so nobody but
an all-versions archive can say how often a repository is pushed, when its state last
moved, or how its hosting changed. That is the surface's edge: **push cadence, active
repositories, and the share hosted on GRASP or hashtree** (GRASP's alone until 2026-10-10)
are numbers nothing else on the network publishes.

Questions the page answers, in order of the pills: how big is Git-on-Nostr and how alive is
it (Overview); which repositories are most active, what they are tagged with and how many
branches and tags they carry (Repositories); where the code is hosted and which relays carry
the collaboration (Hosting); who maintains it and who has adopted GRASP (Maintainers); how
much issue / PR / patch discussion there is and how repositories are starred and zapped
(Collaboration).

## 2. Shape of the whole thing

```
  relays (relay.ngit.dev scoped to the three repo kinds; nos.lol / damus / primal for all;
          relay.zapstore.dev for zap receipts + deletion requests, relay.fountain.fm for zap receipts;
          the current list, with every later addition, is the dataset reference's section 0 item 1)
     |
     v  nozzl producer (announcements captured since 2026-05-02, states + grasp lists
     |  since 2026-06-14, comments since 2026-06-18, reactions 06-19, zap receipts 06-14,
     |  deletion requests 07-01, external-content reactions 10-11; the dev archive was seeded
     |  from an earlier deployment)
  s3://$NOZZL_ARCHIVE_BUCKET  (Parquet, c=<class>/k=<kind>/p=<shard>/dt=<ingest-date>)
     |
     |  nightly 03:15 UTC (ngit_daily) or the audited manual materialize
     v
  ngit_refresh (Dagster, one run)
     |-- ngit_announcements_extract  k=30617, ALL versions        \
     |-- ngit_states_extract         k=30618, ALL versions         \
     |-- ngit_grasp_lists_extract    k=10317, ALL versions          \  Athena UNLOAD
     |-- ngit_comments_extract       k=1111, git subset (K / A tags) /  (gz NDJSON)
     |-- ngit_reactions_extract      k=7, k=30617-tagged reactions   /
     |-- ngit_external_reactions_extract  k=17, k=git-repo + i=30617:... (Iris Git), since 2026-10-11
     |-- ngit_zaps_extract           k=9735, a=30617:... receipts    /
     |-- ngit_deletions_extract      k=5, self-authored (author, coord, created_at) triples
     |         each: flush WAL -> Glue diff-register -> UNLOAD -> parts check
     v
  ngit_snapshot (compute asset, in-container Python, pipeline/ngit.py)
     |   fetch parts -> parse -> horizon -> noise ledger -> repositories (components) ->
     |   push ledger -> hosting -> maintainers -> collaboration -> build_snapshot()
     |   (display names for the kept maintainers come from the serving table of
     |    kind-0 (profile-metadata), looked up BEFORE the pure build)
     |   -> check_invariants() -> anti-shrink floors -> ONE transaction:
     |        TRUNCATE+INSERT ngit_repos     (one row per announcement coordinate, kept
     |                                        or excluded, with its family and metrics)
     |        TRUNCATE+INSERT ngit_pushes    (every state version of a kept repository,
     |                                        and since 2026-10-12 every hashtree push)
     |        DELETE+INSERT   ngit_repo_stats (one row per kept repository, its stars and
     |                                        zaps; since 2026-10-18)
     |        INSERT          ngit_snapshot  (ONE JSONB document, append-only)
     v
  Aurora (serving DB)
     |
     |  ONE statement (web SQL_NGIT_PAGE): the newest ngit_snapshot row (generated_utc DESC,
     |  id DESC) and the Stars and Zaps lists FROM ngit_repo_stats WHERE run_id = that row's;
     |  the landing board: SELECT data FROM ngit_snapshot ... LIMIT 1
     v
  nostrology-web SvelteKit server (+page.server.ts) -> SSR: the document embedded once
     |
     v
  browser: /git, five pills (each its own address, /git/<pill>) rendering client-side from the one document
```

The two properties every snapshot surface defends hold here too: a failed refresh
can never blank, shrink or tear the page (the page reads only the newest COMPLETE row - and
since 2026-10-18, for its Stars and Zaps lists, that row's own `ngit_repo_stats` rows, in the
same statement - rows are appended, every gate runs before the single write transaction), and
every served number
is defined in one place - the dataset reference - and re-derivable from the raw archive
(section 11).

## 3. Sources

| Kind | Role | Extract | Volume (2026-09-25) |
|---|---|---|---|
| kind-30617 (repo-announcements) | The repository roster: identifier, name, description, clone URLs, relays, maintainers, earliest-unique-commit, topics, client fingerprint. ALL versions, for first-seen dates and the republish-loop rule. | `extract_addressable_all.sql` | 92,321 ids, 6,604 coordinates, 2,660 pubkeys (served row, 2026-09-25); 67% of the ids are one bot |
| kind-30618 (repo-states) | The push ledger: branch and tag tips per maintainer coordinate; a new version per push. ALL versions. | `extract_addressable_all.sql` | 87,648 ids (served row, 2026-09-25; 4,010 coordinates in the dry run); 70.5% of the ids are the same bot |
| kind-10317 (grasp-server-lists) | Users with a GRASP server list: `g` tags in the order written. ALL versions (first-seen dates). | `extract_replaceable_all.sql` (new: id-dedup only, all versions) | 119 ids, 101 pubkeys, 24 of them a test harness |
| kind-1111 (comments) | Discussion on issues, PRs and patches: NIP-22 (comments) replies whose `K` is 1617, 1618, 1619, 1621, 30617 or 30618, or that carry an `A`/`a` repository coordinate. Selected IN Athena. | `extract_ngit_comments.sql` (new) | 7,306 in the git subset (served row, 2026-09-25; the dry run: 7,302 of 314,424 comments); 13% carry a repository coordinate, but after the comment ledger only 3.4% of the COUNTED comments do; 34.6% of the 2,485 captured-era ones (created on or after the comment wall) are declared-ephemeral review bots |
| kind-7 (reactions) | Stars: reactions carrying `["k","30617"]` or an `a` tag `30617:...` (gitworkshop, gittr). Selected in Athena. | `extract_ngit_reactions.sql` (new) | 547 reactions in scope: 400 stars on 178 repositories (served row, 2026-09-25; a 6.5 GB kind: seconds in Athena, hours from a workstation) |
| kind-17 (external-content-reactions) | Iris Git's stars: reactions carrying `["k","git-repo"]` AND an `i` tag `30617:...` (Iris Git, hashtree's web app). Each key's latest reaction per coordinate decides, so a `-` unstars (dataset reference 6.2). Selected in Athena. Read since the deploy of 2026-10-11; the archive holds the kind since 2026-10-11 05:02:39 UTC. | `extract_ngit_external_reactions.sql` (new, 2026-10-11) | 31 rows in scope of the 7,094 the archive holds; in a replay of the 2026-10-11 03:18 UTC run's parts, 14 star pairs on 8 repositories (stars 467 to 481), none also a kind-7 (reactions) star |
| kind-9735 (zap-receipts) | Zaps to repositories: receipts whose `a` tag names a kind-30617 (repo-announcements) coordinate. Selected in Athena. | `extract_ngit_zaps.sql` (new) | 26 receipts, 16,719 sats, 14 zappers, 16 repositories (served row, 2026-09-25) |
| kind-5 (event-deletion-requests) | NIP-09 (deletion requests) self-authored requests naming a repository coordinate: the Athena-side reduction to distinct (author, coord, created_at) triples, as /marketplaces does for listings. | `extract_ngit_deletions.sql` (new) | 328 self-authored (author, coordinate) pairs, which lift N6 from 66 (tags and names alone) to 219 (served row, 2026-09-25); ngit itself never authors one |
| kind-0 (profile-metadata) | Display names for the kept maintainers. Read from the `events_kind_0_profiles` serving table the lightning job loads at 00:45 UTC; not extracted again. | same-DB lookup | 3.06M rows (3,063,398 on 2026-09-25) |

Coverage caveats that frame every number (spec notes C.5): relay.ngit.dev was asked only for
the repository kinds and kind-10317 (grasp-server-lists) until the nozzl reload of 2026-10-01,
which added kind-1111 (comments), kind-7 (reactions) and kind-9735 (zap-receipts) and
backfilled its history for them; before that, comments and reactions came from nos.lol,
relay.damus.io and relay.primal.net alone. Zap receipts also come from relay.zapstore.dev and
relay.fountain.fm, and deletion requests also from relay.zapstore.dev (nozzl's `config/relays.toml`;
kind-5 (event-deletion-requests) stays off relay.ngit.dev, where it is bot noise). Since the
nozzl reload of 2026-10-12 10:17:58 UTC the git announcement index `wss://index.ngit.dev`, a
crawler of the git relays and of hashtree's, is a source of kind-30617 (repo-announcements),
kind-10317 (grasp-server-lists) and kind-5 (event-deletion-requests), held by documents from
10:28:34 UTC that day (154 more repositories, and the deletions of repositories the page still showed;
nozzl#54 (Onboard index.ngit.dev for kind-30617 (repo-announcements))); until then it was "the aggregator, not a source".
Since the nozzl reload of 2026-10-12 14:09:27 UTC relay.nostr.net, a general relay that copies
events from others, is a source of kind-17 (external-content-reactions), kind-30618
(repo-states) and kind-10317 (grasp-server-lists), held by documents from 14:21:52 UTC (12 more
Iris Git stars), and since the reload of 18:55:05 UTC of kind-30617 (repo-announcements),
held by documents from 19:07:36 UTC (one more repository): 408 of the 409 announcement
coordinates it added are the nostr.net catalog of other people's projects, which N10 excludes
(N10 406, N8b 2; section 5;
nozzl#45 (Onboard relay.nostr.net once #32 is fixed)). A GRASP server holds back announcements whose git data never arrived. The numbers are therefore lower bounds on the network, and the page
says so once, in the Overview's "what is counted" panel, rather than under every chart.
relay.ngit.dev itself joined on 2026-06-14, six weeks after announcement capture began, so
about 4% of the repositories first seen in May and June 2026 (28 of 532 and 15 of 608,
measured on 2026-09-25) reached the archive on or after 2026-06-14, dated by their surviving
announcement;
the reference (0.2) and the new-repositories caption say so.

Freshness: each extract flushes its kind's WAL first, so a run sees the archive as of its
own start; the surface is "as of the last complete UTC day" - events created on the run's
own partial day wait for the next run, so every series ends where every total ends.
Events dated after the run instant or before 2023 are dropped and counted (`anomalies`),
never admitted into a series (the /routstr lesson: a skewed clock or a hostile timestamp
would otherwise wedge the invariants every night until that date passes, and a year-1
timestamp would zero-fill twenty thousand months into the document).

## 4. Identity - what a "repository" is

Each maintainer publishes their own kind-30617 (repo-announcements) at the coordinate
`30617:<pubkey>:<d>`; the same `d` published by unrelated authors is unrelated repositories
(ngit.dev: "Two people can independently use the same repository identifier without their
state or permissions being combined"). Three levels, all computed, each with a name:

- **Announcement** = one coordinate `(pubkey, d)`, `d` = the first `d` tag value or `""`.
  The unit of extraction and of the noise ledger.
- **Repository** (the headline unit) = a connected component of same-`d` KEPT
  announcements joined by RECIPROCAL maintainer listings: A and B are joined when A's
  current announcement lists B AND B's current announcement lists A. A listing is read from
  the role tags `M`/`m` when any `M`/`m`/`o` tag is present (an entry is active when its
  tag has fewer than four elements or an odd number of them - the open NIP-34 (git
  collaboration) multi-maintainer PR's role-history grammar), else from the legacy
  `maintainers` tag; values are 64-hex or decoded `npub1...`; junk is ignored. The ledger
  runs BEFORE merging, so an excluded coordinate never joins a repository. This is the
  reading ngit v3 and GitWorkshop v4 ship since 2026-09-08 (an invitation confers nothing
  until acknowledged); a one-way listing rule would merge 66 more components today (67, 153
  coordinates, in the dry run), so both counts are published: `multi_maintainer_repos` (reciprocal,
  64) and `shared_listed_repos` (one-way, 130). The **lead** is the member with an active
  self-`M` when exactly one has it, else the member whose announcement has the earliest
  archived version, ties to the lowest pubkey; its `name` and `description` are the
  repository's; the repository key is `<lead pubkey>:<d>`. Clone URLs, relays and topics
  are the UNION over members; the state is the newest kind-30618 (repo-states) over the
  members' coordinates; pushes are the union of the members' push ledgers, each attributed
  to the pushing member.
- **Fork family** = repositories sharing an earliest-unique-commit: the first `["r",
  <40-hex>, "euc"]` tag (a bare 40-hex `r` from ngit v1.0-1.2 counts too). Families are
  listed in the document, never used to count: Shakespeare omits the euc on creation, so its
  repositories are singletons. Explicit forks are counted separately (`forks`):
  announcements carrying a `u` (subordinate fork) tag, or a `t` = `personal-fork` tag with a
  state. The page showed the largest families and both counts until 2026-09-29 (fork 6).

## 5. The noise ledger

The kind is open and squatted. Eleven per-coordinate rules remove what is not a usable
repository; the page publishes the ledger (rule, unit, count, reason) so a reader can see
what the headline excludes. First matching rule wins, in this order; every rule is
data-derived, none is a hand-kept pubkey list except N7, which names a mathematically
public key, and N10, which names the nostr.net catalog's key by the operator's decision of
2026-10-12. Counts are re-derived from the rule text on the 2026-09-25 DuckDB copy
(review 1), not carried over from the profile.

| # | Family | Rule (current version unless noted) | Coordinates |
|---|---|---|---:|
| N0 | Malformed identifier | `d` longer than 200 characters (the longest real one is 143) or containing a control character; counted, but written to no substrate row, so the substrate key is never clipped or NUL-stripped | 0 |
| N1 | Agent-profile squatters | a `clone` or `web` value starts with `https://nostr.life/` | 784 |
| N2 | Test harness | a `clone` value contains `@example.com:`, or a bare `euc` tag exists | 200 |
| N3 | Template copies | `name` equals the NIP-34 (git collaboration) placeholder `<human-readable project name>` | 38 |
| N4 | Date-named snapshots | `d` matches `^20[0-9]{6}$` (a bare date: a daily sync output, not a project). The same key's 375 NAMED GitHub mirrors (329 of them kept, 45 N8b, 1 N8a) are kept on purpose - they are repositories, announced by a sync tool; the tools panel and the concentration figure made them visible until the page stopped showing both (concentration 2026-09-29 and 2026-10-02, tools 2026-10-08), and `tools` in the document still does | 139 |
| N5 | Automated republish loop | >= 1,000 archived versions AND median gap between consecutive versions < 300 s | 1 (61,864 versions on 2026-09-25) |
| N6 | Deleted by its author | `["deleted","true"]`, `["status","deleted"]`, `name` = `deleted` (case-insensitive), or a self-authored kind-5 (event-deletion-requests) naming the coordinate with `created_at` >= the current version's (NIP-09 (deletion requests); a later re-announcement restarts the repository) | 219 |
| N7 | Well-known private key | pubkey `79be667e...f81798` (the public key of secret key 1: anyone can sign as it) | 42 |
| N8a | Private clone hosts only | >= 1 clone URL, every one on a private / loopback host (localhost, 127/8, 10/8, 172.16/12, 192.168/16, 100.64/10, 0.0.0.0, `.local`) | 84 |
| N8b | No public clone URL and no state | no clone value classifies reachable (none at all, or only `private` / `non_network` values such as `todo`, `testing`, `.`) AND no kind-30618 (repo-states) coordinate - nothing to clone, nothing ever pushed; mostly hex-named and `config-test` shells, plus a few web-only announcements (NostrHub) that are excluded knowingly | 635 on the first served run; the reachability clause (review 4) adds about 62 |
| N9 | Stateless personal forks | a `t` = `personal-fork` tag AND no state (the announcements ngit 2.0-2.4 created to host PR branches, and every hashtree personal fork: hashtree never publishes a state, and Iris Git's Fork button and its git remote helper tag any copy or clone of another key's tree `personal-fork`, so real forks fall here too - 59 hashtree coordinates on 2026-10-09, 47 of them Iris test runs and 5 still pushed, `nostr-vpn` among them; not changed: the operator left the gap for hashtree to close, 2026-10-09) | 73 |
| N10 | Catalog of other people's projects | pubkey `62ec33a4...8baaf` (profile "nostr.net"), which on 2025-11-16 re-published 402 of the awesome-nostr list's 413 entries with a code-host link as 408 announcements (6 under two categories): a directory of other people's projects, not repositories it maintains, while NIP-34 (git collaboration) reads an announcement as its author's claim to maintain the project. The operator's decision of 2026-10-12. The last rule, so the catalog's 2 announcements with no clone value stay N8b. The scope the operator chose: it also excludes any later announcement by this key, and a re-publication under another key escapes it (`roster_growth_sane` only warns, and only on a fresh `created_at`) | 0 (none archived then) |

Kept: 4,389 announcement coordinates by 1,250 authors, forming 4,304 repositories (the
served row of 2026-09-25, as of 2026-09-24; the ledger counts above are that row's). Two further ledger rows carry other units: **orphan states** (state coordinates with no kept
announcement: 930 today; 463 of the dry run's 799 were a six-day burst of throwaway keys) are ignored; and a
**state republish loop** (a state coordinate with >= 1,000 versions and a median gap
< 120 s) has its versions dropped from the push ledger. Until the nostrology-data deploy of
2026-10-02 a third row reported the **pushes above the daily cap** (section 6). Left in on purpose: single-use keys with
test-like names (about 270; the regex would be a guess), hashtree repositories announced
with `htree://` clone URLs (358 coordinates, 299 kept, the 296-repository hashtree hosting
class: repositories in a sibling ecosystem, though 175 of row 826's 300 are Iris Git's
automated test runs, kept by the operator's decision of 2026-10-11, section 12 fork 24), Shakespeare's vibe-coded repositories (about 1,050), and the one key announcing 329 kept
GitHub mirrors (the document's `concentration` figure records the top announcers' share; the
Maintainers pill showed it until 2026-10-02).

## 6. Activity - the push ledger

Every kind-30618 (repo-states) version of a kept repository's member coordinates is a
ledger row. Its **ref map** is `{ref name -> commit}` over tags named `refs/heads/*` and
`refs/tags/*`, excluding peeled `^{}` entries (ngit >= 2.2.2 adds them for annotated tags
and backfills them opportunistically); tag order is irrelevant; a HEAD-only change is not a
ref change. Versions are ordered by `created_at` ascending, then id DESCENDING, so the
NIP-01 (protocol basics) tie-break (the lowest id is the surviving version) puts the winner
last. Two flags per row:

- `changed` - the ref map differs from the previous archived version's (a first version is
  `changed`: an initial state).
- `push` - `changed` AND `created_at` on or after the state capture start
  (2026-06-14T03:22:39Z). Everything before the wall is one surviving snapshot per
  coordinate (63 coordinates hold two because relays served different versions at
  backfill), so pre-wall differences are survivorship, not pushes.

**hashtree pushes (since the nostrology-data deploy of 2026-10-12).** hashtree's git remote
helper publishes a push as kind-30064 (hashtree-roots), never a state, and republishes the
repository's kind-30617 (repo-announcements) with each ref of a public push: one version per
changed ref, each from a few seconds to about 107 s after the one before (observed; its
`push.rs` publishes each ref's root and announcement in turn). So the ledger also holds one row per
push at a member coordinate of a `hashtree`-class repository, a push being a group of the
coordinate's versions each less than `HTREE_PUSH_GAP_S` (120 s) after the one before, except
the coordinate's first group (its creation or first push, or what survived to the wall) and any
starting before the state wall: `source` = `htree_announcement` (a state row's is `state`), the
group's first version's id and time, `push` and `changed` true, every count 0, `branches` and
`tags` NULL. Counting versions instead would count refs while capture is live and pushes when it
is late, since a late request gets only the newest version: in October 2026 nozzl got this
author's versions 54 minutes to 5.6 days late, one per push, and in July within seconds, one per
ref (the second review of this change). Only the `hashtree` class counts this way: a
`grasp_mixed` repository on GRASP and hashtree keeps its states alone, so one push sent to both
counts once. Such a row moves `last_push` and the push counts, never `last_state`, the newest
state or Anatomy, so a hashtree repository can be among the most active with no state. It is
mostly a lower bound: private and link-visible pushes republish nothing, Iris Git's web app
never republishes, a repository's first push is not counted (a state repository's first state
is), and a push nozzl did not capture in time is missed. Two cases count too many: an edit that
republishes the announcement counts as a push, and a push whose next ref comes 120 s or more
after the one before counts twice. The rule text is dataset reference section 3; the
operator's decision of 2026-10-12, nostrology-data#16 (/git: count hashtree pushes), with the
grouping added after its review. Measured on row 991's data (the /git pre-launch audit;
the grouped figures from the
review's replay of the archive behind that row): Active repositories (30 d) 668 -> 695, Pushes
(30 d) 9,525 -> 9,742 (217 hashtree pushes; 227 if versions were counted), 973 hashtree pushes
since the wall (1,066 versions); `336f3197:hashtree` among Most active repositories and its
author 3rd among Most active maintainers. kind-30064
(hashtree-roots) is not read: one push's roots arrive 30-107 s apart.

Identical-ref republishes (ngit `repo edit`, `sync --force`, maintainer handoffs) stay in
the ledger with `changed = false` and count for nothing. Per push the ledger keeps what
moved: default-branch tip, other branches, branches added / removed, tags added / moved /
removed, HEAD changed. Commits are not countable (tips only).

**No daily cap (since the nostrology-data deploy of 2026-10-02).** Every push counts. Until
then - the nightly run of 2026-10-02 included - push totals and series
counted at most 50 pushes per repository per UTC day (the substrate's `counted` flag; the
excess was the ledger row `daily_cap`), to bound what one repository could add to a network
figure. Measured on 2026-10-02 it bounded nothing that needed bounding: it
held back 375 of 19,562 pushes (1.9%), on 15 of 5,350 repository-days (0.3%; per
repository-day the median is 2 pushes, p90 8, p99 28, p99.9 68, the maximum 174), all from
seven repositories of five maintainers doing real work (HydrusUI's 174 on 2026-09-12 is a
human-plus-agent project). Without it 11 of the 17 ISO weeks since capture are unchanged,
four rise by 0.2% to 3%, and the other two by 13.1% and 11.9% (the weeks of 2026-06-22 and
2026-09-07; the busiest repository is at most 21% of any week's pushes); the 7 d / 30 d /
90 d push counts rise 2.5% / 3.9% / 1.8%,
and the top 20 maintainers are the same 20 in the same order. The operator dropped it from the
pipeline, the reference, the cross-check, the substrate and the page; reintroduce a cap only
if one repository comes to dominate the network's weekly figures: the non-blocking
`push_concentration_sane` check (section 9) warns when one repository holds more than half the
pushes of the 7 days to `as_of_day`, once they number 100. Without the cap the only rate
defence is the state republish loop rule (>= 1,000 versions, median gap < 120 s): a key moving
a ref every 121 s would add about 5,000 pushes a week, several times the network's, and that
check is what would say so. Distinct-repository counts (`repos_pushed_*`) were never capped
and stay the headline.

**Windows.** `as_of_day` is the last COMPLETE UTC day before the run; "30 d" is the 30
complete UTC days ending on `as_of_day` (never `now - 30*86400`). Weekly series use ISO
weeks (Monday start). `pushes_weekly` begins with the week that CONTAINS the capture start
(2026-06-08, flagged `partial_first_week` because its data begins on Sunday 06-14 at
03:22Z), and its sum equals `pushes_total` by construction (an invariant). The three weekly
series added on 2026-10-02 - new repositories, comments, GRASP list users - each begin with
the week holding their own kind's wall, unflagged: that week mixes what relays still served
with what was captured, and the page starts every weekly chart with the week of 2026-06-29
(the week holding 2026-07-01), after all three walls (section 10). The trailing week is `partial_week` unless `as_of_day` is a
Sunday; `partial_month` likewise (written, not shown since 2026-10-02: the page draws no
monthly series).

**Recency.** A repository's `last_state` (newest state version over its members) is a
real timestamp for every repository with a state; but for a pre-wall repository it is the
newest state *publication*, which may be an identical republish, so the cohorts are
labelled "last state seen", not "last push". `last_push` (newest `push`) is null before
the wall. The cohorts (30 d / 90 d / 180 d / 365 d / older) cover repositories with a state;
the 1,279 repositories without one are shown separately, split by hosting class, because
"never pushed" is the wrong reading for most of them: hashtree keeps its branch and tag
tips inside its own tree, published as kind-30064 (hashtree-roots) since 2026-06-22 and
kind-30078 (NIP-78 app data) before (section 13.1), forge mirrors keep theirs on the forge,
and a GRASP-hosted
repository without a state is a coverage gap (the server would not have accepted a push).
Both the cohorts (`recency`) and the no-state split (`no_state`) are written, not shown on
the page since 2026-10-02.

## 7. Hosting

Clone URLs are parsed once: scheme (`x://`), scp-like `user@host:path`, or none; host =
the hostname without port, lowercased. Host classes, in precedence order: **hashtree**
(`htree://`), **private** (the hosts in N8a), **grasp** (in the GRASP host set), **forge**
(`github.com`, `gitlab.com`, `codeberg.org`, `bitbucket.org`, `git.sr.ht`, `tangled.org`,
`gitea.com`, `gitee.com`, `framagit.org`, `notabug.org`, and since 2026-10-12
`git.gittr.space`, gittr's hosted git server), **non_network** (a scheme that
is not http / https / ssh / git / ws / wss and not scp-like: `gnostr-cloud://`,
`nostr://`, `rns`, `keybase`, `file`, `blossom`, scheme-less values), else
**self_hosted**. Private and non_network hosts are unreachable and never decide a class.
Reachable is read from the URL alone, as everything in this section is: nothing connects to a
host or looks it up, so a dead host is reachable. The page says "public" for it: beside
"nothing checks that a host answers", "reachable" read as a contradiction (the operator,
2026-09-29).

A clone URL is **GRASP-shaped** when its path starts with an npub:
`^(https?|wss?)://[^/?#]+/npub1[02-9ac-hj-np-z]{58}/`. The **GRASP host set** is the set of
hosts that serve at least one GRASP-shaped clone URL of a kept repository, that contain a
dot, that are not private, that are not a forge host (one GRASP-shaped path on github.com
must not turn every GitHub-hosted repository into a GRASP-hosted one - review 4), and that
are on none of three hand-kept denylists: the web UIs (`gitworkshop.dev`, `nostrhub.io`,
`gittr.space`, `ngit.dev` - viewers and doc sites that proxy a few repositories, not
servers), the general-purpose relays that serve no git (`relay.damus.io`, `nos.lol`,
`relay.primal.net`, `relay.nostr.band`, `relay.nostrcheck.me`), and, since the
nostrology-data deploy of 2026-10-12, the hosts a probe showed are not GRASP servers
(`git.jb55.com`, `relay.vanderwarker.family`, `gitrepublic.imwald.eu`,
`denimroad.feeds.relay.tools`;
nostrology-data#17 (/git: 5 of the 63 hosts in the GRASP host set are not GRASP servers
and 14 are dead)). The probe's fifth, `git.gittr.space`, left that list for the forge list
the same day: it serves the repositories of everyone who pushes to or imports through the
gittr.space forge (185 kept repositories of 45 keys name it), so a repository on it is
forge-hosted, not self-hosted (the operator's decision). The rule is syntactic, with no DNS lookup and no connection: a typo host,
a dead host or a `.onion` name with a GRASP-shaped path is in the set (14 of the 63 hosts on
row 991 did not answer the probe), and a `nip.io` name embedding a private address is
private. Probing every host for GRASP support is the decided long-term fix (fork 5). kind-10317 (grasp-server-lists) hosts inform the
adoption panel only; they never classify a URL (three listed hosts today are not GRASP
servers).

A repository's **hosting class** is exclusive, judged over its reachable clone hosts, in
this precedence: `grasp_only` (GRASP servers only, one or several); `grasp_mixed` (GRASP plus
a reachable host of another class - a forge, a self-hosted server or hashtree; a GitHub
mirror, say); `hashtree` (an `htree://` URL, no
GRASP); `forge_only` (only forges); `other_hosts` (a self-hosted host, no GRASP, no
hashtree); `none` (no reachable clone URL). So a repository on a GRASP server and hashtree
is `grasp_mixed`, never also `hashtree`: since 2026-10-08 the page says each repository is in
the first class that fits, ends the `grasp_mixed` definition "counted here, not again below"
and labels the hashtree and self-hosted classes "hashtree, no GRASP" and "Self-hosted, no
GRASP". The bare "hashtree" slice, beside a `grasp_mixed` definition naming hashtree, read
as a double count; that day 5 of the 365 `grasp_mixed` repositories had an `htree://` URL,
and the tools panel's "hashtree 304" (its rule counts any `htree://` URL) was the 299 plus
those 5. "GRASP-hosted" is defined ONCE:
`grasp_repos = grasp_only + grasp_mixed` (an invariant), `grasp_share = grasp_repos /
repos`. The six classes partition the repositories: each class's condition excludes the
other five even without the precedence, every one of the 16 sets of reachable host classes
meets exactly one (`test_repo_hosting_classes_partition_every_reachable_set`, which also
fails on a fifth reachable host class until it has a class of its own), and on row 826
(2026-10-10) each of the 4,596 repositories met exactly one. Since 2026-10-10 the page's
headline share, on its card and in the hosting donut's centre, is GRASP or hashtree,
(`grasp_repos` + the `hashtree` class) / `repos`, which the page computes from the
document: the operator did not want GRASP singled out, hashtree being a host of the same
standing. `grasp_share` is written, not shown. On row 826, 175 of the `hashtree` class's 300
repositories are Iris Git's automated end-to-end test runs, each on a key of its own;
without them and the 2 GRASP-only repositories
with a 13-digit `d`, which are not Iris's, the headline would be 79.5%, not 80.2%. The
operator decided on 2026-10-11 to keep them, under section 5's rule for test-like names
(section 12, fork 24).

Per GRASP server: repositories hosted (kept repositories with a GRASP-shaped URL on that
host), users listing it in a kind-10317 (grasp-server-lists) (harness excluded), and users
naming it FIRST. Redundancy: GRASP servers per GRASP-hosted repository (1 / 2 / 3 / 4+).
ngit.dev recommends listing three to five. Until 2026-10-12 the caption said "ngit.dev advises
keeping at least three", which ngit.dev says only in the middle of a server swap:
nostrology-web#18 (/git: "ngit.dev advises keeping at least three" misquotes ngit.dev).
Collaboration relays: `relays` tag values,
normalized (lowercase, `ws://` / `wss://` stripped, trailing slash stripped; values without
a dot dropped), counted per repository, with the relays-per-repository distribution. Web
hosts: `web` tag hosts (http / https only). Announcing tool (a HAND-KEPT map, shown as a
panel labelled a heuristic until 2026-10-08 and written, not shown, since; first match
wins): `client` containing `shakespeare` -> Shakespeare;
`client` containing `nostrhub` -> NostrHub; any other `client` value -> that value (the top
five, the rest folded into "other client"); no `client` but a `t` = `shakespeare` ->
Shakespeare; an `htree://` clone URL -> hashtree; an `alt` tag starting `git repository:`
-> "ngit / GitWorkshop (inferred)" (neither sets `client`); `refs/heads/*` tags inside the
announcement -> "git-sync mirror (refs in announcement)"; else unknown.

## 8. The document contract

No HTTP API; the API is a database contract: one table read in one query,
one document, two roles - since 2026-10-18 a query that also reads the Stars and Zaps lists
from `ngit_repo_stats` for the document's own run, while the landing board reads the
document alone. Producer = whatever `pipeline/ngit.build_snapshot()` returns; consumer
= the `NgitSnapshot` interface in nostrology-web `src/lib/shared/data-types.ts`. The page
renders nothing that is not in the document, but for the Stars and Zaps lists, which since
2026-10-18 (the operator's choice; fork 27) the page's server reads from
`ngit_repo_stats` for the served row's `run_id`, ranks and cuts, falling back on the
document's own `repos_starred` and `repos_zapped`; the only client-side derivations are
presentation (npub encoding for links, percentage formatting, Top-N folding of rank-ordered
lists). Every list whose cardinality a third party controls is capped in the document (open
kinds are floodable, and the whole document is SSR-embedded). Strings are clipped (`name`
80, `host` 100, `topic` 40, `relay` 100, display name 40; `d` is never clipped - an
identifier over 200 characters is ledger rule N0, so links never break) and NUL-stripped
before Postgres.

```
NgitSnapshot
  generated_utc          "YYYY-MM-DD HH:MM:SSZ"   run instant; the freshness stamp
  as_of_day              "YYYY-MM-DD"             last complete UTC day; every window ends here
  coverage
    announcements_since  "2026-05-02"   states_since "2026-06-14"   comments_since "2026-06-18"
    reactions_since "2026-06-19"  zaps_since "2026-06-14"  deletions_since "2026-07-01"
                         (earliest ingest partition of each social kind; the Sources table only)
    external_reactions_since "2026-10-11"   kind-17 (external-content-reactions), the day capture began;
                         first written 2026-10-11, optional to the page
    ngit_relay_since     "2026-06-14"   the day relay.ngit.dev joined the capture (reference 0.2);
                         optional (absent before 2026-09-26), not shown since 2026-10-02
    partial_first_week   "2026-06-08"   partial_week "YYYY-MM-DD" | null   partial_month "YYYY-MM" | null
                         (partial_first_week and partial_month: not shown since 2026-10-02, optional to the page)
  anomalies              { future_dated: {...}, past_dated: {...} }  per kind: announcements, states, grasp_lists, comments, reactions,
                          external_reactions (first written 2026-10-11, optional to the page), zaps
  summary
    archive_events       { announcements, states, grasp_lists, comments, reactions, external_reactions, zaps, deletions }  distinct ids / pairs
                         (external_reactions: first written 2026-10-11, optional to the page, which keys its kind-17 (external-content-reactions) wording on it)
    announcements_total  all coordinates          announcements_kept  after the ledger
    repos                repositories (components) after the ledger      forks  explicit forks (u / personal-fork with state), not shown since 2026-09-29 (optional to the page)
    repos_with_state     repos_pushed_30d   repos_pushed_90d   pushes_7d   pushes_30d   pushes_total  (every push - capped at 50 per repository per UTC day until 2026-10-02; pushes_total written, not shown since 2026-09-29, optional to the page; hashtree pushes included since 2026-10-12, section 6)
    pushes_hashtree_30d  (the hashtree pushes of pushes_30d; first written by the first run after the deploy of 2026-10-12, and the page's sign that the document counts them)
    maintainers          distinct authors of kept announcements      maintainers_active_30d  pushed in 30 d
    multi_maintainer_repos  (reciprocal)     shared_listed_repos  (one-way)   both not shown since 2026-10-02 (optional to the page)
                         fork_families (euc groups with > 1 repository), not shown since 2026-09-29 (optional to the page)
    grasp_repos          = grasp_only + grasp_mixed      grasp_share  0..1     grasp_servers  |GRASP host set|
                         (grasp_share not shown since 2026-10-10, optional to the page: the headline is GRASP or hashtree)
    grasp_list_users     kind-10317 (grasp-server-lists) pubkeys with >= 1 valid server
    comments_total       comments_30d     comments_90d     comment_threads     comment_participants     comments_attributable_share
                         (comments_90d: first written by the first run after the nostrology-data deploy of
                         2026-10-01; absent from earlier documents, optional to the page)
    stars_total          stars_repos      zap_receipts   zap_msats (capped)   zappers   zaps_repos
                         (stars over kind-7 (reactions) and, since 2026-10-11, kind-17 (external-content-reactions), one pair per key
                         and repository either way; stars_total and zap_msats written, not shown since 2026-10-18, optional
                         to the page; stars_repos not shown either, read only to decide the stars table's sort note)
    stars_iris           the pairs from kind-17 (external-content-reactions) stars; written, not shown; first written 2026-10-11
    first_announcement   "YYYY-MM-DD"     newest_state "YYYY-MM-DD" | null     newest_push "YYYY-MM-DD" | null
    concentration        { top1_share, top10_share }   share of repositories held by the top announcers
                         (not shown: top1 since 2026-09-29, top10 since 2026-10-02; optional to the page)
  exclusions             [{ k, unit: "announcements"|"state coordinates"|"comments", v, why }]
                          (a document written before the nostrology-data deploy of 2026-10-02 also has daily_cap, unit "pushes")
  new_repos_monthly      [{ m, v, grasp, other, s }]  repositories by month of first archived announcement
                          (v = grasp + other, hosting TODAY; s = maintainers whose first kept announcement is that month)
                          not shown since 2026-10-02 (optional to the page)
  new_repos_weekly       [{ w, v, grasp, other, s, by_host }]  the same per ISO week, from the week holding the announcements
                          wall (2026-04-27); first written 2026-10-02, optional to the page
                          by_host [{ k: "grasp"|"hashtree"|"forge"|"self_hosted"|"none", v }]: the week's repositories once per
                          kind of public host they are on TODAY (R of reference 4.4; none = R empty), so it can sum past v;
                          grasp = the row's grasp; first written 2026-10-08, optional to the page
  cum_repos              [{ d, v }]  daily cumulative repositories by first-seen day
  pushes_weekly          [{ w, pushes, repos, maintainers }]  ISO weeks from partial_first_week; sum(pushes) == pushes_total
  recency                [{ k: "30d"|"90d"|"180d"|"365d"|"older", v }]  repositories with a state, by last_state
  no_state               { total, by_class: [{ k: <hosting class>, v }] }
                          (recency and no_state: not shown since 2026-10-02, optional to the page)
  hosting                [{ k: <class>, v }]      clone_hosts [{ host, repos, cls }] top 30 (the page shows 20 since 2026-10-08)
  grasp_servers          [{ host, repos, users, first_choice }] top 20
  grasp_per_repo         [{ k: "1"|"2"|"3"|"4+", v }]
  relays                 [{ relay, repos }] top 25   relays_per_repo [{ k: "0"|"1"|"2-3"|"4-5"|"6+", v }]   web_hosts [{ host, repos }] top 12 (the page shows 10 since 2026-10-08)
  default_branch         [{ k: "main"|"master"|"other"|"detached"|"none", v }]   written, not shown since 2026-09-29 (optional to the page)
  branches_per_repo      [{ k: "0"|"1"|"2-5"|"6-20"|"21+", v }]   tags_per_repo [{ k: "0"|"1-5"|"6-20"|"21+", v }]
  topics                 [{ t, repos }] top 25 (tool and date tags excluded)
  tools                  [{ k, v }]  announcing tool, heuristic; written, not shown since 2026-10-08 (optional to the page)
  repos_active           [{ pubkey, d, name, lead_name, pushes_30d, pushes_90d, last_push, last_state, branches, tags, cls, host_kinds, hosts (<= 5) }] top 25 by pushes_90d
                          (host_kinds: the repository's R, ref 4.4, in HOST_KINDS order - grasp, hashtree, forge, self_hosted -
                          or ["none"] when R is empty; it gives cls exactly; first written 2026-10-11, optional to the page)
  repos_recent_pushes    same rows, top 25 by last_push   written, not shown since 2026-09-29 (optional to the page)
  repos_new              same rows + first_seen, top 25 by first_seen (post-wall only)   written, not shown since 2026-09-29 (optional to the page)
  fork_families_top      [{ euc, repos, names (<= 5) }] top 15   written, not shown since 2026-09-29 (optional to the page)
  maintainers_top        [{ pubkey, name, repos, pushes_90d, last_push }] top 20 by pushes_90d, then repos
  grasp_lists            { users, per_month: [{ m, v }], per_week: [{ w, v }], length: [{ k: "1"|"2"|"3"|"4+", v }], first_choice: [{ host, v }] top 10 }
                          (per_month not shown since 2026-10-02; per_week from the week holding the states wall,
                          2026-06-08, first written 2026-10-02 - both optional to the page)
  comments_monthly       [{ m, issues, prs, patches, repo, other, authors, threads }]  counted comments only;
                          not shown since 2026-10-02 (optional to the page)
  comments_weekly        [{ w, issues, prs, patches, repo, other, authors, threads }]  the same per ISO week, from the
                          week holding the comments wall (2026-06-15); first written 2026-10-02, optional to the page
  repos_discussed        [{ pubkey, d, name, cls, comments, authors }] top 10, attributable only; not shown since 2026-10-02 (optional to the page)
  repos_starred          [{ pubkey, d, name, cls, stars }] top 20      repos_zapped [{ pubkey, d, name, cls, receipts, msats }] top 20
                          by receipts, then msats. Each the top 10 before the first run after the deploy of 2026-10-18
                          06:14Z, the operator's ask; repos_zapped by msats, then receipts before the first
                          run after the deploy of 07:34Z that day, the operator's second ask.
                          (cls: the repository's hosting class, as on repos_active, which picks the row's link;
                          first written 2026-10-09, optional to the page)
```

Formats the listing leaves implicit: every date field (`d`, `w`, `first_seen`, `last_push`,
`last_state`, `newest_*`, `first_announcement`, `as_of_day`) is a `"YYYY-MM-DD"` string, `m`
is `"YYYY-MM"`, `hosts` is a list of host strings, `pubkey` is 64 lowercase hex, and
`generated_utc` is `"YYYY-MM-DD HH:MM:SSZ"`.

Evolution rules as in the marketplaces doc: additive fields land compute -> TS -> page;
breaking changes page-first. `generated_utc` is the user-visible freshness signal and the
pin for the cross-check (section 11); never repurpose it.

Substrate (not read by the page, but for `ngit_repo_stats`; the future MCP source and the
drilldown for "why is this excluded?"): `ngit_repo_stats` (since 2026-10-18: one row per kept
repository - its lead coordinate, name, hosting class, stars, counted zap receipts and msats,
and the `run_id` of the snapshot row written with it - emptied by DELETE, not TRUNCATE, so a
page read never waits on the run's lock; the page lists Stars and Zaps from it, dataset
reference section 9), `ngit_repos` (one row per announcement coordinate, all of them, with `family`
- the ledger rule that matched, or `repo` - and for kept ones the repository key, lead
flag, hosting class, hosts, relays, euc, tool, first / last seen, state summary, push
counts), `ngit_pushes` (one row per state version of a KEPT repository's member
coordinates - bounded by the ledger, never by a flooder - with `changed`, `push` and the
per-push change counts, and since 2026-10-12 one per hashtree push, told apart by `source`;
the `counted` flag went with the daily cap: the first `write_pg` on the new code drops the
column under its TRUNCATE lock, never `schema/init.sql`, which every job applies),
`ngit_snapshot`
(append-only JSONB). All written in the document's transaction and tied to it by
`check_invariants`. The cross-check (section 11) compares `ngit_repos` and `ngit_pushes` row
by row; it does not yet re-derive `ngit_repo_stats`, whose figures the invariants hold to the
summary, which it does.

```sql
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
    grasp            BOOLEAN,                       -- repository class is grasp_only or grasp_mixed (kept rows)
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
    branches         INTEGER,                       -- NULL on a hashtree push (section 6)
    tags             INTEGER,
    source           TEXT     NOT NULL DEFAULT 'state'
                              CHECK (source IN ('state', 'htree_announcement'))
);
-- source, and branches / tags without NOT NULL, since 2026-10-12: a live table is altered by
-- write_pg under its TRUNCATE lock, as `counted` was dropped on 2026-10-02 (section 9). The
-- default is what the pipeline before that change writes without naming the column, so a
-- rollback still inserts.
CREATE INDEX IF NOT EXISTS idx_ngp_repo ON ngit_pushes (repo_key, created_at);
CREATE INDEX IF NOT EXISTS idx_ngr_family ON ngit_repos (family);
-- since 2026-10-18: one row per kept repository; DELETE+INSERT by each run
CREATE TABLE IF NOT EXISTS ngit_repo_stats (
    repo_key         TEXT     PRIMARY KEY,
    run_id           TEXT     NOT NULL,
    pubkey           CHAR(64) NOT NULL,
    d                TEXT     NOT NULL,
    name             TEXT     NOT NULL,
    cls              TEXT     NOT NULL,
    stars            INTEGER  NOT NULL,
    zap_receipts     INTEGER  NOT NULL,
    zap_msats        BIGINT   NOT NULL
);
CREATE TABLE IF NOT EXISTS ngit_snapshot (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    generated_utc TIMESTAMPTZ NOT NULL,
    run_id        TEXT        NOT NULL,
    data          JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ngs_generated ON ngit_snapshot (generated_utc DESC);
```

## 9. The pipeline (nostrology-data)

One Dagster job, `ngit_refresh`, group `ngit`, on the shared `in_process_executor` +
run-scoped `AuroraDial` pattern every surface uses. Schedule `ngit_daily` 03:15 UTC (last in
the nightly order, after `routstr_daily` 02:45; the `QueuedRunCoordinator` serializes it).
The nightly block therefore ends at about 03:45 UTC: the "never deploy the Dagster stack inside
the nightly window" rule in `dagster-orchestration-design.md` and `CLAUDE.md` move to 03:45.

**Extract assets** (eight instances of `_extract_asset`, all `into="ngit"`; seven until the
deploy of 2026-10-11): the two
addressable kinds reuse `extract_addressable_all.sql`; kind-10317 (grasp-server-lists) gets
`extract_replaceable_all.sql` (the replaceable template minus its latest-per-pubkey stage);
the four regular event kinds get their own templates carrying the semantic filter IN the
SQL so only the git subset leaves Athena - comments whose `K` value is one of
`1617`, `1618`, `1619`, `1621`, `30617`, `30618` or whose `A`/`a` value starts `30617:`;
reactions with a `k` = `30617` tag or such an `a` value; Iris Git's external-content
reactions with a `k` = `git-repo` tag AND an `i` value starting `30617:`; zap receipts with
such an `a` value; the kind-5 (event-deletion-requests) extract is a reduction to distinct
self-authored `(author, coord, created_at)` triples, modelled on
`extract_markets_deletions.sql`.
The `extract_nonempty` check is blocking for the roster kinds, comments and the external
reactions, and a WARNING for reactions, zaps and deletions: their subsets are small (547 / 26
/ 328 rows on 2026-09-25), and an empty subset must not wedge a run at the extract; the
`archive_events` gates in `write_pg` (step 5) are what refuse to serve one. The external
reactions are blocking from the start (31 rows on 2026-10-11, an archive that only grows):
on that extract's first run the gate has no served value to compare with, so an empty one
would have written zero Iris Git stars (the pre-deploy review's M1).

**Compute asset** (`ngit_snapshot`), fail-loud in this order - every step before the write
leaves the previous serving data live:

1. Load the verified LNURL provider signer keys (`data/lightning/provider_signers.yaml`,
   read with the /lightning loader's own parser; a broken file fails the run before any
   fetch, never an empty set). Fetch the eight part sets to `NOSTROLOGY_EXTRACT_TMP`; read
   the run instant; parse - the deletion triples first, then the two roster kinds STREAMED
   through `load_roster` (full tags only on each coordinate's current version inside the
   horizon and on the state rows of kept repositories' member coordinates; every other
   version reduced to id, pubkey, created_at and `d`; `sig` and `content` dropped), then the
   other five kinds. The reduction is exact (the document and both substrate tables are
   identical to a full parse, pinned by a test over the fixture plus a 1,000-version flooder,
   and by a replay over a production run's own parts); peak RSS on the 2026-09-25
   production parts fell from about 870 MiB to about 425 MiB in the local replay (review 4),
   and the first production run on this code (2026-09-26) recorded 545 MiB, the
   box's available memory bottoming at 672 MiB against 341 MiB for the old code's run on the
   same night's data; each run records `peak_rss_mib`.
2. Resolve display names for ALL kept maintainers (capped at 2,000 pubkeys): `SELECT
   DISTINCT ON (pubkey) pubkey, name, display_name FROM events_kind_0_profiles WHERE
   pubkey = ANY(%s::bpchar[]) ORDER BY pubkey, created_at DESC` (the table is keyed by event id, so
   one row per pubkey is not guaranteed); an empty or unavailable table degrades to no
   names, never to a failed run; the lookup runs under a 60 s statement timeout, and zero
   names for a non-empty roster is logged at WARNING. Names are clipped and stripped of
   control and format characters (Unicode Cc and Cf); they are presentation only and outside
   the cross-check.
3. `pipeline/ngit.build_snapshot()` - pure (rows in, doc out; no AWS / DB / clock beyond
   the injected `now_ts` and names): the horizon on every kind (rows after `as_of_end` or
   before 2023 dropped, the future- and past-dated ones counted in `anomalies`), the noise ledger, repository components, the push
   ledger, hosting, maintainers, collaboration, and every aggregation of the document.
   Third-party numbers pass a finiteness gate; third-party strings are clipped and
   stripped of control and format characters (a bidi override in one name must not reverse
   its neighbours on the page).
4. `check_invariants()` - partitions sum (ledger rows in announcement units + kept = total;
   hosting classes = repos; `grasp_repos` = `grasp_only` + `grasp_mixed`; recency buckets
   + `no_state.total` = repos; `no_state.by_class` sums to `no_state.total`;
   `new_repos_monthly` sums to repos and each month's `v` = `grasp` + `other`; `cum_repos`
   ends at repos; `pushes_weekly` sums to `pushes_total`; `comments_monthly` sums to
   `comments_total`); the three weekly series added on 2026-10-02 each span their wall's week
   to `as_of_day`'s week (checked by date arithmetic, not by the walk that builds them) and
   re-bucket exactly from the substrate rows, the counted comments
   and the GRASP list users' first-seen instants (`v`, `grasp` and `s`; the root counts,
   authors and threads; the users); since 2026-10-08 every `new_repos_weekly` row's
   `by_host` lists the five kinds in order, its `grasp` equals the row's `grasp` and its `none`
   the week's class-`none` lead rows, and each other kind lies between what the week's
   hosting classes require and allow (the substrate holds no R; the bounds and identities
   together count every repository at least once); the ledger holds no pushes row (the cap
   is gone); every series sorted / unique / no point after `as_of_day`; every list within its
   cap; every top-list row's repository exists in the roster; `repos_pushed_30d
   <= repos_pushed_90d <= repos` (`<= repos_with_state` until 2026-10-12, when a hashtree
   repository began to push without a state), and no more repositories with a state push than
   `repos_with_state`; every `ngit_pushes` row's `source` is `state` or `htree_announcement`, a
   hashtree push is a push with no ref counts on a `hashtree`-class repository, a coordinate's
   hashtree pushes are fewer than its versions and each at least `HTREE_PUSH_GAP_S` after its
   first version and after the push before, a state row has its branch and tag counts,
   `last_state` is the newest state row's, and `pushes_hashtree_30d` re-sums from the
   substrate; `comments_30d <= comments_90d <= comments_total`;
   `maintainers_active_30d <= maintainers`; substrate rows re-sum to the document (pushes per
   repository, families, hosting). Since 2026-10-18 `ngit_repo_stats` too: one row per kept
   repository on its lead's coordinate, class and display name, stars and zaps that add up to
   the summary's, and the document's `repos_starred` and `repos_zapped` exactly its top 20
   under their rankings (dataset reference 6.2 and 6.3, forks 26 and 27). Failure raises
   before any write; success is recorded as the `snapshot_invariants` asset check.
   A second check, `held_keys_absent` (BLOCKING, since 2026-10-12), raises before any write
   too, when a key held off the page has a kept announcement coordinate (`HELD_KEYS`: the
   nostr.net catalog key, whose announcements of other people's projects N10 excludes since
   the operator's decision of 2026-10-12, 406 of the 408 (the other 2 are N8b)).
   It held the catalog until that
   decision and now guards N10: it is not a methodology rule, it never changes the document,
   and it fails only if N10 stops excluding the key.
   A third, NON-blocking check, `roster_growth_sane`, warns when the kept coordinates
   first seen on `as_of_day` exceed five times the trailing-30-day daily median (minimum
   20): a coordinate flood lands as repositories and never trips a version-based rule. A
   fourth, `push_concentration_sane` (NON-blocking, since 2026-10-02), warns when one
   repository holds more than half the pushes of the 7 days to `as_of_day`, once they number
   100 - the question the dropped daily cap answered (section 6). Both write a warning line
   to the run log and a WARN asset check; neither posts to Discord.
5. `write_pg()` - RELATIVE anti-shrink gates: refuse when `announcements_kept`,
   `pushes_total` or any `summary.archive_events.*` count falls below 90% of the newest
   served row's value (the archive is append-only and the roster and ledger only grow
   under unchanged rules; the `archive_events` gates are what protect the three
   allow-empty extracts - an empty deletions extract would otherwise resurrect deleted
   repositories through every other gate); on the first run, absolute floors of 3,500 kept
   coordinates and 12,000 pushes. A deliberate methodology change that shrinks a figure
   sets `SHRINK_OVERRIDE_UNTIL` (an ISO date constant in `pipeline/ngit.py`, parsed
   strictly - a malformed value keeps the gate on) in the same commit; the gate is
   bypassed through that date inclusive and the run logs it. Then ONE transaction:
   TRUNCATE+INSERT `ngit_repos` and `ngit_pushes`, DELETE+INSERT `ngit_repo_stats` (since
   2026-10-18; DELETE, so a page read during the write reads the previous run's rows instead
   of waiting on a lock), INSERT one `ngit_snapshot` row.

**Manual materialization** is audited like a deploy: the deployment's re-run action runs
`dagster job launch` in the running dagster-code container. Submit-only; the run queues like a
scheduled one.

**Unit tests** (`dagster_code/tests/test_pipeline_ngit.py`, mock-free, run on a workstation with
the repo's `.venv-mcp` interpreter): primitive pins (URL parsing and host classes, the
GRASP shape, relay normalization, npub decoding, the role-tag grammar, version ordering,
the ref-map diff with peeled tags, the bolt11 amount gate) and methodology on
hand-verifiable synthetic fixtures (every ledger family including the deletion rule with a
later re-announcement, the reciprocal rule with a one-way listing that must NOT merge and an
excluded member that must not join, the push rule with identical republish / HEAD-only /
peeled-tag noise / first version before and after the wall / a pre-wall pair, the absence
of a daily cap (174 pushes in a day all count), window bounds at a month and week boundary,
the weekly series from each wall's week, `by_host` (a repository under every kind it is
on, today's kinds in the week first seen, R over every member, each bound catching alone and
holding at its limit), every hosting class and the precedence,
the comment ledger, star dedupe and `e`-to-coordinate mapping, the zap cap, caps and
folding, a future-dated event, the invariant checks with a deliberately broken document,
and the write gates refusing before any DB touch, including the override date).

## 10. The page (nostrology-web)

`/git`, five pills over the one embedded document, each with an address of its own since
2026-10-19: `/git` is Overview, then `/git/repositories`,
`/git/hosting`, `/git/maintainers` and `/git/collaboration`. The pill bar is a row of links: a
click opens the pill at once, moves the address to the pill's own and adds a history entry,
so Back returns to the previous pill; it asks the server only for the address's answer
(a body of about 170 bytes), never for the document again. Each address has a page title of its own
("Nostrology - Git on Nostr - Hosting") and is in the sitemap; the link previews stayed as
they were. Until then a pill click wrote `/git#<pill>`; such links still land, moved to the
path. "What is counted" has an address of its own too, `/git/methodology`, which opens
Overview with the section expanded (the site-wide convention for every domain surface,
nostrology-web `CLAUDE.md` and `$lib/methodology.ts`; it was `/git#methodology` from
2026-10-17). Since 2026-10-18 that section is the
`MethodologySection` component, whose head carries a link icon, drawn on hover, that copies
its address. Since 2026-10-19 the page also answers at a
section's own address: `/git/zaps` opens Collaboration with the Zaps section at the top of
the screen, glowing once, and gives link previews the section's own title and description
("Git on Nostr: the most-zapped repositories"). A former slug redirects to the current one,
and an unknown one lands on `/git` with a short note; the rules, and the site-wide house
style for every domain, are in nostrology-web `CLAUDE.md`, "Pills and sections have
addresses". The Zaps card carries a link
icon since the same day: on a desktop it is drawn while the
pointer is on the heading, and a click copies the address; on a phone it is drawn on the one
section with an address whose heading is on screen nearest the top, and a tap opens the
phone's share menu. The page launched on
2026-10-15, with the redesigned landing:
- in the nav as Git-on-Nostr, after Marketplaces;
- on the landing's board, its figure and a 30-day change;
- in the sitemap, and indexable;
- off `/wip`.

Before that it incubated, hidden from the nav, the landing and the sitemap, `noindex` and linked
from `/wip` only. Guardrail tests pin both states. The snapshot's dates are in the footer only
since 2026-10-18, the operator's choice, its first sentence reading "Every window ends on
<as_of_day>, the last complete UTC day before this snapshot was generated at <generated_utc>,
and every count is a lower bound ..."; from 2026-09-26 (web review 4) a line under the title
gave them too, so that a page a failed nightly run leaves on the previous row looked stale
where it is read. Every date in it reads like "Oct 17, 2026" since that day (the operator's
choice; the footer's were ISO), the generation instant as its UTC timestamp. Public wording
says "observation", never "capture", since 2026-10-18 (the operator's preference): "Observation
began on ...", the Sources column "Observed since", "Announcements have been observed since
..." in the footer, and the caveat and Discussion sentences likewise; a page test fails on
"captur" anywhere a reader can see it. Headline cards ride above the
pills from the `sm` breakpoint up; on a phone the pill bar is drawn first (CSS order, the DOM
unchanged), since six stacked cards would push it 1.4 screens down, and a tap on a pill
scrolls the panel it opens into view when that panel starts off screen (web review 4).
Charts follow the site's existing palette; forms are chosen by the data's job (magnitude ->
bars, trend -> line, part-to-whole -> donut or stacked bar, ranked entities -> horizontal
bars or a table), one axis per chart by default, a legend for two or more series, hover
tooltips, and a table wherever a chart has more than seven classes. The bar charts are the
page's own component, `StackedBars.svelte` (stacked, or since 2026-10-08 `grouped`: thin
bars packed side by side, a bucket's group taking 45% of its width, which new repositories
per week draws); /marketplaces' new listings per month has used it too since 2026-10-08, in
place of the deleted `MonthlyBars.svelte`. Three charts carry a second measure as a line on
its own titled right-hand axis, at the operator's request: users with a GRASP server list
(the running total, 2026-10-01), new repositories per week (first-time maintainers) and
participants and threads (threads; both 2026-10-02). Every line is solid since 2026-10-08
(threads had been dashed). The capture wall is drawn by the charts themselves: bars and
points before `coverage.announcements_since` render dimmed with a caption "before observation
began: only what relays still served" ("before capture: ..." until 2026-10-18) - no annotation plugin is available and none is needed. Since
2026-10-02 only the cumulative chart reaches back that far.

| Pill | Panels |
|---|---|
| Overview | cumulative repositories (line) first; new repositories per week (since 2026-10-08 grouped bars, one per kind of host a repository is on today - GRASP, hashtree, Self-hosted, Forges, No public host, in purple, pink, green, amber and slate - in the operator's order since 2026-10-12, bars and key alike, whatever the document's (`by_host` lists grasp, hashtree, forge, self_hosted, none; Forges came before Self-hosted until then), a kind the page does not know yet after the five; each kind in the donut's colour of its class since 2026-10-12, when the operator swapped the donut's blue and amber (the set was the chart's own until then, as the donut's purple and blue cannot be told apart side by side, nor could its teal and green before its hashtree took this pink on 2026-10-11); measured side by side every pair passes with normal vision, and under deuteranopia the pink and the green, neighbours in this order, are 6.6 apart, inside the 6-8 band that needs the key's words; each kind keeping its slot when another is hidden, the bars half as wide and half as far apart within a week as first deployed (the operator's choice, the same day) - a repository counting under each kind it is on, so a week's bars can sum past its new repositories, which the tooltip gives; until then stacked bars, GRASP-hosted today vs other; first-time maintainers as a line on a second axis, in the neutral ink since 2026-10-08 (orange was too close to the amber) - until 2026-10-02 per month, with first-time maintainers as a companion chart); pushes per week (bars) with repositories pushed and maintainers pushing per week beside it (lines); "What is counted" last, collapsed by default since 2026-10-01, with an address of its own since 2026-10-19, `/git/methodology`, which opens this pill with the section expanded just under the nav (`/git#methodology` from 2026-10-17), while the section stays collapsed at `/git` (sources with capture dates, coverage, the exclusion ledger with units, the anomalies, what the numbers are not: since 2026-10-09 also "Not hashtree's pushes", which gave the hashtree class's repository count from the hosting donut, and for a document that counts hashtree's pushes (`pushes_hashtree_30d`, since the nostrology-data deploy of 2026-10-12) "Not every hashtree push", which names the pushes it cannot see and gives the 30-day count). Since 2026-10-09 it names no relay, the operator's choice, because the list changes whenever nozzl onboards one. The paragraph under the sources table that listed the source relays went whole, and the first caveat reads "Not an exhaustive count of repositories on Nostr: only of the ones the relays we read served". The dataset reference, section 0 item 1, keeps the list. Dropped on 2026-10-02: the recency cohorts ("last state seen") and the no-state split by hosting class |
| Repositories | most active repositories table (headed "Most active (90 d)" until 2026-10-10, "Most active repositories" since, the operator's wording; the 90 days are in the caption under it), whose names open on gitworkshop.dev, or on git.iris.to for "hashtree, no GRASP" (since 2026-10-09; until the nostrology-data deploy of 2026-10-12 such a row appeared there only if another tool published the repository's kind-30618 (repo-states), as hashtree's pushes publish none, section 13.1; since then its pushes count, section 6, and the row shows "-" for its last state, branches and tags); its caption said so for two hours on 2026-10-09, until the operator dropped the sentence; its Hosting column names the kinds of host a repository is on, in Clone hosts' words, comma-separated ("GRASP, Forge"), above its hosts, since 2026-10-11 (the operator's choice; the class until then, and still for a document without `host_kinds`, and for a repository with no public host, "No public host"); then topics (horizontal bars) and anatomy side by side from the lg breakpoint (stacked below it), the anatomy's branches per repository above its tags per repository at every width. Dropped on 2026-09-29: the default-branch split, the recently pushed and newest repositories tables, and the largest fork families |
| Hosting | hosting classes (donut, in GRASP only purple, GRASP + other hosts blue, hashtree pink, Forges only amber, Self-hosted green and No public host slate - GRASP + other hosts amber and Forges only blue until 2026-10-12, the operator's swap, which put the purple slice beside the blue one: Delta E 0.3 under deuteranopia and 10.2 with normal vision, below the validator's floors of 6 and 15, the class list naming each class beside its swatch -, with the class definitions beside it, each with its count and, since 2026-10-10 (the operator's request), its share of all repositories in whole percents, as Servers per list gives its buckets', under an intro that defines a GRASP server and, since 2026-10-10, a hashtree repository (since 2026-10-11 the operator's three sentences, "it's a massive paragraph otherwise": "A GRASP server is a git host that is also a Nostr relay. A hashtree repository is a whole git repository, branches and tags included, kept as one content-addressed tree on Blossom file servers and peers. Each repository is in exactly one class judged over its public clone URLs." - the first-that-fits order, what public means and how a GRASP server is recognised went with the cut; the last sentence in its own paragraph since later that day, the operator's request); in precedence order: each repository is in the first that fits, and each definition also excludes the classes above it, so no definition may fit vacuously - GRASP only's reads "only GRASP servers (one or several)" since 2026-10-10, as "every public clone URL is on a GRASP server" also fit a repository with no public clone URL; its centre gives the headline share, GRASP or hashtree since 2026-10-10 as on the card above the pills, GRASP-hosted until then); then, from the lg breakpoint, three rows of two cards, each pair one grid row of one height (stacked below it): clone hosts, the top 20 (horizontal bars, spread down to the end of the table beside it; since 2026-10-10, the operator's choice, each bar wears the donut's colour for its host's kind, from the host class of dataset reference 4.2: GRASP purple, hashtree pink (teal until 2026-10-11, when the operator gave the donut and these bars New repositories per week's pink: the teal was 5.2 from the self-hosted green with normal vision), forge amber (the Forges only blue until the operator's swap of 2026-10-12, so each kind of host is one colour on the whole page since), self-hosted green, private or non-network slate, under a key of the kinds shown - GRASP, hashtree, Self-hosted, Forge, Not public, the operator's order since 2026-10-12 as in New repositories per week (Forge came before Self-hosted until then; "GRASP server" until 2026-10-11, the operator's words) - and the kind is also in words in each bar's hover title and screen-reader text, as the hashtree pink and the self-hosted green are close under colour-vision deficiency (6.6 under deuteranopia; until 2026-10-12 the GRASP purple and the forge blue could not be told apart at all, 0.3); until then "(GRASP)" after a GRASP server's name; a hashtree host that is a valid npub, the authority of an htree:// URL where other URLs name a server, reads "htree://" and the short npub, as htree://npub1xdhn...eycm, with "htree://" and the whole npub on hover - since 2026-10-11, the operator's choice: "Seeing pyramid.fiatjaf.com and git.jb55.com in the list makes me lean towards htree://npub1xdhn...eycm in this case"; it had been the whole 63-character npub, two or three lines in the label column; any other host reads as its text) beside GRASP servers (table: repositories, users listing; first choice dropped 2026-10-01); relays per repository (blue since 2026-10-08, the Forges only colour then, the operator's choice - it stayed blue when Forges only went amber on 2026-10-12, a request that named Clone hosts as its only other change; pink for one day before) beside GRASP servers per repository (GRASP purple; the two swapped places on 2026-10-08, the operator's choice); collaboration relays (horizontal bars) beside web hosts, the top 10. Dropped on 2026-10-08: the announcing tools (a hand-kept heuristic) |
| Maintainers | most active maintainers table ("Top maintainers" until 2026-10-09), the 20 most active by pushes in 90 d and sorted that way until a header is clicked (name or npub, repositories, pushes 90 d, last push; links to njump), its caption saying since 2026-10-09 that a push counts only for the maintainer who published it, never for their co-maintainers, and a repository with several maintainers once for each of them (the operator's choice of wording; the earlier "a repository shared by co-maintainers counts for each of them" read as if one push counted for several); users with a GRASP server list (new users per week as bars - per month until 2026-10-02 - and the running total as a line on a second axis; servers per list with each bucket's share, captioned "How many servers users' lists name." since 2026-10-08; the first-choice chart dropped 2026-10-01, as a list's first host mostly records which client wrote it). Dropped on 2026-10-02: the four maintainer cards (distinct, active 30 d, confirmed vs listed shared repositories, the top-10 concentration) |
| Collaboration | discussion per week by root kind (stacked bars: issues / PRs / patches / repository / other; per month until 2026-10-02), participants and threads (two lines, each on its own axis since 2026-10-02), the comment ledger (ephemeral, imported, mirrored; collapsed by default since 2026-10-01), attributable share and the honest gap (the page says issue and PR roots are not archived yet, and keeps saying so until fork 2 lands, the operator's choice); stars and zaps to repositories with their sortable leaderboards (hidden below five rows; a "hashtree, no GRASP" repository's name opens on git.iris.to since 2026-10-09). Since 2026-10-02 the discussion, stars and zaps panels name no source relays, and since 2026-10-09 no part of the page does (the footer named them until that morning, What is counted until that afternoon), and the table of repositories named in comments is gone: without the issue and pull-request roots, how often a repository is named read as how much it is discussed. Since 2026-10-08 the stars card no longer says a star is one per key and not Sybil-resistant, nor the zaps card that a counted receipt is plausible, not proof of payment, nor since 2026-10-09 that unverified receipts are not counted (the operator's choice; the stars card then gave its total in the heading and its repository count in the leaderboard's cut). Since 2026-10-18, the operator's ask, neither heading carries a total and no cut names its population: the headings read "Stars" and "Zaps", the latter with the site's Lightning bolt (hidden from the heading's accessible name), the tables hold the top 20 (10 before) and each cut names only its own length ("The top 20 repositories by stars"; the population still decides whether "Sorting reorders these 20 only" follows), while the zaps card keeps its "31 receipts to 20 repositories from 17 zappers" line, its one line of figures; later that day the operator had the zaps table ranked by receipts (fork 26): the document ranks it by receipts, then sats from the first run after the deploy of 2026-10-18 07:34Z, and by sats, then receipts before, and the page opens it sorted by receipts whatever the document ranks by. Then, also at the operator's ask, neither cut names a measure: both read "The top 20 repositories." (until then the stars cut had said "by stars", and the zaps cut "by sats", then "by receipts" from 07:30Z that day): a reader can re-sort a table, and a screenshot of it must still read true. For the same reason, in every sortable table on the page the bright ink is on the column sorted at that moment and moves with each header click, the row's name keeping its own ink (the operator's rule; until then each table's ranking figure was bright, and Receipts had taken it from Sats at 07:30Z that day). Since the nostrology-web deploy of 2026-10-18 11:37Z both lists come from `ngit_repo_stats` (fork 27), which every run after the nostrology-data deploy of 11:26Z that day writes, the first at 11:31Z; the two tables' names break anywhere (they have no horizontal scroll, and an 80-character name with no break in it widened a 300 px table to 613 px); What is counted keeps the payment caveat, and dropped its Sybil one on 2026-10-09 (the operator's choice; the dataset reference, section 8, keeps all nine). Since the deploy of 2026-10-11 the stars count Iris Git's kind-17 (external-content-reactions) stars too (fork 25): the stars card's definition names both kinds and, since that afternoon in the operator's words, says an Iris Git star is net of unstars and a kind-7 (reactions) star is not, as the page does not currently read deletion requests for reactions; the Sources table lists kind-17 (external-content-reactions), What is counted's unstar caveat says Iris Git's unstars are read and those of kind-7 (reactions) are not, and the empty-input note names both kinds; a document without `archive_events.external_reactions` renders the old wording exactly |

Since 2026-10-01 every chart with time on its x-axis spans at most the trailing year, ending
at the document's `as_of_day`: the last 12 complete months, 52 complete weeks or year of days,
plus the bucket still filling (web `$lib/ngit/series` `yearWindowStart` / `inWindow`). Since
2026-10-02 every weekly chart - all of them but the cumulative one, which stays a trailing
year of days - starts no earlier than the week of 2026-06-29, the week holding 2026-07-01 - the first month after
every capture wall, so no bucket mixes what relays still served with what was captured and
only the trailing partial week carries a key; from 2027-07 they too span the trailing year
(`flooredWindowStart`). The document still carries each series from its start; the page
cuts it, padding a series that begins after the window's start with empty buckets. The
charts have no table twin since 2026-10-01 ("Show as a table" was removed; the
MCP is the way to the numbers), and the page does not show the archive's version
counts or describe it as keeping every version.

Repository rows link to `https://gitworkshop.dev/<npub>/<encodeURIComponent(d)>` (the
ecosystem's viewer), except, since 2026-10-09, a repository whose hosting class is
`hashtree` (on hashtree and no GRASP server; all 299 such repositories that day were on
hashtree alone), which links to `https://git.iris.to/#/<npub>/<encodeURIComponent(d)>`,
hashtree's own viewer and, for a `d` without a `/`, the address its git remote helper writes
as the `web` tag (the helper keeps a `/` literal, which Iris reads as a path inside the
tree; encoded, the whole `d` is one tree name): gitworkshop.dev reads code over HTTP(S)
only, and for a repository whose clone URLs are all `htree://` shows a "cannot access" card
that points there (section 13.1). Every repository row carries its class (`cls`; the
leaderboard rows since 2026-10-09, and a row without one links to gitworkshop.dev).
Maintainers link to `https://njump.me/<npub>`; npubs are encoded client-side with
nostr-tools from the hex pubkeys the document ships. Names and descriptions are third-party
text: the page escapes, the document bounds.

## 11. Verification

Six layers, deliberately different from the two-implementation agreement gate of
/marketplaces and /routstr - and deliberately not thinner than it in what matters. That
gate's standing cost is questioned in #24 (Consider retiring the adhoc /marketplaces
reference generator and comparator), and this surface has no adhoc predecessor to hold
agreement with. The six:

1. **The dataset reference comes first.** Re-implementing the first draft of this design
   from its text surfaced more than ten ambiguities (letter case in a rule, the weekly
   start, pre-wall pairs, ended roles, private GRASP hosts, class precedence); the reference
   resolves each, and both implementations below are built from it.
2. **Unit tests + invariants** (section 9) pin every rule on fixtures and every partition on
   real runs - and mutation-tested: review 4 ran 73 mutations of the rules and the reduction
   on a scratch copy, and every one fails at least one test (22 of the first 41 had survived
   before its targeted tests were added).
3. **Local real-data dry run before the first deploy**: the profile's DuckDB copy of the
   four roster kinds (a workstation, 2026-09-25, pinned at `ingested_at <= 1790320121`) is
   exported as NDJSON and fed through `build_snapshot()`; the document is diffed row for
   row against the cross-check's output on the same copy (layer 4) - the acceptance bar is
   ZERO differences, met on 2026-09-25 over 6,604 repository rows, 20,656 ledger rows and
   108 summary fields (announcements 6,604, kept 4,541, repositories 4,454, maintainers
   1,279, multi-maintainer 65, with a state 3,155, pushed 30 d / 90 d 582 / 1,415,
   pushes 17,636, GRASP-hosted 3,184 on 68 servers, fork families 242) - and the
   document is the fixture the page is developed against.
4. **Independent row-level cross-check**: `crosscheck/ngit_check.py` re-derives the
   SUBSTRATE with DuckDB SQL straight over the raw S3 archive - every announcement
   coordinate's family, repository key, hosting class, first seen, last state, push counts,
   and every state version's `changed` / `push` flag - written by a fresh-context agent FROM
   THE DATASET REFERENCE ALONE (it must not read `pipeline/ngit.py`), pinned with
   `--now` to the snapshot's `generated_utc` and `--ingested-before` to the Dagster run's
   start (the extracts read the archive minutes before the compute step stamps
   `generated_utc`; a later pin can only add false differences, never hide one), and diffed row by row against the
   `ngit_repos` / `ngit_pushes` tables exported from Aurora; the `summary` block and the
   ledger and partition blocks are compared field by field. The page's other panels
   (top lists, monthly series, hosting tables) are NOT re-derived by the check: they are
   sums and rankings over the substrate rows the check does verify, pinned by the unit
   tests and the invariants instead - except, since 2026-10-02, the four weekly series
   (`pushes_weekly` and the three added that day), which the check derives and compares week
   by week, and since 2026-10-08 `new_repos_weekly`'s `by_host`, entry by entry and as its
   sequence of keys (dated-optional before 2026-10-09). Names are outside the check. The
   three heavy kinds (reactions, zap receipts, deletion requests) are read either from the raw archive per
   `dt` partition or, for a pinned production run, from that run's Athena UNLOAD parts in
   the extracts bucket. It runs after the first production materialization and is the
   acceptance test for every ledger or methodology change.
5. **Replay over a production run's own parts**: `build_snapshot()` over the UNLOAD parts a
   production run wrote (fetched from the extracts bucket) at that run's `now_ts` must
   reproduce the served document exactly, names aside. It is how the memory reduction was
   shown exact and how every rule change of review 4 was measured against the served
   figures before it was deployed.

6. **The top lists, by SQL** (the cross-check derives neither `maintainers_top`, fork 22, nor
   the repository rows' class and kinds of host): the first query below re-derives reference
   5.1 from the substrate the check verifies and diffs it with the served list rank by rank,
   the second holds every repository row's class to the substrate's, and the third (since
   2026-10-11) every repository row's `host_kinds` to its members' substrate hosts. **Zero
   rows is the pass** for each.
   Run them as `nostrology_ro` on the box, streamed from a file (`psql -h "$H" -U
   nostrology_ro -d nostrology -XA -f - < query.sql`).

```sql
-- Post-materialize acceptance for dataset reference 5.1 (maintainers_top = top 20 by pushes
-- in 90 d, then repositories, then pubkey), derived from the substrate the cross-check
-- verifies. Zero rows = the served document's cut and order match the rule.
with doc as (select data from ngit_snapshot order by generated_utc desc, id desc limit 1),
asof as (select (data->>'as_of_day')::date as d from doc),
served as (
  select e->>'pubkey' as pubkey, (e->>'repos')::int as repos, (e->>'pushes_90d')::int as p90, ord
  from doc, jsonb_array_elements(doc.data->'maintainers_top') with ordinality as t(e, ord)),
members as (
  select pubkey::text as pubkey, count(distinct repo_key) as repos from ngit_repos where family = 'repo' group by 1),
p90 as (
  select p.pubkey::text as pubkey, count(*) as p90 from ngit_pushes p, asof
  where p.push and (to_timestamp(p.created_at) at time zone 'UTC')::date between asof.d - 89 and asof.d
  group by 1),
want as (
  select m.pubkey, m.repos, coalesce(p90.p90, 0) as p90,
         row_number() over (order by coalesce(p90.p90, 0) desc, m.repos desc, m.pubkey collate "C") as ord
  from members m left join p90 using (pubkey))
select coalesce(s.ord, w.ord) as ord, left(s.pubkey, 12) as served, left(w.pubkey, 12) as want,
       s.p90 as served_p90, w.p90 as want_p90, s.repos as served_repos, w.repos as want_repos
from served s full join (select * from want where ord <= 20) w on s.ord = w.ord
where s.pubkey is distinct from w.pubkey or s.p90 is distinct from w.p90 or s.repos is distinct from w.repos
order by 1;
```

The repository rows' class (dataset reference 4.4; since 2026-10-09 the leaderboards carry
it too): every row of the six repository lists names its repository's class as the
substrate's lead row does. A document written before that lists every leaderboard row.

```sql
with doc as (select data from ngit_snapshot order by generated_utc desc, id desc limit 1),
served as (
  select l, e->>'pubkey' as pubkey, e->>'d' as d, e->>'cls' as cls
  from doc, unnest(array['repos_active', 'repos_recent_pushes', 'repos_new', 'repos_discussed',
                         'repos_starred', 'repos_zapped']) as l,
       jsonb_array_elements(doc.data->l) as e)
select s.l, left(s.pubkey, 12) as pubkey, s.d, s.cls as served, n.cls as substrate
from served s left join ngit_repos n on n.pubkey = s.pubkey and n.d = s.d and n.is_lead
where s.cls is distinct from n.cls
order by 1, 2, 3;
```

The repository rows' kinds of host (since 2026-10-11, the Hosting column's words): every row
of the three repository lists carries R, reference 4.4, over the substrate hosts of every
member of its repository, in HOST_KINDS order, `["none"]` when R is empty. The invariant
(section 9) holds `host_kinds` only to the row's class, so kinds that are wrong but give the
same class pass it; this catches them. A document written before 2026-10-11 lists every row.
`ngit_repos` keeps 20 hosts a coordinate, so a coordinate with more would list falsely; on
2026-10-11 the most any has is 14, and the same derivation gives the stored class of all 4,607
repositories.

```sql
with doc as (select data from ngit_snapshot order by generated_utc desc, id desc limit 1),
served as (
  select l, e->>'pubkey' as pubkey, e->>'d' as d, e->'host_kinds' as kinds
  from doc, unnest(array['repos_active', 'repos_recent_pushes', 'repos_new']) as l,
       jsonb_array_elements(doc.data->l) as e),
r as (
  select n.repo_key,
         coalesce(bool_or(h->>'cls' = 'grasp'), false) as g, coalesce(bool_or(h->>'cls' = 'hashtree'), false) as t,
         coalesce(bool_or(h->>'cls' = 'forge'), false) as f, coalesce(bool_or(h->>'cls' = 'self_hosted'), false) as s
  from ngit_repos n left join lateral jsonb_array_elements(n.hosts) as h on true
  where n.family = 'repo' group by n.repo_key),
want as (
  select repo_key, coalesce(nullif(to_jsonb(array_remove(array[case when g then 'grasp' end,
         case when t then 'hashtree' end, case when f then 'forge' end,
         case when s then 'self_hosted' end], null)), '[]'::jsonb), '["none"]'::jsonb) as kinds
  from r)
select s.l, left(s.pubkey, 12) as pubkey, s.d, s.kinds as served, w.kinds as substrate
from served s left join want w on w.repo_key = s.pubkey || ':' || s.d
where s.kinds is distinct from w.kinds
order by 1, 2, 3;
```

Live checks after each deploy: the run's `last-run-ngit_refresh.json`, the snapshot row via
`nostrology_ro`, the page over HTTPS (status, the headline cards, each pill's panel ids),
the /wip link, the nav and sitemap absence, and the kind-slug checker over every touched
file.

## 12. Forks decided (2026-09-25)

| # | Fork | Decision | Why |
|---|---|---|---|
| 1 | Unit of counting | Repositories = reciprocal same-`d` maintainer components; announcements and the one-way count published alongside | Every maintainer announces separately; grouping by `d` alone would merge 668 strangers' coordinates, no grouping double-counts shared repositories; reciprocity is the current client rule (ngit v3 / GitWorkshop v4), and it differs from one-way by 66 repositories today (130 one-way listed vs 64 reciprocal). |
| 2 | Activity signal | A push = a state version whose ref map changed, in the captured era; identical republishes and HEAD-only changes are not pushes; every push counts (a cap of 50 per repository per UTC day applied until the nostrology-data deploy of 2026-10-02) | Raw version counts measure bots (one key = 70% of state ids); pre-wall pairs are survivorship; a per-day exclusion threshold would have excluded a real project, and the cap that replaced it trimmed only real work (1.9% of pushes, section 6), so the operator dropped it. |
| 3 | Bots and squatters | A published noise ledger of data-derived rules (N0-N10) with units, plus three ledger rows in other units; nothing silently dropped | The kind is open; the ledger keeps the choice auditable and the page honest. |
| 4 | History before capture | Series start at the wall with a visible marker; recency is labelled "last state seen" and covers repositories with a state; the no-state set is shown by hosting class. Since 2026-10-02 the page's weekly charts start with the week of 2026-06-29, the week holding 2026-07-01, after every wall, and recency and the no-state split are not shown | Relays keep one version per coordinate; "never pushed" would be the wrong reading for hashtree and forge mirrors. |
| 5 | GRASP detection | Clone-URL shape (npub path) over kept repositories, syntactic (a dot, not private, not a forge host), minus hand-kept denylists (web UIs; general relays; since 2026-10-12 a third, hosts a probe showed are not GRASP servers); kind-10317 (grasp-server-lists) hosts inform adoption only | 70 hosts serve GRASP-shaped URLs over kept repositories, localhost, forges (via one poisoned path), general relays and web UIs among them; 24 of the 101 user lists are a test harness and three listed hosts are not servers. |
| 6 | Fork families | Never counted; explicit forks counted. The largest families stay in the document; the page showed them, with both counts, until 2026-09-29 | Shakespeare omits the euc, so families are incomplete by construction. The operator dropped the table on 2026-09-29, with the recently pushed and newest repositories tables. |
| 7 | Discussion | Ecosystem-wide counts by root kind after a comment ledger (ephemeral, imported, mirrored); per-repository only where a coordinate is present (3.4% of counted comments), labelled | Comments carry the root event id, not the repository; 36% of captured-era comments are declared-ephemeral review bots. |
| 8 | Social signals | Stars (kind-7 (reactions), `+` or empty content, one per reactor and repository, `e` mapped through the announcement ids) and zaps (kind-9735 (zap-receipts), bolt11 amounts under the /lightning spam filter and cap, counted only when signed by a verified LNURL provider key from `data/lightning/provider_signers.yaml`, paying a maintainer, with a matching request amount) extracted in Athena; leaderboards hide below five rows; their extracts may be empty | Cheap to extract, canonical forge metrics; small on the first run (547 reactions, 26 receipts). |
| 9 | Deletions | NIP-09 (deletion requests) honoured through the self-authored pair aggregation, with restart semantics | /marketplaces honours it; an author's withdrawal should not keep a repository on the page. |
| 10 | Names | Display names resolved from the kind-0 (profile-metadata) serving table at run time, injected into the pure build, outside the cross-check | Hex prefixes are unreadable in a maintainers table; the table is already loaded nightly. |
| 11 | Verification | Reference first; unit tests + invariants; a real-data dry run; a row-level substrate cross-check written from the reference alone, instead of a second full implementation | The two-implementation gate's standing cost is under review in #24 (Consider retiring the adhoc /marketplaces reference generator and comparator); a row-level re-derivation keeps every number auditable at a fraction of it. |
| 12 | Placement | `/git` (`/ngit` until 2026-09-29), `/wip`-linked only, noindex, hidden from the sitemap; launched 2026-10-15: in the nav as Git-on-Nostr, on the landing's board, in the sitemap, indexable | Incubating until the operator's launch with the redesigned landing; graduation was one deliberate change. |
| 13 | Schedule | 03:15 UTC, after routstr; the deploy-freeze window moves to 03:45 | Same serialization story; reads a complete closed UTC day; the kind-0 (profile-metadata) table is loaded by then. |
| 14 | Deploy order | Data repo first (schema, job, one materialize), then web | `getNgitSnapshot` throws on an empty table by design. |
| 15 | Version tie-break | `created_at` ascending, id descending | NIP-01 (protocol basics): the lowest id survives a same-second tie; ngit v3 grinds a nonce to rely on it (one such pair exists today). |
| 16 | Anti-shrink | Relative to the newest served row (90%), absolute floors on the first run, an explicit dated override | Absolute floors weaken as the ledger grows by ~5,000 pushes a month and a legitimate tightening could cross them. |
| 17 | GRASP host poisoning | A forge host never enters the GRASP set (one announcement with a GRASP-shaped github.com path moved 750 repositories and the headline share from 71% to 88% with every gate green, review 4); the stricter per-repository rule (a URL is `grasp` only on a host THIS repository evidences) was NOT taken | The stricter rule removes all cross-repository influence but reclassifies 52 repositories today (gittr's hex-pubkey paths); revisit if a non-forge host is ever poisoned. |
| 18 | Zap admission | A receipt counts only when signed by a verified LNURL provider key (`data/lightning/provider_signers.yaml`, the /lightning list), paying a maintainer, with the request amount equal to the bolt11 amount | One fabricated receipt moved the Zaps headline 600x (review 4); the filter keeps 25 of the 26 real receipts (21 sats lost) and misses a real provider only until it is listed. |
| 19 | Junk clone values | N8b reads reachability: `todo`, `testing`, `.` are not clone URLs | 62 state-less shells with nothing reachable were kept as repositories (about 1.4%, review 4). |
| 20 | Social card | Kept: the shared meta component ships one on every page and the site's coverage test requires it (/routstr and /wip, both hidden, ship one too) | It exposes only the URL; dropping it for this page alone would need a no-card mode in the shared component plus a test exemption, a site-level change (web review 4). |
| 21 | Name and path | "Git on Nostr" at `/git`, moved from `/ngit` on 2026-09-29 with no redirect; the job, tables, documents and code keep ngit | ngit is one client: ngit and GitWorkshop together announce 38% of repositories by the inferred tool, beside Shakespeare, hashtree, NostrHub and mirrors. The site names a surface several products share by its category (/marketplaces, /nutzaps). /ngit was public (noindex) from 2026-09-25 but never publicized, so it gets no redirect and now answers 404. |
| 22 | Top maintainers | The 20 most active by pushes in 90 d, then repositories, then pubkey; the table opens sorted by pushes | The repositories-first cut listed four keys with no push since capture began (one announcing 329 repositories that has never published a state; three with 37 to 106, whose last ref changes predate the wall) and left out 14 of the 20 most active maintainers (2026-09-29), so a pushes sort over it would have ranked the wrong 20. The cross-check does not derive this list; the acceptance query in section 11 (layer 6) does. |
| 23 | Pill name | "Maintainers" ("People" until 2026-09-29, then "Contributors" for a few hours that day) | The pill counts keys that announce or push to repositories (its GRASP server list section counts list publishers, most of them maintainers too); in git, contributors are commit authors, whom this data cannot see; and a role, unlike "People", covers the AI agents among those keys. The operator's call (2026-09-29). |
| 24 | Iris Git test runs | Kept, under section 5's rule for test-like names on single-use keys; nothing on the page changes. The operator's call (2026-10-11): "Keep them, as now." | 175 of row 826's 300 hashtree-class repositories are Iris Git's end-to-end test runs. Dropping them would have moved the headline from 80.2% to 79.5%, the hashtree class from 300 to 125 and Maintainers from 1,334 to 1,159. An exact rule was on offer (a 13-digit millisecond stamp 0 to 2 s before the announcement, every clone URL `htree://`); a structural one (single-use key, one version, no state) would also have dropped 57 of the 125 real hashtree repositories. |
| 25 | Iris Git stars | kind-17 (external-content-reactions) with `k` = `git-repo` and an `i` value `30617:<owner>:<repo>` counts as a star by Iris Git's own rule: each key's latest reaction per coordinate, `+` or empty after trimming, so a `-` unstars; the lowest id breaks a same-second tie, as in NIP-01 (protocol basics). Mapped through the same member-coordinate map as kind-7 (reactions), and one `(pubkey, repository)` pair per key whichever client starred; a kind-17 (external-content-reactions) `-` never withdraws a kind-7 (reactions) star. `summary.stars_iris` records the pairs that come from kind-17 (external-content-reactions) (written, not shown). Stars on the kept test runs count, as fork 24 keeps them. Read since the deploy of 2026-10-11 (dataset reference 6.2) | Iris Git, hashtree's web app, stars with this kind, so /git saw 4 stars on hashtree repositories where Iris's rule gives 23 (nozzl#47 (Capture kind-17 (external-content-reactions) from all relays)). Counting both kinds puts hashtree's stars on the same footing as GRASP repositories', toward the parity with GRASP the operator asked for (2026-10-11). |
| 26 | Zaps ranking | The 20 repositories with the most counted receipts, then msats, then the key, from the first run after the deploy of 2026-10-18 07:34Z, the operator's ask that day; by msats, then receipts, then the key before. The table opens sorted by receipts | A receipts sort over a cut by sats ranks the wrong 20 as soon as more than 20 repositories are zapped, as fork 22 found for the maintainers. Row 1486 listed all 20 zapped repositories, so the set did not change, only the order: nostrhub's one receipt of 10,000 sats had ranked it first, and applesauce's five (1,005 sats) rank it first since. 15 of the 20 have one receipt, so once a 21st repository is zapped, sats decide which of those make the cut. |
| 27 | Stars and Zaps from a table | The page's server lists Stars and Zaps from `ngit_repo_stats`, one row per kept repository with its stars and zaps, written in the snapshot's transaction; it ranks and cuts them (20 each: stars by stars, then the key, as dataset reference 6.2 has it, and zaps by fork 26's ranking) for the served row's `run_id`, and keeps the document's own lists when the table holds another run. The operator's choice, 2026-10-18 | Which repositories a list holds was the pipeline's to decide, so each change to a list's length or measure took a Dagster image rebuild, a manual run, the reference and the web; it now takes the web alone. The /git MCP domain, coming soon, reads the same rows. Consistency is by `run_id`, never by timing; the invariants hold the document's lists to the table's top 20. |

## 13. Deficiencies, issues and gaps

What the surface cannot show or guard today, grouped by where a fix would land. The
decisions still open are section 15.

### 13.1 Data: what is not captured, and what more we could acquire

- **The regular NIP-34 (git collaboration) kinds are not read**: this surface reads none of
  kind-1621 (git-issues), kind-1617 (git-patches), kind-1618 (git-pull-requests), kind-1619
  (git-pr-updates) and the status kinds kind-1630 (git-status-open) through kind-1633
  (git-status-draft). Until it does there is no per-repository issue or PR count, no open /
  merged / closed state,
  no time-to-merge, no outside contributors, and about 97% of the counted discussion cannot be
  attributed to a repository (fork 2).
- **hashtree's pushes were invisible until 2026-10-12, and since then are counted from its
  announcement republishes** (section 6; nostrology-data#16 (/git: count hashtree pushes)). What
  follows is the gap as it stood, and why the roots are not the signal yet. Its git remote
  helper publishes a push as a kind-30064
  (hashtree-roots) - kind-30078 (NIP-78 app data) before 2026-06-22 - whose encrypted tree
  on Blossom holds the branch and tag tips, never as a kind-30618 (repo-states). So 298 of the
  299 hashtree repositories (247 maintainers) have no state and none has a counted push
  (substrate, 2026-10-09), and every push figure, Most active repositories and Most active
  maintainers leave them out. Capturing the root event would give a push signal (the remote
  helper publishes a new root on each push); the tips themselves would need the tree fetched
  from Blossom, and only public trees can be read. A read-only walk of 54 relays on 2026-10-09
  found the root kind mostly on relay.nostr.net (3,322 roots) and relay.ditto.pub (3,119, back
  to 2024), then relay.damus.io (997), relay.primal.net (606) and nos.lol (309, from
  2026-09-25); hashtree's own default relays, temp.iris.to and relay.snort.social, keep hours,
  and its author now pushes to a relay we read. The kind carries
  every hashtree tree (5,419, mostly Iris Files drives; 126 git trees, 74 of them announced
  repositories) and other apps' events, and live it is mostly hashtree daemons republishing
  their own index roots (about 6,300 a day, against pushes on the order of ten).
  The rest of hashtree is partly NIP-34 (git collaboration): it republishes
  the kind-30617 (repo-announcements) on every public push and publishes pull requests, issues
  (its web app only) and statuses, but no patches and no PR updates, and its replies are kind-1
  (posts), not kind-1111 (comments). A gitworkshop.dev link to a hashtree-only repository opens
  on a "cannot access" card that points to hashtree's own viewer,
  `https://git.iris.to/#/<npub>/<d>`; since 2026-10-09 the page links a hashtree-class
  repository there directly, the leaderboard rows carrying the class (no such repository is in
  a linked list that day: the 25 most active, 10 most starred and 10 most zapped are GRASP or
  other hosts). In the 90 days to 2026-10-08 its repositories' announcements were republished
  840 times (35 repositories, 3 keys, 828 of them by hashtree's author; the archive's versions
  of the 299 hashtree-class coordinates, each one's first left out, read from S3 on
  2026-10-09): about one per public push, none of which /git counts. Since 2026-10-09 the
  page says so (What is counted, "Not hashtree's pushes"). /git does not read the root kind.
  The same day the operator kept N9 as it is (section 5): it also drops hashtree's real forks,
  and that is for hashtree to close. Sources: mmalmi/hashtree a60f3cd0, irislib/iris-apps e115131c,
  DanConwayDev/gitworkshop dc36db64f6, read 2026-10-09. An audit of how /git treats hashtree,
  against row 793 on 2026-10-10, covers, besides pushes and N9:
  - stars: kind-17 (external-content-reactions), not read then; read since the deploy of
    2026-10-11 (fork 25), so Iris Git's stars count beside kind-7 (reactions) ones;
  - Clone hosts: one "host" per owner's npub;
  - Anatomy, and kind-1 (posts) replies;
  - the N5 asymmetry;
  - the test runs that inflate the class.

  Not disclosed on the page: Clone hosts and replies (and stars, until 2026-10-11).
- **Iris Git stars that dev does not have.** By Iris Git's rule, 23 non-test stars on
  hashtree-class repositories were on the wire on 2026-10-09; dev's relays served 11 of them.
  relay.nostr.net held 20 of the 23; it is a source since 2026-10-12 (nozzl#45 (Onboard
  relay.nostr.net once #32 is fixed)), and the first document after it, at 14:21:52 UTC, counts 26 Iris Git stars; `hashtree`
  has 16 stars, 4th in Most starred. A star from Iris's desktop app or built-in browser goes only to the
  local hashtree daemon, which does not forward it, so no archive can count it.
- **relay.ngit.dev for social kinds: done 2026-10-01** for kind-1111 (comments), kind-7
  (reactions) and kind-9735 (zap-receipts) (follow-up 3). kind-5 (event-deletion-requests)
  stays off it: on 2026-07-01 ~99% of its 65k there were bot noise, and nozzl cannot filter
  within a kind.
- **`wss://index.ngit.dev`** (the ecosystem aggregator) was not a source until 2026-10-12. The
  /git pre-launch audit's probe found 227 repositories there the archive lacked, and it has been
  read since (section 3).
- **kind-10018 (NIP-51 git repositories follow list)** and **kind-10017 (NIP-51 git authors
  follow list)** are the only "follows" signal; replaceable, cheap to onboard.
- **Pre-capture history is one snapshot per coordinate**; nothing recovers it.
- **Unstars** of a kind-7 (reactions) star are kind-5 (event-deletion-requests) of the
  reaction; the deletions extract names repository coordinates only, so kind-7 (reactions)
  stars are net of nothing. Iris Git's unstars are a kind-17 (external-content-reactions) `-`, which is read
  since 2026-10-11 (fork 25).
- **Stars are one per key** with nothing behind them: fifty fresh keys make fifty stars (review
  4). Requiring a kind-0 (profile-metadata) row per reactor is the cheap next step, a
  web-of-trust filter the real one.
- **The GRASP host set is syntactic** (shape, a dot, not private, not a forge, not on a
  denylist): a typo host, a dead host, a `.onion` name or an unrelated relay with one
  GRASP-shaped path counts as a server. The probe of 2026-10-12
  found 5 of the 63 hosts on row 991 were
  not GRASP servers (four now on the third denylist, and git.gittr.space, gittr's hosted git
  server, on the forge list), and 14 did not answer. It also found a GRASP
  server mounted under a path, laantungir.net/grasp (10 repositories), which the shape rule
  misses. Probing the hosts (NIP-11 (relay information) `supported_grasps`, or a git request
  for the npub path) would turn the set into an observed one: fork 5, the decided long-term
  fix.
- **Membership has no history**: the current maintainer listing applies to every push in the
  ledger; role intervals would need each announcement version's listing (archived, unused).
- Bots: the noise ledger is rule-based, but a new bot with a new shape reaches the page
  until a rule is added; the ledger, the `roster_growth_sane` and `push_concentration_sane`
  checks, the document's concentration figure (not shown on the page since 2026-10-02) and
  the Top-N tables are the early warning.

### 13.2 Code shared with other surfaces

- **The shared zap parser.** /git reads receipts through `src/shared/zap_parser.py` with
  bounds of its own (`zap_amount_msats`, `zap_request` in `pipeline/ngit.py`): a bolt11 over
  2,048 characters, or any error while reading it, is "no amount" and the receipt is dropped;
  a description over 65,536 characters, or any error while parsing it, is "no zapper" and the
  receipt is kept (reference 6.3, pinned by tests).

### 13.3 Operations

- **A dead Dagster daemon raises no alarm of its own.** `discord_failure_sensor` and
  `freshness_sensor` are daemon-hosted, so they cannot report the daemon's own death; the 26 h
  freshness rule catches a stopped schedule only while the daemon is up. The dead-man is the
  out-of-band heartbeat timer in `deploy/dagster/heartbeat/`, which a deployment installs
  (dagster-orchestration-design.md, "What a deployment provides"; it needs root on the host). Without it a
  dead daemon shows only on a pull: `daemonHealth.allDaemonStatuses` on Dagster's GraphQL, or
  the last-run files going stale.
- **Memory follows the roster.** After the streamed roster parse the compute peaks at
  545 MiB (row 67); the growth left is the N5 flooder's all-versions extract. Whether to cap
  it is section 15, fork 8.
- **The kind-5 (event-deletion-requests) extract shares a cliff with /marketplaces**: both
  deletion extracts scan the same Athena table, 3.2 GiB of the workgroup's enforced 10 GiB
  per-query cutoff when this was written (about 16 months of headroom at that growth); when it
  hits, both jobs fail on the same night. The extract warns at 60%
  of the cutoff; an incremental triple table appended per new ingest day is the long-term fix.

### 13.4 Verification limits

- **The cross-check proves the port, not the rules.** `crosscheck/ngit_check.py` re-derives every
  row and field from the reference alone; four zero-difference production runs say the
  pipeline follows the reference. A rule both sides implement as written passes it: every
  review-4 finding (GRASP poisoning, zap admission, junk clone values) came from reading the
  reference and the code, none from the check.
- **The pin is coarse and the inputs expire.** `--ingested-before` is one instant for all
  kinds while each extract flushes at its own moment, so a row ingested in those seconds is a
  false difference (reference 9.1); the run's UNLOAD parts are deleted after 7 days, after
  which the check stops with exit 2 rather than comparing against empty inputs.
- **On-box verification of the page reaches the Overview pill only.** The server renders the
  Overview pill; the other four render on the client after a pill click, so a check against
  the server's HTML, on the box, cannot see them. They are covered by the page tests and by a
  headless Chrome run against the same commit on a workstation, not on the box.

## 14. Operations

| Question | Answer |
|---|---|
| Did last night's refresh run? | `last-run-ngit_refresh.json`, beside the file `DAGSTER_LAST_RUN_JSON` names (`overall`, `finished`), the read-only Dagster UI's GraphQL, or the Dagit run list |
| Refresh failed - is the page ok? | Yes by construction: it serves the previous snapshot row |
| Force a refresh now | The deployment's re-run action for `ngit_refresh` (queued, serialized) |
| The numbers look wrong | Run `crosscheck/ngit_check.py` with `--now` at the row's `generated_utc` and `--ingested-before` at the Dagster run's START (recipe: dataset reference section 9; a difference confined to rows ingested between the run start and that kind's extract flush is the pin's imprecision, not a bug) against the exported substrate; equality means the dispute is with the methodology (dataset reference), not the pipeline |
| A repository is missing or wrongly excluded | `SELECT family, ... FROM ngit_repos WHERE pubkey LIKE '<prefix>%'` (any read role); the ledger rule that matched is in the row |
| Anti-shrink tripped | The refusal reason is the step's error (Dagit run page, GraphQL `stepStats`, or `compute_logs/<run_id>/.../*.err` on the dagster volume): which count fell and by how much. If the shrink is a deliberate methodology change, set `SHRINK_OVERRIDE_UNTIL` in the same commit (the gate ignores an override more than 14 days past `generated_utc`); if it is a genuine withdrawal (a top announcer filing kind-5 (event-deletion-requests) for its roster can move more than 10%), confirm it in `ngit_repos` (`family = 'N6'` rows with a recent `last_seen`) and set the override for one run; otherwise it is an extract or ledger regression - fix it |
| Failure alerting | `discord_failure_sensor` -> the CI channel, like every surface; since 2026-09-26 its post carries the failed step's first error line and logs `discord: posted` / `no webhook configured` so Loki can tell a silent webhook from an unconfigured one. Until 2026-10-12 that line was Dagster's wrapper ("Error occurred while executing op ..."), not the asset's error; since then the post reads the wrapper's cause, so an anti-shrink refusal or a held key reads as itself |
| A run is never created (tick error, queue stall) or the last-run file goes stale | `freshness_sensor` (daemon-hosted, every 30 min) posts one line per job and UTC day when a job's `last-run-<job>.json` is missing, failed, or older than 26 h (daily schedule + 2 h of slack) |
| The Dagster daemon dies | ALERTED only by the out-of-band heartbeat timer in `deploy/dagster/heartbeat/`, which a deployment installs (dagster-orchestration-design.md, "What a deployment provides"); the sensors are daemon-hosted, so they cannot report it. Without the timer it is DETECTED only on a pull: `daemonHealth.allDaemonStatuses` on Dagster's GraphQL (per daemon type, so a single wedged thread shows too), or the last-run files going stale. |
| A receipt from a real provider is not counted | Its signer is not in `data/lightning/provider_signers.yaml`. Attribute it first: the recipients' lud16 domains at zap time (`events_kind_9735_zaps` joined to `events_kind_0_profiles` on `recipient_pubkey`, as any read role); then GET `https://<domain>/.well-known/lnurlp/<localpart>` and compare `nostrPubkey` (NIP-57 (lightning zaps)); add the key under its domain with `verified_at` / `verified_via`, or with a note when the key is retired and the endpoint has moved on (the Breez case, section 15 fork 7); commit, then deploy the Dagster stack (the YAML is image-baked; /lightning reloads its table nightly); the next run counts it |
| First-ever deploy ordering | data repo first (the schema, then the Dagster stack for the job), ONE materialize so a snapshot row exists, THEN the web deploy |
| Roll the Dagster code back past 2026-10-18 | No action: the old code never writes `ngit_repo_stats`, so the table keeps the last newer run's rows, the newest snapshot row's run is not among them, and the page keeps the document's own lists, logging one warning per run (`[ngit] ngit_repo_stats holds no row of run ...`). Rolling the web back needs none either: the old page reads the document alone. To see which path serves, read `listsFrom` in the page data: `ssh "${NOSTROLOGY_RO_SSH:?}" 'curl -s '"${NOSTROLOGY_WEB_LOOPBACK:?}"'/git \| grep -o "listsFrom:\"[a-z]*\""'` (`$NOSTROLOGY_WEB_LOOPBACK` is the web app's address as the box sees it) |
| Roll the Dagster code back past 2026-10-02 | The first run on the 2026-10-02 code drops `ngit_pushes.counted`; older code inserts it and fails every night until the column is back. First, as the owner: `ALTER TABLE ngit_pushes ADD COLUMN counted boolean NOT NULL DEFAULT false`; the old code's next run fills it. Roll the web back with it (the page then expects the monthly series again) |
| `held_keys_absent` failed | A held key (the nostr.net catalog) has kept announcement coordinates, so N10 no longer excludes it: a regression in N10, or a change to the key's constant. The page keeps the previous row; the Discord post carries the error line (the step's error in full is in Dagit, GraphQL `stepStats` or `compute_logs`). Fix N10 in both implementations (dataset reference 2.2 first), deploy the Dagster stack and re-run the job. To count the catalog instead, a new decision of the operator: drop N10 and the key from `HELD_KEYS` together (with `test_held_keys_hold_the_nostr_net_catalog` and N10's tests), in both implementations |
| `push_concentration_sane` warned | One repository holds more than half the pushes of the 7 days to `as_of_day` (the run log names it). Look at its `ngit_pushes` rows: a script moving a ref every few minutes is a new rate shape the ledger does not cover; decide between a ledger rule and the daily cap (section 6) |

## 15. Open forks (2026-09-26)

Decisions not taken, with what each would change. Section 12 holds the ones taken; section
13 the gaps that need no decision, only work.

1. **GRASP strictness.** A clone URL is `grasp` on any host in the GRASP set (section 7).
   The stricter rule, a URL is `grasp` only on a host THIS repository evidences with a
   GRASP-shaped path, removes all cross-repository influence but reclassifies 52 repositories
   today (gittr's hex-pubkey paths). Revisit if a non-forge host is ever poisoned; the forge
   exclusion and the three denylists are the current defence.
2. **The regular NIP-34 (git collaboration) kinds - ACQUIRED 2026-10-09, not read yet.**
   Issues, patches, pull requests and the status kinds are archived since 2026-10-09 (13.1);
   reading them here is the open part. It would unlock per-repository issue and PR counts,
   open / merged / closed state, time to merge, outside contributors, and attribution for the
   counted comments that name a root event this surface cannot map (96.7% on row 199).
   Measured 2026-09-29 on the top row of the table of repositories named in comments,
   gitworkshop.dev: 11 counted comments by 5 authors name the repository; 209 more counted
   comments (153 on issues, 45 on pull requests, 11 on patches) reply to one of its issues,
   patches or pull requests without naming it (165 of its 166 issues and all its 31 patches and
   105 pull requests are on relay.ngit.dev), so attributing through the root would give 220
   comments by 25 authors. The served 11 reproduce exactly under 6.1. Until this fork lands, the
   page keeps both passages that say the roots are not archived, the Collaboration discussion
   sentence and What is counted's "Not issue or pull-request counts" caveat (the operator's
   choice, 2026-10-09); rewrite both with it.
3. **relay.ngit.dev for the social kinds - DONE 2026-10-01** for kind-1111 (comments),
   kind-7 (reactions) and kind-9735 (zap-receipts): added to relay.ngit.dev's `only_kinds`
   (nozzl's relay list, hot-reload, history backfilled; NIP-45 (event counts)
   that day: 8,088 / 1,613 / 180 there; the backfill took 9,893 events at 15:22 UTC). It
   reached S3 partly with the writers' 15:2x UTC buffer flush (the archive's writers buffer
   12 h) and wholly after an operator-approved ad-hoc flush at 15:46:58 UTC, so documents
   generated from 15:50 UTC that day hold it, and the page named relay.ngit.dev as their
   source from then until 2026-10-09, when it stopped naming source relays (section 10).
   comments_total stepped from 3,012 (03:18 run) to 5,150 on the 15:38 refresh, which already
   held most of the comments. kind-5 (event-deletion-requests)
   stays off relay.ngit.dev (bot noise). Measured before the change, for
   gitworkshop.dev: 52 comments are on relay.ngit.dev and the GRASP relays but on none of our
   three comment sources (2026-09-29, eight relays probed); 51 are admissible under 6.1 (one has
   no `K`), and none names the repository, so without fork 2 they would raise `comments_total`
   only.
4. **Stars and Sybil resistance.** A star is one per key; fifty fresh keys make fifty stars.
   Requiring a kind-0 (profile-metadata) row per reactor is cheap and removes the emptiest
   keys; a web-of-trust filter is the real fix and a site-level one.
5. **GRASP host set: syntactic or observed - the direction DECIDED 2026-10-12, not built.**
   Today a host joins the set on shape, a dot, public DNS, not a forge, not denylisted
   (section 7). Probing each candidate host (NIP-11 (relay information) `supported_grasps`, or
   a git request for the npub path) would turn the set into an observed one, retire the three
   hand-kept denylists, drop the dead hosts (14 of 63 on row 991) and admit path-mounted
   servers; it adds an outbound probe to the nightly run. The operator chose it as the
   long-term fix on 2026-10-12 and, for now, the third denylist (nostrology-data#17 (/git: 5
   of the 63 hosts in the GRASP host set are not GRASP servers and 14 are dead)). A full
   GRASP-01 probe of the set, with one structured record per host, is the starting point for the
   build (one ran on 2026-10-13).
6. **The deletions extract cliff.** The kind-5 (event-deletion-requests) scan is at 3.2 GiB
   of the 10 GiB Athena cutoff, shared with /marketplaces, warning at 60% (13.3). The
   long-term fix is an incremental triple table appended per new ingest day; deciding when
   is a cost question, with about 16 months of headroom.
7. **The zap admission list.** A receipt counts only when its signer is in
   `data/lightning/provider_signers.yaml` (31 keys, verified 2026-05-09 against each
   provider's NIP-57 (lightning zaps) `nostrPubkey`; nostrology-data has no tool that
   re-verifies them), its
   `p` is a maintainer, and the request amount equals the bolt11 amount. Today 25 of the 26
   receipts pass. The excluded one (21 sats, 2026-06-22, to a GitCitadel maintainer) is
   signed by `a6796f84...bbc`, which is no fabricated key: it signed 15,064 receipts in the
   /lightning table for recipients on breez.tips (1,736) and cake.cash (1,713), that is the
   Breez SDK LNURL service, last seen 2026-08-05; both domains publish other keys today
   (breez.tips `623fd8b4...`, cake.cash `597aad8e...`), so it is a retired key that live
   verification can no longer confirm. The list is stale more widely: 11% of the archive's
   receipts (about 400,000 of 3.68 million) carry unlisted signers, among them active ones
   attributable by their recipients' domains to wavlake.com (`7759fb24...`, 7,729 receipts),
   fountain.fm (`b866ce76...`, 40,742) and pay.bitcoinjungle.app (`aa55a479...`, 4,619).
   Options: (a) verify and add the active unlisted signers, and add retired keys on
   recipient-domain evidence with a note saying so; (b) admit by evidence instead of by list:
   a signer counts once it has signed receipts for N distinct recipients whose lud16 sits on
   one domain, which needs no maintenance and admits a provider the day it appears, but lets
   a patient attacker qualify a key; (c) keep the list and accept the loss, 21 sats today.
   The recipe for (a) is a section 14 row. Whichever is chosen also serves /lightning, which
   attributes receipts to providers through the same list.
8. **A memory ceiling.** A hard memory limit on `dagster-code` would end the run at a
   known size instead of letting it slow its host. Setting it means choosing a number that
   the growth in 13.3 will reach, and choosing to fail the run rather than slow the host when
   it does.
9. **Graduation.** DONE 2026-10-15: in the nav as Git-on-Nostr, on the landing's board, in the sitemap, indexable and
   off /wip (fork 12). Its link-preview title became "Git on Nostr, in numbers" the same day
   (it was "Git on Nostr, measured"). Still open from this fork: whether
   an MCP surface comes with it.
10. **The two-implementation gate.** /marketplaces and /routstr each carry a second full
    snapshot builder in `crosscheck/` (DuckDB over the raw S3 archive) that must
    produce the same snapshot as the Dagster pipeline. #24 (Consider retiring the adhoc
    /marketplaces reference generator and comparator), open since 2026-09-14, weighs retiring
    the marketplaces pair, because every methodology change lands twice in
    lockstep, the generator's environment was unpinned until 2026-10-24, and the two share their geo code. /git took
    fork 11 instead: not a second builder but a row-level re-derivation written from the
    reference alone (`crosscheck/ngit_check.py`, about 1,300 lines with 31 self-test cases), its
    author forbidden from reading `pipeline/ngit.py`. That keeps the independence the issue
    questions, but not the cost: review 4's six rule changes were written three times
    (reference, pipeline, checker), and each production re-check takes about ten minutes and
    needs the run's UNLOAD parts within their 7-day life. What the check has bought: four
    zero-difference production runs pinning the port to the reference, and the "numbers look
    wrong" procedure in section 14. What it has not bought: a finding (13.4). Options: keep
    the lockstep permanently; keep the checker for large methodology changes only, accepting
    that a drifted checker is worse than none because it reports false differences; or retire
    it after a final baseline, as the issue proposes for marketplaces, and rely on the unit tests,
    the invariants, the anti-shrink gates and the replay on a run's own parts. Decide together
    with #24 (Consider retiring the adhoc /marketplaces reference generator and comparator),
    since the argument is the same on all three surfaces.
11. **Web choices left as they are** (web review 4, each with the reviewer's point and why
    it stands):
    - *Colour meaning is per pill, not per page.* The review found purple meaning GRASP, all
      repositories and issues on one pill. Now a hosting colour means its hosting class on any
      pill that colour-codes hosting (Overview, Hosting), pinned by `palette.ts` and its test;
      the Repositories, Maintainers and Collaboration pills, which code no hosting class, still use
      the site palette freely, so purple is issues in the five-series discussion chart. A
      page-wide single meaning would need a second, CVD-validated palette for that chart.
    - *No sticky pill bar on phones; a tap scrolls instead.* The review found the pills 1.4
      screens down on a phone. The bar is now drawn first, and since the opened panel then
      starts below the six cards, a tap scrolls to it under the sticky nav's measured height.
      The bar scrolls out of view while reading, so switching again means scrolling back up. A
      sticky bar would keep it in reach at the cost of about a sixth of a phone screen (nav plus
      a two-row bar) and the iOS sticky and fixed quirks recorded for the site's other bars.
    - *"network-wide" stays.* The Collaboration pill says discussion is "counted network-wide
      by the kind of its root"; the review called the phrase misleading because the social
      kinds are read from three general relays and not from relay.ngit.dev. The phrase is the
      dataset reference's (section 8), and the rule is to change the reference first; the
      misleading part is addressed by the relay caveat now on each of the three social cards.
      Open: replace it with "across all repositories" in the reference and the page together.
    - *Hover titles show the raw `pubkey:d`.* Repository links carry the raw coordinate as
      their `title` so a reader can see and copy the identity behind a cleaned name. `d` is
      third-party text and may hold bidi controls; in a `title` attribute the browser draws a
      plain tooltip, so a control there can garble the tooltip only, never the page. Passing
      `d` through `visibleText()` would lose the exact identifier the tooltip exists to show.
    - *The social card stays* (fork 20). The shared `SocialMeta` component ships one on every
      route and the coverage test requires it, hidden routes included; the card exposes the
      URL and title and nothing else. A "no card while incubating" policy is a site-level
      decision: a no-card mode in the component and an exemption in the test.
12. **Unsampled sources.** `wss://index.ngit.dev`, the ecosystem aggregator, was not a source
    until 2026-10-12: the /git pre-launch audit probed it (227 repositories the archive lacked)
    and it has been read since (section 3; resolved). kind-10017
    (NIP-51 (lists) git authors follow list) and kind-10018 (NIP-51 (lists) git repositories
    follow list) are the only "follows" signal for git and are cheap, replaceable kinds to
    onboard.
13. **What the page no longer shows** (2026-09-29, extended 2026-10-01, 2026-10-02 and 2026-10-08). The
    pipeline still writes `default_branch`, `repos_recent_pushes`, `repos_new`,
    `fork_families_top`, `forks`, `fork_families`, `concentration.top1_share` and
    `pushes_total`; since 2026-10-01 both `first_choice` lists; and since 2026-10-02
    `recency`, `no_state`, `new_repos_monthly`, `comments_monthly`, `grasp_lists.per_month`,
    `multi_maintainer_repos`, `shared_listed_repos`, `concentration.top10_share`,
    `coverage.partial_first_week`, `coverage.partial_month`, `coverage.ngit_relay_since` and
    `repos_discussed`; and since 2026-10-08 `tools` and the `grasp` and `other` of the
    `new_repos_weekly` rows (the chart draws their `by_host`). The page reads none of them
    (a web test pins it) and they are optional to it. The four lists are 29% of the
    compact document on row 199 (25% gzipped), shipped in every /git page, and
    `check_invariants` still validates them, so a defect in data nobody sees can fail a
    nightly run. Dropping them is a pipeline, reference and checker change (`forks`,
    `fork_families` and `pushes_total` are three of the 110 summary fields the cross-check
    compares) with its own acceptance run; keeping them is the choice to make deliberately,
    for instance for an MCP surface at graduation (fork 9).
14. **Comment attribution edge cases** (measured 2026-09-29; the largest, reading a
    coordinate from `E`, would add 16 comments network-wide and take gitworkshop.dev's row from
    11 to 17). 6.1 reads the repository coordinate from `A`/`a` only, so 16
    counted comments that put a `30617:` coordinate in `E` stay unattributable (6 of them on
    gitworkshop.dev) and each such coordinate counts as one thread; the mirror rule is per key,
    so the one mirror key's 8 comments in its owner's own words are dropped with its 2,767
    re-posts; a one-way co-listing keeps a fork's comments on the fork (2 for gitworkshop.dev);
    and a comment whose `K` says repository while its `E` is an issue or a pull request is
    filed under repository in `comments_monthly` (2 of gitworkshop.dev's 11).

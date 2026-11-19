"""The /git metrics compute - one snapshot per run from the NIP-34 (git collaboration) kinds.

PRODUCTION HOME of the /git methodology. The rule text it implements is docs/dataset-reference/ngit-dataset-reference.md, which is AUTHORITATIVE: where this module and the reference
disagree, this module is wrong. The surface design (pipeline, page, operations) is
docs/design/ngit-design.md. The independent
cross-check, crosscheck/ngit_check.py, is written from the reference alone (it must not
read this file) and diffs the SUBSTRATE row by row, so a rule change lands in the reference,
here and in the check together. Section numbers in the comments below ("ref 2.2") point at
the reference.

INPUTS (parsed NDJSON rows from the eight Athena UNLOAD extracts - see sql/):
  * announce_events - kind-30617 (repo-announcements), ALL versions, id-deduped,
                      {id, pubkey, kind, created_at, content, tags, sig, d_tag}; the asset
                      passes them through reduce_roster (full tags only on each
                      coordinate's current version inside the horizon), which changes
                      nothing any rule reads - see "the roster reduction" below
  * state_events    - kind-30618 (repo-states), ALL versions, same shape (reduced likewise:
                      full tags only at the member coordinates of kept repositories)
  * grasp_events    - kind-10317 (grasp-server-lists), ALL versions (no d_tag)
  * comment_events  - kind-1111 (comments), the git subset selected in Athena (a superset:
                      the SQL admits ANY `K` tag in the set; ref 6.1's FIRST-`K` rule is
                      re-applied here)
  * reaction_events - kind-7 (reactions) with a k=30617 tag or an a=30617:... tag
  * external_reaction_events - kind-17 (external-content-reactions) with a k=git-repo tag AND
                      an i=30617:... tag: Iris Git's stars (keyword-only, like zap_signers)
  * zap_events      - kind-9735 (zap-receipts) with an a=30617:... tag
  * deletion_rows   - kind-5 (event-deletion-requests) reduced in Athena to the DISTINCT
                      {author, coord, created_at} triples of SELF-AUTHORED requests naming
                      a 30617-coordinate (the horizon applies per triple here)
  * names           - {pubkey: display name}, resolved by the asset from the kind-0
                      (profile-metadata) serving table; presentation only
  * zap_signers     - the verified LNURL provider signer keys (ref 6.3), loaded by the asset
                      from data/lightning/provider_signers.yaml with the /lightning loader's
                      parser (load_provider_signers)
  * now_ts          - the run instant; the ONLY clock this module reads

OUTPUTS: `build_snapshot` returns (state, data): `data` is exactly the page's snapshot
document (docs/design/ngit-design.md section 8), `state` carries the substrate rows (ngit_repos:
one per announcement coordinate; ngit_pushes: one per state version of a kept repository,
source `state`, plus one per hashtree push, source `htree_announcement` - ref 3, since
2026-10-12) plus the few intermediate lists `check_invariants` re-derives the document from.
`write_pg` persists all of it in ONE transaction behind the relative anti-shrink gates.

ARITHMETIC: integer throughout (counts, unix seconds, msats). The three shares
(grasp_share, comments_attributable_share, concentration) are one final division of two
integers, unrounded, so any implementation that divides the same integers gets the same
IEEE double.
"""
import collections
import datetime as dt
import gzip
import importlib.util
import json
import os
import re
import sys
import unicodedata

DAY_S = 86400

# ref 1: the capture walls - the earliest ingested_at of each kind in the dev archive.
ANN_WALL = 1777740626      # 2026-05-02T16:50:26Z  kind-30617 (repo-announcements)
STATE_WALL = 1781407359    # 2026-06-14T03:22:39Z  kind-30618 (repo-states), kind-10317 (grasp-server-lists)
COMMENT_WALL = 1781751569  # 2026-06-18T02:59:29Z  kind-1111 (comments)
# "Captured since" dates of the three social kinds, for the page's Sources table only (nothing
# is computed from them): the earliest dt= (ingest-date) partition under
# s3://$NOZZL_ARCHIVE_BUCKET/c=regular/k=<kind>/, listed 2026-09-25. The same listing reproduces
# the three walls above to the day (k=30617 2026-05-02, k=30618 and k=10317 2026-06-14,
# k=1111 2026-06-18), which is why it is the source.
REACTIONS_SINCE = "2026-06-19"   # kind-7 (reactions)
# kind-17 (external-content-reactions): the archive holds it since 2026-10-11 05:02:39 UTC,
# so its earliest dt= partition is that day's.
EXTERNAL_REACTIONS_SINCE = "2026-10-11"
ZAPS_SINCE = "2026-06-14"        # kind-9735 (zap-receipts)
DELETIONS_SINCE = "2026-07-01"   # kind-5 (event-deletion-requests)
# coverage.ngit_relay_since (ref 0.2): the day relay.ngit.dev, which most GRASP-hosted
# repositories name, joined the capture (nozzl's relay list). It equals states_since only
# by coincidence - a different fact, so the page's "added on" sentence reads this key. Like
# the three dates above it is a constant for the Sources text; nothing is computed from it.
NGIT_RELAY_SINCE = "2026-06-14"
# ref 1.2: rows dated before this are dropped and counted as past-dated (NIP-34 (git
# collaboration) did not exist; such a timestamp is a broken clock or an attack on the
# time axis). The other end of the horizon is as_of_day 23:59:59Z.
PAST_FLOOR = 1672531200    # 2023-01-01T00:00:00Z

# ref 2.2: the noise ledger's literals.
D_MAX = 200                # N0: a longer d is an oversized identifier (the longest real one is 143)
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")   # N0: a d with a control character is malformed
SQUATTER_PREFIX = "https://nostr.life/"
HARNESS_CLONE_MARK = "@example.com:"
TEMPLATE_NAME = "<human-readable project name>"
SECRET_KEY_1_PUBKEY = "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
# N10: the key (profile "nostr.net") that re-published 402 of the awesome-nostr list's 413
# entries with a code-host link as 408 announcements of other people's projects on 2025-11-16
# (6 of them under two categories); excluded by the operator's decision of 2026-10-12.
# It also excludes any later announcement by this key, and a re-publication under another key
# escapes both N10 and HELD_KEYS: the scope the operator chose.
NOSTR_NET_CATALOG_PUBKEY = "62ec33a4090f0959e5c547a33f22a3253688bba121667a69e64a2dc758f8baaf"
ANN_LOOP_MIN_VERSIONS = 1000
ANN_LOOP_MAX_MEDIAN_GAP_S = 300     # N5: the median gap must be BELOW this
# ref 3: a state coordinate this busy is a republish loop, not a push history.
STATE_LOOP_MIN_VERSIONS = 1000
STATE_LOOP_MAX_MEDIAN_GAP_S = 120

# ref 4.2 / 4.3: host classes. git.gittr.space is gittr's hosted git server: it serves the
# repositories of everyone who pushes or imports through the gittr.space forge (185 kept
# repositories of 45 keys name it on 2026-10-12), so it is a forge, not a self-hosted server;
# the operator's decision on nostrology-data#17 (/git: 5 of the 63 hosts in the GRASP host set
# are not GRASP servers and 14 are dead), 2026-10-12.
FORGE_HOSTS = frozenset({
    "github.com", "gitlab.com", "codeberg.org", "bitbucket.org", "git.sr.ht", "tangled.org",
    "gitea.com", "gitee.com", "framagit.org", "notabug.org", "git.gittr.space",
})
NETWORK_SCHEMES = frozenset({"http", "https", "ssh", "git", "ws", "wss"})
GRASP_SCHEMES = frozenset({"http", "https", "ws", "wss"})
# ref 4.3: the GRASP host set never admits a forge host (FORGE_HOSTS above: one GRASP-shaped
# path on github.com must not turn every GitHub-hosted repository into a GRASP-hosted one,
# review 4 M1) nor a host on any of the three hand-kept denylists - web viewers and
# documentation sites that proxy a few repositories under GRASP-shaped paths, general-purpose
# relays that serve no git (review 4 m2), and hosts probed and shown not to be GRASP servers.
# Each list is extended only when a host is shown to be one.
GRASP_WEB_UI_DENYLIST = frozenset({"gitworkshop.dev", "nostrhub.io", "gittr.space", "ngit.dev"})
GRASP_RELAY_DENYLIST = frozenset({
    "relay.damus.io", "nos.lol", "relay.primal.net", "relay.nostr.band", "relay.nostrcheck.me",
})
# A GRASP server is a git host that is also a Nostr relay. Each host here was probed on
# 2026-10-12 with one NIP-11 (relay information) request and one git info/refs request for a
# repository it is named for, and is not one
# (nostrology-data#17 (/git: 5 of the 63 hosts in the GRASP host set are not GRASP
# servers and 14 are dead)). A dead host is not listed: the rule stays syntactic, and an
# observed host set (design fork 5, the decided long-term fix) would drop dead hosts as well.
# git.gittr.space, the fifth host the probe found, left this list for FORGE_HOSTS the same day.
GRASP_NOT_SERVER_DENYLIST = frozenset({
    "git.jb55.com",                 # a static git host, 404 at the npub paths clients write for it; no relay
    "relay.vanderwarker.family",    # nostr-rs-relay 0.10.0; serves no git
    "gitrepublic.imwald.eu",        # a personal web page; no relay
    "denimroad.feeds.relay.tools",  # a newlay relay; serves git only under /git/<hex>/, 404 at npub paths
})
REPO_CLASSES = ("grasp_only", "grasp_mixed", "hashtree", "forge_only", "other_hosts", "none")
# The host-class precedence of ref 4.2, also used to pick ONE class for a host seen under
# several classes in `clone_hosts`.
HOST_CLASS_ORDER = ("hashtree", "private", "grasp", "forge", "non_network", "self_hosted")
REACHABLE = frozenset({"hashtree", "grasp", "forge", "self_hosted"})
# ref 7 by_host: the reachable host classes in the 4.4 precedence, then none (R empty).
HOST_KINDS = ("grasp", "hashtree", "forge", "self_hosted", "none")

# ref 4.5: topics and the tools heuristic.
TOPIC_EXCLUDE = frozenset({
    "shakespeare", "mkstack", "luvstack", "nutstack", "applestack", "personal-fork", "github-fork",
})
TOOL_OTHER_CLIENT = "other client"
TOOL_TOP_CLIENTS = 5

# ref 6: collaboration.
COMMENT_ROOTS = {
    "1621": "issues", "1618": "prs", "1619": "prs", "1617": "patches",
    "30617": "repo", "30618": "repo",
}
MIRROR_MIN_COMMENTS = 50
# A mirror key re-posts foreign authors: MORE than 90% of its comments match (n*10 > t*9).
_MIRROR_LINE_RE = re.compile(r"\*\*@[^*]+\*\*")
ZAP_SPAM_MSATS = 21_000_000_000   # above 0.21 BTC: dropped (the /lightning spam filter)
ZAP_CAP_MSATS = 10_000_000_000    # each counted amount capped at 0.1 BTC (the /lightning cap)
# ref 6.3: third-party inputs to the shared parser are bounded, and ANY error while parsing
# one means "unreadable".
BOLT11_MAX_LEN = 2048
DESCRIPTION_MAX_LEN = 65536

# Document caps (docs/design/ngit-design.md section 8): every list whose cardinality a third party
# controls is bounded, and third-party strings are clipped.
NAME_MAX = 80
HOST_MAX = 100
TOPIC_MAX = 40
RELAY_MAX = 100
DISPLAY_NAME_MAX = 40
TOOL_MAX = 40
TOP_CLONE_HOSTS = 30
TOP_GRASP_SERVERS = 20
TOP_RELAYS = 25
TOP_WEB_HOSTS = 12
TOP_TOPICS = 25
TOP_REPO_ROWS = 25
REPO_ROW_HOSTS = 5
TOP_FORK_FAMILIES = 15
FORK_FAMILY_NAMES = 5
TOP_MAINTAINERS = 20
TOP_FIRST_CHOICE = 10
TOP_DISCUSSED = 10
TOP_STARRED = 20              # 10 before the deploy of 2026-10-18 06:14Z, the operator's ask (ref 6.2)
TOP_ZAPPED = 20               # 10 before the deploy of 2026-10-18 06:14Z, the operator's ask (ref 6.3)
SUBSTRATE_LIST_MAX = 20       # ngit_repos.hosts / .relays
NAMES_LOOKUP_MAX = 2000       # display names are resolved for at most this many maintainers

# Anti-shrink (docs/design/ngit-design.md section 9 step 5). RELATIVE to the newest served row:
# refuse when announcements_kept or pushes_total falls below 90% of it (the roster and the
# ledger only grow under unchanged rules). With no served row yet, absolute floors. A
# deliberate methodology change that shrinks a figure sets SHRINK_OVERRIDE_UNTIL ("YYYY-MM-DD")
# in the same commit: the gates are bypassed through that UTC date and the run logs it.
SHRINK_KEEP_NUM, SHRINK_KEEP_DEN = 9, 10
# The archive is append-only and the horizon only moves forward, so every
# summary.archive_events count is non-decreasing under unchanged rules; gating them is what
# protects the three allow-empty extracts (an empty deletions extract would otherwise
# resurrect deleted repositories through every other gate). No first-run floor for these.
ARCHIVE_EVENT_FIELDS = ("announcements", "states", "grasp_lists", "comments", "reactions",
                        "external_reactions", "zaps", "deletions")
MIN_ANNOUNCEMENTS_KEPT_FIRST_RUN = 3500
MIN_PUSHES_TOTAL_FIRST_RUN = 12000
SHRINK_OVERRIDE_UNTIL = None

# The non-blocking push_concentration_sane check: with no daily cap (ref 3), one repository's
# share of the pushes in the 7 days ending on as_of_day may not exceed half, once there are at
# least 100 of them. The busiest repository held at most 21% of any week's pushes when the cap
# was dropped; past half, the cap's question is back (design doc, section 6).
PUSH_CONCENTRATION_MAX_SHARE = 0.5
PUSH_CONCENTRATION_MIN_PUSHES = 100

# The non-blocking roster_growth_sane check: kept coordinates first seen on as_of_day may
# not exceed max(20, 5 x the median daily count over the 30 days before it).
ROSTER_GROWTH_MIN = 20
ROSTER_GROWTH_FACTOR = 5
ROSTER_GROWTH_DAYS = 30

# The blocking held_keys_absent check: keys whose announcements are held off the page. A kept
# announcement coordinate by one stops the run before anything is written, so the served row
# stays and the run-failure alert fires. 62ec33a4 is the nostr.net catalog, 408
# kind-30617 (repo-announcements) of other people's projects published on 2025-11-16: held
# here until the operator decided how /git counts them, and excluded by the ledger's N10 since
# that decision (2026-10-12). The hold stays as N10's guard: a kept coordinate of the key means N10 no longer
# excludes it (counted, the catalog would have moved the headline share from 80.06% to 73.79%
# on 2026-10-12).
HELD_KEYS = frozenset({
    "62ec33a4090f0959e5c547a33f22a3253688bba121667a69e64a2dc758f8baaf",
})

# The shared kind-9735 (zap-receipts) parser, loaded by FILE PATH (it lives outside any
# package: src/shared/, beside the loader that also uses it). Fail loud when it is missing -
# a broken image must not silently zero the zap figures.
ZAP_PARSER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "src", "shared", "zap_parser.py")

# ref 6.3: the verified LNURL provider signer keys - every key in the `signers` list of every
# provider in this file, the list /lightning loads nightly into lightning_provider_signers - read
# with the /lightning loader's OWN parser (_parse_lightning_signers_yaml, the one its
# --load-lightning-signers mode runs), so the two surfaces cannot disagree on the list. Both
# files are baked into the image with the rest of the repo (COPY . /app).
PROVIDER_SIGNERS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data", "lightning", "provider_signers.yaml")
LIGHTNING_LOADER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "loader", "load_jsonl_exports_into_postgres.py")

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
_MIN_TS = -62135596800        # 0001-01-01T00:00:00Z, the floor of the date helpers
_MAX_TS = 253402300799        # 9999-12-31T23:59:59Z

_HEX64_RE = re.compile(r"[0-9a-fA-F]{64}")
_HEX40_RE = re.compile(r"[0-9a-f]{40}")
_DATE_D_RE = re.compile(r"20[0-9]{6}")
_TOPIC_NUM_RE = re.compile(r"[0-9]{4,8}")
_SCHEME_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://")
_SCP_RE = re.compile(r"[^@/\s]+@([^:/\s]+):")
_PORT_RE = re.compile(r":[0-9]*\Z")    # \Z: `$` would also match before a trailing newline
_GRASP_PATH_RE = re.compile(r"/npub1[02-9ac-hj-np-z]{58}/")
_IPV4_RE = re.compile(r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}")
# ref 4.2: a nip.io / sslip.io name embeds an IPv4 as the four octets right before the suffix,
# separated by `.` or `-`, starting the host or following a `.` (`10.0.0.1.nip.io`,
# `grasp.127.0.0.1.nip.io`, `x.10-0-0-1.sslip.io` - but not `app-10-0-0-1.nip.io`).
_WILDCARD_DNS_RE = re.compile(
    r"(?:.*\.)?([0-9]{1,3})[.-]([0-9]{1,3})[.-]([0-9]{1,3})[.-]([0-9]{1,3})\.(?:nip|sslip)\.io")


# --- NDJSON input ---------------------------------------------------------------------------

def iter_ndjson_gz(paths):
    """Stream Athena UNLOAD .gz JSON-lines parts (sorted by path), one row dict at a time."""
    for p in sorted(paths):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def parse_ndjson_gz(paths) -> list[dict]:
    """Parse Athena UNLOAD .gz JSON-lines parts into row dicts (one object per line)."""
    return list(iter_ndjson_gz(paths))


# --- dates ----------------------------------------------------------------------------------

def utc_date(ts) -> dt.date:
    """The UTC date of a unix timestamp. Clamped to years 1..9999 so a hostile created_at
    can never crash the build (the horizon already drops everything after the run)."""
    ts = min(max(int(ts), _MIN_TS), _MAX_TS)
    return (_EPOCH + dt.timedelta(seconds=ts)).date()


def day_str(ts) -> str:
    return utc_date(ts).isoformat()


def month_of(d: dt.date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def week_monday(d: dt.date) -> dt.date:
    """The Monday of d's ISO week (the label of a week, ref 1.5)."""
    return d - dt.timedelta(days=d.weekday())


def _next_month(d: dt.date) -> dt.date:
    return dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def _months(first: str, last: str) -> list[str]:
    """Every "YYYY-MM" from first to last inclusive (empty when first > last)."""
    out = []
    d = dt.date(int(first[:4]), int(first[5:7]), 1)
    end = dt.date(int(last[:4]), int(last[5:7]), 1)
    while d <= end:
        out.append(month_of(d))
        d = _next_month(d)
    return out


def _weeks(first: dt.date, last: dt.date) -> list[dt.date]:
    """The Monday of every ISO week from first's week to last's week inclusive (empty when
    first's week is after last's)."""
    out = []
    w, end = week_monday(first), week_monday(last)
    while w <= end:
        out.append(w)
        w += dt.timedelta(days=7)
    return out


def in_window(d: dt.date, as_of: dt.date, days: int) -> bool:
    """ref 1.5: an "N d" window is the N complete UTC days ending on as_of_day, inclusive."""
    return as_of - dt.timedelta(days=days - 1) <= d <= as_of


# --- tags -----------------------------------------------------------------------------------

def _tags(ev) -> list:
    t = ev.get("tags")
    return t if isinstance(t, list) else []


def first_value(tags, name):
    """The value (tag[1]) of the FIRST tag named `name`; None when there is no such tag or
    that tag has no string value (a later tag of the same name is NOT consulted)."""
    for t in tags:
        if isinstance(t, list) and t and t[0] == name:
            v = t[1] if len(t) >= 2 else None
            return v if isinstance(v, str) else None
    return None


def all_values(tags, name) -> list:
    """The values (tag[1:]) of EVERY tag named `name`, strings only, in tag order."""
    out = []
    for t in tags:
        if isinstance(t, list) and t and t[0] == name:
            out.extend(v for v in t[1:] if isinstance(v, str))
    return out


def clean_values(tags, name) -> list:
    """ref 2.1: the values of ALL `name` tags, trimmed (leading and trailing whitespace of any
    kind), empty strings dropped, in order, duplicates removed."""
    seen, out = set(), []
    for v in all_values(tags, name):
        v = v.strip()
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def has_tag(tags, name) -> bool:
    return any(isinstance(t, list) and t and t[0] == name for t in tags)


def coord_d(tags) -> str:
    """ref 1.3: the value of the first `d` tag, or "" when there is none."""
    v = first_value(tags, "d")
    return v if v is not None else ""


def version_sorted(rows) -> list:
    """ref 1.4: created_at ascending, then id DESCENDING - the last row is the current
    version (NIP-01 (protocol basics): among equal created_at the lowest id survives)."""
    out = sorted(rows, key=lambda r: r["id"], reverse=True)
    out.sort(key=lambda r: r["created_at"])
    return out


def _median_below(gaps, limit: int) -> bool:
    """True when the median of `gaps` (non-empty ints) is below `limit` - exact integer
    arithmetic: the mean of the two middle values for an even count."""
    g = sorted(gaps)
    n = len(g)
    if n % 2:
        return g[n // 2] < limit
    return g[n // 2 - 1] + g[n // 2] < 2 * limit


def _is_loop(versions, min_versions: int, max_median_gap: int) -> bool:
    """>= min_versions archived versions AND a median consecutive gap below max_median_gap
    (versions in version order, so every gap is >= 0)."""
    if len(versions) < min_versions or len(versions) < 2:
        return False
    gaps = [b["created_at"] - a["created_at"] for a, b in zip(versions, versions[1:])]
    return _median_below(gaps, max_median_gap)


# --- npub (NIP-19 (bech32-encoded entities)) ----------------------------------------------

_BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_BECH32_GEN = (0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3)


def _bech32_polymod(values) -> int:
    chk = 1
    for v in values:
        b = chk >> 25
        chk = ((chk & 0x1FFFFFF) << 5) ^ v
        for i in range(5):
            if (b >> i) & 1:
                chk ^= _BECH32_GEN[i]
    return chk


def bech32_decode(s: str):
    """BIP-173 bech32 (NOT bech32m) -> (hrp, 5-bit data without the checksum), or None."""
    if not isinstance(s, str) or not s:
        return None
    if any(ord(ch) < 33 or ord(ch) > 126 for ch in s):
        return None
    if s.lower() != s and s.upper() != s:
        return None
    s = s.lower()
    pos = s.rfind("1")
    if pos < 1 or pos + 7 > len(s):
        return None
    data = []
    for ch in s[pos + 1:]:
        i = _BECH32_CHARSET.find(ch)
        if i == -1:
            return None
        data.append(i)
    hrp = s[:pos]
    expanded = [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]
    if _bech32_polymod(expanded + data) != 1:
        return None
    return hrp, data[:-6]


def _convert_bits(data, frombits: int, tobits: int):
    """Regroup 5-bit words into 8-bit bytes WITHOUT padding (non-zero padding is invalid)."""
    acc = bits = 0
    out = []
    maxv = (1 << tobits) - 1
    for value in data:
        acc = (acc << frombits) | value
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            out.append((acc >> bits) & maxv)
    if bits >= frombits or ((acc << (tobits - bits)) & maxv):
        return None
    return out


def npub_to_hex(s: str):
    """An `npub1...` string -> its 64-hex pubkey, or None (bad checksum, charset, hrp or
    length)."""
    dec = bech32_decode(s)
    if dec is None or dec[0] != "npub":
        return None
    raw = _convert_bits(dec[1], 5, 8)
    if raw is None or len(raw) != 32:
        return None
    return bytes(raw).hex()


def norm_pubkey(v):
    """ref 2.1: a 64-hex string lowercased, an `npub1...` string bech32-decoded to hex;
    anything else (including a value with surrounding whitespace) -> None."""
    if not isinstance(v, str):
        return None
    if _HEX64_RE.fullmatch(v):
        return v.lower()
    if v.startswith("npub1"):
        return npub_to_hex(v)
    return None


def role_tag_active(t) -> bool:
    """ref 2.1: an `M`/`m` role tag is active when it has fewer than four elements or an odd
    number of them (the role-history grammar: a 4-element tag has ended, a 5-element one
    was re-activated)."""
    return len(t) < 4 or len(t) % 2 == 1


def maintainer_listing(tags, own_pubkey: str):
    """ref 2.1 -> (listing: set of hex pubkeys without the author, self_lead: bool)."""
    role_mode = any(isinstance(t, list) and t and t[0] in ("M", "m", "o") for t in tags)
    raw = []
    self_lead = False
    if role_mode:
        for t in tags:
            if not (isinstance(t, list) and t and t[0] in ("M", "m")) or not role_tag_active(t):
                continue
            raw.extend(v for v in t[1:] if isinstance(v, str))
            if t[0] == "M" and len(t) >= 2 and norm_pubkey(t[1]) == own_pubkey:
                self_lead = True
    else:
        raw = all_values(tags, "maintainers")
    listing = {pk for pk in (norm_pubkey(v) for v in raw) if pk is not None}
    listing.discard(own_pubkey)
    return listing, self_lead


# --- URLs and hosts (ref 4.1-4.3) -----------------------------------------------------------

def parse_url(u: str):
    """ref 4.1 -> (scheme | None, scp_like, host | None, path | None).

    scheme: the lowercase prefix before `://`. host: for a scheme URL, the authority (up to
    the first `/`, `?` or `#`) minus any `userinfo@` (up to the LAST `@`) and any `:port`
    suffix (`:` + digits at the end), lowercased; for scp-like `user@host:path`, the text
    between `@` and `:`, lowercased; else None (an empty host is None too). path: the text
    after the authority (scheme URLs only)."""
    m = _SCHEME_RE.match(u)
    if m:
        scheme = m.group(0)[:-3].lower()
        rest = u[m.end():]
        cut = len(rest)
        for ch in "/?#":
            i = rest.find(ch)
            if i != -1 and i < cut:
                cut = i
        hostport = rest[:cut].rsplit("@", 1)[-1]
        host = _PORT_RE.sub("", hostport).lower()
        return scheme, False, (host or None), rest[cut:]
    m = _SCP_RE.match(u)
    if m:
        return None, True, (m.group(1).lower() or None), None
    return None, False, None, None


def is_grasp_shaped(parsed) -> bool:
    """ref 4.1: scheme http/https/ws/wss AND the path starts `/npub1<58 bech32 chars>/`."""
    scheme, _scp, host, path = parsed
    return (scheme in GRASP_SCHEMES and host is not None and path is not None
            and _GRASP_PATH_RE.match(path) is not None)


def _private_ipv4(octets) -> bool:
    """Four octet strings (1-3 digits each) -> True when they form an IPv4 (each at most 255)
    in 127/8, 10/8, 172.16/12, 192.168/16 or 100.64/10."""
    o = [int(x) for x in octets]
    if any(x > 255 for x in o):
        return False
    a, b = o[0], o[1]
    return (a in (10, 127) or (a == 172 and 16 <= b <= 31) or (a == 192 and b == 168)
            or (a == 100 and 64 <= b <= 127))


def is_private_host(h) -> bool:
    """ref 4.2: localhost, 0.0.0.0, `.local`, a dotted IPv4 in 127/8, 10/8, 172.16/12,
    192.168/16 or 100.64/10, or a nip.io / sslip.io name whose embedded IPv4 (the four octets
    right before the suffix, separated by `.` or `-`, each at most 255, starting the host or
    following a `.`) is in one of those ranges - `grasp.127.0.0.1.nip.io` resolves to the
    loopback (review 4 m2)."""
    if not h:
        return False
    if h in ("localhost", "0.0.0.0") or h.endswith(".local"):
        return True
    if _IPV4_RE.fullmatch(h):
        return _private_ipv4(h.split("."))
    if h.endswith((".nip.io", ".sslip.io")):
        m = _WILDCARD_DNS_RE.fullmatch(h)
        return m is not None and _private_ipv4(m.groups())
    return False


def host_class(parsed, grasp_hosts) -> str:
    """ref 4.2, in precedence: hashtree, private, grasp, forge, non_network, self_hosted."""
    scheme, scp, host, _path = parsed
    if scheme == "htree":
        return "hashtree"
    if host is not None and is_private_host(host):
        return "private"
    if host is not None and host in grasp_hosts:
        return "grasp"
    if host is not None and host in FORGE_HOSTS:
        return "forge"
    if host is None or (scheme not in NETWORK_SCHEMES and not scp):
        return "non_network"
    return "self_hosted"


def url_reachable(parsed) -> bool:
    """ref 2.2 N8b: the value is reachable (4.2: hashtree, grasp, forge or self_hosted) when
    classified with the GRASP host set taken as EMPTY. N8b decides who is kept and the set is
    built from kept repositories (4.3), so the ledger must not read the set. The only values
    that read differently: a non-network scheme on a GRASP host (`nostr://relay.ngit.dev/...`)
    is `grasp` under the full precedence but `non_network` here."""
    return host_class(parsed, frozenset()) in REACHABLE


def grasp_set_host(parsed):
    """ref 4.3: the host a clone value adds to the GRASP host set, or None. GRASP-shaped, a
    dot in the host, not private (4.2), not a forge host, on none of the three denylists.
    Syntactic - no DNS or reachability check."""
    if not is_grasp_shaped(parsed):
        return None
    h = parsed[2]
    if ("." not in h or is_private_host(h) or h in FORGE_HOSTS or h in GRASP_WEB_UI_DENYLIST
            or h in GRASP_RELAY_DENYLIST or h in GRASP_NOT_SERVER_DENYLIST):
        return None
    return h


def repo_hosting_class(reachable) -> str:
    """ref 4.4 over R, the set of REACHABLE host classes of a repository's clone values."""
    if "grasp" in reachable:
        return "grasp_only" if len(reachable) == 1 else "grasp_mixed"
    if "hashtree" in reachable:
        return "hashtree"
    if reachable == {"forge"}:
        return "forge_only"
    if "self_hosted" in reachable:
        return "other_hosts"
    return "none"


def norm_relay(v: str):
    """ref 4.5: lowercase, strip ONE leading ws:// or wss://, strip trailing slashes; None
    when the result has no dot."""
    s = _strip_ws_scheme(v)
    return s if "." in s else None


def _strip_ws_scheme(v: str) -> str:
    s = v.lower()
    if s.startswith("wss://"):
        s = s[6:]
    elif s.startswith("ws://"):
        s = s[5:]
    return s.rstrip("/")


def grasp_list_value(v: str):
    """ref 5.2 -> (normalized value, host) when VALID, else None. Normalized like a relay
    (lowercase, ws:// / wss:// stripped, trailing `/` stripped - no trimming); the host is the
    text before the first `/`; valid when it contains a dot and does not end with example.com."""
    s = _strip_ws_scheme(v)
    host = s.split("/", 1)[0]
    if "." not in host or host.endswith("example.com"):
        return None
    return s, host


# --- the shared zap parser ------------------------------------------------------------------

_ZAP_PARSER = None


def load_zap_parser(path: str = None):
    """Load src/shared/zap_parser.py by file path (cached). Raises when it is missing."""
    global _ZAP_PARSER
    path = path or ZAP_PARSER_PATH
    if _ZAP_PARSER is not None and getattr(_ZAP_PARSER, "__file__", None) == path:
        return _ZAP_PARSER
    if not os.path.isfile(path):
        raise RuntimeError(f"zap parser not found at {path} - the zap figures cannot be computed")
    spec = importlib.util.spec_from_file_location("nostrology_ngit_zap_parser", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception:
        sys.modules.pop(spec.name, None)
        raise
    _ZAP_PARSER = mod
    return mod


def _last_value(tags, name):
    """ref 6.3: the value (tag[1]) of the LAST tag named `name` that HAS a value - scanned the
    way the shared zap_parser scans (a tag with fewer than two elements is skipped; a later
    tag with a value replaces an earlier one, whatever that value is) - or None. A non-string
    value comes back None: an unreadable amount, an unparseable description."""
    out = None
    for t in tags:
        if isinstance(t, list) and len(t) >= 2 and t[0] == name:
            out = t[1]
    return out if isinstance(out, str) else None


def zap_amount_msats(zp, tags):
    """ref 6.3: the amount of the LAST `bolt11` tag that has a value, via the shared parser's
    bolt11 amount parser; None when unreadable - a value longer than BOLT11_MAX_LEN, a parse
    that raises anything, or a result that fails the finiteness gate (a non-negative int)."""
    b = _last_value(tags, "bolt11")
    if not b or len(b) > BOLT11_MAX_LEN:
        return None
    try:
        amount = zp._parse_bolt11_amount(b)
    except Exception:  # noqa: BLE001 - any error: "unreadable"
        return None
    if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
        return None
    return amount


def zap_request(tags):
    """ref 6.3: the zapper parse of the embedded zap request - kind-9734 (NIP-57 zap request),
    the LAST `description` tag that has a value - as a JSON object with a non-empty string
    `pubkey`; None when it does not parse: a value longer than DESCRIPTION_MAX_LEN, a parse
    that raises anything, a result that is not an object, or no non-empty string pubkey."""
    desc = _last_value(tags, "description")
    if desc is None or len(desc) > DESCRIPTION_MAX_LEN:
        return None
    try:
        req = json.loads(desc)
    except Exception:  # noqa: BLE001 - any error: "does not parse"
        return None
    if not isinstance(req, dict):
        return None
    pubkey = req.get("pubkey")
    return req if isinstance(pubkey, str) and pubkey else None


def zap_sender(tags):
    """ref 6.3: the zapper - the embedded request's pubkey when the request parses; None
    otherwise (the receipt still counts, with no zapper)."""
    req = zap_request(tags)
    return req["pubkey"] if req is not None else None


def zap_request_amount(tags):
    """ref 6.3: the value of the embedded request's FIRST `amount` tag, as published, when the
    request parses (the zapper parse) and that value is non-empty; None when there is nothing
    to check - no request (a request without a pubkey does not parse), no `amount` tag, or a
    first `amount` tag without a value or with an empty one - which never blocks a receipt.
    A receipt with a value counts only when it equals the bolt11 amount in millisatoshis as
    decimal text (NIP-57 (lightning zaps) appendix F): `0500000`, or a number instead of a
    string, is a mismatch."""
    req = zap_request(tags)
    rtags = req.get("tags") if req is not None else None
    if not isinstance(rtags, list):
        return None
    for t in rtags:
        if isinstance(t, list) and t and t[0] == "amount":
            v = t[1] if len(t) >= 2 else None
            return None if v is None or v == "" else v
    return None


def load_provider_signers(path: str = None) -> frozenset:
    """ref 6.3: the verified LNURL provider signer keys - every key of every provider's
    `signers` list in data/lightning/provider_signers.yaml (PROVIDER_SIGNERS_PATH), read with
    the /lightning loader's own parser, which validates the file (64-hex keys, lowercased, each
    under one provider) and raises on a malformed one. An empty list raises too: it would
    silently zero every zap figure. The loader imports psycopg2 and, lazily, PyYAML - both in
    the Dagster image, PyYAML not in the workstation test venv."""
    loader_path = LIGHTNING_LOADER_PATH
    loader_dir = os.path.dirname(loader_path)
    added = loader_dir not in sys.path
    if added:
        sys.path.insert(0, loader_dir)       # the loader imports its sibling nutzap_mints
    try:
        spec = importlib.util.spec_from_file_location("nostrology_ngit_lightning_loader", loader_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        if added:
            sys.path.remove(loader_dir)
    keys = frozenset(row[0] for row in mod._parse_lightning_signers_yaml(path or PROVIDER_SIGNERS_PATH))
    if not keys:
        raise ValueError(f"no provider signer keys in {path or PROVIDER_SIGNERS_PATH} - the zap "
                         "figures cannot be computed")
    return keys


# --- names ----------------------------------------------------------------------------------

def clean_text(v):
    """ref 5.1 / 9: control and format characters (Unicode categories Cc and Cf) removed - a
    bidi override (U+202E) or a zero-width character (U+200B) in one third-party name must not
    reverse or hide its neighbours on the page (web review 4). NUL is a Cc character, so the
    result is Postgres-safe too. Anything but a string comes back unchanged."""
    if not isinstance(v, str):
        return v
    return "".join(ch for ch in v if unicodedata.category(ch) not in ("Cc", "Cf"))


def clean_display_name(v):
    """Presentation only: control and format characters removed, trimmed, clipped to 40;
    None when empty or not a string."""
    if not isinstance(v, str):
        return None
    s = clean_text(v).strip()
    s = s[:DISPLAY_NAME_MAX]
    return s or None


def display_name(display, name):
    """ref 5.1: the kind-0 (profile-metadata) `display_name`, else `name` (after cleaning)."""
    return clean_display_name(display) or clean_display_name(name)


# --- preprocessing (ref 1) ------------------------------------------------------------------

def _norm_event(r) -> dict:
    content = r.get("content")
    return {
        "id": str(r["id"]),
        "pubkey": str(r["pubkey"]),
        "created_at": int(r["created_at"]),
        "content": content if isinstance(content, str) else "",
        "tags": _tags(r),
    }


def _dedup(rows) -> list:
    """ref 1.1 (the extracts already did it; this is a no-op guard): one row per id."""
    seen, out = set(), []
    for r in rows:
        ev = _norm_event(r)
        if ev["id"] in seen:
            continue
        seen.add(ev["id"])
        out.append(ev)
    return out


def as_of_bounds(now_ts: int):
    """ref 1.2 / 1.5 -> (as_of_day, as_of_end): the last complete UTC day and its last second."""
    as_of = utc_date(int(now_ts) - DAY_S)
    return as_of, (as_of - dt.date(1970, 1, 1)).days * DAY_S + DAY_S - 1


def _horizon(rows, as_of_end: int, now_ts: int):
    """ref 1.2: keep rows dated in [PAST_FLOOR, as_of_end] -> (kept, future, past).

    A row after as_of_end is dropped (a row on the run's own partial day waits for the next
    run); only those after the run instant are counted as future-dated. A row before
    PAST_FLOOR is dropped and counted as past-dated."""
    kept, future, past = [], 0, 0
    for r in rows:
        t = r["created_at"]
        if t > as_of_end:
            future += t > now_ts
        elif t < PAST_FLOOR:
            past += 1
        else:
            kept.append(r)
    return kept, future, past


def comment_in_scope(tags) -> bool:
    """ref 6.1: the FIRST `K` value is a git root kind, or an `A`/`a` value starts 30617:."""
    if first_value(tags, "K") in COMMENT_ROOTS:
        return True
    return any(isinstance(t, list) and len(t) >= 2 and t[0] in ("A", "a")
               and isinstance(t[1], str) and t[1].startswith("30617:") for t in tags)


def reaction_in_scope(tags) -> bool:
    """ref 6.2: a `k` tag valued 30617, or an `a` value starting 30617:."""
    return any(isinstance(t, list) and len(t) >= 2 and isinstance(t[1], str)
               and ((t[0] == "k" and t[1] == "30617") or (t[0] == "a" and t[1].startswith("30617:")))
               for t in tags)


def external_reaction_in_scope(tags) -> bool:
    """ref 6.2: a `k` tag valued git-repo AND an `i` value starting 30617: (Iris Git's star)."""
    has_k = has_i = False
    for t in tags:
        if isinstance(t, list) and len(t) >= 2 and isinstance(t[1], str):
            has_k = has_k or (t[0] == "k" and t[1] == "git-repo")
            has_i = has_i or (t[0] == "i" and t[1].startswith("30617:"))
    return has_k and has_i


def zap_in_scope(tags) -> bool:
    """ref 6.3: an `a` value starting 30617:."""
    return any(isinstance(t, list) and len(t) >= 2 and t[0] == "a" and isinstance(t[1], str)
               and t[1].startswith("30617:") for t in tags)


def repo_coordinate(tags, names=("A", "a")):
    """ref 6.1: the first `A` value starting 30617:, else the first such `a` value (in the
    order of `names`), parsed as 30617:<pubkey>:<d> (d = everything after the second colon)
    -> (pubkey, d), or None when absent or unparseable. The stars read it with ("a",) and
    with ("i",) (ref 6.2)."""
    for name in names:
        for t in tags:
            if (isinstance(t, list) and len(t) >= 2 and t[0] == name and isinstance(t[1], str)
                    and t[1].startswith("30617:")):
                parts = t[1].split(":", 2)
                return (parts[1], parts[2]) if len(parts) == 3 else None
    return None


# --- announcements --------------------------------------------------------------------------

def announcement_attrs(ev) -> dict:
    """ref 2.1: the attributes a rule reads, from ONE version (the current one)."""
    tags = ev["tags"]
    pk = ev["pubkey"]
    euc = None
    for t in tags:
        if (isinstance(t, list) and len(t) >= 3 and t[0] == "r" and t[2] == "euc"
                and isinstance(t[1], str) and _HEX40_RE.fullmatch(t[1])):
            euc = t[1]
            break
    if euc is None:
        for t in tags:
            if (isinstance(t, list) and len(t) == 2 and t[0] == "r"
                    and isinstance(t[1], str) and _HEX40_RE.fullmatch(t[1])):
                euc = t[1]
                break
    listing, self_lead = maintainer_listing(tags, pk)
    name = first_value(tags, "name")
    description = first_value(tags, "description")
    return {
        "name": name if name is not None else "",
        "description": description if description is not None else "",
        "clone": clean_values(tags, "clone"),
        "web": clean_values(tags, "web"),
        "relays": clean_values(tags, "relays"),
        "t": [v.lower().strip() for v in all_values(tags, "t")],
        "client": first_value(tags, "client"),
        "alt": first_value(tags, "alt"),
        "euc": euc,
        "listing": listing,
        "self_lead": self_lead,
        "has_u": has_tag(tags, "u"),
        "bare_euc_tag": has_tag(tags, "euc"),
        "refs_heads_tag": any(isinstance(t, list) and t and isinstance(t[0], str)
                              and t[0].startswith("refs/heads/") for t in tags),
        "deleted_tag": any(isinstance(t, list) and t[:2] == ["deleted", "true"] for t in tags),
        "status_deleted": any(isinstance(t, list) and t[:2] == ["status", "deleted"] for t in tags),
    }


def ledger_family(coord, attrs, versions, has_state: bool, deleted_at) -> str:
    """ref 2.2: the FIRST matching rule, else "repo". `deleted_at` is the newest non-future
    self-authored kind-5 (event-deletion-requests) request naming the coordinate, or None."""
    pk, d = coord
    clones = attrs["clone"]
    if len(d) > D_MAX or _CONTROL_RE.search(d):
        return "N0"
    if any(v.startswith(SQUATTER_PREFIX) for v in clones + attrs["web"]):
        return "N1"
    if any(HARNESS_CLONE_MARK in v for v in clones) or attrs["bare_euc_tag"]:
        return "N2"
    if attrs["name"] == TEMPLATE_NAME:
        return "N3"
    if _DATE_D_RE.fullmatch(d):
        return "N4"
    if _is_loop(versions, ANN_LOOP_MIN_VERSIONS, ANN_LOOP_MAX_MEDIAN_GAP_S):
        return "N5"
    if (attrs["deleted_tag"] or attrs["status_deleted"] or attrs["name"].lower() == "deleted"
            or (deleted_at is not None and deleted_at >= versions[-1]["created_at"])):
        return "N6"
    if pk == SECRET_KEY_1_PUBKEY:
        return "N7"
    if clones:
        hosts = [parse_url(v)[2] for v in clones]
        if all(h is not None and is_private_host(h) for h in hosts):
            return "N8a"
    # N8b: no REACHABLE clone value (none at all, or every one private or non_network - the
    # hostless `todo`, `testing`, `.` included; review 4 m3) and no state
    if not has_state and not any(url_reachable(parse_url(v)) for v in clones):
        return "N8b"
    if "personal-fork" in attrs["t"] and not has_state:
        return "N9"
    if pk == NOSTR_NET_CATALOG_PUBKEY:
        return "N10"
    return "repo"


# ref 2.2 / 9: (family code = the ledger key, the one-line reason), in rule order.
LEDGER_FAMILIES = (
    ("N0",
     "d is longer than 200 characters (the longest real one is 143) or contains a control "
     "character - a malformed identifier; counted here, written to no substrate row"),
    ("N1",
     "a clone or web URL under https://nostr.life/ - agent profiles squatting the kind, not repositories"),
    ("N2",
     "a clone URL containing @example.com: or a bare euc tag - a test harness"),
    ("N3",
     "name is the NIP-34 (git collaboration) placeholder <human-readable project name> - verbatim template copies"),
    ("N4",
     "d is a bare date (20YYMMDD) - daily sync output, not a project"),
    ("N5",
     "republished at least 1,000 times with a median gap under 300 s - a bot re-signing its announcement"),
    ("N6",
     "a deleted / status=deleted tag, the name 'deleted', or a NIP-09 (deletion requests) "
     "self-authored kind-5 (event-deletion-requests) request naming the coordinate"),
    ("N7",
     "signed by the public key of secret key 1 - anyone can sign as it"),
    ("N8a",
     "every clone URL is on a private or loopback host - nothing anyone else can clone"),
    ("N8b",
     "no public clone URL (none at all, or only private or non-network values such as todo, "
     "testing or a bare dot) and no kind-30618 (repo-states) - nothing to clone, nothing ever pushed"),
    ("N9",
     "a personal-fork topic and no kind-30618 (repo-states) - ngit 2.0-2.4 PR scaffolding and "
     "every hashtree personal fork, real forks included, since hashtree never publishes a state"),
    ("N10",
     "signed by the key that re-published the awesome-nostr list's code-host entries as "
     "announcements - other people's projects, not repositories it maintains"),
)
FAMILY_CODES = tuple(code for code, _why in LEDGER_FAMILIES)
# ref 9: the ledger (`exclusions`) keys, exactly and in this order.
EXCLUSION_KEYS = FAMILY_CODES + ("state_republish_loop", "orphan_states",
                                 "ephemeral", "imported", "mirrored")


def tool_label(attrs):
    """ref 4.5 tools heuristic, first match -> (label, raw_client): raw_client is the
    LOWERCASED client value when the "any other client" branch matched (the caller keeps the
    five with the most repositories and folds the rest into "other client"), else None.
    `contains` is tested on the lowercased client value; a client value that is empty after
    trimming counts as no client."""
    client = attrs["client"]
    if client is not None and client.strip():
        lc = client.lower()
        if "shakespeare" in lc:
            return "Shakespeare", None
        if "nostrhub" in lc:
            return "NostrHub", None
        return lc, lc
    if "shakespeare" in attrs["t"]:
        return "Shakespeare", None
    if any(parse_url(v)[0] == "htree" for v in attrs["clone"]):
        return "hashtree", None
    if attrs["alt"] is not None and attrs["alt"].startswith("git repository:"):
        return "ngit / GitWorkshop (inferred)", None
    if attrs["refs_heads_tag"]:
        return "git-sync mirror (refs in announcement)", None
    return "unknown", None


# --- state versions (ref 3) -----------------------------------------------------------------

def ref_map_and_head(tags):
    """ref 3: {name -> value} over tags with at least two elements named refs/heads/* or
    refs/tags/* NOT ending ^{} (a one-element ref tag is ignored; the first occurrence of a
    name wins), and the first HEAD tag's value (None when absent)."""
    refs = {}
    head = None
    head_seen = False
    for t in tags:
        if not (isinstance(t, list) and t and isinstance(t[0], str)):
            continue
        name = t[0]
        val = t[1] if len(t) >= 2 and isinstance(t[1], str) else None
        if name == "HEAD":
            if not head_seen:
                head_seen, head = True, val
            continue
        if (len(t) >= 2 and (name.startswith("refs/heads/") or name.startswith("refs/tags/"))
                and not name.endswith("^{}")):
            if name not in refs:
                refs[name] = val
    return refs, head


def _split(refs):
    heads = {k: v for k, v in refs.items() if k.startswith("refs/heads/")}
    tags = {k: v for k, v in refs.items() if k.startswith("refs/tags/")}
    return heads, tags


# ref 3: a ledger row's source - a kind-30618 (repo-states) version, or (since 2026-10-12) a
# hashtree push, a hashtree-class repository's kind-30617 (repo-announcements) republish.
SOURCE_STATE = "state"
SOURCE_HTREE = "htree_announcement"
# ref 3: a hashtree push's flags. It is a push by definition; it moves no ref this ledger can
# read (the refs live inside the hashtree tree), so every count is 0 and it has no branch or tag
# count (NULL in ngit_pushes).
HTREE_PUSH_FLAGS = {
    "changed": True, "push": True, "head_changed": False,
    "heads_moved": 0, "default_moved": False,
    "branches_added": 0, "branches_removed": 0,
    "tags_added": 0, "tags_removed": 0, "tags_moved": 0,
    "branches": None, "tags": None,
}


# ref 3: hashtree's git remote helper republishes the announcement once per changed ref (its
# push.rs loops over a push's refs and publishes each one's root with the announcement), one
# ref's version from a few seconds to about 107 s after the one before (observed); the next push
# comes minutes or hours later. A version less than this many seconds after the coordinate's
# previous version is the same push. The page says "two minutes".
HTREE_PUSH_GAP_S = 120


def htree_push_starts(versions) -> list:
    """ref 3: a coordinate's announcement versions, in version order, grouped into pushes (a
    version less than HTREE_PUSH_GAP_S after the one before joins its push); each push's first
    version. Counting versions instead would count refs where capture is live and pushes where
    it is late, since a late request gets only the newest version."""
    starts, prev = [], None
    for ev in versions:
        if prev is None or ev["created_at"] - prev >= HTREE_PUSH_GAP_S:
            starts.append(ev)
        prev = ev["created_at"]
    return starts


def _tally_push(rr, row, as_of) -> None:
    """ref 3, per repository: a `push` row (either source) moves last_push and the push
    counts; last_state and the newest state are the state rows' alone."""
    if not row["push"]:
        return
    if rr["last_push"] is None or row["created_at"] > rr["last_push"]:
        rr["last_push"] = row["created_at"]
    rr["pushes_total"] += 1
    if in_window(row["date"], as_of, 30):
        rr["pushes_30d"] += 1
    if in_window(row["date"], as_of, 90):
        rr["pushes_90d"] += 1


def version_flags(prev, cur, created_at: int) -> dict:
    """ref 3: the flags + change counts of one state version given the previous version's
    (refs, head) - or None for a first version."""
    refs, head = cur
    heads, tags = _split(refs)
    if prev is None:
        return {
            "changed": True, "head_changed": False, "push": created_at >= STATE_WALL,
            "heads_moved": 0, "default_moved": False,
            "branches_added": len(heads), "branches_removed": 0,
            "tags_added": len(tags), "tags_removed": 0, "tags_moved": 0,
            "branches": len(heads), "tags": len(tags),
        }
    prefs, phead = prev
    pheads, ptags = _split(prefs)
    changed = refs != prefs
    moved = {k for k in heads if k in pheads and pheads[k] != heads[k]}
    default_moved = (isinstance(head, str) and head.startswith("ref: refs/heads/")
                     and head[len("ref: "):] in moved)
    return {
        "changed": changed,
        "head_changed": head != phead,
        "push": changed and created_at >= STATE_WALL,
        "heads_moved": len(moved),
        "default_moved": bool(default_moved),
        "branches_added": sum(1 for k in heads if k not in pheads),
        "branches_removed": sum(1 for k in pheads if k not in heads),
        "tags_added": sum(1 for k in tags if k not in ptags),
        "tags_removed": sum(1 for k in ptags if k not in tags),
        "tags_moved": sum(1 for k in tags if k in ptags and ptags[k] != tags[k]),
        "branches": len(heads), "tags": len(tags),
    }


def default_branch_bucket(head) -> str:
    if head is None:
        return "none"
    if head == "ref: refs/heads/main":
        return "main"
    if head == "ref: refs/heads/master":
        return "master"
    if head.startswith("ref: "):
        return "other"
    return "detached"


def _bucket(n: int, edges) -> str:
    """edges: ((max_inclusive | None, label), ...) in ascending order."""
    for hi, label in edges:
        if hi is None or n <= hi:
            return label
    raise AssertionError("unreachable")


_BRANCH_BUCKETS = ((0, "0"), (1, "1"), (5, "2-5"), (20, "6-20"), (None, "21+"))
_TAG_BUCKETS = ((0, "0"), (5, "1-5"), (20, "6-20"), (None, "21+"))
_RELAY_BUCKETS = ((0, "0"), (1, "1"), (3, "2-3"), (5, "4-5"), (None, "6+"))
_FOUR_BUCKETS = ((1, "1"), (2, "2"), (3, "3"), (None, "4+"))
RECENCY_KEYS = ("30d", "90d", "180d", "365d", "older")


def recency_bucket(days: int) -> str:
    """ref 3: 30d (0..29), 90d (30..89), 180d (90..179), 365d (180..364), older (365+). A
    last state AFTER as_of_day (the run's own partial day, negative days) is in 30d."""
    if days <= 29:
        return "30d"
    if days <= 89:
        return "90d"
    if days <= 179:
        return "180d"
    if days <= 364:
        return "365d"
    return "older"


def _kv(keys, counter) -> list:
    return [{"k": k, "v": int(counter.get(k, 0))} for k in keys]


def _clip(s, n: int):
    return s[:n] if isinstance(s, str) else s


def _clip_tail(s, n: int):
    """ref 4.5: a tool label longer than n keeps its LAST n-3 characters behind "...". The long
    values are nsite hostnames (<random subdomain>.<name>.nsite.lol), whose recognisable part
    is the tail; a head clip showed 40 characters of noise (review 3, 2026-09-25)."""
    return s if not isinstance(s, str) or len(s) <= n else "..." + s[-(n - 3):]


# --- the build ------------------------------------------------------------------------------

def _prepare(announce_events, state_events, deletion_rows, now_ts: int, *,
             assume_state: bool = False) -> dict:
    """Preprocessing + the noise ledger + repository components: everything that decides
    WHO is on the roster. Shared by build_snapshot and kept_maintainers (the asset resolves
    display names for the roster BEFORE the pure build). `assume_state` runs the ledger as if
    every coordinate had a kind-30618 (repo-states) event (reduce_roster's first phase)."""
    NOW = int(now_ts)
    as_of, as_of_end = as_of_bounds(NOW)
    anns, fut_ann, past_ann = _horizon(_dedup(announce_events), as_of_end, NOW)
    states, fut_state, past_state = _horizon(_dedup(state_events), as_of_end, NOW)

    # kind-5 (event-deletion-requests): the DISTINCT (author, coord, created_at) triples of
    # self-authored requests (author = the coordinate's pubkey, NIP-09 (deletion requests)).
    # The horizon applies to each TRIPLE, like every kind (ref 1.2, no anomalies slot), and
    # the newest surviving triple of a pair decides N6 - so a future-dated request can never
    # mask an earlier valid one. A coord without a second colon (`30617:<author>`) names no
    # repository and matches no announcement below, but its pair still counts in
    # archive_events.deletions (ref 1.2, review 4 m1).
    del_max: dict = {}
    for r in deletion_rows:
        author, coord, ts = str(r["author"]), str(r["coord"]), int(r["created_at"])
        parts = coord.split(":", 2)
        if (ts > as_of_end or ts < PAST_FLOOR or len(parts) < 2 or parts[0] != "30617"
                or parts[1] != author):
            continue
        if ts > del_max.get((author, coord), ts - 1):
            del_max[(author, coord)] = ts

    ann_by_coord = collections.defaultdict(list)
    ann_id_coord = {}
    for ev in anns:
        c = (ev["pubkey"], coord_d(ev["tags"]))
        ann_by_coord[c].append(ev)
        ann_id_coord[ev["id"]] = c
    state_by_coord = collections.defaultdict(list)
    for ev in states:
        state_by_coord[(ev["pubkey"], coord_d(ev["tags"]))].append(ev)

    coords = {}
    for c in sorted(ann_by_coord):
        versions = version_sorted(ann_by_coord[c])
        attrs = announcement_attrs(versions[-1])
        deleted_at = del_max.get((c[0], f"30617:{c[0]}:{c[1]}"))
        fam = ledger_family(c, attrs, versions, assume_state or c in state_by_coord, deleted_at)
        coords[c] = {
            "pubkey": c[0], "d": c[1], "family": fam, "attrs": attrs,
            "versions": len(versions), "first_seen": versions[0]["created_at"],
            "last_seen": versions[-1]["created_at"],
        }
    kept = sorted(c for c, r in coords.items() if r["family"] == "repo")

    # ref 2.3: reciprocal components over KEPT announcements with the same d.
    parent = {c: c for c in kept}

    def find(x, par):
        while par[x] != x:
            par[x] = par[par[x]]
            x = par[x]
        return x

    def union(a, b, par):
        ra, rb = find(a, par), find(b, par)
        if ra != rb:
            if rb < ra:
                ra, rb = rb, ra
            par[rb] = ra

    one_way = {c: c for c in kept}
    kept_set = set(kept)
    for c in kept:
        pk, d = c
        for other in coords[c]["attrs"]["listing"]:
            oc = (other, d)
            if oc not in kept_set:
                continue
            union(c, oc, one_way)
            if pk in coords[oc]["attrs"]["listing"]:
                union(c, oc, parent)
    comps = collections.defaultdict(list)
    for c in kept:
        comps[find(c, parent)].append(c)
    one_way_sizes = collections.Counter(find(c, one_way) for c in kept)

    repos = {}
    for members in comps.values():
        members = sorted(members)
        self_leads = [m for m in members if coords[m]["attrs"]["self_lead"]]
        if len(self_leads) == 1:
            lead = self_leads[0]
        else:
            lead = min(members, key=lambda m: (coords[m]["first_seen"], m[0]))
        d = lead[1]
        key = f"{lead[0]}:{d}"
        ordered = [lead] + [m for m in members if m != lead]

        def _union(field):
            seen, out = set(), []
            for m in ordered:
                for v in coords[m]["attrs"][field]:
                    if v not in seen:
                        seen.add(v)
                        out.append(v)
            return out

        euc = coords[lead]["attrs"]["euc"]
        if euc is None:
            euc = next((coords[m]["attrs"]["euc"] for m in members
                        if coords[m]["attrs"]["euc"] is not None), None)
        repos[key] = {
            "key": key, "d": d, "lead": lead, "members": members,
            "name": coords[lead]["attrs"]["name"],
            "first_seen": min(coords[m]["first_seen"] for m in members),
            "clone": _union("clone"), "web": _union("web"), "relays": _union("relays"),
            "t": _union("t"), "euc": euc,
        }
    member_repo = {m: key for key, r in repos.items() for m in r["members"]}
    return {
        "now": NOW, "as_of": as_of, "as_of_end": as_of_end,
        "fut_ann": fut_ann, "fut_state": fut_state, "past_ann": past_ann, "past_state": past_state,
        "anns": anns, "states": states, "del_pairs": len(del_max), "ann_id_coord": ann_id_coord,
        "ann_by_coord": ann_by_coord, "state_by_coord": state_by_coord, "coords": coords,
        "kept": kept,
        "repos": repos, "member_repo": member_repo,
        "shared_listed_repos": sum(1 for n in one_way_sizes.values() if n > 1),
    }


def kept_maintainers(announce_events, state_events, deletion_rows, now_ts: int) -> list:
    """The pubkeys of every kept announcement (the roster's maintainers), most kept
    coordinates first, then by pubkey - the order in which the asset's display-name lookup
    is capped (NAMES_LOOKUP_MAX)."""
    prep = _prepare(announce_events, state_events, deletion_rows, now_ts)
    n = collections.Counter(pk for pk, _d in prep["kept"])
    return sorted(n, key=lambda pk: (-n[pk], pk))


# --- the roster reduction (memory) ----------------------------------------------------------
# The two roster kinds are extracted with ALL versions, and one bot coordinate (the N5
# announcement flooder and its orphan state coordinate) holds about two thirds of them, so a
# full parse makes the compute's footprint follow the version count (review 4: 879 MiB peak on
# 2026-09-25, +250 MiB a month). No rule reads the tags of an announcement version other than
# the coordinate's current one, nor the tags of a state row outside a kept repository's member
# coordinates, so those rows are reduced to what the rules DO read: id (the star e-lookup),
# pubkey, created_at (first seen, N5, the state-loop test, the horizon and its anomaly counts)
# and the coordinate's `d`. `sig`, `content`, `kind` and `d_tag` are dropped from both kinds
# (read by no rule). build_snapshot over the reduced rows equals build_snapshot over the full
# rows (pinned by a unit test and, on the production parts of 2026-09-25, by a replay).

def _roster_row(r):
    """(id, pubkey, created_at, tags) normalized exactly as _norm_event does; the pubkey is
    interned (one bot key signs tens of thousands of versions)."""
    return str(r["id"]), sys.intern(str(r["pubkey"])), int(r["created_at"]), _tags(r)


def _reduce_announcements(rows, now_ts: int):
    """kind-30617 (repo-announcements), ALL versions -> (rows, tagged): every version,
    id-deduped (the first occurrence of an id wins, as in _dedup), as {id, pubkey, created_at,
    tags}, with the full tags only on each coordinate's current version INSIDE THE HORIZON and
    every other version's tags reduced to [["d", d]]; `tagged` counts the rows that keep them.
    The horizon (ref 1.2) is applied BEFORE the newest is picked (ref 1.4): a coordinate's
    newest row can lie on the run's own partial day, and picking it first keeps the wrong
    version's tags (measured, review 4: 2 kept coordinates and 2 stars lost)."""
    _as_of, as_of_end = as_of_bounds(now_ts)
    out, seen, current, small = [], set(), {}, {}
    for r in rows:
        eid, pk, t, tags = _roster_row(r)
        if eid in seen:
            continue
        seen.add(eid)
        d = coord_d(tags)
        c = (pk, d)
        reduced = small.get(c)
        if reduced is None:
            reduced = small[c] = [["d", d]]
        row = {"id": eid, "pubkey": pk, "created_at": t, "tags": reduced}
        out.append(row)
        if PAST_FLOOR <= t <= as_of_end:
            best = current.get(c)
            # version order (ref 1.4): created_at ascending, then id DESCENDING - the current
            # version has the greatest created_at and, among equals, the lowest id
            if best is None or t > best["created_at"] or (t == best["created_at"] and eid < best["id"]):
                if best is not None:
                    best["tags"] = reduced
                row["tags"] = tags
                current[c] = row
    return out, len(current)


def _reduce_states(rows, members, now_ts: int):
    """kind-30618 (repo-states), ALL versions -> (rows, tagged): every version, id-deduped
    (first occurrence), as {id, pubkey, created_at, tags}, the full tags only for rows inside
    the horizon at a coordinate in `members`, every other row's tags reduced to [["d", d]] -
    an orphan state coordinate is only counted (ref 3), a row outside the horizon only dated
    (ref 1.2)."""
    _as_of, as_of_end = as_of_bounds(now_ts)
    out, seen, small, tagged = [], set(), {}, 0
    for r in rows:
        eid, pk, t, tags = _roster_row(r)
        if eid in seen:
            continue
        seen.add(eid)
        d = coord_d(tags)
        c = (pk, d)
        if c in members and PAST_FLOOR <= t <= as_of_end:
            tagged += 1
        else:
            tags = small.get(c)
            if tags is None:
                tags = small[c] = [["d", d]]
        out.append({"id": eid, "pubkey": pk, "created_at": t, "tags": tags})
    return out, tagged


def reduce_roster(announce_rows, state_rows, deletion_rows, now_ts: int):
    """The two-phase roster reduction -> (announce_rows, state_rows, stats).

    Phase 1 reduces the announcements (_reduce_announcements) and runs the noise ledger over
    them AS IF every coordinate had a state; phase 2 then streams the states and keeps tags
    only at the coordinates that ledger keeps. Exact: has_state is the only ledger input the
    assumption changes (N8b and N9 read it), and a state row that survives the horizon makes
    its own coordinate's has_state true - so for every coordinate that has a surviving state
    the assumed family IS its family, and the surviving rows kept with tags are exactly those
    at the member coordinates of kept repositories (every kept coordinate is a member). The
    extra coordinates the assumption keeps (N8b / N9 ones) have no surviving state to keep.
    `state_rows` may be a one-shot iterator: it is consumed only after phase 1."""
    anns, ann_tagged = _reduce_announcements(announce_rows, now_ts)
    members = set(_prepare(anns, (), deletion_rows, now_ts, assume_state=True)["kept"])
    states, state_tagged = _reduce_states(state_rows, members, now_ts)
    stats = {"announce_rows": len(anns), "announce_tagged": ann_tagged,
             "state_rows": len(states), "state_tagged": state_tagged,
             "member_coords": len(members)}
    return anns, states, stats


def load_roster(announce_paths, state_paths, deletion_rows, now_ts: int):
    """reduce_roster streamed straight from the UNLOAD parts: no full kind-30617
    (repo-announcements) or kind-30618 (repo-states) row list is ever held in memory."""
    return reduce_roster(iter_ndjson_gz(announce_paths), iter_ndjson_gz(state_paths),
                         deletion_rows, now_ts)


def peak_rss_mib() -> float:
    """This process's peak resident set size in MiB (ru_maxrss: KiB on Linux, bytes on macOS)."""
    import resource
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


def build_snapshot(announce_events, state_events, grasp_events, comment_events,
                   reaction_events, zap_events, deletion_rows, names, now_ts: int, *,
                   zap_signers, external_reaction_events):
    """The full metrics compute. Pure: rows in, (state, data) out - no AWS, no DB, no clock
    beyond `now_ts`; `names` ({pubkey: display name}) is presentation only; `zap_signers` is
    the verified LNURL provider signer set (ref 6.3, load_provider_signers) - required, so no
    caller can forget it and count receipts anyone can fabricate. `external_reaction_events`
    (the kind-17 (external-content-reactions) git subset, Iris Git's stars, ref 6.2) is
    required for the same reason: a caller that forgot it would silently drop those stars."""
    prep = _prepare(announce_events, state_events, deletion_rows, now_ts)
    NOW, as_of, as_of_end = prep["now"], prep["as_of"], prep["as_of_end"]
    coords, repos, member_repo = prep["coords"], prep["repos"], prep["member_repo"]
    names = {pk: clean_display_name(v) for pk, v in (names or {}).items()}
    names = {pk: v for pk, v in names.items() if v}

    # ---- the push ledger (ref 3)
    state_by_coord = prep["state_by_coord"]
    orphan_coords = [c for c in state_by_coord if c not in member_repo]
    loop_coords = []
    ledger = []           # one dict per version of a member coordinate (loops excluded)
    for c in sorted(state_by_coord):
        key = member_repo.get(c)
        if key is None:
            continue
        versions = version_sorted(state_by_coord[c])
        if _is_loop(versions, STATE_LOOP_MIN_VERSIONS, STATE_LOOP_MAX_MEDIAN_GAP_S):
            loop_coords.append(c)
            continue
        prev = None
        for ev in versions:
            cur = ref_map_and_head(ev["tags"])
            flags = version_flags(prev, cur, ev["created_at"])
            ledger.append({
                "event_id": ev["id"], "pubkey": c[0], "d": c[1], "repo_key": key,
                "created_at": ev["created_at"], "date": utc_date(ev["created_at"]),
                "head": cur[1], "source": SOURCE_STATE, **flags,
            })
            prev = cur

    # per repository
    rrec = {key: {"last_state": None, "last_push": None, "newest": None,
                  "pushes_total": 0, "pushes_30d": 0, "pushes_90d": 0} for key in repos}
    for row in ledger:
        rr = rrec[row["repo_key"]]
        if rr["last_state"] is None or row["created_at"] > rr["last_state"]:
            rr["last_state"] = row["created_at"]
        # the newest state = the maximum (created_at, -id): the last row in version order
        n = rr["newest"]
        if (n is None or row["created_at"] > n["created_at"]
                or (row["created_at"] == n["created_at"] and row["event_id"] < n["event_id"])):
            rr["newest"] = row
        _tally_push(rr, row, as_of)

    # ---- hosting (ref 4)
    parsed = {}

    def _p(u):
        v = parsed.get(u)
        if v is None:
            v = parsed[u] = parse_url(u)
        return v

    grasp_hosts = set()
    for r in repos.values():
        for u in r["clone"]:
            h = grasp_set_host(_p(u))
            if h is not None:
                grasp_hosts.add(h)
    for key, r in repos.items():
        classes = [host_class(_p(u), grasp_hosts) for u in r["clone"]]
        rr = rrec[key]
        # ref 4.4: R, the reachable host classes over the clone values
        rr["reach"] = frozenset(c for c in classes if c in REACHABLE)
        rr["cls"] = repo_hosting_class(rr["reach"])
        # ref 4.4: the substrate flag is exactly "GRASP-hosted"
        rr["grasp"] = rr["cls"] in ("grasp_only", "grasp_mixed")
        # ref 4.5 grasp_per_repo: GRASP-set hosts among ALL clone values, any URL shape
        rr["grasp_hosts"] = sorted({_p(u)[2] for u in r["clone"] if _p(u)[2] in grasp_hosts})
        hosts = []
        for u in r["clone"]:
            h = _p(u)[2]
            if h is not None and h not in hosts:
                hosts.append(h)
        rr["hosts"] = hosts
        relays = []
        for v in r["relays"]:
            nr = norm_relay(v)
            if nr is not None and nr not in relays:
                relays.append(nr)
        rr["relays"] = relays
        web = []
        for u in r["web"]:
            pu = _p(u)
            # ref 4.5: a private host (127.0.0.1, a LAN name) is nobody's web host (review 4 m4)
            if (pu[0] in ("http", "https") and pu[2] is not None and not is_private_host(pu[2])
                    and pu[2] not in web):
                web.append(pu[2])
        rr["web_hosts"] = web
        rr["topics"] = sorted({t for t in r["t"] if t and not _TOPIC_NUM_RE.fullmatch(t)
                               and t not in TOPIC_EXCLUDE})

    # ---- hashtree pushes (ref 3, since 2026-10-12): hashtree's git remote helper publishes a
    # push as kind-30064 (hashtree-roots), never a kind-30618 (repo-states), and republishes the
    # repository's kind-30617 (repo-announcements) with each ref of a public push. So a member
    # coordinate of a repository whose class is hashtree has one push per group of versions
    # less than HTREE_PUSH_GAP_S apart (htree_push_starts), except its first group (the
    # repository's creation or first push, or what survived to the wall) and any dated before
    # STATE_WALL. The class alone decides it: a grasp_mixed repository (GRASP and hashtree)
    # keeps its state pushes only, so one push sent to both counts once. Such a row moves no
    # ref this ledger can see (the refs live inside the tree), so it never touches last_state,
    # the newest state or Anatomy.
    ann_by_coord = prep["ann_by_coord"]
    for key in sorted(repos):
        if rrec[key]["cls"] != "hashtree":
            continue
        for c in repos[key]["members"]:
            for ev in htree_push_starts(version_sorted(ann_by_coord[c]))[1:]:
                if ev["created_at"] < STATE_WALL:
                    continue
                row = {"event_id": ev["id"], "pubkey": c[0], "d": c[1], "repo_key": key,
                       "created_at": ev["created_at"], "date": utc_date(ev["created_at"]),
                       "head": None, "source": SOURCE_HTREE, **HTREE_PUSH_FLAGS}
                ledger.append(row)
                _tally_push(rrec[key], row, as_of)

    # tools: raw labels per coordinate; the five most frequent "other client" values over
    # REPOSITORIES (lead announcements) are kept, the rest fold into "other client".
    raw_tool = {c: tool_label(rec["attrs"]) for c, rec in coords.items()}
    other_clients = collections.Counter(raw_tool[r["lead"]][1] for r in repos.values()
                                        if raw_tool[r["lead"]][1] is not None)
    top_clients = {v for v, _n in sorted(other_clients.items(), key=lambda kv: (-kv[1], kv[0]))
                   [:TOOL_TOP_CLIENTS]}

    def _tool(c):
        label, raw = raw_tool[c]
        return label if raw is None or raw in top_clients else TOOL_OTHER_CLIENT

    # ---- GRASP lists (ref 5.2)
    grasp_in, fut_grasp, past_grasp = _horizon(_dedup(grasp_events), as_of_end, NOW)
    lists_by_pk = collections.defaultdict(list)
    for ev in grasp_in:
        lists_by_pk[ev["pubkey"]].append(ev)
    grasp_users = {}
    for pk in sorted(lists_by_pk):
        versions = version_sorted(lists_by_pk[pk])
        seen, lst = set(), []
        for t in versions[-1]["tags"]:
            if not (isinstance(t, list) and len(t) >= 2 and t[0] == "g" and isinstance(t[1], str)):
                continue
            gv = grasp_list_value(t[1])
            if gv is not None and gv[0] not in seen:
                seen.add(gv[0])
                lst.append(gv)
        if lst:
            grasp_users[pk] = {"hosts": [h for _v, h in lst], "first_seen": versions[0]["created_at"]}

    # ---- comments (ref 6.1)
    comments, fut_comments, past_comments = _horizon(
        [ev for ev in _dedup(comment_events) if comment_in_scope(ev["tags"])], as_of_end, NOW)
    per_author = collections.Counter(ev["pubkey"] for ev in comments)
    mirror_hits = collections.Counter(ev["pubkey"] for ev in comments
                                      if _MIRROR_LINE_RE.match(ev["content"]))
    mirror_keys = {pk for pk, n in per_author.items()
                   if n >= MIRROR_MIN_COMMENTS and mirror_hits[pk] * 10 > n * 9}
    comment_ledger = collections.Counter()
    counted_comments = []
    for ev in comments:
        tags = ev["tags"]
        if has_tag(tags, "expiration"):
            comment_ledger["ephemeral"] += 1
            continue
        if has_tag(tags, "imported"):
            comment_ledger["imported"] += 1
            continue
        if ev["pubkey"] in mirror_keys:
            comment_ledger["mirrored"] += 1
            continue
        coord = repo_coordinate(tags)
        counted_comments.append({
            "date": utc_date(ev["created_at"]).isoformat(),
            "root": COMMENT_ROOTS.get(first_value(tags, "K"), "other"),
            "pubkey": ev["pubkey"],
            "thread": first_value(tags, "E"),
            "repo": member_repo.get(coord) if coord is not None else None,
        })

    # ---- stars (ref 6.2)
    reactions, fut_reactions, past_reactions = _horizon(
        [ev for ev in _dedup(reaction_events) if reaction_in_scope(ev["tags"])], as_of_end, NOW)
    star_pairs = set()
    for ev in reactions:
        if ev["content"] not in ("+", ""):
            continue
        tags = ev["tags"]
        if any(isinstance(t, list) and len(t) >= 2 and t[0] == "a" and isinstance(t[1], str)
               and t[1].startswith("30617:") for t in tags):
            target = repo_coordinate(tags, names=("a",))
        else:
            target = prep["ann_id_coord"].get(first_value(tags, "e"))
        key = member_repo.get(target) if target is not None else None
        if key is not None:
            star_pairs.add((ev["pubkey"], key))
    # ... and kind-17 (external-content-reactions), Iris Git's: not one star per row but each
    # key's CURRENT reaction to a coordinate (the version order of ref 1.4), so a later "-"
    # unstars it. It withdraws only that kind's star: a kind-7 (reactions) pair stays.
    ext_reactions, fut_ext_reactions, past_ext_reactions = _horizon(
        [ev for ev in _dedup(external_reaction_events) if external_reaction_in_scope(ev["tags"])],
        as_of_end, NOW)
    ext_by_key_coord = collections.defaultdict(list)
    for ev in ext_reactions:
        target = repo_coordinate(ev["tags"], names=("i",))
        if target is not None:
            ext_by_key_coord[(ev["pubkey"], target)].append(ev)
    iris_pairs = set()
    for (pubkey, target), evs in ext_by_key_coord.items():
        if version_sorted(evs)[-1]["content"].strip() not in ("+", ""):
            continue
        key = member_repo.get(target)
        if key is not None:
            iris_pairs.add((pubkey, key))
    star_pairs |= iris_pairs          # one pair per key and repository, whichever client starred
    stars_by_repo = collections.Counter(key for _pk, key in star_pairs)

    # ---- zaps (ref 6.3)
    zp = load_zap_parser()
    zaps, fut_zaps, past_zaps = _horizon(
        [ev for ev in _dedup(zap_events) if zap_in_scope(ev["tags"])], as_of_end, NOW)
    zap_signers = frozenset(zap_signers)
    zap_receipts = zap_msats = 0
    zappers = set()
    zaps_by_repo = collections.defaultdict(lambda: [0, 0])
    for ev in zaps:
        amount = zap_amount_msats(zp, ev["tags"])
        if amount is None or amount > ZAP_SPAM_MSATS:
            continue
        coord = repo_coordinate(ev["tags"], names=("a",))
        key = member_repo.get(coord) if coord is not None else None
        if key is None:
            continue
        # ref 6.3 (review 4 M3): signed by a verified LNURL provider key - a receipt anyone can
        # sign under a fresh key counts nowhere; paying (first `p`) a member of the repository
        # it names; and, when the embedded request states an amount, for exactly that amount
        if ev["pubkey"] not in zap_signers:
            continue
        if first_value(ev["tags"], "p") not in {m[0] for m in repos[key]["members"]}:
            continue
        stated = zap_request_amount(ev["tags"])
        if stated is not None and stated != str(amount):     # decimal text, exactly
            continue
        counted = min(amount, ZAP_CAP_MSATS)
        zap_receipts += 1
        zap_msats += counted
        zaps_by_repo[key][0] += 1
        zaps_by_repo[key][1] += counted
        sender = zap_sender(ev["tags"])
        if sender is not None:
            zappers.add(sender)

    # ---- the substrate: ngit_repos (one row per announcement coordinate)
    lead_of = {r["lead"]: key for key, r in repos.items()}
    # ref 9: d is stored as is - never clipped, never NUL-stripped (N0 keeps every d written
    # here within 200 characters and free of control characters) - and repo_key carries it.
    sub_d = {c: c[1] for c in coords}
    repos_rows = []
    for c in sorted(coords):
        rec = coords[c]
        if rec["family"] == "N0":
            continue                      # counted in the ledger, written to no row
        attrs = rec["attrs"]
        key = member_repo.get(c)
        rr = rrec.get(key) if key is not None else None
        hosts_json = []
        for u in attrs["clone"]:
            pu = _p(u)
            if pu[2] is None:
                continue
            item = {"host": _clip(pu[2], HOST_MAX), "cls": host_class(pu, grasp_hosts)}
            if item not in hosts_json:
                hosts_json.append(item)
        relays_json = []
        for v in attrs["relays"]:
            nr = norm_relay(v)
            if nr is not None and _clip(nr, RELAY_MAX) not in relays_json:
                relays_json.append(_clip(nr, RELAY_MAX))
        repos_rows.append({
            "pubkey": c[0], "d": sub_d[c], "family": rec["family"],
            "repo_key": _sub_key(repos[key]["lead"], sub_d) if key is not None else None,
            "is_lead": c in lead_of,
            "name": _clip(clean_text(attrs["name"]), NAME_MAX),
            "first_seen": rec["first_seen"], "last_seen": rec["last_seen"],
            "versions": rec["versions"],
            "cls": rr["cls"] if rr else None,
            "grasp": rr["grasp"] if rr else None,
            "hosts": hosts_json[:SUBSTRATE_LIST_MAX],
            "relays": relays_json[:SUBSTRATE_LIST_MAX],
            "euc": attrs["euc"],
            "tool": _tool(c),
            "has_state": c in state_by_coord,
            "last_state": rr["last_state"] if rr else None,
            "last_push": rr["last_push"] if rr else None,
            "pushes_total": rr["pushes_total"] if rr else 0,
            "pushes_30d": rr["pushes_30d"] if rr else 0,
            "pushes_90d": rr["pushes_90d"] if rr else 0,
        })
    push_rows = []
    for row in ledger:
        push_rows.append({
            "event_id": row["event_id"], "pubkey": row["pubkey"],
            "d": sub_d[(row["pubkey"], row["d"])],
            "repo_key": _sub_key(repos[row["repo_key"]]["lead"], sub_d),
            "created_at": row["created_at"],
            **{f: row[f] for f in PUSH_FLAG_COLUMNS},
            "source": row["source"],
        })
    push_rows.sort(key=lambda r: (r["repo_key"], r["created_at"], r["event_id"]))

    # ---- the document
    def display(key):
        """ref 2.3: the lead's name, falling back to d when empty; control and format
        characters removed (ref 9), clipped to 80."""
        r = repos[key]
        return _clip(clean_text(r["name"]) or clean_text(sub_d[r["lead"]]), NAME_MAX)

    def repo_row(key):
        r, rr = repos[key], rrec[key]
        newest = rr["newest"]
        lead_pk = r["lead"][0]
        return {
            "pubkey": lead_pk, "d": sub_d[r["lead"]],
            "name": display(key),
            "lead_name": names.get(lead_pk),
            "pushes_30d": rr["pushes_30d"], "pushes_90d": rr["pushes_90d"],
            "last_push": day_str(rr["last_push"]) if rr["last_push"] is not None else None,
            "last_state": day_str(rr["last_state"]) if rr["last_state"] is not None else None,
            "branches": newest["branches"] if newest else None,
            "tags": newest["tags"] if newest else None,
            "cls": rr["cls"],
            # ref 4.4's R, the kinds of public host the repository's clone URLs are on, in the
            # HOST_KINDS order, ["none"] when R is empty: the page's repository tables name
            # them since 2026-10-11, where the class alone cannot say which hosts a grasp_mixed
            # or other_hosts repository is on besides
            "host_kinds": [k for k in HOST_KINDS if k in rr["reach"]] or ["none"],
            "hosts": [_clip(h, HOST_MAX) for h in rr["hosts"][:REPO_ROW_HOSTS]],
        }

    def ref(key):
        """A leaderboard row's repository (discussed, starred, zapped). Its hosting class (ref
        4.4), which repo_row has always carried, is on these rows too since 2026-10-09: the page
        links a hashtree-class repository to git.iris.to, hashtree's own viewer, as
        gitworkshop.dev cannot read htree:// URLs."""
        r = repos[key]
        return {"pubkey": r["lead"][0], "d": sub_d[r["lead"]], "name": display(key),
                "cls": rrec[key]["cls"]}

    keys = sorted(repos)
    n_repos = len(keys)
    hosting = collections.Counter(rrec[k]["cls"] for k in keys)
    grasp_repo_keys = [k for k in keys if rrec[k]["cls"] in ("grasp_only", "grasp_mixed")]
    with_state = [k for k in keys if rrec[k]["last_state"] is not None]
    recency = collections.Counter(
        recency_bucket((as_of - utc_date(rrec[k]["last_state"])).days) for k in with_state)
    no_state = collections.Counter(rrec[k]["cls"] for k in keys if rrec[k]["last_state"] is None)

    # ref 3: every push counts (the per-repository daily cap was dropped on 2026-10-02)
    pushed = [r for r in ledger if r["push"]]
    maintainers = sorted({pk for pk, _d in prep["kept"]})

    # clone hosts: repositories per host, the host's class by ref 4.2 precedence over the
    # classes its values fall in.
    host_repos = collections.defaultdict(set)
    host_classes = collections.defaultdict(set)
    for key in keys:
        for u in repos[key]["clone"]:
            pu = _p(u)
            if pu[2] is None:
                continue
            host_repos[pu[2]].add(key)
            host_classes[pu[2]].add(host_class(pu, grasp_hosts))
    clone_hosts = [
        {"host": _clip(h, HOST_MAX), "repos": len(ks),
         "cls": next(c for c in HOST_CLASS_ORDER if c in host_classes[h])}
        for h, ks in sorted(host_repos.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    ][:TOP_CLONE_HOSTS]

    # grasp_servers.repos: repositories with a GRASP-SHAPED clone value on the host (ref 4.5)
    grasp_repo_by_host = collections.Counter(
        h for k in keys for h in {_p(u)[2] for u in repos[k]["clone"]
                                  if is_grasp_shaped(_p(u)) and _p(u)[2] in grasp_hosts})
    users_by_host = collections.Counter()
    first_by_host = collections.Counter()
    for u in grasp_users.values():
        for h in set(u["hosts"]):
            users_by_host[h] += 1
        first_by_host[u["hosts"][0]] += 1
    grasp_servers = sorted(
        ({"host": h, "repos": grasp_repo_by_host[h], "users": users_by_host[h],
          "first_choice": first_by_host[h]} for h in grasp_hosts),
        key=lambda r: (-r["repos"], -r["users"], r["host"]))[:TOP_GRASP_SERVERS]
    for r in grasp_servers:
        r["host"] = _clip(r["host"], HOST_MAX)

    relay_repos = collections.Counter(nr for k in keys for nr in rrec[k]["relays"])
    web_repos = collections.Counter(h for k in keys for h in rrec[k]["web_hosts"])
    topic_repos = collections.Counter(t for k in keys for t in rrec[k]["topics"])
    tools = collections.Counter(_tool(repos[k]["lead"]) for k in keys)

    anatomy_heads = collections.Counter()
    anatomy_branches = collections.Counter()
    anatomy_tags = collections.Counter()
    for k in with_state:
        n = rrec[k]["newest"]
        anatomy_heads[default_branch_bucket(n["head"])] += 1
        anatomy_branches[_bucket(n["branches"], _BRANCH_BUCKETS)] += 1
        anatomy_tags[_bucket(n["tags"], _TAG_BUCKETS)] += 1

    # fork families: euc groups with more than one repository (listed, never counted; the
    # page stopped showing them on 2026-09-29)
    euc_groups = collections.defaultdict(list)
    for k in keys:
        if repos[k]["euc"] is not None:
            euc_groups[repos[k]["euc"]].append(k)
    families = {e: ks for e, ks in euc_groups.items() if len(ks) > 1}
    fork_families_top = []
    for e, ks in sorted(families.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:TOP_FORK_FAMILIES]:
        ordered = sorted(ks, key=lambda k: (repos[k]["first_seen"], k))[:FORK_FAMILY_NAMES]
        fork_families_top.append({
            "euc": e, "repos": len(ks),
            "names": [display(k) for k in ordered],
        })

    # maintainers (ref 5.1)
    member_of = collections.Counter(m[0] for k in keys for m in repos[k]["members"])
    push90_by_pk = collections.Counter(r["pubkey"] for r in pushed if in_window(r["date"], as_of, 90))
    last_push_by_pk = {}
    for r in pushed:
        if r["created_at"] > last_push_by_pk.get(r["pubkey"], r["created_at"] - 1):
            last_push_by_pk[r["pubkey"]] = r["created_at"]
    maintainers_top = [
        {"pubkey": pk, "name": names.get(pk), "repos": member_of[pk],
         "pushes_90d": push90_by_pk[pk],
         "last_push": day_str(last_push_by_pk[pk]) if pk in last_push_by_pk else None}
        # ref 5.1: the most active maintainers - pushes in 90 d, then repositories, then pubkey
        for pk in sorted(member_of, key=lambda pk: (-push90_by_pk[pk], -member_of[pk], pk))
    ][:TOP_MAINTAINERS]
    led = collections.Counter(repos[k]["lead"][0] for k in keys)
    led_rank = sorted(led, key=lambda pk: (-led[pk], pk))
    top1 = sum(led[pk] for pk in led_rank[:1])
    top10 = sum(led[pk] for pk in led_rank[:10])

    # the time series (ref 7), each ending at as_of_day
    as_of_month = month_of(as_of)
    new_by_month = collections.defaultdict(lambda: [0, 0])
    for k in keys:
        m = month_of(utc_date(repos[k]["first_seen"]))
        new_by_month[m][0 if rrec[k]["cls"] in ("grasp_only", "grasp_mixed") else 1] += 1
    maint_first = {}
    for pk, d in prep["kept"]:
        fs = coords[(pk, d)]["first_seen"]
        if fs < maint_first.get(pk, fs + 1):
            maint_first[pk] = fs
    maint_by_month = collections.Counter(month_of(utc_date(fs)) for fs in maint_first.values())
    new_repos_monthly = []
    if keys:
        first_month = min(new_by_month)
        for m in _months(first_month, as_of_month):
            g, o = new_by_month.get(m, (0, 0))
            new_repos_monthly.append({"m": m, "v": g + o, "grasp": g, "other": o,
                                      "s": maint_by_month.get(m, 0)})
    # ref 7: the same per ISO week, zero-filled from the week holding the announcements wall;
    # a repository or maintainer first seen before that week is in no week. by_host counts
    # each week's repositories once in every kind of public host they are on today (R), or
    # under none when R is empty (2026-10-08)
    new_by_week = collections.defaultdict(lambda: [0, 0])
    host_by_week = collections.defaultdict(collections.Counter)
    for k in keys:
        w = week_monday(utc_date(repos[k]["first_seen"]))
        new_by_week[w][0 if rrec[k]["cls"] in ("grasp_only", "grasp_mixed") else 1] += 1
        host_by_week[w].update(rrec[k]["reach"] or ("none",))
    maint_by_week = collections.Counter(week_monday(utc_date(fs)) for fs in maint_first.values())
    new_repos_weekly = []
    for w in _weeks(utc_date(ANN_WALL), as_of):
        g, o = new_by_week.get(w, (0, 0))
        new_repos_weekly.append({"w": w.isoformat(), "v": g + o, "grasp": g, "other": o,
                                 "s": maint_by_week.get(w, 0),
                                 "by_host": _kv(HOST_KINDS, host_by_week.get(w, {}))})

    cum_repos = []
    if keys:
        by_day = collections.Counter(utc_date(repos[k]["first_seen"]) for k in keys)
        first_day = min(by_day)
        run = 0
        prev_v = None
        for d in sorted(by_day):
            if d > as_of:
                break
            run += by_day[d]
            cum_repos.append({"d": d.isoformat(), "v": run})
            prev_v = run
        if first_day <= as_of and (not cum_repos or cum_repos[-1]["d"] != as_of.isoformat()):
            cum_repos.append({"d": as_of.isoformat(), "v": prev_v or 0})

    first_week = week_monday(utc_date(STATE_WALL))
    last_week = week_monday(as_of)
    wk_pushes = collections.Counter()
    wk_repos = collections.defaultdict(set)
    wk_pks = collections.defaultdict(set)
    for r in pushed:
        w = week_monday(r["date"])
        wk_repos[w].add(r["repo_key"])
        wk_pks[w].add(r["pubkey"])
        wk_pushes[w] += 1
    pushes_weekly = []
    w = first_week
    while w <= last_week:
        pushes_weekly.append({"w": w.isoformat(), "pushes": wk_pushes.get(w, 0),
                              "repos": len(wk_repos.get(w, ())), "maintainers": len(wk_pks.get(w, ()))})
        w += dt.timedelta(days=7)

    # comments (ref 6.1) - both series end at as_of_day's bucket like the others
    def comment_series(field, bucket_of, labels):
        """Counted comments per bucket: the five root counts, distinct authors and distinct
        threads, one row per label (zero-filled); a comment whose bucket is not among the
        labels is in no row."""
        agg = collections.defaultdict(lambda: {"issues": 0, "prs": 0, "patches": 0, "repo": 0,
                                               "other": 0, "authors": set(), "threads": set()})
        for c in counted_comments:
            rec = agg[bucket_of(c["date"])]
            rec[c["root"]] += 1
            rec["authors"].add(c["pubkey"])
            if c["thread"] is not None:
                rec["threads"].add(c["thread"])
        out = []
        for label in labels:
            rec = agg.get(label)
            out.append({
                field: label,
                "issues": rec["issues"] if rec else 0, "prs": rec["prs"] if rec else 0,
                "patches": rec["patches"] if rec else 0, "repo": rec["repo"] if rec else 0,
                "other": rec["other"] if rec else 0,
                "authors": len(rec["authors"]) if rec else 0,
                "threads": len(rec["threads"]) if rec else 0,
            })
        return out

    comments_monthly = []
    if counted_comments:
        comments_monthly = comment_series(
            "m", lambda d: d[:7], _months(min(c["date"] for c in counted_comments)[:7], as_of_month))
    # per ISO week, zero-filled from the week holding the comments wall
    comments_weekly = comment_series(
        "w", lambda d: week_monday(dt.date.fromisoformat(d)).isoformat(),
        [w.isoformat() for w in _weeks(utc_date(COMMENT_WALL), as_of)])
    attributable = [c for c in counted_comments if c["repo"] is not None]
    disc = collections.defaultdict(lambda: [0, set()])
    for c in attributable:
        disc[c["repo"]][0] += 1
        disc[c["repo"]][1].add(c["pubkey"])

    per_month_users = collections.Counter(month_of(utc_date(u["first_seen"])) for u in grasp_users.values())
    grasp_per_month = []
    if grasp_users:
        grasp_per_month = [{"m": m, "v": per_month_users.get(m, 0)}
                           for m in _months(min(per_month_users), as_of_month)]
    # per ISO week, zero-filled from the week holding the states wall (ref 5.2)
    per_week_users = collections.Counter(week_monday(utc_date(u["first_seen"])) for u in grasp_users.values())
    grasp_per_week = [{"w": w.isoformat(), "v": per_week_users.get(w, 0)}
                      for w in _weeks(utc_date(STATE_WALL), as_of)]
    list_length = collections.Counter(_bucket(len(u["hosts"]), _FOUR_BUCKETS) for u in grasp_users.values())
    first_choice = collections.Counter(u["hosts"][0] for u in grasp_users.values())

    def _share(a: int, b: int):
        return a / b if b else 0

    summary = {
        # ref 1.2: the rows surviving id-dedup and the horizon, per kind
        "archive_events": {
            "announcements": len(prep["anns"]), "states": len(prep["states"]),
            "grasp_lists": len(grasp_in), "comments": len(comments),
            "reactions": len(reactions), "external_reactions": len(ext_reactions),
            "zaps": len(zaps), "deletions": prep["del_pairs"],
        },
        "announcements_total": len(coords),
        "announcements_kept": len(prep["kept"]),
        "repos": n_repos,
        "forks": sum(1 for k in keys if coords[repos[k]["lead"]]["attrs"]["has_u"]
                     or "personal-fork" in coords[repos[k]["lead"]]["attrs"]["t"]),
        "repos_with_state": len(with_state),
        "repos_pushed_30d": len({r["repo_key"] for r in pushed if in_window(r["date"], as_of, 30)}),
        "repos_pushed_90d": len({r["repo_key"] for r in pushed if in_window(r["date"], as_of, 90)}),
        "pushes_7d": sum(1 for r in pushed if in_window(r["date"], as_of, 7)),
        "pushes_30d": sum(1 for r in pushed if in_window(r["date"], as_of, 30)),
        "pushes_total": len(pushed),
        # ref 3: the pushes of pushes_30d that are hashtree announcement republishes; first
        # written by the first run after the nostrology-data deploy of 2026-10-12, and its
        # presence tells the page that hashtree's pushes are counted
        "pushes_hashtree_30d": sum(1 for r in pushed if r["source"] == SOURCE_HTREE
                                   and in_window(r["date"], as_of, 30)),
        "maintainers": len(maintainers),
        "maintainers_active_30d": len({r["pubkey"] for r in pushed if in_window(r["date"], as_of, 30)}),
        "multi_maintainer_repos": sum(1 for k in keys if len(repos[k]["members"]) > 1),
        "shared_listed_repos": prep["shared_listed_repos"],
        "fork_families": len(families),
        "grasp_repos": len(grasp_repo_keys),
        "grasp_share": _share(len(grasp_repo_keys), n_repos),
        "grasp_servers": len(grasp_hosts),
        "grasp_list_users": len(grasp_users),
        "comments_total": len(counted_comments),
        "comments_30d": sum(1 for c in counted_comments
                            if in_window(dt.date.fromisoformat(c["date"]), as_of, 30)),
        "comments_90d": sum(1 for c in counted_comments
                            if in_window(dt.date.fromisoformat(c["date"]), as_of, 90)),
        "comment_threads": len({c["thread"] for c in counted_comments if c["thread"] is not None}),
        "comment_participants": len({c["pubkey"] for c in counted_comments}),
        "comments_attributable_share": _share(len(attributable), len(counted_comments)),
        "stars_total": len(star_pairs),
        "stars_repos": len(stars_by_repo),
        "stars_iris": len(iris_pairs),
        "zap_receipts": zap_receipts,
        "zap_msats": zap_msats,
        "zappers": len(zappers),
        "zaps_repos": len(zaps_by_repo),
        "first_announcement": (day_str(min(repos[k]["first_seen"] for k in keys)) if keys else None),
        "newest_state": (day_str(max(rrec[k]["last_state"] for k in with_state)) if with_state else None),
        "newest_push": (day_str(max(rrec[k]["last_push"] for k in keys if rrec[k]["last_push"] is not None))
                        if any(rrec[k]["last_push"] is not None for k in keys) else None),
        "concentration": {"top1_share": _share(top1, n_repos), "top10_share": _share(top10, n_repos)},
    }

    # ref 9: exactly these keys, in this order, every one present even when zero
    exclusions = [
        {"k": code, "unit": "announcements",
         "v": sum(1 for r in coords.values() if r["family"] == code), "why": why}
        for code, why in LEDGER_FAMILIES
    ] + [
        {"k": "state_republish_loop", "unit": "state coordinates", "v": len(loop_coords),
         "why": "a kept repository's kind-30618 (repo-states) coordinate with at least 1,000 "
                "versions and a median gap under 120 s - its versions are left out of the push ledger"},
        {"k": "orphan_states", "unit": "state coordinates", "v": len(orphan_coords),
         "why": "kind-30618 (repo-states) coordinates that are not a kept repository's member "
                "coordinate - ignored"},
        {"k": "ephemeral", "unit": "comments", "v": comment_ledger["ephemeral"],
         "why": "kind-1111 (comments) carrying an expiration tag - declared temporary by their "
                "author (review bots)"},
        {"k": "imported", "unit": "comments", "v": comment_ledger["imported"],
         "why": "kind-1111 (comments) carrying an imported tag - copied in from elsewhere"},
        {"k": "mirrored", "unit": "comments", "v": comment_ledger["mirrored"],
         "why": "kind-1111 (comments) by a mirror key: at least 50 comments, more than 90% "
                "re-posting a foreign author line"},
    ]

    active_keys = sorted((k for k in keys if rrec[k]["pushes_90d"] > 0),
                         key=lambda k: (-rrec[k]["pushes_90d"], -rrec[k]["pushes_30d"],
                                        -(rrec[k]["last_push"] or 0), k))[:TOP_REPO_ROWS]
    recent_keys = sorted((k for k in keys if rrec[k]["last_push"] is not None),
                         key=lambda k: (-rrec[k]["last_push"], k))[:TOP_REPO_ROWS]
    new_keys = sorted((k for k in keys if repos[k]["first_seen"] >= ANN_WALL),
                      key=lambda k: (-repos[k]["first_seen"], k))[:TOP_REPO_ROWS]

    data = {
        "generated_utc": (_EPOCH + dt.timedelta(seconds=NOW)).strftime("%Y-%m-%d %H:%M:%SZ"),
        "as_of_day": as_of.isoformat(),
        "coverage": {
            "announcements_since": day_str(ANN_WALL),
            "states_since": day_str(STATE_WALL),
            "comments_since": day_str(COMMENT_WALL),
            "reactions_since": REACTIONS_SINCE,
            "external_reactions_since": EXTERNAL_REACTIONS_SINCE,
            "zaps_since": ZAPS_SINCE,
            "deletions_since": DELETIONS_SINCE,
            "ngit_relay_since": NGIT_RELAY_SINCE,
            "partial_first_week": first_week.isoformat(),
            "partial_week": last_week.isoformat() if as_of.weekday() != 6 else None,
            "partial_month": (as_of_month if (as_of + dt.timedelta(days=1)).month == as_of.month
                              else None),
        },
        "anomalies": {
            "future_dated": {
                "announcements": prep["fut_ann"], "states": prep["fut_state"],
                "grasp_lists": fut_grasp, "comments": fut_comments,
                "reactions": fut_reactions, "external_reactions": fut_ext_reactions,
                "zaps": fut_zaps,
            },
            "past_dated": {
                "announcements": prep["past_ann"], "states": prep["past_state"],
                "grasp_lists": past_grasp, "comments": past_comments,
                "reactions": past_reactions, "external_reactions": past_ext_reactions,
                "zaps": past_zaps,
            },
        },
        "summary": summary,
        "exclusions": exclusions,
        "new_repos_monthly": new_repos_monthly,
        "new_repos_weekly": new_repos_weekly,
        "cum_repos": cum_repos,
        "pushes_weekly": pushes_weekly,
        "recency": _kv(RECENCY_KEYS, recency),
        "no_state": {"total": n_repos - len(with_state), "by_class": _kv(REPO_CLASSES, no_state)},
        "hosting": _kv(REPO_CLASSES, hosting),
        "clone_hosts": clone_hosts,
        "grasp_servers": grasp_servers,
        "grasp_per_repo": _kv(("1", "2", "3", "4+"), collections.Counter(
            _bucket(len(rrec[k]["grasp_hosts"]), _FOUR_BUCKETS) for k in grasp_repo_keys)),
        "relays": [{"relay": _clip(r, RELAY_MAX), "repos": n}
                   for r, n in sorted(relay_repos.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_RELAYS]],
        "relays_per_repo": _kv(("0", "1", "2-3", "4-5", "6+"), collections.Counter(
            _bucket(len(rrec[k]["relays"]), _RELAY_BUCKETS) for k in keys)),
        "web_hosts": [{"host": _clip(h, HOST_MAX), "repos": n}
                      for h, n in sorted(web_repos.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_WEB_HOSTS]],
        "default_branch": _kv(("main", "master", "other", "detached", "none"), anatomy_heads),
        "branches_per_repo": _kv(("0", "1", "2-5", "6-20", "21+"), anatomy_branches),
        "tags_per_repo": _kv(("0", "1-5", "6-20", "21+"), anatomy_tags),
        "topics": [{"t": _clip(t, TOPIC_MAX), "repos": n}
                   for t, n in sorted(topic_repos.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_TOPICS]],
        "tools": [{"k": _clip_tail(k, TOOL_MAX), "v": n}
                  for k, n in sorted(tools.items(), key=lambda kv: (-kv[1], kv[0]))],
        "repos_active": [repo_row(k) for k in active_keys],
        "repos_recent_pushes": [repo_row(k) for k in recent_keys],
        "repos_new": [{**repo_row(k), "first_seen": day_str(repos[k]["first_seen"])} for k in new_keys],
        "fork_families_top": fork_families_top,
        "maintainers_top": maintainers_top,
        "grasp_lists": {
            "users": len(grasp_users),
            "per_month": grasp_per_month,
            "per_week": grasp_per_week,
            "length": _kv(("1", "2", "3", "4+"), list_length),
            "first_choice": [{"host": _clip(h, HOST_MAX), "v": n} for h, n in
                             sorted(first_choice.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_FIRST_CHOICE]],
        },
        "comments_monthly": comments_monthly,
        "comments_weekly": comments_weekly,
        "repos_discussed": [
            {**ref(k), "comments": disc[k][0], "authors": len(disc[k][1])}
            for k in sorted(disc, key=lambda k: (-len(disc[k][1]), -disc[k][0], k))[:TOP_DISCUSSED]],
        "repos_starred": [
            {**ref(k), "stars": n}
            for k, n in sorted(stars_by_repo.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_STARRED]],
        # by receipts, then msats, then the key: the operator's ask of 2026-10-18 (ref 6.3; by
        # msats, then receipts before it)
        "repos_zapped": [
            {**ref(k), "receipts": v[0], "msats": v[1]}
            for k, v in sorted(zaps_by_repo.items(), key=lambda kv: (-kv[1][0], -kv[1][1], kv[0]))[:TOP_ZAPPED]],
    }

    # ---- the per-repository figures (ref 9.2): one row per kept repository, the table the
    # page lists Stars and Zaps from since 2026-10-18 (the operator's choice; the coming MCP
    # domain's source too). The document's two lists are its top 20 under their rankings,
    # which check_invariants holds.
    stats_rows = []
    for k in keys:
        receipts, msats = zaps_by_repo.get(k, (0, 0))      # .get: a defaultdict, not grown here
        stats_rows.append({"repo_key": _sub_key(repos[k]["lead"], sub_d), **ref(k),
                           "stars": stars_by_repo.get(k, 0), "zap_receipts": receipts, "zap_msats": msats})

    state = {
        "repos_rows": repos_rows,
        "push_rows": push_rows,
        "stats_rows": stats_rows,
        "counted_comments": counted_comments,
        # the first-seen instant of every GRASP list user, for check_invariants' re-bucketing
        "grasp_first_seen": [u["first_seen"] for u in grasp_users.values()],
    }
    return state, data


PUSH_FLAG_COLUMNS = (
    "changed", "push", "head_changed", "heads_moved", "default_moved",
    "branches_added", "branches_removed", "tags_added", "tags_removed", "tags_moved",
    "branches", "tags",
)


def _sub_key(lead, sub_d) -> str:
    """ref 9: `<lead pubkey>:<d>` with d as stored (never clipped, never NUL-stripped)."""
    return f"{lead[0]}:{sub_d[lead]}"


# --- integrity ------------------------------------------------------------------------------

def check_invariants(state, data) -> list[str]:
    """Cross-checks between the document's own numbers and between the document and the
    substrate rows (docs/design/ngit-design.md section 9 step 4). Every failure is a wrong-metric
    bug; the caller turns a non-empty list into a failed run before any write."""
    p: list[str] = []
    s = data["summary"]
    as_of = dt.date.fromisoformat(data["as_of_day"])
    as_of_month = month_of(as_of)
    rows = state["repos_rows"]
    pushes = state["push_rows"]
    kept_rows = [r for r in rows if r["family"] == "repo"]
    leads = {r["repo_key"]: r for r in kept_rows if r["is_lead"]}

    # -- the ledger partition, in announcement units, and the substrate's families
    fam = collections.Counter(r["family"] for r in rows)
    ann_rows = [e for e in data["exclusions"] if e["unit"] == "announcements"]
    if sum(e["v"] for e in ann_rows) + s["announcements_kept"] != s["announcements_total"]:
        p.append("ledger rows (announcement units) + kept != announcements_total")
    ledger_keys = [e["k"] for e in data["exclusions"]]
    if ledger_keys != list(EXCLUSION_KEYS):
        p.append("the ledger keys are not exactly " + ", ".join(EXCLUSION_KEYS))
    n0 = 0
    if [e["k"] for e in ann_rows] != list(FAMILY_CODES):
        p.append("the announcement-unit ledger rows are not N0..N10 in order")
    else:
        n0 = ann_rows[0]["v"]
        for (code, _w), e in zip(LEDGER_FAMILIES[1:], ann_rows[1:]):
            if fam.get(code, 0) != e["v"]:
                p.append(f"ngit_repos family {code} count {fam.get(code, 0)} != ledger row {e['v']}")
    # N0 coordinates are counted in the ledger and written to no row (ref 2.2)
    if fam.get("N0", 0):
        p.append("an N0 coordinate was written to ngit_repos")
    if len(rows) + n0 != s["announcements_total"]:
        p.append("ngit_repos rows + N0 != announcements_total")
    if any(len(r["d"]) > D_MAX for r in rows):
        p.append("an ngit_repos d is longer than 200 characters")
    if fam.get("repo", 0) != s["announcements_kept"]:
        p.append("ngit_repos kept rows != announcements_kept")
    if set(fam) - set(FAMILY_CODES) - {"repo"}:
        p.append("ngit_repos carries an unknown family")
    if len({(r["pubkey"], r["d"]) for r in rows}) != len(rows):
        p.append("ngit_repos (pubkey, d) is not unique")
    if any(r["repo_key"] is not None or r["is_lead"] or r["cls"] is not None
           or r["pushes_total"] for r in rows if r["family"] != "repo"):
        p.append("an excluded ngit_repos row carries repository-level values")

    # -- repositories: one lead per key, repos, maintainers
    keys = {r["repo_key"] for r in kept_rows}
    if any(r["repo_key"] is None for r in kept_rows):
        p.append("a kept ngit_repos row has no repo_key")
    if len(leads) != len(keys) or sum(1 for r in kept_rows if r["is_lead"]) != len(keys):
        p.append("not exactly one lead row per repository")
    if len(keys) != s["repos"]:
        p.append(f"distinct repo_keys {len(keys)} != repos {s['repos']}")
    if len({r["pubkey"] for r in kept_rows}) != s["maintainers"]:
        p.append("distinct kept pubkeys != maintainers")
    members = collections.Counter(r["repo_key"] for r in kept_rows)
    if sum(1 for n in members.values() if n > 1) != s["multi_maintainer_repos"]:
        p.append("multi-member repositories != multi_maintainer_repos")

    # -- hosting partitions
    hosting = {e["k"]: e["v"] for e in data["hosting"]}
    if sum(hosting.values()) != s["repos"]:
        p.append("hosting classes do not sum to repos")
    lead_cls = collections.Counter(r["cls"] for r in leads.values())
    if any(hosting.get(c, 0) != lead_cls.get(c, 0) for c in REPO_CLASSES) or set(lead_cls) - set(REPO_CLASSES):
        p.append("hosting classes != the lead rows' classes")
    if s["grasp_repos"] != hosting.get("grasp_only", 0) + hosting.get("grasp_mixed", 0):
        p.append("grasp_repos != grasp_only + grasp_mixed")
    if not 0 <= s["grasp_share"] <= 1:
        p.append("grasp_share out of [0, 1]")
    if sum(e["v"] for e in data["grasp_per_repo"]) != s["grasp_repos"]:
        p.append("grasp_per_repo does not sum to grasp_repos")
    if any(r["grasp"] != (r["cls"] in ("grasp_only", "grasp_mixed")) for r in kept_rows):
        p.append("an ngit_repos grasp flag disagrees with its hosting class")

    # -- recency and no-state partitions
    recency = sum(e["v"] for e in data["recency"])
    ns = data["no_state"]
    if recency + ns["total"] != s["repos"]:
        p.append("recency buckets + no_state.total != repos")
    if sum(e["v"] for e in ns["by_class"]) != ns["total"]:
        p.append("no_state.by_class does not sum to no_state.total")
    with_state = [r for r in leads.values() if r["last_state"] is not None]
    if len(with_state) != s["repos_with_state"] or recency != s["repos_with_state"]:
        p.append("repos_with_state != lead rows with a last_state / the recency buckets")
    ns_cls = collections.Counter(r["cls"] for r in leads.values() if r["last_state"] is None)
    if any(e["v"] != ns_cls.get(e["k"], 0) for e in ns["by_class"]):
        p.append("no_state.by_class != the lead rows without a state")
    for grp in ("default_branch", "branches_per_repo", "tags_per_repo"):
        if sum(e["v"] for e in data[grp]) != s["repos_with_state"]:
            p.append(f"{grp} does not sum to repos_with_state")
    if sum(e["v"] for e in data["relays_per_repo"]) != s["repos"]:
        p.append("relays_per_repo does not sum to repos")

    # -- the horizon (ref 1.2): every admitted timestamp lies in [PAST_FLOOR, as_of_end]
    as_of_end = (as_of - dt.date(1970, 1, 1)).days * DAY_S + DAY_S - 1
    stamps = [r[f] for r in rows for f in ("first_seen", "last_seen", "last_state", "last_push")
              if r[f] is not None] + [r["created_at"] for r in pushes]
    if stamps and (min(stamps) < PAST_FLOOR or max(stamps) > as_of_end):
        p.append("a substrate timestamp lies outside the horizon")

    # -- the monthly roster series
    first_seen = {}
    for r in kept_rows:
        k = r["repo_key"]
        first_seen[k] = min(first_seen.get(k, r["first_seen"]), r["first_seen"])
    by_month = collections.Counter(month_of(utc_date(v)) for v in first_seen.values())
    nrm = data["new_repos_monthly"]
    if any(e["v"] != e["grasp"] + e["other"] for e in nrm):
        p.append("a new_repos_monthly month has v != grasp + other")
    if sum(e["v"] for e in nrm) != s["repos"]:
        p.append("new_repos_monthly does not sum to repos")
    if any(by_month.get(e["m"], 0) != e["v"] for e in nrm):
        p.append("new_repos_monthly != the substrate's first-seen months")
    grasp_by_month = collections.Counter(month_of(utc_date(first_seen[k])) for k, r in leads.items()
                                         if r["cls"] in ("grasp_only", "grasp_mixed"))
    if any(grasp_by_month.get(e["m"], 0) != e["grasp"] for e in nrm):
        p.append("new_repos_monthly grasp != the substrate's GRASP-hosted repositories")

    # -- the weekly series each span the week holding their wall .. as_of_day's week: checked by
    # date arithmetic, not through _weeks, so a bug in the walk the series are built with fails
    def spans(rows, wall) -> bool:
        first = utc_date(wall) - dt.timedelta(days=utc_date(wall).weekday())
        last = as_of - dt.timedelta(days=as_of.weekday())
        try:
            got = [dt.date.fromisoformat(e["w"]) for e in rows]
        except (TypeError, ValueError):
            return False
        return (bool(got) and got[0] == first and got[-1] == last
                and len(got) == (last - first).days // 7 + 1
                and all((b - a).days == 7 for a, b in zip(got, got[1:])))
    nrw = data["new_repos_weekly"]
    if not spans(nrw, ANN_WALL):
        p.append("new_repos_weekly does not span the announcements wall's week .. as_of_day's week")
    if any(e["v"] != e["grasp"] + e["other"] for e in nrw):
        p.append("a new_repos_weekly week has v != grasp + other")
    by_week = collections.Counter(week_monday(utc_date(v)).isoformat() for v in first_seen.values())
    grasp_by_week = collections.Counter(week_monday(utc_date(first_seen[k])).isoformat()
                                        for k, r in leads.items() if r["cls"] in ("grasp_only", "grasp_mixed"))
    maint_first = {}
    for r in kept_rows:
        maint_first[r["pubkey"]] = min(maint_first.get(r["pubkey"], r["first_seen"]), r["first_seen"])
    maint_by_week = collections.Counter(week_monday(utc_date(v)).isoformat() for v in maint_first.values())
    if any((e["v"], e["grasp"], e["s"]) != (by_week.get(e["w"], 0), grasp_by_week.get(e["w"], 0),
                                             maint_by_week.get(e["w"], 0)) for e in nrw):
        p.append("new_repos_weekly != the substrate's first-seen weeks")
    # by_host against the lead rows' classes (the substrate has no R): GRASP is exactly the
    # GRASP-hosted, none exactly class none, and each other kind within what the classes allow
    # (hashtree is in R only for hashtree and grasp_mixed, a forge never for grasp_only or none,
    # self_hosted only for other_hosts, hashtree and grasp_mixed). The lower bounds and the two
    # identities already count every repository at least once, so the entries sum to v or more
    cls_by_week = collections.Counter((week_monday(utc_date(first_seen[k])).isoformat(), r["cls"])
                                      for k, r in leads.items())
    if any([c["k"] for c in e.get("by_host") or ()] != list(HOST_KINDS) for e in nrw):
        p.append("a new_repos_weekly week's by_host is not every host kind in order")
    else:
        for e in nrw:
            h = {c["k"]: c["v"] for c in e["by_host"]}
            n = {c: cls_by_week.get((e["w"], c), 0) for c in REPO_CLASSES}
            if h["grasp"] != e["grasp"] or h["none"] != n["none"]:
                p.append("a new_repos_weekly week's by_host grasp != grasp, or none != the substrate's class none")
                break
            if not (n["hashtree"] <= h["hashtree"] <= n["hashtree"] + n["grasp_mixed"]
                    and n["forge_only"] <= h["forge"] <= e["v"] - n["grasp_only"] - n["none"]
                    and n["other_hosts"] <= h["self_hosted"] <= n["other_hosts"] + n["hashtree"] + n["grasp_mixed"]):
                p.append("a new_repos_weekly week's by_host is outside what its hosting classes allow")
                break

    # -- the cumulative series ends at as_of_day, at repos
    cum = data["cum_repos"]
    if cum:
        if cum[-1]["d"] != as_of.isoformat() or cum[-1]["v"] != s["repos"]:
            p.append("cum_repos does not end at as_of_day with repos")
        if any(b["v"] < a["v"] for a, b in zip(cum, cum[1:])):
            p.append("cum_repos is not monotone")
        first_day = min(utc_date(v) for v in first_seen.values()) if first_seen else None
        if first_day is None or cum[0]["d"] != first_day.isoformat():
            p.append("cum_repos does not start at the earliest first_seen")
    elif s["repos"]:
        p.append("cum_repos is empty but repositories exist")

    # -- pushes: the weekly series, the windows and the substrate re-sum
    pushed = [r for r in pushes if r["push"]]
    if len(pushed) != s["pushes_total"]:
        p.append("push rows of ngit_pushes != pushes_total")
    if any(r["push"] and not r["changed"] for r in pushes):
        p.append("a ngit_pushes row is a push without changed")
    if any(r["push"] and r["created_at"] < STATE_WALL for r in pushes):
        p.append("a push before the capture wall")
    if any(e["unit"] == "pushes" for e in data["exclusions"]):
        p.append("the ledger holds a pushes row (the daily cap was dropped)")
    weekly = data["pushes_weekly"]
    if sum(e["pushes"] for e in weekly) != s["pushes_total"]:
        p.append("pushes_weekly does not sum to pushes_total")
    if weekly:
        last_w = dt.date.fromisoformat(weekly[-1]["w"])
        if weekly[0]["w"] != data["coverage"]["partial_first_week"] or last_w != week_monday(as_of):
            p.append("pushes_weekly does not span partial_first_week .. as_of_day's week")
        wk = collections.defaultdict(lambda: [0, set(), set()])
        for r in pushed:
            e = wk[week_monday(utc_date(r["created_at"])).isoformat()]
            e[0] += 1
            e[1].add(r["repo_key"])
            e[2].add(r["pubkey"])
        for e in weekly:
            want = wk.get(e["w"], [0, set(), set()])
            if (e["pushes"], e["repos"], e["maintainers"]) != (want[0], len(want[1]), len(want[2])):
                p.append(f"pushes_weekly {e['w']} != the substrate")
                break
    for n, field in ((7, "pushes_7d"), (30, "pushes_30d")):
        if sum(1 for r in pushed if in_window(utc_date(r["created_at"]), as_of, n)) != s[field]:
            p.append(f"{field} != push rows of ngit_pushes in the window")
    push_by_repo = collections.defaultdict(lambda: [0, 0, 0])
    for r in pushed:
        e = push_by_repo[r["repo_key"]]
        d = utc_date(r["created_at"])
        e[0] += 1
        e[1] += in_window(d, as_of, 30)
        e[2] += in_window(d, as_of, 90)
    for r in kept_rows:
        if [r["pushes_total"], r["pushes_30d"], r["pushes_90d"]] != push_by_repo.get(r["repo_key"], [0, 0, 0]):
            p.append(f"ngit_repos pushes for {r['repo_key'][:24]} != its push rows in ngit_pushes")
            break
    if sum(r["pushes_total"] for r in leads.values()) != s["pushes_total"]:
        p.append("lead rows' pushes_total do not sum to pushes_total")
    if any(r["repo_key"] not in leads for r in pushes):
        p.append("a ngit_pushes row names no repository")
    if len({r["event_id"] for r in pushes}) != len(pushes):
        p.append("ngit_pushes event_id is not unique")
    # -- the two sources (ref 3): a state row, or a hashtree push - a push by definition, on a
    # hashtree-class repository, past the wall, moving no ref the ledger can read
    if any(r["source"] not in (SOURCE_STATE, SOURCE_HTREE) for r in pushes):
        p.append("a ngit_pushes row has an unknown source")
    htree = [r for r in pushes if r["source"] == SOURCE_HTREE]
    if any(not (r["push"] and r["changed"]) or r["head_changed"] or r["default_moved"]
           or any(r[f] for f in ("heads_moved", "branches_added", "branches_removed", "tags_added",
                                 "tags_removed", "tags_moved"))
           or r["branches"] is not None or r["tags"] is not None for r in htree):
        p.append("a hashtree push row carries a state's flags or counts")
    if any(leads[r["repo_key"]]["cls"] != "hashtree" for r in htree if r["repo_key"] in leads):
        p.append("a hashtree push row on a repository whose class is not hashtree")
    if any(r["branches"] is None or r["tags"] is None for r in pushes if r["source"] == SOURCE_STATE):
        p.append("a state row without its branch and tag counts")
    if sum(1 for r in htree if in_window(utc_date(r["created_at"]), as_of, 30)) != s["pushes_hashtree_30d"]:
        p.append("pushes_hashtree_30d != hashtree push rows of ngit_pushes in 30 d")
    # one hashtree push per group of a coordinate's versions after its first group (ref 3):
    # fewer pushes than versions, each at least HTREE_PUSH_GAP_S after the coordinate's first
    # version and after the push before it
    coord_rows = {(r["pubkey"], r["d"]): r for r in rows}
    htree_at = collections.defaultdict(list)
    for r in htree:
        htree_at[(r["pubkey"], r["d"])].append(r["created_at"])
    for c, ts in htree_at.items():
        ts.sort()
        row = coord_rows.get(c)
        if (row is None or len(ts) > row["versions"] - 1
                or ts[0] - row["first_seen"] < HTREE_PUSH_GAP_S
                or any(b - a < HTREE_PUSH_GAP_S for a, b in zip(ts, ts[1:]))):
            p.append("a coordinate's hashtree push rows do not fit its versions (ref 3)")
            break
    # last_state is the newest STATE row (a hashtree push is no state)
    last_state = collections.defaultdict(int)
    for r in pushes:
        if r["source"] == SOURCE_STATE:
            last_state[r["repo_key"]] = max(last_state[r["repo_key"]], r["created_at"])
    if any(r["last_state"] != (last_state[k] if k in last_state else None) for k, r in leads.items()):
        p.append("ngit_repos last_state != the newest state row of ngit_pushes")
    # a repository can push without a state since 2026-10-12 (a hashtree push), so the windows
    # are bounded by the roster, and the state pushers by the repositories with a state
    if not s["repos_pushed_30d"] <= s["repos_pushed_90d"] <= s["repos"]:
        p.append("repos_pushed_30d <= repos_pushed_90d <= repos does not hold")
    if len({r["repo_key"] for r in pushed if r["source"] == SOURCE_STATE}) > s["repos_with_state"]:
        p.append("more repositories with a state push than repos_with_state")
    if s["maintainers_active_30d"] > s["maintainers"]:
        p.append("maintainers_active_30d > maintainers")
    for n, field in ((30, "repos_pushed_30d"), (90, "repos_pushed_90d")):
        if len({r["repo_key"] for r in pushed if in_window(utc_date(r["created_at"]), as_of, n)}) != s[field]:
            p.append(f"{field} != the substrate")

    # -- comments
    cc = state["counted_comments"]
    cm = data["comments_monthly"]
    roots = ("issues", "prs", "patches", "repo", "other")
    if sum(sum(e[k] for k in roots) for e in cm) != s["comments_total"] or len(cc) != s["comments_total"]:
        p.append("comments_monthly does not sum to comments_total")
    if not 0 <= s["comments_attributable_share"] <= 1:
        p.append("comments_attributable_share out of [0, 1]")
    if s["comment_participants"] > s["comments_total"] or s["comment_threads"] > s["comments_total"]:
        p.append("comment participants / threads exceed comments_total")
    if not s["comments_30d"] <= s["comments_90d"] <= s["comments_total"]:
        p.append("comments_30d <= comments_90d <= comments_total does not hold")
    cw = data["comments_weekly"]
    if not spans(cw, COMMENT_WALL):
        p.append("comments_weekly does not span the comments wall's week .. as_of_day's week")
    cw_want = collections.defaultdict(lambda: [collections.Counter(), set(), set()])
    for c in cc:
        a = cw_want[week_monday(dt.date.fromisoformat(c["date"])).isoformat()]
        a[0][c["root"]] += 1
        a[1].add(c["pubkey"])
        if c["thread"] is not None:
            a[2].add(c["thread"])
    for e in cw:
        a = cw_want.get(e["w"], [collections.Counter(), set(), set()])
        if ([e[k] for k in roots], e["authors"], e["threads"]) != ([a[0][k] for k in roots], len(a[1]), len(a[2])):
            p.append(f"comments_weekly {e['w']} != the counted comments")
            break

    # -- GRASP lists
    gl = data["grasp_lists"]
    if gl["users"] != s["grasp_list_users"] or sum(e["v"] for e in gl["length"]) != gl["users"]:
        p.append("grasp_lists.users / length != grasp_list_users")
    if sum(e["v"] for e in gl["per_month"]) != gl["users"]:
        p.append("grasp_lists.per_month does not sum to grasp_list_users")
    gfs = state["grasp_first_seen"]
    if len(gfs) != gl["users"]:
        p.append("grasp_first_seen != grasp_list_users")
    if not spans(gl["per_week"], STATE_WALL):
        p.append("grasp_lists.per_week does not span the states wall's week .. as_of_day's week")
    gl_week = collections.Counter(week_monday(utc_date(v)).isoformat() for v in gfs)
    if any(gl_week.get(e["w"], 0) != e["v"] for e in gl["per_week"]):
        p.append("grasp_lists.per_week != the users' first-seen weeks")

    # -- social
    if s["stars_repos"] > s["repos"] or s["zaps_repos"] > s["repos"] or s["stars_repos"] > s["stars_total"]:
        p.append("stars / zaps repositories out of range")
    # ref 6.2: the Iris Git pairs are a subset of all pairs, and need an input to come from
    if s["stars_iris"] > s["stars_total"]:
        p.append("stars_iris > stars_total")
    if s["stars_iris"] and not s["archive_events"]["external_reactions"]:
        p.append("stars_iris without a kind-17 (external-content-reactions) row read")
    if s["zappers"] > s["zap_receipts"] or s["zap_msats"] > s["zap_receipts"] * ZAP_CAP_MSATS:
        p.append("zappers / zap_msats out of range")
    for f in ("top1_share", "top10_share"):
        if not 0 <= s["concentration"][f] <= 1:
            p.append(f"concentration.{f} out of [0, 1]")

    # -- every series sorted, unique, and never past as_of_day
    for name, pts, field, bound in (
        ("new_repos_monthly", nrm, "m", as_of_month),
        ("new_repos_weekly", nrw, "w", as_of.isoformat()),
        ("cum_repos", cum, "d", as_of.isoformat()),
        ("pushes_weekly", weekly, "w", as_of.isoformat()),
        ("comments_monthly", cm, "m", as_of_month),
        ("comments_weekly", cw, "w", as_of.isoformat()),
        ("grasp_lists.per_month", gl["per_month"], "m", as_of_month),
        ("grasp_lists.per_week", gl["per_week"], "w", as_of.isoformat()),
    ):
        vals = [e[field] for e in pts]
        if vals != sorted(vals) or len(vals) != len(set(vals)):
            p.append(f"{name} is not sorted / unique")
        if vals and vals[-1] > bound:
            p.append(f"{name} has a point after as_of_day")
    for name in ("newest_state", "newest_push", "first_announcement"):
        v = s[name]
        if v is not None and v > data["as_of_day"]:
            p.append(f"{name} is after as_of_day")

    # -- every list within its cap
    for name, cap in (("clone_hosts", TOP_CLONE_HOSTS), ("grasp_servers", TOP_GRASP_SERVERS),
                      ("relays", TOP_RELAYS), ("web_hosts", TOP_WEB_HOSTS), ("topics", TOP_TOPICS),
                      ("repos_active", TOP_REPO_ROWS), ("repos_recent_pushes", TOP_REPO_ROWS),
                      ("repos_new", TOP_REPO_ROWS), ("fork_families_top", TOP_FORK_FAMILIES),
                      ("maintainers_top", TOP_MAINTAINERS), ("repos_discussed", TOP_DISCUSSED),
                      ("repos_starred", TOP_STARRED), ("repos_zapped", TOP_ZAPPED),
                      ("tools", TOOL_TOP_CLIENTS + 7)):
        if len(data[name]) > cap:
            p.append(f"{name} exceeds its cap of {cap}")
    if len(gl["first_choice"]) > TOP_FIRST_CHOICE:
        p.append("grasp_lists.first_choice exceeds its cap")
    # -- the per-repository figures (ref 9.2), which the page lists Stars and Zaps from: one row
    # per kept repository on its lead's coordinate and class, figures that add up to the
    # summary's, and the document's two lists exactly the table's top under each ranking (the
    # page's own query ranks the same way, the row's repo_key breaking a full tie)
    st = state["stats_rows"]
    if sorted(r["repo_key"] for r in st) != sorted(leads):
        p.append("ngit_repo_stats is not one row per kept repository")
    elif any((r["pubkey"], r["d"], r["cls"]) != (leads[r["repo_key"]]["pubkey"], leads[r["repo_key"]]["d"],
                                                 leads[r["repo_key"]]["cls"]) for r in st):
        p.append("an ngit_repo_stats row is not on its repository's lead coordinate and class")
    # ... and its display name: the lead's (as ngit_repos holds it), else its d - what the page
    # and the coming MCP domain show for every row, not only the top 20s
    elif any(r["name"] != (leads[r["repo_key"]]["name"] or _clip(clean_text(leads[r["repo_key"]]["d"]), NAME_MAX))
             for r in st):
        p.append("an ngit_repo_stats name is not its repository's display name")
    if any(min(r["stars"], r["zap_receipts"], r["zap_msats"]) < 0 for r in st):
        p.append("an ngit_repo_stats figure is negative")
    if (sum(r["stars"] for r in st), sum(1 for r in st if r["stars"])) != (s["stars_total"], s["stars_repos"]):
        p.append("ngit_repo_stats stars do not add up to stars_total and stars_repos")
    if ((sum(r["zap_receipts"] for r in st), sum(r["zap_msats"] for r in st),
         sum(1 for r in st if r["zap_receipts"])) != (s["zap_receipts"], s["zap_msats"], s["zaps_repos"])):
        p.append("ngit_repo_stats zaps do not add up to zap_receipts, zap_msats and zaps_repos")
    ref_of = lambda r: {"pubkey": r["pubkey"], "d": r["d"], "name": r["name"], "cls": r["cls"]}
    for name, cap, want in (
            ("repos_starred", TOP_STARRED,
             [{**ref_of(r), "stars": r["stars"]}
              for r in sorted((r for r in st if r["stars"]), key=lambda r: (-r["stars"], r["repo_key"]))]),
            ("repos_zapped", TOP_ZAPPED,
             [{**ref_of(r), "receipts": r["zap_receipts"], "msats": r["zap_msats"]}
              for r in sorted((r for r in st if r["zap_receipts"]),
                              key=lambda r: (-r["zap_receipts"], -r["zap_msats"], r["repo_key"]))])):
        if data[name] != want[:cap]:
            p.append(f"{name} is not the top {cap} of ngit_repo_stats")
    # ... and the stars and zaps lists each their population's top, in rank order (ref 6.2,
    # 6.3; zaps by receipts, then msats since the operator's ask of 2026-10-18). The key that
    # breaks a full tie is not in the document, so the figures are checked, not the key.
    for name, cap, total, rank in (
            ("repos_starred", TOP_STARRED, s["stars_repos"], lambda r: (r["stars"],)),
            ("repos_zapped", TOP_ZAPPED, s["zaps_repos"], lambda r: (r["receipts"], r["msats"]))):
        if len(data[name]) != min(cap, total):
            p.append(f"{name} lists {len(data[name])} rows, not the top {min(cap, total)}")
        if any(rank(a) < rank(b) for a, b in zip(data[name], data[name][1:])):
            p.append(f"{name} is out of rank order")
    for name in ("repos_active", "repos_recent_pushes", "repos_new"):
        if any(len(r["hosts"]) > REPO_ROW_HOSTS for r in data[name]):
            p.append(f"{name} carries more than {REPO_ROW_HOSTS} hosts in a row")
    if any(len(f["names"]) > FORK_FAMILY_NAMES for f in data["fork_families_top"]):
        p.append("fork_families_top carries more than 5 names in a row")
    if any(len(r["hosts"]) > SUBSTRATE_LIST_MAX or len(r["relays"]) > SUBSTRATE_LIST_MAX for r in rows):
        p.append("an ngit_repos hosts / relays list exceeds 20")

    # -- every top-list row names a repository on the roster
    lead_pd = {(r["pubkey"], r["d"]) for r in leads.values()}
    for name in ("repos_active", "repos_recent_pushes", "repos_new", "repos_discussed",
                 "repos_starred", "repos_zapped"):
        if any((r["pubkey"], r["d"]) not in lead_pd for r in data[name]):
            p.append(f"{name} names a repository that is not on the roster")
    # ... and carries that repository's hosting class, which picks the page's link
    lead_cls = {(r["pubkey"], r["d"]): r["cls"] for r in leads.values()}
    for name in ("repos_active", "repos_recent_pushes", "repos_new", "repos_discussed",
                 "repos_starred", "repos_zapped"):
        if any(r.get("cls") != lead_cls.get((r["pubkey"], r["d"])) for r in data[name]):
            p.append(f"{name} carries a hosting class that is not its repository's")
    # ... and the repository rows their kinds of host: known kinds in the HOST_KINDS order, no
    # repeats, ["none"] alone or reachable kinds only, giving exactly that class
    # (repo_hosting_class over R). A malformed list and a wrong class are told apart, so a
    # failed run points at the list or at the class logic.
    for name in ("repos_active", "repos_recent_pushes", "repos_new"):
        for r in data[name]:
            k = r.get("host_kinds")
            if (not isinstance(k, list) or not k or k != [x for x in HOST_KINDS if x in k]
                    or ("none" in k and k != ["none"])):
                p.append(f"{name} carries host_kinds that are empty, unknown, repeated, out of "
                         "the HOST_KINDS order, or none beside a kind")
                break
            if repo_hosting_class(frozenset(k) - {"none"}) != r.get("cls"):
                p.append(f"{name} carries host_kinds that do not give its hosting class")
                break
    member_pks = {r["pubkey"] for r in kept_rows}
    if any(r["pubkey"] not in member_pks for r in data["maintainers_top"]):
        p.append("maintainers_top names a pubkey that maintains no kept repository")
    # fork families group by the REPOSITORY euc (ref 2.3): the lead's, else the first member's
    # by pubkey that has one - ngit_repos.euc is each COORDINATE's own
    by_repo = collections.defaultdict(list)
    for r in kept_rows:
        by_repo[r["repo_key"]].append(r)
    repo_euc = {}
    for k, members in by_repo.items():
        lead_euc = leads[k]["euc"] if k in leads else None
        repo_euc[k] = lead_euc if lead_euc is not None else next(
            (r["euc"] for r in sorted(members, key=lambda r: r["pubkey"]) if r["euc"] is not None), None)
    eucs = collections.Counter(e for e in repo_euc.values() if e is not None)
    if sum(1 for n in eucs.values() if n > 1) != s["fork_families"]:
        p.append("fork_families != the euc values shared by more than one repository")
    if any(eucs.get(f["euc"], 0) != f["repos"] or f["repos"] < 2 for f in data["fork_families_top"]):
        p.append("fork_families_top names a family that does not exist in the substrate")
    return p


def roster_growth(state, data) -> dict:
    """The non-blocking roster_growth_sane check: kept coordinates first seen on as_of_day
    against max(20, 5 x the median daily count over the 30 days before it). A coordinate
    flood lands as repositories and never trips a version-based rule."""
    as_of = dt.date.fromisoformat(data["as_of_day"])
    per_day = collections.Counter(utc_date(r["first_seen"]) for r in state["repos_rows"]
                                  if r["family"] == "repo")
    today = per_day.get(as_of, 0)
    window = sorted(per_day.get(as_of - dt.timedelta(days=i), 0)
                    for i in range(1, ROSTER_GROWTH_DAYS + 1))
    n = len(window)
    mid2 = window[n // 2 - 1] + window[n // 2] if n % 2 == 0 else 2 * window[n // 2]
    limit2 = max(2 * ROSTER_GROWTH_MIN, ROSTER_GROWTH_FACTOR * mid2)   # twice the limit
    return {"first_seen_on_as_of_day": today, "median_30d": mid2 / 2, "limit": limit2 / 2,
            "passed": 2 * today <= limit2}


def held_key_repos(state) -> dict:
    """The blocking held_keys_absent check: kept announcement coordinates per key of
    HELD_KEYS, lead or not (a held key's coordinate that joins another key's repository adds a
    maintainer and can move its class), empty when there is none."""
    return dict(sorted(collections.Counter(
        r["pubkey"] for r in state["repos_rows"]
        if r["family"] == "repo" and r["pubkey"] in HELD_KEYS).items()))


def require_no_held_keys(state) -> None:
    """Raise when a held key has a kept announcement coordinate. ngit_snapshot calls it before
    write_pg, so nothing is written and the served row stays."""
    held = held_key_repos(state)
    if held:
        raise RuntimeError(
            "held keys have kept announcement coordinates (nothing written): "
            + ", ".join(f"{k[:8]} {n}" for k, n in held.items())
            + " - their announcements must stay excluded (pipeline/ngit.py HELD_KEYS)")


def push_concentration(state, data) -> dict:
    """The non-blocking push_concentration_sane check: the busiest repository's share of the
    `push` rows in the 7 days ending on as_of_day. It fails above PUSH_CONCENTRATION_MAX_SHARE
    once those pushes number PUSH_CONCENTRATION_MIN_PUSHES - the condition under which a
    per-repository cap would be worth reintroducing. Integer comparison, no float."""
    as_of = dt.date.fromisoformat(data["as_of_day"])
    per_repo = collections.Counter(r["repo_key"] for r in state["push_rows"]
                                   if r["push"] and in_window(utc_date(r["created_at"]), as_of, 7))
    total = sum(per_repo.values())
    top_key, top = min(per_repo.items(), key=lambda kv: (-kv[1], kv[0])) if per_repo else (None, 0)
    # top / total > 1/2, without a division
    over = total >= PUSH_CONCENTRATION_MIN_PUSHES and 2 * top > total
    return {"top_repo": top_key, "top_pushes": top, "pushes_7d": total,
            "share": top / total if total else 0.0, "passed": not over}


# --- persistence ----------------------------------------------------------------------------

def strip_nul(v):
    """Strip NUL bytes from strings (recursing into containers, keys included) - Postgres
    TEXT/JSONB reject the NUL codepoint."""
    if isinstance(v, str):
        return v.replace(chr(0), "")
    if isinstance(v, list):
        return [strip_nul(x) for x in v]
    if isinstance(v, dict):
        return {strip_nul(k): strip_nul(x) for k, x in v.items()}
    return v


REPO_COLUMNS = (
    "pubkey", "d", "family", "repo_key", "is_lead", "name", "first_seen", "last_seen",
    "versions", "cls", "grasp", "hosts", "relays", "euc", "tool", "has_state", "last_state",
    "last_push", "pushes_total", "pushes_30d", "pushes_90d",
)
PUSH_COLUMNS = ("event_id", "pubkey", "d", "repo_key", "created_at") + PUSH_FLAG_COLUMNS + ("source",)
STATS_COLUMNS = ("repo_key", "run_id", "pubkey", "d", "name", "cls", "stars", "zap_receipts", "zap_msats")

_UNSET = object()


_ISO_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def parse_override(value, log=None):
    """SHRINK_OVERRIDE_UNTIL, parsed STRICTLY: a "YYYY-MM-DD" string -> that date; None or ""
    -> None (no override); anything else -> None with a logged warning, so a malformed value
    keeps the gate ON rather than bypassing it indefinitely."""
    if value is None or value == "":
        return None
    try:
        if not (isinstance(value, str) and _ISO_DATE_RE.fullmatch(value)):
            raise ValueError("not YYYY-MM-DD")
        return dt.date.fromisoformat(value)
    except ValueError:
        (log or (lambda _m: None))(
            f"[ngit] SHRINK_OVERRIDE_UNTIL={value!r} is not a YYYY-MM-DD date - ignored, the "
            "anti-shrink gate stays on")
        return None


def shrink_gate(prev_summary, data, *, override_until=_UNSET,
                min_kept_first_run: int = None, min_pushes_first_run: int = None, log=None):
    """The anti-shrink decision, pure -> (problems, bypassed). `prev_summary` is the newest
    served row's summary (None when no row exists yet)."""
    if override_until is _UNSET:
        override_until = SHRINK_OVERRIDE_UNTIL
    if min_kept_first_run is None:
        min_kept_first_run = MIN_ANNOUNCEMENTS_KEPT_FIRST_RUN
    if min_pushes_first_run is None:
        min_pushes_first_run = MIN_PUSHES_TOTAL_FIRST_RUN
    s = data["summary"]
    problems = []
    if prev_summary:
        for field in ("announcements_kept", "pushes_total"):
            prev = int(prev_summary.get(field) or 0)
            if s[field] * SHRINK_KEEP_DEN < prev * SHRINK_KEEP_NUM:
                problems.append(f"{field} {s[field]} < 90% of the served row's {prev}")
        prev_ae = prev_summary.get("archive_events") or {}
        for field in ARCHIVE_EVENT_FIELDS:
            prev = int(prev_ae.get(field) or 0)
            new = s["archive_events"][field]
            if new * SHRINK_KEEP_DEN < prev * SHRINK_KEEP_NUM:
                problems.append(f"archive_events.{field} {new} < 90% of the served row's {prev}")
    else:
        if s["announcements_kept"] < min_kept_first_run:
            problems.append(f"announcements_kept {s['announcements_kept']} < first-run floor {min_kept_first_run}")
        if s["pushes_total"] < min_pushes_first_run:
            problems.append(f"pushes_total {s['pushes_total']} < first-run floor {min_pushes_first_run}")
    until = parse_override(override_until, log) if problems else None
    bypassed = (bool(problems) and until is not None
                and dt.date.fromisoformat(data["generated_utc"][:10]) <= until)
    return problems, bypassed


def write_pg(conn, state, data, run_id: str, *, override_until=_UNSET,
             min_kept_first_run: int = None, min_pushes_first_run: int = None, log=None) -> dict:
    """Persist one refresh: read the newest served summary, run the anti-shrink gates, then
    ONE transaction - TRUNCATE+INSERT ngit_repos and ngit_pushes, DELETE+INSERT ngit_repo_stats
    (since 2026-10-18), INSERT one ngit_snapshot row. A refusal raises before any
    write, so the previous serving data stays live."""
    log = log or (lambda _m: None)
    with conn.cursor() as cur:
        cur.execute("SELECT data->'summary' FROM ngit_snapshot "
                    "ORDER BY generated_utc DESC, id DESC LIMIT 1")
        row = cur.fetchone()
    prev = row[0] if row else None
    if isinstance(prev, str):
        prev = json.loads(prev)
    problems, bypassed = shrink_gate(prev, data, override_until=override_until,
                                     min_kept_first_run=min_kept_first_run,
                                     min_pushes_first_run=min_pushes_first_run, log=log)
    if problems and not bypassed:
        raise RuntimeError(
            "anti-shrink: " + "; ".join(problems) + " - refusing to write (set "
            "SHRINK_OVERRIDE_UNTIL in the same commit if the shrink is a deliberate "
            "methodology change)")
    if problems:
        log("[ngit] anti-shrink BYPASSED by SHRINK_OVERRIDE_UNTIL: " + "; ".join(problems))

    from psycopg2.extras import Json, execute_values

    rrows = []
    for r in state["repos_rows"]:
        r = strip_nul(r)
        rrows.append(tuple(Json(r[c]) if c in ("hosts", "relays") else r[c] for c in REPO_COLUMNS))
    prows = [tuple(strip_nul(r[c]) for c in PUSH_COLUMNS) for r in state["push_rows"]]
    srows = [tuple(run_id if c == "run_id" else r[c] for c in STATS_COLUMNS)
             for r in map(strip_nul, state["stats_rows"])]
    generated = dt.datetime.strptime(data["generated_utc"], "%Y-%m-%d %H:%M:%SZ").replace(
        tzinfo=dt.timezone.utc)
    with conn.cursor() as cur:
        # both locks at once: a reader joining the two tables can never deadlock against us
        cur.execute("TRUNCATE ngit_repos, ngit_pushes")
        # The daily cap's `counted` column went on 2026-10-02 (ref 3). It is dropped here, under
        # the lock the TRUNCATE already holds, never in schema/init.sql: every job applies that
        # file in one transaction, and an ALTER there would queue behind any reader of
        # ngit_pushes while holding the locks the file's earlier ALTERs took on other surfaces'
        # tables (data review, 2026-10-02). A no-op once dropped.
        cur.execute("ALTER TABLE ngit_pushes DROP COLUMN IF EXISTS counted")
        # Since 2026-10-12 the ledger also holds hashtree pushes (ref 3): `source` names a row's
        # kind, and such a row has no branch or tag count. Added here for the same reason, under
        # the same lock, on a table the TRUNCATE has just emptied. DEFAULT 'state' keeps a
        # rollback working: the pipeline before this change inserts no `source` and writes state
        # rows only (this code always names it). A no-op once applied.
        cur.execute("ALTER TABLE ngit_pushes ADD COLUMN IF NOT EXISTS source TEXT NOT NULL "
                    "DEFAULT 'state' CHECK (source IN ('state', 'htree_announcement'))")
        cur.execute("ALTER TABLE ngit_pushes ALTER COLUMN branches DROP NOT NULL, "
                    "ALTER COLUMN tags DROP NOT NULL")
        execute_values(cur, f"INSERT INTO ngit_repos ({', '.join(REPO_COLUMNS)}) VALUES %s",
                       rrows, page_size=1000)
        execute_values(cur, f"INSERT INTO ngit_pushes ({', '.join(PUSH_COLUMNS)}) VALUES %s",
                       prows, page_size=2000)
        # The per-repository figures (ref 9.2), which the page reads: emptied by DELETE, not in
        # the TRUNCATE above, so a page request during this transaction reads the previous
        # run's rows instead of waiting on an exclusive lock. It reads them only beside the
        # ngit_snapshot row of their run_id, so it never mixes two runs.
        cur.execute("DELETE FROM ngit_repo_stats")
        execute_values(cur, f"INSERT INTO ngit_repo_stats ({', '.join(STATS_COLUMNS)}) VALUES %s",
                       srows, page_size=1000)
        cur.execute("INSERT INTO ngit_snapshot (generated_utc, run_id, data) VALUES (%s, %s, %s)",
                    (generated, run_id, Json(strip_nul(data))))
    conn.commit()
    return {"repo_rows": len(rrows), "push_rows": len(prows), "stats_rows": len(srows),
            "shrink_bypassed": bool(problems)}

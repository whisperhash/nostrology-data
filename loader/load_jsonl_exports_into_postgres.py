#!/usr/bin/env python3
"""
Load Nostr events from JSONL exports into Postgres tables.

Target tables (defined in schema/init.sql):
  - events_kind_0_profiles:             FTS on name+display_name+about
  - events_kind_9321_nutzaps:           indexed by (mint_url, created_at) + recipient
  - events_kind_9735_zaps:              indexed by target/sender/recipient
  - events_kind_9735_zaps_zapstore:     derived sub-load (--into zapstore)
  - events_kind_10019_nutzap_configs:   mint_urls TEXT[] GIN-indexed
  - events_kind_10002_relay_lists:      relay_urls / read_relay_urls / write_relay_urls TEXT[] GIN-indexed
  - events_kind_30063_app_releases:     Zapstore app releases
  - events_kind_30267_app_stacks:       Zapstore curator app stacks
  - events_kind_32267_app_catalog:      indexed by (pubkey, d_tag) for Zapstore catalog
  - relay_stats_mv:                     materialized view, /relays leaderboard
  - relay_per_user_distribution_mv:     materialized view, per-user histogram
  - relay_tier_user_counts_mv:          materialized view, per-tier histogram

The loader still knows six more tables, which schema/init.sql does not define and no job
loads: events_kind_1_posts, events_kind_6_reposts, events_kind_7_reactions,
events_kind_7376_nutzap_redemptions, follows and follower_counts.

These tables should be queried instead of scanning JSONL files for pubkey and
profile lookups.

Supports kinds 0, 1, 3, 6, 7, 7376, 9321, 9735, 10002, 10019, 30063, 30267, 32267.
Each kind has its own table with kind-specific columns. Multiple files load in one
pass; the event-id-keyed kinds load via a raw COPY (drop ALL indexes incl. the PK ->
COPY -> rebuild; the upstream Athena extract already globally dedups by id, so no
per-row conflict check is needed), while kind-3 follows keeps INSERT ... ON CONFLICT
DO NOTHING (it has no event_id key — composite edge uniqueness, no upstream dedup).

Usage:
    python3 load_jsonl_exports_into_postgres.py \
        --input part-0.gz part-1.gz \
        --kind 10002 \
        --db-url postgresql://nostr_brain:changeme@localhost:5432/nostr_brain \
        [--truncate]
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import NamedTuple

import psycopg2

# Sibling module; the loader runs as a script from loader/, which puts this directory on
# sys.path (the tests insert it themselves).
import nutzap_mints


BATCH_SIZE = 50_000
# Bound the per-batch in-memory buffer. COPY streams one StringIO buffer per batch
# (text escaping grows a field only modestly — \\, \t, \n, \r doublings); cap the
# accumulated payload at 256 MB so a pathological wide-content batch can't balloon
# RAM on the memory-starved box. Batches flush when EITHER row count OR byte cap is
# hit. (Only kind-1 tracks bytes — its content column is the one unbounded field;
# the other kinds' rows are small and bounded by BATCH_SIZE alone, as before.)
BATCH_MAX_BYTES = 256 * 1024 * 1024  # 256 MB

# Top-level ["a","32267:<dev_pubkey>:<app_d_tag>"] tag marking a kind-9735
# zap receipt as attributed to a Zapstore app entry. Used by
# load_kind_9735_zaps_zapstore() to filter the kind-9735 stream into the
# events_kind_9735_zaps_zapstore derived table.
_ZAPSTORE_A_TAG_REGEX = re.compile(r"^32267:([0-9a-f]{64}):(.+)$")

# Kind-32267 publisher-emitted tag taxonomy. Closed set per the upstream
# zapstore/zsp Go publisher (BuildAppMetadataEvent in internal/nostr/events.go);
# the relay's legacy parser accepts the same names. Anything outside these two
# sets is collected into the JSONB `extra_tags` catch-all column. See
# docs/plans (cozy-wigderson) for the taxonomy investigation.
_K32267_SINGLETON_TAGS = {"name", "summary", "icon", "url", "repository", "license", "a"}
_K32267_MULTI_TAGS = {"image", "t", "f", "h"}
# Map raw tag name → Postgres column. `a` is renamed to `a_tag` to avoid the
# overloaded bare `a` SQL identifier; the rest pass through unchanged.
_K32267_SINGLETON_COLUMN = {
    "name": "name",
    "summary": "summary",
    "icon": "icon",
    "url": "url",
    "repository": "repository",
    "license": "license",
    "a": "a_tag",
}
_K32267_MULTI_COLUMN = {
    "image": "images",
    "t": "topics",
    "f": "platforms",
    "h": "h_tags",
}


def _open_text(path):
    """Open a JSONL input for reading, transparently decompressing gzip by suffix.

    Athena `UNLOAD … format='JSON'` emits gzip-compressed NDJSON. Reading the .gz
    directly (streaming gunzip in-process, line by line) avoids materializing the
    ~6x-larger decompressed copy on disk — the per-line load loop is byte-identical
    either way. A plain (uncompressed) .jsonl input still works unchanged, so this
    is a safe drop-in for every `open(path, ...)` input read.
    """
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def _parse_kind_32267_tags(tags):
    """Parse a kind-32267 event's tags into (d_tag, singletons, multis, extras).

    Pure function (no DB). Walks the tag list once: the d-tag, the singleton-typed
    columns, and the multi-valued columns are bucketed per the publisher taxonomy
    above; everything else is preserved verbatim in `extras` for the JSONB catch-all.

    Web-app/PWA publishers (tagged `client=localhost`) describe an app with the
    NIP-generic `title`/`description` tags instead of Zapstore's `name`/`summary`,
    so without a fallback the typed columns are empty and the app renders as its
    d-tag slug on /zapstore. We fall back name<-title and summary<-description when
    the Zapstore tag is absent; the raw `title`/`description` tags still remain in
    `extras` for provenance. (We do NOT fall back icon<-image: an `image` tag is a
    screenshot, not an icon — matching the upstream zapstore/webapp App model — so
    icon-less apps stay null and the frontend renders a letter-tile placeholder.)
    Returns d_tag=None when no `d` tag is present.
    """
    d_tag = None
    singletons = {col: None for col in _K32267_SINGLETON_COLUMN.values()}
    multis = {col: [] for col in _K32267_MULTI_COLUMN.values()}
    extras = []
    for tag in tags or []:
        if not (isinstance(tag, list) and len(tag) >= 2):
            continue
        k = tag[0]
        v = tag[1]
        if k == "d":
            if d_tag is None and isinstance(v, str):
                d_tag = v
        elif k in _K32267_SINGLETON_TAGS:
            col = _K32267_SINGLETON_COLUMN[k]
            if singletons[col] is None and isinstance(v, str):
                singletons[col] = v
        elif k in _K32267_MULTI_TAGS:
            col = _K32267_MULTI_COLUMN[k]
            if isinstance(v, str):
                multis[col].append(v)
        else:
            extras.append(tag)

    # name<-title / summary<-description fallback for non-Zapstore-CLI publishers.
    if singletons["name"] is None or singletons["summary"] is None:
        for tag in extras:
            if len(tag) < 2 or not isinstance(tag[1], str):
                continue
            if tag[0] == "title" and singletons["name"] is None:
                singletons["name"] = tag[1]
            elif tag[0] == "description" and singletons["summary"] is None:
                singletons["summary"] = tag[1]

    return d_tag, singletons, multis, extras


# Zapstore's own definition of an app, lifted from `zapstore/relay`
# pkg/events/app.go (commit 3812bf5): `PlatformIdentifiers` L15-30 is the closed
# set of `f` values the relay recognises, and `Validate()` L51-62 requires a
# non-empty `d`, a non-empty `name`, and at least one recognised platform. The
# relay drops an unrecognised `f` silently during parse, so a listing whose only
# platform is `macos-x86_64` has, to Zapstore, no platform at all.
#
# Copied, not imported - so it can drift. `test_zapstore_conformance.py` states
# the provenance and the drift check: an `f` value in our data that is in neither
# this list nor NIP-82 (software applications) Appendix A is a signal that
# Zapstore's list has moved. Platform validation entered the relay on 2026-01-19
# (3dfc063); the relay's `legacy/app.go` parser had a looser 12-value list and no
# platform requirement, so a pre-2026 listing may have been admitted under rules
# this test now fails. That is why `no_platform` records the date too.
ZAPSTORE_PLATFORMS = frozenset({
    "android-arm64-v8a", "android-armeabi-v7a", "android-x86", "android-x86_64",
    "darwin-arm64", "darwin-x86_64",
    "linux-aarch64", "linux-x86_64", "linux-armv7l", "linux-riscv64",
    "windows-aarch64", "windows-x86_64",
    "ios-arm64", "web",
})

# Hosts whose `client` tag marks a web applet rather than an installable app.
# These are LABELS, not the gate - see classify_kind_32267 and the schema comment.
_NAPPLET_CLIENTS = frozenset({"napplet.run"})

# A napplet manifest coordinate: kind-35129 (NIP-5D named-napplet manifest) or
# kind-15129 (NIP-5D root napplet), any signer. The shape the operator decided on
# 2026-10-09. Anchored and hex-checked so a tag value that merely starts with the
# digits cannot be mistaken for one.
_NAPPLET_MANIFEST_RE = re.compile(r"^(?:35129|15129):[0-9a-f]{64}:")

# The App Store pointers of the iOS-only publisher. Not a rule - a tripwire, see the
# end of load_kind_32267_app_catalog.
_APPSTORE_D_TAG_RE = re.compile(r"^appstore\.[0-9]+$")


def classify_kind_32267(d_tag, singletons, multis, extras):
    """Return None when this listing is a Zapstore app, else the label saying why not.

    The decision is Zapstore's own `Validate()`: a `d`, a `name`, and at least one
    recognised platform. The label that comes back is diagnostic only - every
    branch below returns a non-None value for exactly the rows `Validate()` would
    reject, so re-ordering the labels can never change WHICH rows are excluded,
    only what they are called. That property is what `test_zapstore_conformance.py`
    pins, and it is why a napplet publisher changing its marker (soyLI's `latest`
    tag vs napplet.run's `client` tag) cannot let a napplet back into the catalog.

    Pure function: no DB, no I/O. `extras` is the catch-all from
    _parse_kind_32267_tags, every element a list of length >= 2.

    Two known laxnesses, both one-directional (they can admit a row the relay would
    refuse, never exclude one it accepts):
      - `singletons["name"]` has already had the name<-title fallback applied, so a
        listing carrying `title` but no `name` tag passes the name check here
        although the relay would reject it. Tightening it means classifying before
        the fallback.
      - the relay's ParseApp errors out on a duplicate `d`/`name`/`summary`/`icon`/
        `repository`/`license` tag, so such an event never reaches Validate() at all;
        _parse_kind_32267_tags keeps the first of each instead.
    Neither matters today - every listing we hold that either one affects is
    platform-less and excluded anyway - and the walk gate in
    test_zapstore_conformance.py checks the relay's catalogue against BOTH this
    function and a strict ParseApp+Validate() reimplementation, so the claim that
    nothing the relay serves fails our rule does not rest on the laxness.
    """
    if not d_tag:
        # Defensive only: the caller skips an event with no d-tag before it gets here,
        # so this label never reaches the `excl` column.
        return "no_d_tag"
    if not (singletons.get("name") or "").strip():
        return "no_name"
    if any(p in ZAPSTORE_PLATFORMS for p in multis.get("platforms") or ()):
        return None

    # No platform Zapstore recognises, so the listing is out either way. Everything
    # below only NAMES the population, taking the first tag that identifies one - the
    # publisher's tag order decides, not a precedence of ours, because no listing we
    # hold matches two of these and a tie would be arbitrary anyway.
    for tag in extras:
        if not isinstance(tag[1], str):
            continue
        if tag[0] == "client":
            if tag[1] in _NAPPLET_CLIENTS:
                return "napplet_client"
            if tag[1] == "localhost":
                return "localhost_web_app"
        elif _NAPPLET_MANIFEST_RE.match(tag[1]):
            # soyLI's `latest` tag, and any other tag naming a napplet manifest.
            # Matched on the VALUE, not the tag name, because the tag name is the
            # part soyLI is free to rename; matched for any signer, because a
            # listing may name someone else's napplet.
            return "napplet_manifest"
        elif tag[0] == "type" and tag[1] == "business":
            return "business_directory"
    return "no_platform"


# --- OUR policy layer, on top of Zapstore's rule ----------------------------
#
# Everything above is a copy of Zapstore's `Validate()`. This is not: it is a
# deliberate narrowing the operator decided on 2026-10-28, and it must stay
# separate so the claim "nothing the relay serves fails our rule" keeps meaning
# what it says. The walk gate checks the policy against the live store too, so a
# future value added here that the relay DOES serve fails the test rather than
# quietly dropping real apps.
#
# `ios-arm64` is in Zapstore's platform list, but Zapstore does not distribute iOS
# software: of the 4,915 listings relay.zapstore.dev served on 2026-10-28, ZERO
# carry `ios-arm64` (or `web`), and the client repo (zapstore/zapstore, Flutter) has
# an `android/` build directory and no `ios/`. The 56 iOS-only listings we hold are
# `appstore.<numeric id>` pointers to Apple's App Store from a single publisher,
# off-relay, with no releases and no zaps - App Store entries catalogued on Nostr,
# not apps Zapstore serves. They were rank 1 on the Publishers leaderboard.
#
# `web` is deliberately NOT here although the relay serves zero of those either: the
# operator decided on 2026-10-28 that a web app meeting Zapstore's definition counts
# in. That asymmetry is a judgement about what the two platforms mean, not an
# oversight - a `web` listing names something a user can actually open, an
# `ios-arm64` listing in an Android-only store names something it cannot install.
# We hold exactly 1 web-only listing today.
ZAPSTORE_POLICY_EXCLUDED_PLATFORMS = frozenset({"ios-arm64"})


def zapstore_policy_exclusion(multis):
    """Return a label when OUR policy rejects a listing Zapstore's rule accepts.

    Runs only after classify_kind_32267 has returned None, i.e. the listing is a
    Zapstore app by Zapstore's definition. Keyed on the listing's platforms, never
    on its publisher: one publisher owns all 56 of today's rows, but a per-pubkey
    rule would miss the next one and would wrongly take that publisher's other apps
    if they ever shipped an Android build.
    """
    recognised = {p for p in multis.get("platforms") or () if p in ZAPSTORE_PLATFORMS}
    if recognised and recognised <= ZAPSTORE_POLICY_EXCLUDED_PLATFORMS:
        return "ios_only"
    return None


# Kind-30063 Zapstore app releases — same closed-set + JSONB catch-all pattern
# as kind-32267. d_tag is '<package>@<version>'; the loader falls back to
# splitting on the last '@' when the bare 'i'/'version' tags are absent
# (~48% of corpus events). Tags 'alt' and 'release_notes' are sub-rare (<3
# events in 10K) but kept typed for parity with kind-32267's schema.
_K30063_SINGLETON_TAGS = {"i", "version", "c", "a", "url", "r", "commit", "alt", "release_notes"}
_K30063_MULTI_TAGS = {"e", "f"}
_K30063_SINGLETON_COLUMN = {
    "i": "app_d_tag",
    "version": "version",
    "c": "channel",
    "a": "a_tag",
    "url": "url",
    "r": "repo_url",
    "commit": "commit_sha",
    "alt": "alt",
    "release_notes": "release_notes",
}
_K30063_MULTI_COLUMN = {
    "e": "e_refs",
    "f": "platforms",
}

# Kind-30267 Zapstore "app stacks". `name`/`description`/`title` are typed
# singletons; `f`/`h` typed multi-value arrays; `a` collected into the
# `app_refs TEXT[]` column (filtered through _ZAPSTORE_A_TAG_REGEX so only
# valid kind-32267 references land in the column — empirically 100% of the
# 2125 observed `a` values match this regex, so this is a sanity-check
# rather than a real filter today).
#
# IMPORTANT — what gets stored from `a` tags: only element[1], the value
# string ("32267:<pubkey>:<d_tag>"). NIP-01 allows a relay-hint at element[2]
# (~3.3% of observed `a` tags carry "wss://relay.zapstore.dev"); we drop
# element[2] since it's metadata about how to fetch the referenced event,
# not part of the reference identity.
#
# IMPORTANT — public/private filtering: this loader does NOT filter encrypted
# stacks at the row level; private-stack events still land in the main table
# (they have other tag data we may want for future queries). The leaderboard
# query handles the public/private split via the same filter Zapstore's own
# webapp uses (`array_length(app_refs, 1) > 0`, equivalent to the webapp's
# `!event.content` check). See init.sql comment block on this table.
_K30267_SINGLETON_TAGS = {"name", "description", "title"}
_K30267_MULTI_TAGS = {"f", "h"}
_K30267_SINGLETON_COLUMN = {
    "name": "name",
    "description": "description",
    "title": "title",
}
_K30267_MULTI_COLUMN = {
    "f": "platforms",
    "h": "h_tags",
}


def emit_timing(*parts: str | int | float) -> None:
    """Write a structured TIMING line. Format:
        TIMING <stage> <item> <elapsed_seconds> [extra...]
    All integer/float values are stringified without formatting to keep the
    parser trivial."""
    print("TIMING " + " ".join(str(p) for p in parts), flush=True)

PROGRESS_INTERVAL_SEC = 15  # min seconds between per-file progress lines


def _fmt_duration(sec: float) -> str:
    sec = max(0, int(sec))
    if sec < 60:
        return f"{sec}s"
    m, s = divmod(sec, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"


def _fmt_bytes_pair(pos: int, total: int) -> str:
    """Format 'pos/total UNIT' sharing the unit that suits total."""
    if total < 1024 ** 2:
        return f"{pos // 1024}/{total // 1024} KB"
    if total < 1024 ** 3:
        return f"{pos / 1024**2:.1f}/{total / 1024**2:.1f} MB"
    return f"{pos / 1024**3:.1f}/{total / 1024**3:.1f} GB"


class _ProgressPrinter:
    """Time-gated per-file progress indicator for JSONL loads.

    Emits at most one line per PROGRESS_INTERVAL_SEC. Rate and ETA are computed
    over the window between the previous emit and now, not cumulatively from
    file-open — so they track current throughput when it drops during a load
    (autovacuum contention, WAL pressure, growing b-tree dedup cost) instead
    of being dragged by the fast startup burst.
    """

    def __init__(self, path: str):
        self.path = path
        self.total_bytes = os.path.getsize(path)
        self.start = time.monotonic()
        self.last_print = self.start
        self.last_pos = 0
        self.last_time = self.start
        self.last_lines = 0

    def should_print(self) -> bool:
        now = time.monotonic()
        if now - self.last_print < PROGRESS_INTERVAL_SEC:
            return False
        self.last_print = now
        return True

    @staticmethod
    def _pos(file_obj) -> int:
        # Byte position for the %/ETA, measured against the on-disk file size
        # (self.total_bytes = os.path.getsize, i.e. the COMPRESSED size for a .gz).
        # TextIOWrapper.tell() raises OSError during `for line in f`, so go via
        # .buffer:
        #   - plain text: .buffer is a BufferedReader; .tell() is the file byte offset.
        #   - gzip (gzip.open(...,"rt")): .buffer is a GzipFile whose .tell() is the
        #     DECOMPRESSED offset (~6x the compressed file → %/ETA blow past 100%).
        #     Read the underlying compressed-file offset via .buffer.fileobj.tell()
        #     so progress stays honest. A GzipFile has .fileobj; a BufferedReader
        #     does not — that's the discriminator.
        buf = getattr(file_obj, "buffer", None)
        if buf is None:
            return file_obj.tell()  # non-text stream / test mock
        raw = getattr(buf, "fileobj", None)  # GzipFile's underlying compressed file
        return raw.tell() if raw is not None else buf.tell()

    def emit(self, file_obj, summary: str) -> None:
        now = time.monotonic()
        elapsed = now - self.start
        pos = self._pos(file_obj)
        pct = (pos / self.total_bytes * 100) if self.total_bytes > 0 else 0
        window = now - self.last_time
        byte_rate = (pos - self.last_pos) / window if window > 0 else 0
        if pos >= self.total_bytes and self.total_bytes > 0:
            eta_str = "0s"
        elif byte_rate > 0:
            eta_str = _fmt_duration((self.total_bytes - pos) / byte_rate)
        else:
            eta_str = "?"
        name = os.path.basename(self.path)
        pair = _fmt_bytes_pair(pos, self.total_bytes)
        print(
            f"    [{_fmt_duration(elapsed)}] {name}: {summary} · {pair} ({pct:.0f}%) · ETA {eta_str}",
            flush=True,
        )
        self.last_pos = pos
        self.last_time = now

    def maybe_emit_insert(self, file_obj, lines: int, inserted: int) -> None:
        if not self.should_print():
            return
        now = time.monotonic()
        window = now - self.last_time
        rate = (lines - self.last_lines) / window if window > 0 else 0
        self.emit(file_obj, f"{lines:,} lines → {inserted:,} inserted @ {rate:,.0f}/s")
        self.last_lines = lines


# Unpaired UTF-16 surrogates (U+D800–U+DFFF). A profile's JSON content can carry a
# bare "\ud83c" escape — half of an emoji surrogate pair from a malformed client —
# which json.loads decodes into a lone surrogate char. It is valid in a Python str
# but cannot be UTF-8 encoded, so it crashes psycopg2's mogrify/COPY on insert
# (UnicodeEncodeError: surrogates not allowed) and Postgres TEXT can't store it
# anyway. In a Python 3 str any surrogate code point is by definition unpaired
# (well-formed chars are full code points), so stripping the whole range is
# non-lossy for valid text.
_LONE_SURROGATE_RE = re.compile("[\ud800-\udfff]")


def _pg_safe_text(s: str) -> str:
    """Strip characters Postgres TEXT cannot store: NUL bytes and unpaired UTF-16
    surrogates (U+D800–U+DFFF). Both are valid in a Python str but cannot be UTF-8
    encoded, so a single offending event crashes psycopg2's mogrify/COPY on insert
    (UnicodeEncodeError: surrogates not allowed). Shared by every content/text field
    that reaches Postgres so the guard lives in one place — the `.search()` short-
    circuits the (rare) `.sub()` allocation on the clean common case."""
    s = s.replace("\x00", "")
    if _LONE_SURROGATE_RE.search(s):
        s = _LONE_SURROGATE_RE.sub("", s)
    return s


def _str_or_none(v):
    """Coerce to string or None, then make Postgres-TEXT-safe (NUL + surrogate strip
    via _pg_safe_text). Some profile fields are dicts/lists."""
    if v is None:
        return None
    s = str(v) if not isinstance(v, str) else v
    return _pg_safe_text(s)


# ---------------------------------------------------------------------------
# COPY (bulk heap append) row encoding
# ---------------------------------------------------------------------------
# The per-kind loaders stream each batch into Postgres via `COPY <table> (...)
# FROM STDIN` (psycopg2 copy_expert), NOT a multi-row INSERT ... ON CONFLICT.
# A raw COPY into an index-free heap (the per-kind loaders drop ALL indexes
# incl. the PK before loading — see _drop_indexes_for_load) is a cache-friendly
# sequential append: no per-row b-tree maintenance, no per-row uniqueness probe.
# The PK + every secondary/extra index is rebuilt in one bulk pass after the
# COPY (orders of magnitude cheaper than incremental maintenance once the index
# outgrows shared_buffers — the degradation this replaces). Conflict handling is
# dropped because the Athena extract GLOBALLY de-duplicates by id before UNLOAD
# (row_number() OVER (PARTITION BY id ...) = 1 in every sql/extract_*.sql), so
# the NDJSON parts carry NO duplicate event_ids and the old ON CONFLICT
# (event_id) DO NOTHING never fired. If a duplicate ever did slip through, the
# post-load PK rebuild fails LOUDLY (the load errors) rather than silently
# dropping it — fail-loud preserved, never silent divergence.
#
# Encoding targets the COPY *text* format (the psycopg2/libpq default):
#   - fields are TAB-separated, rows NEWLINE-terminated;
#   - SQL NULL is the unquoted token \N (distinct from an empty field "");
#   - literal backslash/tab/newline/CR in a field are escaped \\ \t \n \r.
# Array (TEXT[]) and JSONB columns are rendered to their Postgres external text
# representation first (see _pg_copy_array), then that string is escaped as one
# COPY field like any other text — the COPY parser un-escapes once to recover the
# array literal, then the array parser un-escapes element quoting once more, so
# the double escaping below is exactly what round-trips.

# Fields whose Python value must be rendered as a Postgres array literal.
# (psycopg2's INSERT path adapted list[str] natively; COPY text needs the literal.)


def _pg_copy_array(values: list) -> str:
    """Render a Python list as a Postgres array EXTERNAL TEXT representation, e.g.
    ['a', 'b c'] -> '{"a","b c"}'. Every element is double-quoted (Postgres accepts
    always-quoted elements) with embedded `\\` and `"` backslash-escaped, so commas,
    braces, whitespace, and the literal token NULL inside an element are unambiguous.
    A None element becomes the unquoted token NULL (a real SQL NULL array element);
    in practice the loaders never put None inside an array (they filter first), but
    this keeps the encoder total. The returned string is later escaped again by
    _pg_copy_field as a single COPY field.

    NUL bytes are stripped (Postgres TEXT cannot store them) defensively; callers
    already _clean array elements, so this is belt-and-suspenders.
    """
    parts = []
    for v in values:
        if v is None:
            parts.append("NULL")
            continue
        s = v if isinstance(v, str) else str(v)
        s = s.replace("\x00", "")
        s = s.replace("\\", "\\\\").replace('"', '\\"')
        parts.append('"' + s + '"')
    return "{" + ",".join(parts) + "}"


def _pg_copy_field(value, *, is_array: bool = False) -> str:
    """Encode one Python value as a single COPY *text*-format field.

    None            -> '\\N'            (SQL NULL)
    list (is_array) -> escaped Postgres array literal (via _pg_copy_array)
    everything else -> str(value) with COPY text escaping

    COPY text escaping (applied last, on the already-stringified field): backslash
    FIRST (so we don't double-escape the escapes we add next), then tab/newline/CR.
    bool is intentionally NOT special-cased — no loader column is boolean; an int
    stringifies as its digits, which is the correct BIGINT COPY representation.
    """
    if value is None:
        return "\\N"
    if is_array:
        s = _pg_copy_array(value)
    elif isinstance(value, str):
        s = value
    else:
        s = str(value)
    # Backslash must be escaped first.
    return (
        s.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _copy_rows_via_stdin(cur, table: str, columns: list[str], batch: list[tuple],
                         array_cols: frozenset[int] = frozenset()) -> int:
    """Bulk-append `batch` into `table` via COPY ... FROM STDIN (text format).

    Replaces the per-kind multi-row INSERT ... ON CONFLICT DO NOTHING. Returns the
    number of rows copied (== len(batch); COPY has no conflict path, and the Athena
    extract guarantees unique event_ids — see the module note above). Building the
    whole batch into one in-memory text buffer mirrors the old mogrify approach's
    memory profile (one string per batch); the batch-size / byte caps that bound it
    are unchanged.

    array_cols: positional indices (into each row tuple) of TEXT[] columns, rendered
    as Postgres array literals. All other positions are scalar (str/int/None/JSON
    string). The JSONB column (extra_tags) is already a pre-encoded JSON *string* or
    None from json.dumps(), so it flows through the scalar path unchanged — a JSON
    string is valid JSONB input text.
    """
    buf = io.StringIO()
    for row in batch:
        buf.write(
            "\t".join(
                _pg_copy_field(val, is_array=(i in array_cols))
                for i, val in enumerate(row)
            )
        )
        buf.write("\n")
    buf.seek(0)
    collist = ", ".join(columns)
    # FORMAT text is the libpq/psycopg2 default; state it explicitly for clarity.
    cur.copy_expert(
        f"COPY {table} ({collist}) FROM STDIN WITH (FORMAT text)", buf
    )
    return cur.rowcount


# nostrology-data layout: this vendored loader lives in loader/, with blocklists/
# a sibling at the repo root (loader/ -> nostrology-data/).
# NOTE: _SRC_DIR is consulted ONLY by the kind-9735 zap loaders (src/shared/
# zap_parser), which are NOT invoked for M1's `--kind 10002`. When /lightning is
# onboarded, vendor src/shared and fix _SRC_DIR to point at it.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_SRC_DIR = _REPO_ROOT / "src"  # placeholder; only the (unused-in-M1) 9735 paths read it
BLOCKLISTS_DIR = _REPO_ROOT / "blocklists"

# ---------------------------------------------------------------------------
# Index management for bulk loads
# ---------------------------------------------------------------------------
# During --truncate loads, ALL indexes — every secondary index below, the PRIMARY
# KEY, AND any init.sql-only index not in this table (the idx_k9735_signer_created
# class, captured from the catalog at load time) — are dropped before the COPY and
# rebuilt after it. A raw COPY into an index-free heap is a cache-friendly
# sequential append; once an index (esp. the PK b-tree) outgrows shared_buffers,
# incremental per-row maintenance turns every insert into random disk I/O (the 6×
# degradation this replaces). Bulk index builds after the COPY are one sequential
# pass each — far cheaper.
#
# Dropping the PK is safe because conflict handling was only ever needed for dedup,
# and the Athena extract already de-duplicates by id globally before UNLOAD (see the
# COPY note above). The PK is rebuilt after the COPY; a stray duplicate would fail
# that rebuild LOUDLY rather than corrupt silently.
#
# SECONDARY_INDEXES is the static, self-healing source of truth for the non-PK,
# non-extra index DDL — it must match schema/init.sql exactly (the test_*_schema.py
# contract tests cross-check every loader-managed init.sql index against this dict).
# The PK and the init-only "extra" indexes are NOT enumerated here; they are
# discovered + restored from the live catalog generically (see
# _drop_indexes_for_load / _rebuild_indexes_after_load), so this dict stays the
# single hand-maintained inventory and the end-state index set provably equals the
# pre-load set (= init.sql, which the schema apply asserts before every refresh).
# ---------------------------------------------------------------------------

SECONDARY_INDEXES: dict[int | str, list[str]] = {
    1: [
        "CREATE INDEX idx_k1_fts ON events_kind_1_posts USING GIN (content_tsv)",
        "CREATE INDEX idx_k1_pubkey ON events_kind_1_posts (pubkey, created_at)",
        "CREATE INDEX idx_k1_created ON events_kind_1_posts (created_at)",
    ],
    0: [
        "CREATE INDEX idx_k0_fts ON events_kind_0_profiles USING GIN (content_tsv)",
        "CREATE INDEX idx_k0_pubkey ON events_kind_0_profiles (pubkey, created_at)",
        "CREATE INDEX idx_k0_nip05 ON events_kind_0_profiles (left(nip05, 255)) WHERE nip05 IS NOT NULL",
    ],
    7: [
        "CREATE INDEX idx_k7_target ON events_kind_7_reactions (target_event_id) WHERE target_event_id IS NOT NULL",
        "CREATE INDEX idx_k7_pubkey ON events_kind_7_reactions (pubkey, created_at)",
    ],
    6: [
        "CREATE INDEX idx_k6_target ON events_kind_6_reposts (target_event_id) WHERE target_event_id IS NOT NULL",
        "CREATE INDEX idx_k6_pubkey ON events_kind_6_reposts (pubkey, created_at)",
    ],
    9735: [
        "CREATE INDEX idx_k9735_target ON events_kind_9735_zaps (target_event_id) WHERE target_event_id IS NOT NULL",
        "CREATE INDEX idx_k9735_sender ON events_kind_9735_zaps (sender_pubkey, created_at)",
        "CREATE INDEX idx_k9735_recipient ON events_kind_9735_zaps (recipient_pubkey, created_at)",
    ],
    3: [
        "CREATE INDEX idx_follows_followee ON follows (followee_pubkey)",
    ],
    9321: [
        "CREATE INDEX idx_k9321_created ON events_kind_9321_nutzaps (created_at)",
        "CREATE INDEX idx_k9321_mint_created ON events_kind_9321_nutzaps (mint_url, created_at)",
        "CREATE INDEX idx_k9321_recipient ON events_kind_9321_nutzaps (recipient_pubkey, created_at)",
    ],
    10019: [
        "CREATE INDEX idx_k10019_pubkey_created ON events_kind_10019_nutzap_configs (pubkey, created_at DESC)",
        "CREATE INDEX idx_k10019_created ON events_kind_10019_nutzap_configs (created_at)",
        "CREATE INDEX idx_k10019_mint_urls ON events_kind_10019_nutzap_configs USING GIN (mint_urls)",
    ],
    10002: [
        "CREATE INDEX idx_k10002_pubkey_created ON events_kind_10002_relay_lists (pubkey, created_at DESC)",
        "CREATE INDEX idx_k10002_created ON events_kind_10002_relay_lists (created_at)",
        "CREATE INDEX idx_k10002_relay_urls ON events_kind_10002_relay_lists USING GIN (relay_urls)",
        "CREATE INDEX idx_k10002_read_relay_urls ON events_kind_10002_relay_lists USING GIN (read_relay_urls)",
        "CREATE INDEX idx_k10002_write_relay_urls ON events_kind_10002_relay_lists USING GIN (write_relay_urls)",
    ],
    7376: [
        "CREATE INDEX idx_k7376_created ON events_kind_7376_nutzap_redemptions (created_at)",
        "CREATE INDEX idx_k7376_pubkey ON events_kind_7376_nutzap_redemptions (pubkey, created_at)",
        "CREATE INDEX idx_k7376_target ON events_kind_7376_nutzap_redemptions (target_event_id) WHERE target_event_id IS NOT NULL",
        "CREATE INDEX idx_k7376_mint ON events_kind_7376_nutzap_redemptions (mint_url) WHERE mint_url IS NOT NULL",
    ],
    32267: [
        "CREATE INDEX idx_k32267_pubkey_d ON events_kind_32267_app_catalog (pubkey, d_tag)",
        "CREATE INDEX idx_k32267_created ON events_kind_32267_app_catalog (created_at)",
        "CREATE INDEX idx_k32267_extra_tags ON events_kind_32267_app_catalog USING GIN (extra_tags jsonb_path_ops) WHERE extra_tags IS NOT NULL",
    ],
    30063: [
        "CREATE INDEX idx_k30063_app ON events_kind_30063_app_releases (pubkey, app_d_tag, created_at DESC)",
        "CREATE INDEX idx_k30063_created ON events_kind_30063_app_releases (created_at)",
        "CREATE INDEX idx_k30063_extra_tags ON events_kind_30063_app_releases USING GIN (extra_tags jsonb_path_ops) WHERE extra_tags IS NOT NULL",
    ],
    30267: [
        "CREATE INDEX idx_k30267_pubkey_d ON events_kind_30267_app_stacks (pubkey, d_tag)",
        "CREATE INDEX idx_k30267_created ON events_kind_30267_app_stacks (created_at)",
        "CREATE INDEX idx_k30267_app_refs ON events_kind_30267_app_stacks USING GIN (app_refs) WHERE coalesce(array_length(app_refs, 1), 0) > 0",
        "CREATE INDEX idx_k30267_extra_tags ON events_kind_30267_app_stacks USING GIN (extra_tags jsonb_path_ops) WHERE extra_tags IS NOT NULL",
    ],
    # Derived sub-loads keyed by f"{kind}-{into}" — see _drop_secondary_indexes
    # / _create_secondary_indexes for the lookup, and the --into argparse arg
    # for the supported targets.
    "9735-zapstore": [
        "CREATE INDEX idx_k9735zs_app ON events_kind_9735_zaps_zapstore (app_pubkey, app_d_tag, created_at)",
        "CREATE INDEX idx_k9735zs_created ON events_kind_9735_zaps_zapstore (created_at)",
        "CREATE INDEX idx_k9735zs_target ON events_kind_9735_zaps_zapstore (target_event_id) WHERE target_event_id IS NOT NULL",
    ],
}


# Static intended PRIMARY KEY per loader-COPY-managed table (table -> column-list SQL) —
# the PK analogue of SECONDARY_INDEXES, and likewise == schema/init.sql (the
# test_primary_keys.py contract cross-checks it). _capture_primary_key reads the LIVE
# catalog first (exact, incl. the auto name <table>_pkey) and falls back to THIS map when
# the catalog has no PK. That fallback matters because the PK drop COMMITS before the COPY
# (_drop_indexes_for_load), so a load that FAILS its PK rebuild leaves the table committed
# PK-less; without the fallback the next run would capture None and never restore the PK —
# silently surrendering the event_id uniqueness invariant. Only event tables the loader
# COPY-loads are listed; lightning_provider_signers (PK signer_pubkey, loaded by
# --load-lightning-signers) and active_user_counts_kind_9321_nutzaps (PK
# (period_start,granularity), rebuilt by schema/nutzap_active_users.sql) are NOT
# loader-COPY-managed and are intentionally absent.
PRIMARY_KEYS: dict[str, str] = {
    "events_kind_10002_relay_lists": "(event_id)",
    "events_kind_0_profiles": "(event_id)",
    "events_kind_9735_zaps": "(event_id)",
    "events_kind_32267_app_catalog": "(event_id)",
    "events_kind_32267_app_catalog_excluded": "(event_id)",
    "events_kind_30063_app_releases": "(event_id)",
    "events_kind_30267_app_stacks": "(event_id)",
    "events_kind_9735_zaps_zapstore": "(event_id)",
    "events_kind_9321_nutzaps": "(event_id)",
    "events_kind_10019_nutzap_configs": "(event_id)",
}


# ---------------------------------------------------------------------------
# Retry / reconnection for batch inserts
# ---------------------------------------------------------------------------
# A multi-hour bulk load can lose its Postgres connection (machine sleep,
# Docker VM suspension, network glitch). Rather than crash and lose all
# progress for the current kind, we catch connection errors, reconnect,
# and retry the failed batch. Previously-committed batches are safe.
#
# A single batch — whether a multi-row INSERT (kind-3 follows) or a COPY ...
# FROM STDIN (every event-id-keyed kind) — is atomic within its transaction:
# the commit happens only after the statement returns, so if it throws (or the
# connection drops mid-statement) nothing from that batch was committed.
# Retrying the same batch on a fresh connection is therefore safe — there are no
# duplicate event_ids to re-insert (the Athena extract pre-dedups), so the COPY
# retry can't double-load.
# ---------------------------------------------------------------------------

RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 5  # seconds; multiplied by attempt number


def _insert_batch_with_retry(batch_fn, cur, batch, conn, db_url: str, context: str):
    """Call batch_fn(cur, batch) + conn.commit() with retry on connection loss.

    On psycopg2 connection errors: logs the error, reconnects to Postgres,
    and retries the batch with a fresh cursor.

    Returns (inserted_count, conn, cur) — conn and cur may be new objects
    after a reconnect. Callers must use the returned values.

    Raises on exhausted retries or unrecoverable reconnection failure.
    """
    last_err = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            inserted = batch_fn(cur, batch)
            conn.commit()
            return inserted, conn, cur
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            last_err = e
            print(f"  ERROR ({context}, attempt {attempt}/{RETRY_MAX_ATTEMPTS}): {e}",
                  file=sys.stderr, flush=True)
            if attempt < RETRY_MAX_ATTEMPTS:
                delay = RETRY_BASE_DELAY * attempt
                print(f"  Reconnecting in {delay}s...", file=sys.stderr, flush=True)
                time.sleep(delay)
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    conn = psycopg2.connect(db_url)
                    cur = conn.cursor()
                    print(f"  Reconnected successfully.", file=sys.stderr, flush=True)
                except Exception as reconnect_err:
                    print(f"  Reconnection failed: {reconnect_err}",
                          file=sys.stderr, flush=True)
                    raise
    # All attempts exhausted
    print(f"  FATAL: {RETRY_MAX_ATTEMPTS} attempts exhausted for {context}.",
          file=sys.stderr, flush=True)
    raise last_err  # type: ignore[misc]


def _derive_kind_label(kind: int, inputs: list[str]) -> str:
    """Label for a kind like 'kind-1-posts', derived from the first input path's
    acquisition directory (.../data/acquisition/relays/<kind-dir>/exports/<file>).
    Falls back to 'kind-<N>' when the path doesn't match that shape (e.g. test
    fixtures outside the repo layout)."""
    if inputs:
        parent = Path(inputs[0]).resolve().parent.parent.name
        if parent.startswith(f"kind-{kind}-"):
            return parent
    return f"kind-{kind}"


def _drop_secondary_indexes(cur, conn, kind: int, into: str | None = None) -> list[str]:
    """Drop non-PK indexes for a kind (optionally a derived sub-load).

    When `into` is None the lookup key is the integer `kind`, matching the 6
    canonical main-table loads. When `into` is set the lookup key is
    f"{kind}-{into}" — used by derived sub-loads like ("9735", "zapstore").

    Returns the list of index names dropped.
    """
    key = f"{kind}-{into}" if into else kind
    indexes = SECONDARY_INDEXES.get(key, [])
    names = []
    for ddl in indexes:
        idx_name = ddl.split()[2]  # "CREATE INDEX idx_name ..."
        cur.execute(f"DROP INDEX IF EXISTS {idx_name}")
        names.append(idx_name)
    conn.commit()
    return names


def _print_truncated(table: str, dropped: list[str]) -> None:
    """Print the `Truncated <table>[, dropped N indexes: ...]` summary with
    one line per dropped index name."""
    if not dropped:
        print(f"  Truncated {table}")
        return
    n = len(dropped)
    suffix = "" if n == 1 else "es"
    print(f"  Truncated {table}, dropped {n} secondary index{suffix}:")
    for name in dropped:
        print(f"    - {name}")


def _create_secondary_indexes(cur, conn, kind: int, table: str, into: str | None = None) -> None:
    """Recreate non-PK indexes for a kind (optionally a derived sub-load) after
    bulk load. Lookup key is f"{kind}-{into}" if `into` is set else `kind`.

    Logs per-index timing and emits a TIMING line per index keyed by
    <table>/<idx_name>.
    """
    key = f"{kind}-{into}" if into else kind
    indexes = SECONDARY_INDEXES.get(key, [])
    if not indexes:
        return
    print(f"  Rebuilding {len(indexes)} index(es)...")
    for ddl in indexes:
        idx_name = ddl.split()[2]
        t0 = time.monotonic()
        cur.execute(ddl)
        conn.commit()
        elapsed = time.monotonic() - t0
        print(f"    {idx_name}: {elapsed:.1f}s")
        emit_timing("pg-load", f"{table}/{idx_name}", f"{elapsed:.1f}")


def _secondary_index_names(kind: int, into: str | None) -> set[str]:
    """The set of index names the static SECONDARY_INDEXES inventory manages for
    this (kind, into). Used to subtract the known set from the live catalog so the
    'extra' (init.sql-only) indexes — e.g. idx_k9735_signer_created — can be
    captured + restored generically without hardcoding them here."""
    key = f"{kind}-{into}" if into else kind
    return {ddl.split()[2] for ddl in SECONDARY_INDEXES.get(key, [])}


def _capture_primary_key(cur, table: str) -> tuple[str, str] | None:
    """Return (constraint_name, columns_sql) for `table`'s PRIMARY KEY, or None if
    it has none. columns_sql is the ordered, comma-separated, identifier-quoted PK
    column list (e.g. '"event_id"' or '"period_start", "granularity"') ready for
    ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (...). Reads the LIVE catalog first
    (so we restore EXACTLY the constraint that existed, generically), then falls back to
    the static PRIMARY_KEYS map when the catalog has no PK — which happens if a prior load
    FAILED its PK rebuild and left the table committed PK-less (the PK drop commits before
    the COPY). Without the fallback the next run would capture None and never restore the
    PK (review 2026-06-27)."""
    cur.execute(
        """
        SELECT con.conname,
               pg_get_constraintdef(con.oid)
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE con.contype = 'p'
          AND rel.relname = %s
          AND nsp.nspname = ANY (current_schemas(false))
        """,
        (table,),
    )
    row = cur.fetchone()
    if row is not None:
        conname, condef = row
        # condef looks like 'PRIMARY KEY (event_id)' — keep the parenthesized column
        # list verbatim (it preserves order + quoting as Postgres reports it).
        m = re.search(r"PRIMARY KEY\s*(\(.*\))", condef)
        if m:
            return conname, m.group(1)
    # No live PK (or an unparsable def): restore the STATIC intended PK so a prior failed
    # rebuild can't permanently drop it. The default name <table>_pkey matches init.sql's
    # inline `event_id CHAR(64) PRIMARY KEY`. An unknown (non-loader-COPY-managed) table
    # falls through to None, preserving the original behaviour.
    cols = PRIMARY_KEYS.get(table)
    if cols is not None:
        return f"{table}_pkey", cols
    return None


def _capture_extra_indexes(cur, table: str, known: set[str], pk_name: str | None) -> list[tuple[str, str]]:
    """Return [(index_name, create_index_sql), ...] for every index on `table` that
    is NOT the PK's backing index and NOT in the loader's `known` (SECONDARY_INDEXES)
    set — i.e. the init.sql-only 'extra' indexes (idx_k9735_signer_created today).
    indexdef is pg_indexes' exact CREATE INDEX statement, which round-trips, so a
    captured extra is rebuilt byte-faithfully after the COPY. Excludes the PK index
    by name (the PK is dropped/rebuilt as a CONSTRAINT, not a plain index)."""
    cur.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = %s
          AND schemaname = ANY (current_schemas(false))
        """,
        (table,),
    )
    extras = []
    for indexname, indexdef in cur.fetchall():
        if indexname in known:
            continue
        if pk_name is not None and indexname == pk_name:
            continue
        extras.append((indexname, indexdef))
    return extras


def _drop_indexes_for_load(cur, conn, kind: int, table: str, into: str | None = None) -> dict:
    """Drop ALL indexes on `table` before a bulk COPY: the static secondary set, the
    PRIMARY KEY constraint, and any init.sql-only 'extra' indexes. Returns a capture
    dict the post-load rebuild consumes to restore the EXACT pre-load index set:
        {"dropped": [names...], "pk": (name, cols_sql) | None, "extras": [(name, ddl)...]}

    Order: capture PK + extras from the catalog FIRST (so their definitions survive
    the drop), then drop secondary indexes, then the extras, then the PK constraint.
    """
    known = _secondary_index_names(kind, into)
    pk = _capture_primary_key(cur, table)
    pk_name = pk[0] if pk else None
    extras = _capture_extra_indexes(cur, table, known, pk_name)

    dropped = _drop_secondary_indexes(cur, conn, kind, into=into)

    for idx_name, _ddl in extras:
        cur.execute(f"DROP INDEX IF EXISTS {idx_name}")
        dropped.append(idx_name)
    if pk is not None:
        # DROP CONSTRAINT also drops the PK's backing unique index.
        cur.execute(f'ALTER TABLE {table} DROP CONSTRAINT IF EXISTS "{pk[0]}"')
        dropped.append(pk[0])
    conn.commit()
    return {"dropped": dropped, "pk": pk, "extras": extras}


def _rebuild_indexes_after_load(cur, conn, kind: int, table: str, capture: dict, into: str | None = None) -> None:
    """Rebuild every index dropped by _drop_indexes_for_load, in dependency-safe
    order: the init.sql-only 'extra' indexes, then the static secondary set, then the
    PRIMARY KEY constraint LAST. Each step is timed + emits a per-index TIMING line
    keyed <table>/<name> (the PK as <table>/<pk_name>) so the rebuild phase shows up
    in the pipeline summary alongside the COPY phase.

    Rebuilding the PK last (a) keeps the cheap secondary builds off the critical path
    of the unique-constraint validation and (b) means a stray duplicate event_id —
    which the Athena extract's id-dedup makes impossible — surfaces as a LOUD failure
    of the final ADD CONSTRAINT, never silent data loss."""
    extras = capture.get("extras") or []
    pk = capture.get("pk")
    n_total = len(extras) + len(SECONDARY_INDEXES.get(f"{kind}-{into}" if into else kind, [])) + (1 if pk else 0)
    if n_total:
        print(f"  Rebuilding {n_total} index(es) + constraint(s)...")

    for idx_name, ddl in extras:
        t0 = time.monotonic()
        cur.execute(ddl)
        conn.commit()
        elapsed = time.monotonic() - t0
        print(f"    {idx_name}: {elapsed:.1f}s")
        emit_timing("pg-load", f"{table}/{idx_name}", f"{elapsed:.1f}")

    _create_secondary_indexes(cur, conn, kind, table, into=into)

    if pk is not None:
        pk_name, pk_cols = pk
        t0 = time.monotonic()
        cur.execute(f'ALTER TABLE {table} ADD CONSTRAINT "{pk_name}" PRIMARY KEY {pk_cols}')
        conn.commit()
        elapsed = time.monotonic() - t0
        print(f"    {pk_name} (PRIMARY KEY): {elapsed:.1f}s")
        emit_timing("pg-load", f"{table}/{pk_name}", f"{elapsed:.1f}")

    # ANALYZE after a full TRUNCATE+COPY+reindex. Written when the cluster still paused between
    # refreshes (min_capacity=0, until 2026-08-03), so autovacuum/autoanalyze might never run,
    # leaving stale planner stats and bad first-query plans. It no longer pauses; the explicit
    # ANALYZE stays so the first queries after a load do not depend on when autoanalyze reaches
    # the table. The loader owns the connection (cheap; bounded by the table size).
    t0 = time.monotonic()
    cur.execute(f"ANALYZE {table}")
    conn.commit()
    elapsed = time.monotonic() - t0
    print(f"    ANALYZE {table}: {elapsed:.1f}s")
    emit_timing("pg-load", f"{table}/analyze", f"{elapsed:.1f}")


def _load_blocklist() -> set[str]:
    """Load all blocklisted pubkeys from the combined blocklist directory.

    Returns a set of hex pubkey strings. Used to skip bot/spam events
    during loading.
    """
    pks: set[str] = set()
    for root, _dirs, files in os.walk(BLOCKLISTS_DIR):
        for f in files:
            if not f.endswith(".txt"):
                continue
            with open(os.path.join(root, f)) as fh:
                for line in fh:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        pks.add(line)
    return pks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Nostr events into Postgres FTS tables")
    parser.add_argument("--input", nargs="+", help="JSONL file(s) to load (required for event-kind loads)")
    parser.add_argument("--kind", type=int, choices=[0, 1, 3, 6, 7, 7376, 9321, 9735, 10002, 10019, 30063, 30267, 32267], help="Event kind (0=profiles, 1=posts, 3=follows, 6=reposts, 7=reactions, 7376=nutzap-redemptions, 9321=nutzaps, 9735=zaps, 10002=relay-lists, 10019=nutzap-configs, 30063=zapstore-app-releases, 30267=zapstore-app-stacks, 32267=zapstore-app-catalog)")
    parser.add_argument("--into", type=str, default=None, choices=["zapstore"],
        help="Optional derived-table target for this kind. Default (omitted): write to the kind's main table.")
    parser.add_argument("--db-url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--truncate", action="store_true", help="TRUNCATE table before loading")
    parser.add_argument(
        "--min-rows",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Refuse to load (and, with --truncate, refuse to TRUNCATE) when the total "
            "number of non-empty input lines is below N. Defense-in-depth against a "
            "truncated/empty upstream export wiping a populated table (the per-kind "
            "loaders TRUNCATE before reading). Checked BEFORE any TRUNCATE. Default 0 (off)."
        ),
    )
    parser.add_argument(
        "--no-blocklist",
        action="store_true",
        help="Skip blocklist filtering (load all events including bots/spam)",
    )
    parser.add_argument(
        "--no-refresh-mv",
        action="store_true",
        help=(
            "Skip the in-loader REFRESH MATERIALIZED VIEW step (affects kinds 10002 and 3). The "
            "Dagster pipeline lifts MV refresh into a separate downstream asset, so the load is "
            "load-only; omit this flag for the legacy behavior (refresh inline at end of load)."
        ),
    )
    parser.add_argument(
        "--load-lightning-signers",
        type=str,
        default=None,
        metavar="YAML_PATH",
        help=(
            "Special mode: load lightning_provider_signers from the given YAML file "
            "(NIP-57 LNURL signer pubkey -> lud16 domain mapping). When set, --input "
            "and --kind are not used; the table is TRUNCATEd and reinserted."
        ),
    )
    parser.add_argument(
        "--classify-nutzap-mints",
        type=str,
        default=None,
        metavar="YAML_PATH",
        help=(
            "Special mode: rebuild nutzap_mint_classes from the given mint registry "
            "(data/nutzaps/mint_registry.yaml) and the mint URLs already loaded in the "
            "kind-9321 (nutzaps) and kind-10019 (nutzap-configs) tables. One transaction; "
            "a failure leaves the previous classification in place."
        ),
    )
    args = parser.parse_args()
    # Mode-specific argument validation. The YAML modes bypass --input/--kind;
    # all other modes require them.
    if args.load_lightning_signers is None and args.classify_nutzap_mints is None:
        if args.input is None or args.kind is None:
            parser.error("--input and --kind are required unless --load-lightning-signers is set")
    return args


def load_kind_1_posts(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-1 posts into events_kind_1_posts."""
    table = "events_kind_1_posts"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 1, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocked = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        batch_bytes = 0
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                content = e.get("content")

                if not all([event_id, pubkey, created_at is not None, content is not None]):
                    total_skipped += 1
                    continue

                if blocklist and pubkey in blocklist:
                    total_blocked += 1
                    continue

                # Postgres TEXT columns cannot contain NUL bytes
                clean_content = _pg_safe_text(str(content))
                batch.append((str(event_id), str(pubkey), int(created_at), clean_content))
                batch_bytes += len(clean_content) + 200  # content + fixed overhead per row

                if len(batch) >= BATCH_SIZE or batch_bytes >= BATCH_MAX_BYTES:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_1_posts, cur, batch, conn, db_url,
                        f"kind-1 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    batch_bytes = 0
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            # Final batch
            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_1_posts, cur, batch, conn, db_url,
                    f"kind-1 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 1, "events_kind_1_posts", capture)

    blocked_msg = f", {total_blocked:,} blocklisted" if total_blocked else ""
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{blocked_msg}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_1_posts(cur, batch: list[tuple]) -> int:
    """Bulk-append a batch via COPY FROM STDIN (no conflict path — the Athena extract
    pre-dedups event_ids). Returns rows copied (== len(batch))."""
    return _copy_rows_via_stdin(
        cur, "events_kind_1_posts",
        ["event_id", "pubkey", "created_at", "content"],
        batch,
    )


def load_kind_0_profiles(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-0 profiles into events_kind_0_profiles."""
    table = "events_kind_0_profiles"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 0, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocked = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                raw_content = e.get("content", "")

                if not all([event_id, pubkey, created_at is not None]):
                    total_skipped += 1
                    continue

                if blocklist and pubkey in blocklist:
                    total_blocked += 1
                    continue

                # Parse the JSON content field for profile metadata
                try:
                    meta = json.loads(raw_content) if raw_content else {}
                    if not isinstance(meta, dict):
                        meta = {}
                except (json.JSONDecodeError, TypeError):
                    meta = {}

                batch.append((
                    str(event_id),
                    str(pubkey),
                    int(created_at),
                    _str_or_none(meta.get("name")),
                    _str_or_none(meta.get("display_name")),
                    _str_or_none(meta.get("about")),
                    _str_or_none(meta.get("nip05")),
                    _str_or_none(meta.get("lud16")),
                    _str_or_none(meta.get("picture")),
                ))

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_0_profiles, cur, batch, conn, db_url,
                        f"kind-0 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_0_profiles, cur, batch, conn, db_url,
                    f"kind-0 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 0, "events_kind_0_profiles", capture)

    blocked_msg = f", {total_blocked:,} blocklisted" if total_blocked else ""
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{blocked_msg}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_0_profiles(cur, batch: list[tuple]) -> int:
    """Bulk-append kind-0 profiles via COPY FROM STDIN (no conflict path)."""
    return _copy_rows_via_stdin(
        cur, "events_kind_0_profiles",
        ["event_id", "pubkey", "created_at", "name", "display_name", "about",
         "nip05", "lud16", "picture"],
        batch,
    )


def load_kind_7_reactions(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-7 reactions into events_kind_7_reactions."""
    table = "events_kind_7_reactions"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 7, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocked = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                content = e.get("content", "")

                if not all([event_id, pubkey, created_at is not None]):
                    total_skipped += 1
                    continue

                if blocklist and pubkey in blocklist:
                    total_blocked += 1
                    continue

                # Extract first 'e' tag as target event
                target_event_id = None
                for tag in e.get("tags", []):
                    if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "e":
                        target_event_id = tag[1]
                        break

                clean_content = _pg_safe_text(str(content)) if content else ""
                batch.append((str(event_id), str(pubkey), int(created_at), target_event_id, clean_content))

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_7_reactions, cur, batch, conn, db_url,
                        f"kind-7 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_7_reactions, cur, batch, conn, db_url,
                    f"kind-7 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 7, "events_kind_7_reactions", capture)

    blocked_msg = f", {total_blocked:,} blocklisted" if total_blocked else ""
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{blocked_msg}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_7_reactions(cur, batch: list[tuple]) -> int:
    return _copy_rows_via_stdin(
        cur, "events_kind_7_reactions",
        ["event_id", "pubkey", "created_at", "target_event_id", "content"],
        batch,
    )


def load_kind_6_reposts(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-6 reposts into events_kind_6_reposts."""
    table = "events_kind_6_reposts"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 6, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocked = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")

                if not all([event_id, pubkey, created_at is not None]):
                    total_skipped += 1
                    continue

                if blocklist and pubkey in blocklist:
                    total_blocked += 1
                    continue

                target_event_id = None
                for tag in e.get("tags", []):
                    if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "e":
                        target_event_id = tag[1]
                        break

                batch.append((str(event_id), str(pubkey), int(created_at), target_event_id))

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_6_reposts, cur, batch, conn, db_url,
                        f"kind-6 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_6_reposts, cur, batch, conn, db_url,
                    f"kind-6 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 6, "events_kind_6_reposts", capture)

    blocked_msg = f", {total_blocked:,} blocklisted" if total_blocked else ""
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{blocked_msg}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_6_reposts(cur, batch: list[tuple]) -> int:
    return _copy_rows_via_stdin(
        cur, "events_kind_6_reposts",
        ["event_id", "pubkey", "created_at", "target_event_id"],
        batch,
    )


def _parse_kind_9735_for_loader(event: dict, parse_zap_receipt) -> tuple | None:
    """Pure-function row-builder for kind-9735 zap receipts. Returns the
    7-tuple inserted into events_kind_9735_zaps, or None if the event is
    malformed and should be skipped.

    Tuple order matches the INSERT in _copy_batch_kind_9735_zaps:
        (event_id, sender_pubkey, recipient_pubkey, created_at,
         target_event_id, amount_msats, lnurl_signer_pubkey)

    The lnurl_signer_pubkey is the raw event.pubkey of the kind-9735 — the
    LNURL service's NIP-57 nostr signer. Captured for at-the-time provider
    attribution (joined against lightning_provider_signers in /lightning
    trends queries). The signer's claimed identity is verified out-of-band
    against each provider's published `nostrPubkey` at the LNURL-pay endpoint.

    Extracted as a pure function so the per-event extraction is unit-testable
    without a live Postgres or live JSONL — mirrors the _parse_kind_10002_event
    pattern.
    """
    event_id = event.get("id")
    if not event_id:
        return None
    parsed = parse_zap_receipt(event, warn=False, line_num=0)
    if parsed is None:
        return None
    return (
        str(event_id),
        str(parsed.sender_pubkey),
        parsed.recipient_pubkey,
        int(parsed.created_at),
        parsed.target_event_id,
        parsed.amount_msats,
        event.get("pubkey"),
    )


def load_kind_9735_zaps(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-9735 zap receipts into events_kind_9735_zaps.

    Uses the shared zap_parser module to extract sender, recipient, target,
    and amount from the embedded kind-9734 zap request. Also captures
    event.pubkey (the LNURL service signer's nostr pubkey) into
    lnurl_signer_pubkey for at-the-time provider attribution.

    Events that fail to parse are skipped (counted in total_skipped).
    Blocklist is accepted for interface consistency but not currently applied
    to zap receipts — all successfully parsed zaps are loaded.
    """
    sys.path.insert(0, str(_SRC_DIR / "shared"))
    from zap_parser import parse_zap_receipt

    table = "events_kind_9735_zaps"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 9735, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocked = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                row = _parse_kind_9735_for_loader(e, parse_zap_receipt)
                if row is None:
                    total_skipped += 1
                    continue

                batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_9735_zaps, cur, batch, conn, db_url,
                        f"kind-9735 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_9735_zaps, cur, batch, conn, db_url,
                    f"kind-9735 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 9735, "events_kind_9735_zaps", capture)

    blocked_msg = f", {total_blocked:,} blocklisted" if total_blocked else ""
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{blocked_msg}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_9735_zaps(cur, batch: list[tuple]) -> int:
    return _copy_rows_via_stdin(
        cur, "events_kind_9735_zaps",
        ["event_id", "sender_pubkey", "recipient_pubkey", "created_at",
         "target_event_id", "amount_msats", "lnurl_signer_pubkey"],
        batch,
    )


def load_kind_9735_zaps_zapstore(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load Zapstore-attributed kind-9735 zap receipts into
    events_kind_9735_zaps_zapstore.

    Reads the same JSONL the base kind-9735 loader reads but keeps only
    receipts whose first ["a", ...] tag matches "32267:<dev_pubkey>:<app_d_tag>".
    Reuses parse_zap_receipt() from src/shared/zap_parser.py for sender,
    recipient, target, and amount extraction (no duplicate bolt11 logic).

    Skips:
      - Malformed events (parse_zap_receipt returns None) — counted in total_skipped.
      - Receipts whose first "a" tag doesn't match the Zapstore pattern —
        counted separately in total_non_zapstore (expected, not a failure).
      - Blocklisted recipients — applied for consistency with the base loader.
    """
    sys.path.insert(0, str(_SRC_DIR / "shared"))
    from zap_parser import parse_zap_receipt

    table = "events_kind_9735_zaps_zapstore"
    # Receipts whose listing the catalog load rejected are dropped with it.
    excluded_coords = excluded_listing_coords(conn) if truncate else set()
    followed = 0
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 9735, table, into="zapstore")
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocked = 0
    total_non_zapstore = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                if not event_id:
                    total_skipped += 1
                    continue

                # Find first ["a", ...] tag matching the Zapstore pattern.
                # Also capture first ["e", ...] tag as target_event_id (optional).
                app_pubkey = None
                app_d_tag = None
                target_event_id = None
                for tag in e.get("tags", []):
                    if not (isinstance(tag, list) and len(tag) >= 2):
                        continue
                    if tag[0] == "a" and app_pubkey is None:
                        m = _ZAPSTORE_A_TAG_REGEX.match(tag[1])
                        if m:
                            app_pubkey = m.group(1)
                            app_d_tag = m.group(2)
                    elif tag[0] == "e" and target_event_id is None:
                        target_event_id = tag[1]

                if app_pubkey is None:
                    total_non_zapstore += 1
                    continue

                parsed = parse_zap_receipt(e, warn=(total_skipped <= 10), line_num=file_lines)
                if parsed is None:
                    total_skipped += 1
                    continue

                if blocklist and parsed.recipient_pubkey and parsed.recipient_pubkey in blocklist:
                    total_blocked += 1
                    continue

                clean_d = str(app_d_tag).replace("\x00", "")
                # Follow the listing: a receipt a-tagged at a listing that is not a
                # Zapstore app is not a Zapstore zap. Keeping it would leave the app and
                # publisher zap leaderboards ranking a row the catalog cannot name.
                if (str(app_pubkey), clean_d) in excluded_coords:
                    followed += 1
                    continue
                batch.append((
                    str(event_id),
                    str(app_pubkey),
                    clean_d,
                    str(parsed.sender_pubkey),
                    parsed.recipient_pubkey,
                    int(parsed.created_at),
                    target_event_id,
                    parsed.amount_msats,
                    e.get("pubkey"),  # LNURL signer (NIP-57 nostrPubkey) — same field as the base loader
                ))

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_9735_zaps_zapstore, cur, batch, conn, db_url,
                        f"kind-9735-zapstore {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_9735_zaps_zapstore, cur, batch, conn, db_url,
                    f"kind-9735-zapstore {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    if excluded_coords:
        print(f"  Followed the listing: {followed:,} zap receipt(s) dropped with an excluded listing")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 9735, table, capture, into="zapstore")

    blocked_msg = f", {total_blocked:,} blocklisted" if total_blocked else ""
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, "
          f"{total_skipped:,} malformed, {total_non_zapstore:,} non-zapstore{blocked_msg}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_9735_zaps_zapstore(cur, batch: list[tuple]) -> int:
    return _copy_rows_via_stdin(
        cur, "events_kind_9735_zaps_zapstore",
        ["event_id", "app_pubkey", "app_d_tag", "sender_pubkey", "recipient_pubkey",
         "created_at", "target_event_id", "amount_msats", "lnurl_signer_pubkey"],
        batch,
    )


def _parse_lightning_signers_yaml(yaml_path: str) -> list[tuple]:
    """Pure-function YAML parser for the lightning_provider_signers manifest.

    Expected YAML shape:
        providers:
          - domain: primal.net
            signers: ['<64-hex>', '<64-hex>', ...]
            verified_at: '2026-05-09'   # optional, ISO date
            verified_via: 'nip57.nostrPubkey'  # optional, free-form
            notes: 'Two signers observed; both verified'  # optional

    Returns a flat list of (signer_pubkey, provider_domain, verified_at,
    verified_via, notes) tuples ready for INSERT. Validates: signer must be
    64-char hex; domain must be a non-empty string. Raises ValueError on
    structural problems so a typo doesn't silently produce an empty mapping.
    """
    import yaml as _yaml  # imported lazily so the rest of the loader doesn't depend on PyYAML
    import re as _re
    with open(yaml_path, "r", encoding="utf-8") as f:
        doc = _yaml.safe_load(f)
    if not isinstance(doc, dict) or "providers" not in doc:
        raise ValueError(f"{yaml_path}: missing top-level 'providers' key")
    providers = doc["providers"]
    if not isinstance(providers, list):
        raise ValueError(f"{yaml_path}: 'providers' must be a list")
    rows: list[tuple] = []
    seen_signers: set[str] = set()
    hex64 = _re.compile(r"^[0-9a-f]{64}$")
    for i, entry in enumerate(providers):
        if not isinstance(entry, dict):
            raise ValueError(f"{yaml_path}: providers[{i}] is not a mapping")
        domain = entry.get("domain")
        if not isinstance(domain, str) or not domain.strip():
            raise ValueError(f"{yaml_path}: providers[{i}] missing/empty 'domain'")
        domain = domain.strip().lower()
        signers = entry.get("signers", [])
        if not isinstance(signers, list) or not signers:
            raise ValueError(f"{yaml_path}: providers[{i}] ({domain}) missing/empty 'signers'")
        verified_at = entry.get("verified_at")
        # PyYAML may parse YYYY-MM-DD as a datetime.date — accept that or a string.
        if verified_at is not None and not isinstance(verified_at, (str,)) and hasattr(verified_at, "isoformat"):
            verified_at = verified_at.isoformat()
        verified_via = entry.get("verified_via")
        notes = entry.get("notes")
        for signer in signers:
            if not isinstance(signer, str) or not hex64.match(signer.lower()):
                raise ValueError(f"{yaml_path}: providers[{i}] ({domain}) has non-64-hex signer: {signer!r}")
            sk = signer.lower()
            if sk in seen_signers:
                raise ValueError(f"{yaml_path}: signer {sk} appears under multiple providers (must be unique per signer)")
            seen_signers.add(sk)
            rows.append((sk, domain, verified_at, verified_via, notes))
    return rows


def load_lightning_provider_signers(yaml_path: str, conn) -> int:
    """Load the lightning_provider_signers mapping from a YAML manifest.

    TRUNCATEs the table and reinserts every entry — the YAML is the single
    source of truth, so additions/removals/edits in YAML propagate cleanly.
    Returns the row count inserted.
    """
    table = "lightning_provider_signers"
    rows = _parse_lightning_signers_yaml(yaml_path)
    cur = conn.cursor()
    _t0 = time.monotonic()
    cur.execute(f"TRUNCATE {table}")
    if rows:
        values_list = ",".join(
            cur.mogrify("(%s, %s, %s, %s, %s)", r).decode() for r in rows
        )
        cur.execute(
            f"INSERT INTO {table} (signer_pubkey, provider_domain, verified_at, verified_via, notes) "
            f"VALUES {values_list}"
        )
    conn.commit()
    elapsed = time.monotonic() - _t0
    print(f"  Loaded {len(rows):,} signer→provider mappings into {table} in {elapsed:.1f}s")
    emit_timing("pg-load", table, f"{elapsed:.1f}", len(rows))
    return len(rows)


# A mint nobody has reviewed is counted. Each classification lists the ones whose first
# payment is this recent, so a new testnut-like mint gets looked at instead of silently counted.
_NEW_MINT_REVIEW_DAYS = 14


def classify_nutzap_mints(registry_path: str, conn, *, now: float | None = None) -> int:
    """Rebuild nutzap_mint_classes: one row per mint URL loaded in the kind-9321 (nutzaps) or
    kind-10019 (nutzap-configs) table, with its class from nutzap_mints.classify.

    The registry is parsed and validated BEFORE anything is written, and the rebuild is a
    DELETE + INSERT in ONE transaction, so a bad registry or a failure mid-way leaves the
    previous classification serving. Every reader treats a URL missing from the table as
    unreviewed (counted), which is also what a mint seen for the first time is.
    Returns the number of URLs classified.
    """
    registry = nutzap_mints.load_registry(registry_path)
    table = "nutzap_mint_classes"
    now = time.time() if now is None else now
    _t0 = time.monotonic()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.mint_url,
               COALESCE(p.payments, 0), COALESCE(p.sats, 0), p.first_payment
          FROM (SELECT mint_url FROM events_kind_9321_nutzaps
                UNION
                SELECT UNNEST(mint_urls) FROM events_kind_10019_nutzap_configs) u
          LEFT JOIN (SELECT mint_url, COUNT(*) AS payments, SUM(amount_sats) AS sats,
                            MIN(created_at) AS first_payment
                       FROM events_kind_9321_nutzaps
                      WHERE value_source = 'proof_tags'
                      GROUP BY mint_url) p USING (mint_url)
         ORDER BY u.mint_url
        """
    )
    urls = cur.fetchall()

    rows: list[tuple] = []
    by_class: dict[str, int] = {}
    excluded_payments = excluded_sats = 0
    to_review: list[tuple] = []
    for url, payments, sats, first_payment in urls:
        try:
            mc = nutzap_mints.classify(url, registry)
        except Exception as ex:  # noqa: BLE001 - one odd URL must not fail the classification
            print(f"    WARN mint classification failed on {url[:120]!r}: {type(ex).__name__}: {str(ex)[:120]}")
            mc = nutzap_mints.MintClass("unreviewed", "none", None)
        rows.append((url, mc.mint_class, mc.excluded, mc.source, mc.evidence))
        by_class[mc.mint_class] = by_class.get(mc.mint_class, 0) + 1
        if mc.excluded:
            excluded_payments += payments
            excluded_sats += sats
        elif (mc.mint_class == "unreviewed" and first_payment is not None
              and first_payment >= now - _NEW_MINT_REVIEW_DAYS * 86400):
            to_review.append((url, payments, sats))

    try:
        cur.execute(f"DELETE FROM {table}")
        for i in range(0, len(rows), 1000):
            chunk = rows[i:i + 1000]
            values = ",".join(cur.mogrify("(%s,%s,%s,%s,%s)", r).decode() for r in chunk)
            cur.execute(
                f"INSERT INTO {table} (mint_url, mint_class, excluded, class_source, evidence) VALUES {values}"
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    elapsed = time.monotonic() - _t0
    mix = ", ".join(f"{c}={n:,}" for c, n in sorted(by_class.items()))
    print(f"  Mint classes: {len(rows):,} URLs [{mix}]; excluded: {excluded_payments:,} payments, "
          f"{excluded_sats:,} sats")
    unused = sorted(set(registry.by_url) - {r[0] for r in rows})
    if unused:
        print(f"  Info: {len(unused):,} registry url entries match no loaded URL, e.g. {unused[:3]}")
    if to_review:
        # repr(): the URL is third-party text; keep control characters out of the log.
        listed = "; ".join(f"{u[:200]!r} ({p:,} payments, {s:,} sats)" for u, p, s in to_review[:10])
        print(f"  Review: {len(to_review):,} unreviewed mint(s) with a first payment in the last "
              f"{_NEW_MINT_REVIEW_DAYS} days - add each to data/nutzaps/mint_registry.yaml if it is "
              f"a test mint: {listed}")
    else:
        print(f"  Review: no unreviewed mint had its first payment in the last {_NEW_MINT_REVIEW_DAYS} days")
    emit_timing("pg-load", table, f"{elapsed:.1f}", len(rows))
    return len(rows)


# Mint URL spelling, the URL byte cap and its text checks live in nutzap_mints (shared with
# the mint classification, #9); kind-10002 (relay-list-metadata) relay URLs reuse the checks.
# The cap: mint and relay URLs land in btree / GIN indexes whose entries max out near 2,700
# bytes; an incompressible URL longer than that loads, then fails the index rebuild, and
# init.sql (applied before EVERY surface's load) then fails on it every night until the row
# is removed by hand. The longest archived kind-10019 (nutzap-configs) mint URL is 380
# bytes and the longest kind-10002 (relay-list-metadata) relay URL 985, so the cap drops no
# real data.
_MAX_URL_BYTES = nutzap_mints.MAX_URL_BYTES
_url_text_ok = nutzap_mints.url_text_ok
_normalize_mint_url = nutzap_mints.normalize_mint_url


_HEX64_RE = re.compile(r"[0-9a-fA-F]{64}")


def _hex64_or_none(value) -> str | None:
    """A 64-hex id or pubkey from a tag (lowercased), else None.

    Tag values are unvalidated third-party text. A kind-9321 (nutzaps) `p` or `e` tag
    longer than 64 characters, or carrying a NUL, would fail the COPY into its CHAR(64)
    column after the load has already TRUNCATEd the table, leaving it empty on every
    refresh. A malformed reference is kept as NULL instead.
    """
    if isinstance(value, str) and _HEX64_RE.fullmatch(value):
        return value.lower()
    return None


def _amount_tag_to_sats(tags: list, event_id: str = "?", warn: bool = True) -> int | None:
    """Interpret a NIP-60/61 `amount` tag as SATS, honoring the companion `unit`.

    NOT a kind-9321 (nutzaps) value source: nutzap value is `_nutzap_value`, the
    proof sum and nothing else (nostrology-data#8). Two callers remain:
      - kind-7376 (nutzap-redemptions), whose content is NIP-44 (encrypted payloads)
        ciphertext, so this
        tag is the only value it exposes. That value is SELF-REPORTED by the
        redeemer and unverifiable; label it so wherever it is ever shown.
      - the kind-9321 (nutzaps) tag-disagreement counter, which is logged and
        never used for value (warn=False, so it adds no per-event noise).

    The tag is a non-standard client convenience whose unit is governed by a
    sibling `unit` tag - `sat` (default) or `msat`. msat amounts are divided by
    1000 (floor). Cashu also defines non-sat units (usd/eur/btc/...) which we
    cannot convert here; those are refused (None) rather than mislabeled as sats.
    Returns sats, or None when there is no usable/convertible `amount` tag. The
    first `amount` tag wins (clients emit one); the `unit` tag governs it.
    """
    unit = "sat"
    raw = None
    for tag in tags:
        if not isinstance(tag, list) or len(tag) < 2:
            continue
        if tag[0] == "unit" and tag[1]:
            unit = str(tag[1]).lower()
        elif tag[0] == "amount" and raw is None:
            raw = tag[1]
    if raw is None:
        return None
    try:
        val = int(raw)
    except (ValueError, TypeError):
        if warn:
            print(f"    WARN nutzap amount tag not an integer on {event_id}: {raw!r}")
        return None
    if unit == "sat":
        return val
    if unit == "msat":
        return val // 1000
    # usd / eur / btc / unknown - not convertible to sats here; refuse rather than
    # silently store a non-sat amount as if it were sats.
    if warn:
        print(f"    WARN nutzap amount tag in unsupported unit {unit!r} on {event_id}: {raw!r}")
    return None


# -- kind-9321 (nutzaps) value: the proofs in `proof` tags, nothing else ---------------
#
# nostrology-data#8. Value is the sum of `amount` over the Cashu proofs carried as JSON
# objects in `proof` tags. Nothing else is ever read for value: not the `amount` tag
# (NIP-61 (nutzaps) removed it on 2025-02-04, it was frequently in millisats, and no current
# receiver reads it), not the `unit` tag (its meaning changed twice), and not ecash
# carried anywhere other than a `proof` tag (2024 draft proofs in content, cashuA/B
# tokens, encrypted payloads). An event with no countable proof keeps its row, worth 0
# and classified `value_source = 'none'` with a `no_value_reason`, so it counts as a
# nutzap-shaped event but never as a payment.

# The four fields NUT-00 (Cashu notation) defines on a Proof. A JSON object in a `proof`
# tag missing any of them is not a proof. Every proof in the archive carries all four.
_NUT00_PROOF_FIELDS = ("amount", "id", "secret", "C")

# Upper bound on one proof's amount and on one event's sum: all 21M bitcoin, in sats.
# Anything larger is not sats, and it would also overflow the BIGINT column and fail
# the whole COPY batch.
_MAX_PLAUSIBLE_SATS = 21_000_000 * 100_000_000

# A Cashu token string (NUT-00 (Cashu notation) V3 `cashuA` / V4 `cashuB`), wherever it appears.
_CASHU_TOKEN_RE = re.compile(r"cashu[AB][A-Za-z0-9_+/=-]{16,}")

NUTZAP_VALUE_SOURCES = ("proof_tags", "none")

# created_at ceiling for a loaded kind-9321 (nutzaps) event: 2100-01-01 UTC.
_MAX_NUTZAP_CREATED_AT = 4_102_444_800

# Why an event carries no countable value. Stable strings: the serving table stores
# them, so a rename is a schema change. First match wins, in this order.
NUTZAP_NO_VALUE_REASONS = (
    "implausible-amount",        # proofs parsed but their sum exceeds all bitcoin
    "encrypted",                 # an `encryption` tag ("Encrypted NutZap", 2026-09)
    "cashu-token-in-proof-tag",  # cashuA/B string or wrapped {proofs, mint} in a `proof` tag
    "invalid-proof-tag",         # `proof` tags present, none a valid NUT-00 (Cashu notation) proof
    "draft-content",             # 2024-07 draft: proofs in content, unit as amount[2] msat(s)
    "content-proofs",            # a JSON proof list in content (WesternBTC bridge, 2026)
    "content-token",             # a cashuA/B token in content (PWANS / DENOS)
    "zero-ecash",                # no ecash and `amount "0"` (NDK failed-mint publish)
    "no-proof",                  # nothing that looks like ecash at all
)


class NutzapValue(NamedTuple):
    """kind-9321 (nutzaps) value of one event, and how it was decided."""
    amount_sats: int
    value_source: str             # one of NUTZAP_VALUE_SOURCES
    no_value_reason: str | None   # one of NUTZAP_NO_VALUE_REASONS when value_source == "none"
    proofs_counted: int           # proofs whose amount is in amount_sats
    proofs_rejected: int          # `proof` tags that were not a countable proof (incl. duplicates)


def _cashu_proof_amount(raw) -> int | None:
    """A Cashu proof `amount` as a positive integer, or None.

    Accepts a JSON integer or a decimal-digit string (cashu-ts 4 serializes amounts as
    strings unless the app opts into `serializeProofs`). Refuses booleans, floats
    (including 1.0), signs, whitespace, exponents, non-ASCII digits, zero, negatives,
    and anything above `_MAX_PLAUSIBLE_SATS`: Cashu amounts are positive powers of two.
    """
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        val = raw
    elif isinstance(raw, str) and raw.isascii() and raw.isdigit() and len(raw) <= 19:
        # The length guard comes first: int() refuses strings over 4,300 digits
        # (ValueError, Python 3.11+), and a hostile public event must not be able to
        # abort the load. 19 digits already exceeds _MAX_PLAUSIBLE_SATS.
        val = int(raw)
    else:
        return None
    if val <= 0 or val > _MAX_PLAUSIBLE_SATS:
        return None
    return val


def _parse_nutzap_proof(raw) -> tuple[int, tuple[str, str]] | str:
    """One `proof` tag value -> (amount, (secret, C)) when it is a countable proof, else
    the rejection cause: "token" (a Cashu token, not a proof), "not-json", "not-proof"
    (JSON but not a NUT-00 (Cashu notation) proof object) or "bad-amount"."""
    if not isinstance(raw, str):
        return "not-proof"
    try:
        obj = json.loads(raw)
    except (ValueError, RecursionError):
        return "token" if _CASHU_TOKEN_RE.match(raw.strip()) else "not-json"
    if not isinstance(obj, dict):
        return "not-proof"
    if not all(f in obj for f in _NUT00_PROOF_FIELDS):
        # The wrapped {proofs, mint} object (Robots Building Education's older builder)
        # is a token, not a proof: no receiver reads it as one.
        return "token" if isinstance(obj.get("proofs"), list) else "not-proof"
    if not (isinstance(obj["secret"], str) and obj["secret"]
            and isinstance(obj["C"], str) and obj["C"]):
        return "not-proof"
    amount = _cashu_proof_amount(obj["amount"])
    if amount is None:
        return "bad-amount"
    return amount, (obj["secret"], obj["C"])


def _content_proof_list(content) -> bool:
    """True when content is a non-empty JSON list of objects that each carry `amount`
    (the 2024 draft and the 2026 WesternBTC bridge put their proofs there)."""
    if not isinstance(content, str) or not content.lstrip().startswith("["):
        return False
    try:
        obj = json.loads(content)
    except (ValueError, RecursionError):
        return False
    return bool(obj) and isinstance(obj, list) and all(
        isinstance(p, dict) and "amount" in p for p in obj)


def _nutzap_value(event: dict) -> NutzapValue:
    """kind-9321 (nutzaps) value in SATS: the sum of the Cashu proofs in `proof` tags.

    Each proof counts once per event, keyed by (secret, C); a repeated tag is rejected,
    not double-counted. (Counting a proof once ACROSS events is not done, and would not
    be this function.) Proofs are treated as sat-denominated: every keyset an archived
    nutzap uses that could be resolved is sat. Never
    raises; malformed input only ever lowers the value.
    """
    tags = event.get("tags") or []
    total = 0
    counted = 0
    rejected = 0
    causes = set()
    seen = set()
    has_proof_tag = False
    for tag in tags:
        if not isinstance(tag, list) or len(tag) < 2 or tag[0] != "proof":
            continue
        has_proof_tag = True
        parsed = _parse_nutzap_proof(tag[1])
        if isinstance(parsed, str):
            rejected += 1
            causes.add(parsed)
            continue
        amount, key = parsed
        if key in seen:
            rejected += 1
            causes.add("duplicate")
            continue
        seen.add(key)
        total += amount
        counted += 1

    if counted:
        if total > _MAX_PLAUSIBLE_SATS:
            return NutzapValue(0, "none", "implausible-amount", 0, rejected + counted)
        return NutzapValue(total, "proof_tags", None, counted, rejected)

    def none(reason: str) -> NutzapValue:
        return NutzapValue(0, "none", reason, 0, rejected)

    names = {t[0] for t in tags if isinstance(t, list) and t and isinstance(t[0], str)}
    if "encryption" in names:
        return none("encrypted")
    if has_proof_tag:
        return none("cashu-token-in-proof-tag" if "token" in causes else "invalid-proof-tag")
    content = event.get("content")
    if _content_proof_list(content):
        unit = None
        for t in tags:
            if isinstance(t, list) and len(t) >= 2 and t[0] == "amount":
                unit = str(t[2]).lower() if len(t) >= 3 else None
                break
        return none("draft-content" if unit in ("msat", "msats") else "content-proofs")
    if isinstance(content, str) and _CASHU_TOKEN_RE.search(content):
        return none("content-token")
    for t in tags:
        if isinstance(t, list) and len(t) >= 2 and t[0] == "amount":
            if str(t[1]).strip() == "0":
                return none("zero-ecash")
            break
    return none("no-proof")


def load_kind_9321_nutzaps(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-9321 nutzaps (NIP-61) into events_kind_9321_nutzaps.

    Each event represents one nutzap payment. We extract:
      - sender from the event's top-level pubkey
      - recipient from the first `p` tag that is 64-hex (lowercased), else NULL
      - target_event_id from the first `e` tag that is 64-hex (NIP-01 (basic
        protocol) reference), else NULL
      - mint_url from the `u` tag (first occurrence, normalized)
      - amount_sats, value_source, no_value_reason via `_nutzap_value`: the sum
        of the Cashu proofs in `proof` tags and nothing else. An event with no
        countable proof is kept, worth 0, with value_source 'none' and the reason;
        readers count only value_source = 'proof_tags' rows as payments.

    Events without a normalizable `u` tag are skipped — we can't attribute the
    payment to a mint without it. Other fields are best-effort.

    The `amount` tag is never read for value. Where proofs exist, it is compared
    with their sum and the disagreements are only counted and logged.
    """
    table = "events_kind_9321_nutzaps"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 9321, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocked = 0
    total_no_mint = 0
    total_bad_time = 0
    value_events = {"proof_tags": 0, "none": 0}
    value_sats = 0
    no_value_reasons: dict[str, int] = {}
    proofs_rejected = 0
    tag_disagreements = 0
    tag_disagreement_ids: list[str] = []

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                if not all([event_id, pubkey, created_at is not None]):
                    total_skipped += 1
                    continue

                if blocklist and pubkey in blocklist:
                    total_blocked += 1
                    continue

                if not isinstance(created_at, int) or not 0 <= created_at < _MAX_NUTZAP_CREATED_AT:
                    # nozzl keeps future-dated events (clamping u64 to i64::MAX). Past
                    # 2100 to_timestamp() overflows in the MAU rebuild and the per-mint
                    # page, so the whole rebuild would fail every night.
                    total_bad_time += 1
                    continue

                recipient = None
                target = None
                mint_url = None
                for tag in e.get("tags", []):
                    if not isinstance(tag, list) or len(tag) < 2:
                        continue
                    name = tag[0]
                    if name == "p" and recipient is None:
                        recipient = _hex64_or_none(tag[1])
                    elif name == "e" and target is None:
                        target = _hex64_or_none(tag[1])
                    elif name == "u" and mint_url is None:
                        mint_url = _normalize_mint_url(tag[1])

                if not mint_url:
                    total_no_mint += 1
                    continue

                # Value = the proofs in `proof` tags, nothing else. See _nutzap_value.
                try:
                    value = _nutzap_value(e)
                except Exception as ex:  # noqa: BLE001 - one event must never abort the load
                    # _nutzap_value is written not to raise. This is the structural
                    # guarantee: the TRUNCATE has already committed, so an exception
                    # here would leave the serving table empty on every refresh.
                    print(f"    WARN kind-9321 value parse failed on {event_id}: {type(ex).__name__}: {str(ex)[:120]}")
                    value = NutzapValue(0, "none", "invalid-proof-tag", 0, 0)
                value_events[value.value_source] += 1
                value_sats += value.amount_sats
                proofs_rejected += value.proofs_rejected
                if value.no_value_reason:
                    no_value_reasons[value.no_value_reason] = no_value_reasons.get(value.no_value_reason, 0) + 1
                else:
                    # Observability only: how often the `amount` tag disagrees with
                    # the proofs it rides along with. Never used for value.
                    tag_sats = _amount_tag_to_sats(e.get("tags", []), str(event_id), warn=False)
                    if tag_sats is not None and tag_sats != value.amount_sats:
                        tag_disagreements += 1
                        if len(tag_disagreement_ids) < 5:
                            tag_disagreement_ids.append(str(event_id)[:16])

                batch.append((
                    str(event_id),
                    str(pubkey),
                    int(created_at),
                    recipient,
                    target,
                    mint_url,
                    value.amount_sats,
                    value.value_source,
                    value.no_value_reason,
                ))

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_9321_nutzaps, cur, batch, conn, db_url,
                        f"kind-9321 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_9321_nutzaps, cur, batch, conn, db_url,
                    f"kind-9321 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 9321, table, capture)

    extra = ""
    if total_blocked:
        extra += f", {total_blocked:,} blocklisted"
    if total_no_mint:
        extra += f", {total_no_mint:,} no/invalid mint url"
    if total_bad_time:
        extra += f", {total_bad_time:,} created_at out of range"
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{extra}")
    # Counted over the rows handed to COPY (before the id dedup, which the upstream
    # extract makes a no-op).
    reasons = ", ".join(f"{r}={no_value_reasons[r]:,}" for r in NUTZAP_NO_VALUE_REASONS if r in no_value_reasons)
    print(f"  Value: {value_events['proof_tags']:,} events with proof-tag value ({value_sats:,} sats), "
          f"{value_events['none']:,} with none [{reasons}]; {proofs_rejected:,} proof tags rejected")
    if tag_disagreements:
        print(f"  Info: {tag_disagreements:,} events' amount tag disagrees with their proof sum "
              f"(not used for value), e.g. {', '.join(tag_disagreement_ids)}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_9321_nutzaps(cur, batch: list[tuple]) -> int:
    return _copy_rows_via_stdin(
        cur, "events_kind_9321_nutzaps",
        ["event_id", "pubkey", "created_at", "recipient_pubkey", "target_event_id",
         "mint_url", "amount_sats", "value_source", "no_value_reason"],
        batch,
    )


def load_kind_10019_nutzap_configs(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-10019 nutzap configs (NIP-61) into events_kind_10019_nutzap_configs.

    Kind-10019 is a replaceable kind: each pubkey has at most one current config
    in the relay's view. We store everything we receive (PK on event_id) so the
    same event from multiple relays is deduplicated, and let queries resolve
    the latest per pubkey via DISTINCT ON. mint_urls is a deduplicated array
    of normalized URLs from each `mint` tag in the event.
    """
    table = "events_kind_10019_nutzap_configs"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 10019, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocked = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                if not all([event_id, pubkey, created_at is not None]):
                    total_skipped += 1
                    continue

                if blocklist and pubkey in blocklist:
                    total_blocked += 1
                    continue

                seen: set[str] = set()
                mint_urls: list[str] = []
                for tag in e.get("tags", []):
                    if not isinstance(tag, list) or len(tag) < 2 or tag[0] != "mint":
                        continue
                    norm = _normalize_mint_url(tag[1])
                    if norm and norm not in seen:
                        seen.add(norm)
                        mint_urls.append(norm)

                batch.append((
                    str(event_id),
                    str(pubkey),
                    int(created_at),
                    mint_urls,
                ))

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_10019_nutzap_configs, cur, batch, conn, db_url,
                        f"kind-10019 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_10019_nutzap_configs, cur, batch, conn, db_url,
                    f"kind-10019 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 10019, table, capture)

    extra = f", {total_blocked:,} blocklisted" if total_blocked else ""
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{extra}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_10019_nutzap_configs(cur, batch: list[tuple]) -> int:
    # col 3 (mint_urls) is TEXT[].
    return _copy_rows_via_stdin(
        cur, "events_kind_10019_nutzap_configs",
        ["event_id", "pubkey", "created_at", "mint_urls"],
        batch,
        array_cols=frozenset({3}),
    )


_RELAY_HOSTNAME_RE = re.compile(r"^[a-zA-Z0-9.-]+$")


def _normalize_relay_url(url: str | None) -> str | None:
    """Normalize a Nostr relay URL: strip whitespace, drop trailing slash,
    require ws(s) scheme + DNS-like hostname, lowercase scheme + host
    (preserve path case). Returns None for invalid input. Used by the
    kind-10002 loader so multiple users listing the same relay (with
    case-different host or trailing-slash variants) are counted together.
    """
    from urllib.parse import urlparse, urlunparse
    if not url or not isinstance(url, str):
        return None
    s = url.strip().rstrip("/")
    if not _url_text_ok(s):
        return None
    # RFC 3986 schemes are case-insensitive, so check the prefix in lowercase.
    s_lower = s.lower()
    if not (s_lower.startswith("ws://") or s_lower.startswith("wss://")):
        return None
    try:
        parsed = urlparse(s)
    except Exception:
        return None
    # urlparse can succeed but `.hostname` / `.port` accessors may still
    # raise on weird payloads (e.g. `wss://\\\\relay.fountain.fm`). Wrap.
    try:
        host = parsed.hostname
        port = parsed.port
    except (ValueError, AttributeError):
        return None
    if not host:
        return None
    # Reject non-DNS hostnames (backslashes, spaces, etc.). urlparse will
    # happily produce these from malformed input — we'd rather drop the URL
    # than store a value that can't possibly resolve.
    if not _RELAY_HOSTNAME_RE.match(host):
        return None
    # Lowercase scheme + host; keep path/query as-is (paths can be
    # case-sensitive depending on relay implementation).
    netloc = host.lower()
    if port:
        netloc = f"{netloc}:{port}"
    return urlunparse((parsed.scheme.lower(), netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))


def _parse_kind_10002_event(event: dict) -> tuple[list[str], list[str], list[str]]:
    """Extract (relay_urls, read_relay_urls, write_relay_urls) from a single
    NIP-65 kind-10002 event. Pure function — easy to unit-test without a DB.

    NIP-65 marker handling:
      - `["r", "wss://X"]`            → no marker; default = both read and write
      - `["r", "wss://X", "read"]`    → read only
      - `["r", "wss://X", "write"]`   → write only
      - any other marker              → treated as no marker (both); we'd rather
                                        over-count than silently drop a relay
                                        the user did declare

    Within one event, duplicates of the same (relay, marker) are deduplicated.
    A user can list the same relay twice with separate `read` and `write`
    markers; that's equivalent to a single no-marker entry — both arrays end
    up containing the relay.

    URLs are normalized via _normalize_relay_url first; un-normalizable URLs
    are silently dropped.
    """
    read_set: set[str] = set()
    write_set: set[str] = set()
    read_urls: list[str] = []
    write_urls: list[str] = []
    all_set: set[str] = set()
    all_urls: list[str] = []
    for tag in event.get("tags", []):
        if not isinstance(tag, list) or len(tag) < 2 or tag[0] != "r":
            continue
        norm = _normalize_relay_url(tag[1])
        if not norm:
            continue
        marker = tag[2].strip().lower() if len(tag) >= 3 and isinstance(tag[2], str) else ""
        if marker == "read":
            if norm not in read_set:
                read_set.add(norm)
                read_urls.append(norm)
        elif marker == "write":
            if norm not in write_set:
                write_set.add(norm)
                write_urls.append(norm)
        else:
            # No marker (or unknown marker): NIP-65 says both.
            if norm not in read_set:
                read_set.add(norm)
                read_urls.append(norm)
            if norm not in write_set:
                write_set.add(norm)
                write_urls.append(norm)
        if norm not in all_set:
            all_set.add(norm)
            all_urls.append(norm)
    return all_urls, read_urls, write_urls


def load_kind_10002_relay_lists(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = "", refresh_mv: bool = True):
    """Load kind-10002 relay-list metadata (NIP-65) into events_kind_10002_relay_lists.

    Kind-10002 is a replaceable kind: each pubkey has at most one current
    relay list in the relay's view. We store everything we receive (PK on
    event_id) so the same event from multiple sync sources is deduplicated,
    and let queries resolve the latest per pubkey via DISTINCT ON.

    Each `r` tag carries a relay URL plus an optional second arg ("read" or
    "write"). No marker = both. We expand into three deduplicated arrays:
      - relay_urls       — every relay mentioned (union of read and write)
      - read_relay_urls  — relays the user reads from (no-marker OR "read")
      - write_relay_urls — relays the user writes to (no-marker OR "write")
    Storing both expanded arrays makes the per-relay leaderboard's read /
    write / both columns a single GIN index lookup each, no per-row tag
    inspection at query time.
    """
    table = "events_kind_10002_relay_lists"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 10002, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocked = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                if not all([event_id, pubkey, created_at is not None]):
                    total_skipped += 1
                    continue

                if blocklist and pubkey in blocklist:
                    total_blocked += 1
                    continue

                all_urls, read_urls, write_urls = _parse_kind_10002_event(e)

                batch.append((
                    str(event_id),
                    str(pubkey),
                    int(created_at),
                    all_urls,
                    read_urls,
                    write_urls,
                ))

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_10002_relay_lists, cur, batch, conn, db_url,
                        f"kind-10002 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_10002_relay_lists, cur, batch, conn, db_url,
                    f"kind-10002 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 10002, table, capture)

    # --- Refresh /relays materialized views ---
    # All three MVs derive from this table's latest-per-pubkey snapshot;
    # mirror the follower_counts pattern (refreshed inside the kind-3
    # loader). CONCURRENTLY needs the unique indexes declared in init.sql.
    # The Dagster pipeline passes --no-refresh-mv (refresh_mv=False) and runs
    # the refresh in a separate downstream MV asset instead; the inline path
    # below is the legacy/standalone behavior.
    if refresh_mv:
        for mv in (
            "relay_stats_mv",
            "relay_per_user_distribution_mv",
            "relay_tier_user_counts_mv",
        ):
            print(f"  Refreshing {mv} materialized view...")
            t0 = time.monotonic()
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
            conn.commit()
            elapsed_mv = time.monotonic() - t0
            print(f"    Done in {elapsed_mv:.1f}s")
            emit_timing("pg-load", f"{mv}/matview-refresh", f"{elapsed_mv:.1f}")

    extra = f", {total_blocked:,} blocklisted" if total_blocked else ""
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{extra}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_10002_relay_lists(cur, batch: list[tuple]) -> int:
    # cols 3,4,5 (relay_urls, read_relay_urls, write_relay_urls) are TEXT[].
    # Empty lists encode as '{}' (matches the column DEFAULT '{}' and the old
    # psycopg2 []-adaptation), never NULL — these columns are NOT NULL.
    return _copy_rows_via_stdin(
        cur, "events_kind_10002_relay_lists",
        ["event_id", "pubkey", "created_at", "relay_urls", "read_relay_urls",
         "write_relay_urls"],
        batch,
        array_cols=frozenset({3, 4, 5}),
    )


def load_kind_7376_nutzap_redemptions(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-7376 nutzap-redemption / NIP-60 spending-history events into
    events_kind_7376_nutzap_redemptions.

    Content is encrypted (NIP-44/NIP-04) and stored verbatim. Public tags
    that ~22% of events expose — `e` (consumed token), `p` (recipient),
    `mint` (mint URL), `amount` (sats) — are lifted into typed columns so
    callers can join without scanning JSONB. Only the first occurrence of
    each tag name is kept; the rest are ignored.
    """
    table = "events_kind_7376_nutzap_redemptions"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 7376, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocked = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                content = e.get("content", "")
                if not all([event_id, pubkey, created_at is not None]):
                    total_skipped += 1
                    continue

                if blocklist and pubkey in blocklist:
                    total_blocked += 1
                    continue

                target = None
                recipient = None
                mint_url = None
                for tag in e.get("tags", []):
                    if not isinstance(tag, list) or len(tag) < 2:
                        continue
                    name = tag[0]
                    if name == "e" and target is None:
                        target = tag[1]
                    elif name == "p" and recipient is None:
                        recipient = tag[1]
                    elif name == "mint" and mint_url is None:
                        mint_url = _normalize_mint_url(tag[1])

                # kind-7376 (nutzap-redemptions) carries no Cashu proofs and its content
                # is NIP-44 (encrypted payloads) ciphertext, so the unit-aware `amount` tag is the only value
                # it exposes (None when absent). It is SELF-REPORTED by the redeemer:
                # label it so wherever it is shown. Nothing reads it today.
                amount_sats = _amount_tag_to_sats(e.get("tags", []), str(event_id))

                batch.append((
                    str(event_id),
                    str(pubkey),
                    int(created_at),
                    content,
                    target,
                    recipient,
                    mint_url,
                    amount_sats,
                ))

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_7376_nutzap_redemptions, cur, batch, conn, db_url,
                        f"kind-7376 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_7376_nutzap_redemptions, cur, batch, conn, db_url,
                    f"kind-7376 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 7376, table, capture)

    extra = f", {total_blocked:,} blocklisted" if total_blocked else ""
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{extra}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_7376_nutzap_redemptions(cur, batch: list[tuple]) -> int:
    return _copy_rows_via_stdin(
        cur, "events_kind_7376_nutzap_redemptions",
        ["event_id", "pubkey", "created_at", "content", "target_event_id",
         "recipient_pubkey", "mint_url", "amount_sats"],
        batch,
    )


def load_kind_3_follows(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = "", refresh_mv: bool = True):
    """Load kind-3 follow lists into follows table + refresh follower_counts view.

    Two-pass approach to keep memory low:
      Pass 1: Stream JSONL to find latest event per pubkey (by created_at).
      Pass 2: Re-stream, explode p-tags from latest events into follows edges.
    Blocklist filtering on both follower (author) and followee (p-tag target).
    """
    cur = conn.cursor()
    _table_start = time.monotonic()

    if truncate:
        _td_start = time.monotonic()
        cur.execute("TRUNCATE follows")
        dropped = _drop_secondary_indexes(cur, conn, 3)
        conn.commit()
        _print_truncated("follows", dropped)
        emit_timing("pg-load", "follows/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    # --- Pass 1: find latest event per pubkey ---
    print("  Pass 1: scanning for latest event per pubkey...")
    t0 = time.monotonic()
    latest: dict[str, tuple[int, str]] = {}  # pubkey -> (created_at, event_id)
    total_events = 0
    total_blocked_authors = 0

    for path in paths:
        progress = _ProgressPrinter(path)
        with _open_text(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue

                total_events += 1
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                event_id = e.get("id")

                if not all([pubkey, created_at is not None, event_id]):
                    continue

                if blocklist and pubkey in blocklist:
                    total_blocked_authors += 1
                    continue

                if pubkey not in latest or created_at > latest[pubkey][0]:
                    latest[pubkey] = (created_at, event_id)

                if total_events % 50_000 == 0 and progress.should_print():
                    progress.emit(
                        f,
                        f"{total_events:,} events scanned, {len(latest):,} unique pubkeys",
                    )

    elapsed1 = time.monotonic() - t0
    print(f"    {total_events:,} events scanned in {elapsed1:.1f}s — "
          f"{len(latest):,} unique pubkeys, {total_blocked_authors:,} blocked authors")
    emit_timing("pg-load", "follows/pass1", f"{elapsed1:.1f}")

    # --- Pass 2: explode p-tags from latest events into edges ---
    print("  Pass 2: inserting follow edges...")
    t0 = time.monotonic()
    total_edges = 0
    total_blocked_followees = 0
    batch: list[tuple[str, str]] = []

    for path in paths:
        progress = _ProgressPrinter(path)
        with _open_text(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue

                pubkey = e.get("pubkey")
                event_id = e.get("id")
                if not pubkey or not event_id:
                    continue

                # Only process this event if it's the latest for this pubkey
                entry = latest.get(pubkey)
                if not entry or entry[1] != event_id:
                    continue

                for tag in e.get("tags", []):
                    if not (isinstance(tag, list) and len(tag) >= 2 and tag[0] == "p"):
                        continue
                    followee = tag[1]
                    if not followee or len(followee) != 64:
                        continue
                    if blocklist and followee in blocklist:
                        total_blocked_followees += 1
                        continue
                    batch.append((str(pubkey), str(followee)))

                    if len(batch) >= BATCH_SIZE:
                        inserted, conn, cur = _insert_batch_with_retry(
                            _copy_batch_kind_3_follows, cur, batch, conn, db_url,
                            f"kind-3 edges batch at {total_edges + len(batch)} edges")
                        total_edges += inserted
                        batch = []
                        if progress.should_print():
                            progress.emit(f, f"{total_edges:,} edges inserted")

    if batch:
        inserted, conn, cur = _insert_batch_with_retry(
            _copy_batch_kind_3_follows, cur, batch, conn, db_url,
            "kind-3 edges final batch")
        total_edges += inserted

    elapsed2 = time.monotonic() - t0
    rate = total_edges / elapsed2 if elapsed2 > 0 else 0
    print(f"    {total_edges:,} edges inserted in {elapsed2:.1f}s ({rate:,.0f} edges/s)")
    if total_blocked_followees:
        print(f"    {total_blocked_followees:,} blocklisted followee edges filtered")
    emit_timing("pg-load", "follows/pass2", f"{elapsed2:.1f}")

    if truncate:
        _create_secondary_indexes(cur, conn, 3, "follows")

    # --- Refresh materialized view --- (skipped under --no-refresh-mv; see kind-10002 note)
    if refresh_mv:
        print("  Refreshing follower_counts materialized view...")
        t0 = time.monotonic()
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY follower_counts")
        conn.commit()
        elapsed3 = time.monotonic() - t0
        print(f"    Done in {elapsed3:.1f}s")
        emit_timing("pg-load", "follower_counts/matview-refresh", f"{elapsed3:.1f}")

    print(f"  Total: {total_edges:,} follow edges loaded")
    emit_timing("pg-load", "follows", f"{time.monotonic() - _table_start:.1f}", total_edges)
    return total_edges


def _copy_batch_kind_3_follows(cur, batch: list[tuple]) -> int:
    """Insert a batch of (follower_pubkey, followee_pubkey) into follows."""
    values_list = ",".join(
        cur.mogrify("(%s, %s)", row).decode() for row in batch
    )
    sql = f"INSERT INTO follows (follower_pubkey, followee_pubkey) VALUES {values_list} ON CONFLICT DO NOTHING"
    cur.execute(sql)
    return cur.rowcount


def load_kind_32267_app_catalog(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-32267 Zapstore app catalog into events_kind_32267_app_catalog.

    Parameterized-replaceable events: each (pubkey, d_tag) tuple is one app.
    Dedup is by event_id, and the extract (pipeline/sql.py ->
    extract_addressable_latest.sql) hands this loader the LATEST event per
    (pubkey, d_tag), so one coordinate is one row. The Zapstore/not-Zapstore split
    below depends on that: if two versions of one coordinate ever arrived in the
    same load and classified differently, the coordinate would sit in both tables
    and the follow would delete a live app's releases and zaps. The assertion after
    the read loop is what fails loudly if the extract ever changes. The d_tag is
    extracted from the first ["d", ...] entry in the tags array; events without one
    are skipped (the kind-32267 (zapstore-app-catalog) spec requires it).

    Blocklist accepted for interface consistency but not applied — the Zapstore
    catalog is small (~2.6K events) and not subject to the kind-1/kind-7 spam
    filters.

    Listings that are not Zapstore apps go to events_kind_32267_app_catalog_excluded
    instead, labelled with the reason — see classify_kind_32267 for the rule (it is
    Zapstore's own) and the schema comment for why they are sorted here rather than
    flagged in place. Every reader of the catalog table therefore sees Zapstore apps
    only, with no query change.
    """
    table = "events_kind_32267_app_catalog"
    excl_table = "events_kind_32267_app_catalog_excluded"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if not truncate:
        # The split is only coherent over a whole catalog. On an append load, an event
        # already in the catalog from a load that predates this classification would
        # ALSO land in the excluded table - counted in both - and a re-run would hit the
        # excluded table's live primary key. The pipeline always truncates (it is the
        # only mode it has), so refuse the other path loudly rather than carry a
        # double-counting branch nothing exercises. Everything below therefore assumes
        # truncate=True.
        raise SystemExit(
            "kind-32267 (zapstore-app-catalog) must be loaded with --truncate: the "
            "Zapstore/not-Zapstore split classifies the whole catalog at once.")

    # ---- pass 1: read and classify, touching no table --------------------
    #
    # The whole catalogue is read and classified BEFORE anything is truncated,
    # so the disjointness check below can refuse a contradictory classification
    # while the live tables are still intact. ~5.7K rows at about a kilobyte
    # each is a few MB resident; that is affordable for this kind and is the
    # price of a safe refusal. Do NOT copy this shape to a firehose kind.
    total_skipped = 0
    total_missing_d = 0
    total_lines = 0
    kept_rows: list[tuple] = []
    excluded_rows: list[tuple] = []
    excl_counts: dict[str, int] = {}
    # Coordinates on each side, keyed on the CLEANED d_tag - the same value the
    # row stores and excluded_listing_coords reads back, so the check here and
    # the follow in the three dependent loads compare like with like.
    kept_coords: set[tuple[str, str]] = set()
    excl_coords: set[tuple[str, str]] = set()
    kept_appstore = 0

    for path in paths:
        t0 = time.monotonic()
        file_lines = 0
        print(f"  Reading {path}...")
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                content = e.get("content", "")

                if not all([event_id, pubkey, created_at is not None]):
                    total_skipped += 1
                    continue

                # Walk every tag once, bucketing into singleton / multi / extras
                # columns (+ the name<-title / summary<-description fallback for
                # web-app/PWA publishers). The d-tag is required for the
                # parameterized-replaceable kind; everything outside the
                # publisher taxonomy is preserved verbatim in the `extra_tags`
                # JSONB catch-all so we can query the long tail without a
                # schema migration. See _parse_kind_32267_tags.
                d_tag, singletons, multis, extras = _parse_kind_32267_tags(e.get("tags", []))

                if not d_tag:
                    total_missing_d += 1
                    continue

                def _clean(s):
                    return s.replace("\x00", "") if isinstance(s, str) else s

                extra_tags_jsonb = json.dumps(extras) if extras else None
                clean_content = _pg_safe_text(str(content)) if content else ""
                row = (
                    str(event_id),
                    str(pubkey),
                    int(created_at),
                    _clean(d_tag),
                    clean_content,
                    _clean(singletons["name"]),
                    _clean(singletons["summary"]),
                    _clean(singletons["icon"]),
                    _clean(singletons["url"]),
                    _clean(singletons["repository"]),
                    _clean(singletons["license"]),
                    _clean(singletons["a_tag"]),
                    [_clean(s) for s in multis["images"]] or None,
                    [_clean(s) for s in multis["topics"]] or None,
                    [_clean(s) for s in multis["platforms"]] or None,
                    [_clean(s) for s in multis["h_tags"]] or None,
                    extra_tags_jsonb,
                )

                # Zapstore's rule first, ours only over what it accepts, and the
                # column records WHICH said no - the separation is the whole design
                # and it should survive into the data, not live only in the code.
                excl, excl_source = classify_kind_32267(d_tag, singletons, multis, extras), "zapstore"
                if excl is None:
                    excl, excl_source = zapstore_policy_exclusion(multis), "policy"
                if excl is not None:
                    excluded_rows.append(row + (excl, excl_source))
                    excl_counts[excl] = excl_counts.get(excl, 0) + 1
                    excl_coords.add((row[1], row[3]))
                    continue
                kept_coords.add((row[1], row[3]))
                kept_rows.append(row)
                if _APPSTORE_D_TAG_RE.match(row[3]):
                    kept_appstore += 1

        total_lines += file_lines
        elapsed = time.monotonic() - t0
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_lines:,} lines read in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    both = kept_coords & excl_coords
    if both:
        # See the docstring: one coordinate must land on exactly one side, or the
        # three dependent loads delete a live app's rows. This runs before the
        # TRUNCATE, so refusing here leaves both live tables exactly as they were.
        raise SystemExit(
            f"kind-32267 (zapstore-app-catalog): {len(both):,} coordinate(s) classified "
            f"BOTH ways, e.g. {sorted(both)[:3]}. The extract is no longer latest-per-"
            "coordinate, and the follow would drop a kept listing's releases and zaps.")

    # ---- pass 2: truncate and load ---------------------------------------
    _td_start = time.monotonic()
    cur.execute(f"TRUNCATE {table}")
    capture = _drop_indexes_for_load(cur, conn, 32267, table)
    dropped = capture["dropped"]
    _print_truncated(table, dropped)
    emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    _c0 = time.monotonic()
    total_loaded = 0
    total_dupes = 0
    for i in range(0, len(kept_rows), BATCH_SIZE):
        batch = kept_rows[i:i + BATCH_SIZE]
        inserted, conn, cur = _insert_batch_with_retry(
            _copy_batch_kind_32267_app_catalog, cur, batch, conn, db_url,
            f"kind-32267 (zapstore-app-catalog) catalog batch {i // BATCH_SIZE + 1}")
        total_loaded += inserted
        total_dupes += len(batch) - inserted
    _copy_elapsed = time.monotonic() - _c0
    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    _rebuild_indexes_after_load(cur, conn, 32267, table, capture)

    # The excluded rows go in LAST, after the catalog's indexes are back, and the
    # TRUNCATE that clears them is in the SAME transaction as the COPY that
    # refills them - one commit for the pair. That matters because the three
    # dependent loads read this table to decide what to drop: if the TRUNCATE
    # committed on its own, any failure before the COPY landed would leave the
    # table committed-EMPTY, a follower re-materialised afterwards would drop
    # nothing, and `loaded_at` could not report it (an empty table has no age).
    # Rolling back instead leaves the PREVIOUS refresh's rows, which is wrong in a
    # way `loaded_at` names out loud. No retry wrapper here for the same reason:
    # a reconnect mid-transaction must discard the TRUNCATE with it.
    _x0 = time.monotonic()
    excl_loaded = 0
    try:
        cur.execute(f"TRUNCATE {excl_table}")
        for i in range(0, len(excluded_rows), BATCH_SIZE):
            excl_loaded += _copy_batch_kind_32267_app_catalog_excluded(
                cur, excluded_rows[i:i + BATCH_SIZE])
        conn.commit()
    except Exception:
        conn.rollback()
        print(f"  ERROR: {excl_table} not written; the previous refresh's exclusions are "
              "still in place and every dependent load will report their age",
              file=sys.stderr, flush=True)
        raise
    # Unconditional, so a zero-exclusion run leaves a 0 in the pipeline summary
    # rather than a hole a reader has to interpret.
    emit_timing("pg-load", excl_table, f"{time.monotonic() - _x0:.1f}", excl_loaded)
    print(f"  Not Zapstore apps: {len(excluded_rows):,} listing(s) -> {excl_table} "
          f"({', '.join(f'{k} {v:,}' for k, v in sorted(excl_counts.items())) or 'none'})")

    # The shadow population, called out because it is the only one where dropping a
    # listing also drops rows belonging to an app the catalog still holds. These are
    # one app published twice under two keys - the relay's AppOwnership allows only
    # one, so the loser is a coordinate relay.zapstore.dev no longer serves - and the
    # dependent loads drop its releases, zaps and stack references along with it. 3 on
    # 2026-10-28 (Amber, Citrine, URLCheck), each a platform-less row shadowing a live
    # one. A jump here means duplicate d-tags are growing, not that the rule changed.
    kept_d_tags = {d for (_, d) in kept_coords}
    shadowed = {c for c in excl_coords if c[1] in kept_d_tags}
    if shadowed:
        print(f"  Of those, {len(shadowed):,} share an app id with a listing the catalog KEEPS "
              "(one app, two publisher keys) - their releases and zaps are dropped too")

    # Tripwire for the one way the iOS policy can be walked around: it keys on
    # platforms, so the day an App Store pointer also tags `web` or an android
    # platform it is kept and goes straight back onto the Publishers leaderboard.
    # Every `appstore.<digits>` listing we have ever seen is one of those 56. A
    # non-zero count here is not a failure, it is a prompt to go and look.
    if kept_appstore:
        print(f"  NOTE: {kept_appstore:,} kept listing(s) have an `appstore.<id>` app id - the "
              "iOS-only policy did not catch them, so they now carry another platform")

    missing_msg = f", {total_missing_d:,} missing d-tag" if total_missing_d else ""
    print(f"  Total: {total_loaded:,} inserted from {total_lines:,} lines, "
          f"{total_dupes:,} duplicates skipped, {total_skipped:,} malformed{missing_msg}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded




# How old the exclusions may be before a dependent load refuses to follow them. The
# refresh is daily, so anything past a day means the catalog load did not run in it.
_EXCLUSIONS_MAX_AGE_S = 24 * 3600


def excluded_listing_coords(conn) -> set[tuple[str, str]]:
    """The (pubkey, d_tag) of every listing the catalog load decided is not a Zapstore app.

    Read by the three loads that hang off the catalog - kind-30063 (zapstore-app-releases),
    kind-30267 (zapstore-app-stacks) and the kind-9735 (zap-receipts) zapstore subset - so a
    release, a stack reference or a zap receipt belonging to one of those listings is dropped
    with it. Zapstore's relay would not serve the listing, so the things that hang off it are
    not Zapstore data either; leaving them in is how the /zapstore growth chart ends up
    counting apps the catalog on the same page says do not exist.

    REQUIRES the catalog load to have run first. Within a refresh Dagster enforces that with
    an explicit asset dependency (`_kind_chain(after=[zapstore_catalog_table])`), and
    zapstore_refresh_job runs the whole group in one in-process executor. That ordering is
    per RUN, not per table: re-materialising one of these three assets on its own - the
    normal recovery action after a failed load - is allowed and reads whatever is in this
    table, which may be a previous refresh's. Hence `loaded_at`: the age is printed every
    time, and a stale read says so in the log instead of passing silently.

    A couple of hundred rows, so one round-trip into a set beats a join per batch.
    """
    cur = conn.cursor()
    # `OVER ()` cannot attach to EXTRACT - in the grammar a window clause hangs off
    # func_application, and EXTRACT(x FROM y) is a func_expr_common_subexpr - so the
    # window goes on max() and EXTRACT wraps the result. Written the other way round
    # this is a syntax error that fails every dependent load on every run.
    cur.execute(
        "SELECT pubkey, d_tag, extract(epoch FROM now() - max(loaded_at) OVER ()) "
        "FROM events_kind_32267_app_catalog_excluded")
    rows = cur.fetchall()
    cur.close()
    coords = {(pk, d) for pk, d, _ in rows}
    if not rows:
        print("  Excluded listings to follow: 0 (the exclusions table is EMPTY)")
        return coords
    age_s = float(rows[0][2])
    if age_s > _EXCLUSIONS_MAX_AGE_S:
        # Refuse rather than warn. Following a stale rule produces a green run whose
        # output is wrong in a way nothing downstream can see, and the recovery is one
        # command: materialise zapstore_catalog_table, then this asset again.
        raise SystemExit(
            f"kind-32267 (zapstore-app-catalog) exclusions are {age_s / 3600:.1f}h old "
            f"(limit {_EXCLUSIONS_MAX_AGE_S / 3600:.0f}h): the catalog load has not run this "
            "refresh, and following an out-of-date exclusion set would drop the wrong rows. "
            "Materialise zapstore_catalog_table first.")
    age = f"{age_s / 3600:.1f}h old" if age_s >= 3600 else f"{age_s:.0f}s old"
    print(f"  Excluded listings to follow: {len(coords):,} ({age})")
    return coords


def _copy_batch_kind_32267_app_catalog_excluded(cur, batch: list[tuple]) -> int:
    # The catalog's 17 columns in the same order, plus `excl` at 17 and `excl_source`
    # at 18 — so the array columns keep the indices
    # _copy_batch_kind_32267_app_catalog uses.
    return _copy_rows_via_stdin(
        cur, "events_kind_32267_app_catalog_excluded",
        ["event_id", "pubkey", "created_at", "d_tag", "content",
         "name", "summary", "icon", "url", "repository", "license", "a_tag",
         "images", "topics", "platforms", "h_tags", "extra_tags", "excl", "excl_source"],
        batch,
        array_cols=frozenset({12, 13, 14, 15}),
    )


def _copy_batch_kind_32267_app_catalog(cur, batch: list[tuple]) -> int:
    # 17 columns: 5 base (event_id, pubkey, created_at, d_tag, content) +
    # 7 singleton TEXTs + 4 TEXT[] at indices 12..15 (images/topics/platforms/h_tags)
    # + 1 JSONB at index 16 (extra_tags, a pre-encoded JSON string or None — flows
    # through the scalar path; JSON text is valid JSONB COPY input).
    return _copy_rows_via_stdin(
        cur, "events_kind_32267_app_catalog",
        ["event_id", "pubkey", "created_at", "d_tag", "content",
         "name", "summary", "icon", "url", "repository", "license", "a_tag",
         "images", "topics", "platforms", "h_tags", "extra_tags"],
        batch,
        array_cols=frozenset({12, 13, 14, 15}),
    )


def load_kind_30063_app_releases(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-30063 Zapstore app releases into events_kind_30063_app_releases.

    Parameterized-replaceable events keyed on (pubkey, d_tag), where d_tag is
    '<package>@<version>'. Same typed-publisher-tags + JSONB catch-all shape as
    kind-32267. When the bare 'i'/'version' tags are missing (~48% of corpus),
    we fall back to splitting d_tag on the last '@' to recover them.

    Blocklist accepted for interface consistency but not applied — Zapstore
    app releases are publisher-emitted (one CLI per project) and not subject
    to kind-1/kind-7 spam filters.
    """
    table = "events_kind_30063_app_releases"
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0
    # Releases of listings the catalog load rejected are dropped with them (see
    # excluded_listing_coords). Empty on an append load, where the catalog's excluded
    # table may not describe this input.
    excluded_coords = excluded_listing_coords(conn) if truncate else set()
    followed = 0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 30063, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_missing_d = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                content = e.get("content", "")

                if not all([event_id, pubkey, created_at is not None]):
                    total_skipped += 1
                    continue

                d_tag = None
                singletons: dict[str, str | None] = {
                    col: None for col in _K30063_SINGLETON_COLUMN.values()
                }
                multis: dict[str, list[str]] = {
                    col: [] for col in _K30063_MULTI_COLUMN.values()
                }
                extras: list[list] = []
                for tag in e.get("tags", []):
                    if not (isinstance(tag, list) and len(tag) >= 2):
                        continue
                    k = tag[0]
                    v = tag[1]
                    if k == "d":
                        if d_tag is None and isinstance(v, str):
                            d_tag = v
                    elif k in _K30063_SINGLETON_TAGS:
                        col = _K30063_SINGLETON_COLUMN[k]
                        if singletons[col] is None and isinstance(v, str):
                            singletons[col] = v
                    elif k in _K30063_MULTI_TAGS:
                        col = _K30063_MULTI_COLUMN[k]
                        if isinstance(v, str):
                            multis[col].append(v)
                    else:
                        extras.append(tag)

                if not d_tag:
                    total_missing_d += 1
                    continue

                # Fallback parse from d_tag when the bare 'i' / 'version' tags
                # are absent. Split on the last '@' so package ids that
                # somehow contain '@' (none observed today) still recover the
                # version segment correctly.
                if singletons["app_d_tag"] is None or singletons["version"] is None:
                    if "@" in d_tag:
                        parsed_app, parsed_version = d_tag.rsplit("@", 1)
                        if singletons["app_d_tag"] is None:
                            singletons["app_d_tag"] = parsed_app
                        if singletons["version"] is None:
                            singletons["version"] = parsed_version
                    elif singletons["app_d_tag"] is None:
                        # No '@' — assume the whole d_tag is the package id.
                        singletons["app_d_tag"] = d_tag

                def _clean(s):
                    return s.replace("\x00", "") if isinstance(s, str) else s

                # Follow the listing: a release of an app that is not a Zapstore app is
                # not a Zapstore release. Both coordinates the rest of the stack uses to
                # group releases are checked - the publisher's own (pubkey, app id), and
                # any `a` tag naming a listing, which is how a release can belong to
                # another key's listing.
                if excluded_coords:
                    coords = {(str(pubkey), singletons["app_d_tag"])}
                    a_tag = singletons["a_tag"]
                    if isinstance(a_tag, str):
                        m = _ZAPSTORE_A_TAG_REGEX.match(a_tag)
                        if m:
                            coords.add((m.group(1), m.group(2)))
                    if coords & excluded_coords:
                        followed += 1
                        continue

                extra_tags_jsonb = json.dumps(extras) if extras else None
                clean_content = _pg_safe_text(str(content)) if content else ""
                batch.append((
                    str(event_id),
                    str(pubkey),
                    int(created_at),
                    _clean(d_tag),
                    clean_content,
                    _clean(singletons["app_d_tag"]),
                    _clean(singletons["version"]),
                    _clean(singletons["channel"]),
                    _clean(singletons["a_tag"]),
                    _clean(singletons["url"]),
                    _clean(singletons["repo_url"]),
                    _clean(singletons["commit_sha"]),
                    _clean(singletons["alt"]),
                    _clean(singletons["release_notes"]),
                    [_clean(s) for s in multis["e_refs"]] or None,
                    [_clean(s) for s in multis["platforms"]] or None,
                    extra_tags_jsonb,
                ))

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_30063_app_releases, cur, batch, conn, db_url,
                        f"kind-30063 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_30063_app_releases, cur, batch, conn, db_url,
                    f"kind-30063 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    if excluded_coords:
        print(f"  Followed the listing: {followed:,} release(s) dropped with an excluded listing")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 30063, table, capture)

    missing_msg = f", {total_missing_d:,} missing d-tag" if total_missing_d else ""
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{missing_msg}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_30063_app_releases(cur, batch: list[tuple]) -> int:
    # 17 columns: 5 base + 9 singleton TEXTs + 2 TEXT[] at indices 14,15
    # (e_refs, platforms) + 1 JSONB at index 16 (extra_tags).
    return _copy_rows_via_stdin(
        cur, "events_kind_30063_app_releases",
        ["event_id", "pubkey", "created_at", "d_tag", "content",
         "app_d_tag", "version", "channel", "a_tag", "url", "repo_url", "commit_sha",
         "alt", "release_notes", "e_refs", "platforms", "extra_tags"],
        batch,
        array_cols=frozenset({14, 15}),
    )


def load_kind_30267_app_stacks(paths: list[str], conn, truncate: bool, blocklist: set[str] | None = None, db_url: str = ""):
    """Load kind-30267 Zapstore app stacks into events_kind_30267_app_stacks.

    Stacks are parameterized-replaceable on (pubkey, d_tag) where pubkey is the
    stack curator (the user who created/signed the event), NOT any app
    developer. App developer pubkeys live inside the `a` tag values
    ('32267:<DEV_PUBKEY>:<app_d_tag>') — pointed-at identities, not signers.

    BLOCKLIST IS APPLIED HERE — and intentionally only to event.pubkey
    (the stack curator/owner). Rationale: the App Stack Appearances
    leaderboard counts how many distinct stacks reference each app, so a
    spam/bot account could otherwise create many stacks listing whichever
    apps it wants to inflate and game the metric. Filtering at the curator
    level drops those events wholesale before they ever populate app_refs.

    The blocklist is *deliberately not* applied to app developer pubkeys
    inside the `a` tag values: a clean curator citing any developer
    (blocklisted or not) is kept; a blocklisted curator citing any
    developer (blocklisted or not) is dropped. The blocklist exists to
    drop spam/bot SIGNERS, and a developer being pointed-at by a clean
    curator's stack isn't spam regardless of who that developer is.

    Sibling kind-30063 / kind-32267 loaders take `blocklist` for interface
    consistency but do NOT filter on it — those events are publisher-emitted
    by zapstore-cli (one CLI per project) and not subject to the same
    spam-curation concern that motivates the kind-30267 filter. Don't copy
    that behaviour over here without thinking through the gaming vector.

    All event rows that pass the blocklist check land in this table —
    including private/encrypted stacks (Zapstore's per-user "Saved Apps" /
    "Installed Apps" features, see the init.sql comment block on this
    table). Encrypted stacks have empty `app_refs` because their app list
    is inside `event.content`, not in plaintext `a` tags. The leaderboard
    query filters by `array_length(app_refs, 1) > 0`, mirroring the
    public-stack filter used by Zapstore's own webapp (`!event.content`).

    Only element[1] of each `a` tag is stored ('32267:<pubkey>:<d_tag>').
    NIP-01 allows a relay-hint at element[2] (~3.3% of observed `a` tags
    carry one); we drop it as it's metadata about how to fetch the
    referenced event, not part of the reference identity.
    """
    table = "events_kind_30267_app_stacks"
    # A stack's references to listings the catalog load rejected are dropped from
    # app_refs. The readers unnest that array, so a dropped reference disappears from the
    # app-stack leaderboard and from each app's "appears in N stacks" count without any
    # query change. A stack the follow empties completely is dropped outright instead of
    # being stored with no references: `[...] or None` would write NULL, and every reader
    # (postgres.ts getZapstoreAppStackLeaderboards and the /zapstore app page,
    # nostrology_mcp/metric.py app_stack_count) reads NULL app_refs as ENCRYPTED, so an
    # emptied public stack would be counted as a private one. 1 stack is emptied today
    # (2026-10-28): c4e59ca0..., 47 references, every one an iOS-only listing.
    excluded_coords = excluded_listing_coords(conn) if truncate else set()
    followed = 0            # references removed from a stack that still loads
    emptied_by_follow = 0   # stacks dropped because every reference went
    emptied_refs = 0        # the references those dropped stacks held
    cur = conn.cursor()
    _table_start = time.monotonic()
    _copy_elapsed = 0.0

    if truncate:
        _td_start = time.monotonic()
        cur.execute(f"TRUNCATE {table}")
        capture = _drop_indexes_for_load(cur, conn, 30267, table)
        dropped = capture["dropped"]
        _print_truncated(table, dropped)
        emit_timing("pg-load", f"{table}/truncate-drop", f"{time.monotonic() - _td_start:.1f}")

    total_loaded = 0
    total_skipped = 0
    total_dupes = 0
    total_blocklisted = 0
    total_missing_d = 0

    for path in paths:
        t0 = time.monotonic()
        file_loaded = 0
        file_lines = 0
        print(f"  Loading {path}...")
        progress = _ProgressPrinter(path)

        batch = []
        with _open_text(path) as f:
            for line in f:
                file_lines += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    total_skipped += 1
                    continue

                event_id = e.get("id")
                pubkey = e.get("pubkey")
                created_at = e.get("created_at")
                content = e.get("content", "")

                if not all([event_id, pubkey, created_at is not None]):
                    total_skipped += 1
                    continue

                if blocklist is not None and pubkey in blocklist:
                    total_blocklisted += 1
                    continue

                d_tag = None
                singletons: dict[str, str | None] = {
                    col: None for col in _K30267_SINGLETON_COLUMN.values()
                }
                multis: dict[str, list[str]] = {
                    col: [] for col in _K30267_MULTI_COLUMN.values()
                }
                app_refs: list[str] = []
                # Counted per event and folded into `followed` only once the event is
                # known to be loadable: a stack later skipped for a missing d-tag must
                # not report its references as dropped by the follow when the whole row
                # never arrived.
                ev_followed = 0
                extras: list[list] = []
                for tag in e.get("tags", []):
                    if not (isinstance(tag, list) and len(tag) >= 2):
                        continue
                    k = tag[0]
                    v = tag[1]
                    if k == "d":
                        if d_tag is None and isinstance(v, str):
                            d_tag = v
                    elif k == "a":
                        m = _ZAPSTORE_A_TAG_REGEX.match(v) if isinstance(v, str) else None
                        if m:
                            if (m.group(1), m.group(2)) in excluded_coords:
                                ev_followed += 1
                            else:
                                app_refs.append(v)
                    elif k in _K30267_SINGLETON_TAGS:
                        col = _K30267_SINGLETON_COLUMN[k]
                        if singletons[col] is None and isinstance(v, str):
                            singletons[col] = v
                    elif k in _K30267_MULTI_TAGS:
                        col = _K30267_MULTI_COLUMN[k]
                        if isinstance(v, str):
                            multis[col].append(v)
                    else:
                        extras.append(tag)

                if not d_tag:
                    total_missing_d += 1
                    continue
                if ev_followed and not app_refs:
                    # Every app this stack named is excluded, so the stack is not a
                    # Zapstore app stack either - drop the row rather than store an
                    # empty app_refs. `[...] or None` would write NULL, which every
                    # reader's `array_length(app_refs, 1) > 0` filter reads as
                    # ENCRYPTED (the app list is in event.content), and an emptied
                    # public stack is not a private one. ev_followed > 0 is what
                    # distinguishes the two: a genuinely encrypted stack carries no
                    # resolvable `a` tag to drop in the first place.
                    emptied_by_follow += 1
                    emptied_refs += ev_followed
                    continue
                followed += ev_followed

                def _clean(s):
                    return s.replace("\x00", "") if isinstance(s, str) else s

                extra_tags_jsonb = json.dumps(extras) if extras else None
                clean_content = _pg_safe_text(str(content)) if content else ""
                batch.append((
                    str(event_id),
                    str(pubkey),
                    int(created_at),
                    _clean(d_tag),
                    clean_content,
                    _clean(singletons["name"]),
                    _clean(singletons["description"]),
                    _clean(singletons["title"]),
                    [_clean(s) for s in multis["platforms"]] or None,
                    [_clean(s) for s in multis["h_tags"]] or None,
                    [_clean(s) for s in app_refs] or None,
                    extra_tags_jsonb,
                ))

                if len(batch) >= BATCH_SIZE:
                    inserted, conn, cur = _insert_batch_with_retry(
                        _copy_batch_kind_30267_app_stacks, cur, batch, conn, db_url,
                        f"kind-30267 {os.path.basename(path)} line {file_lines}")
                    file_loaded += inserted
                    total_dupes += len(batch) - inserted
                    batch = []
                    progress.maybe_emit_insert(f, file_lines, file_loaded)

            if batch:
                inserted, conn, cur = _insert_batch_with_retry(
                    _copy_batch_kind_30267_app_stacks, cur, batch, conn, db_url,
                    f"kind-30267 {os.path.basename(path)} final batch")
                file_loaded += inserted
                total_dupes += len(batch) - inserted

        elapsed = time.monotonic() - t0
        _copy_elapsed += elapsed
        total_loaded += file_loaded
        rate = file_lines / elapsed if elapsed > 0 else 0
        print(f"    {file_loaded:,} inserted from {file_lines:,} lines in {elapsed:.1f}s ({rate:,.0f} lines/s)")

    if excluded_coords:
        print(f"  Followed the listing: {followed:,} reference(s) dropped from stacks that still "
              f"load, {emptied_by_follow:,} stack(s) dropped entirely "
              f"({emptied_refs:,} reference(s), every app excluded)")

    emit_timing("pg-load", f"{table}/copy", f"{_copy_elapsed:.1f}")

    if truncate:
        _rebuild_indexes_after_load(cur, conn, 30267, table, capture)

    extra_msg = ""
    if total_blocklisted:
        extra_msg += f", {total_blocklisted:,} blocklisted"
    if total_missing_d:
        extra_msg += f", {total_missing_d:,} missing d-tag"
    print(f"  Total: {total_loaded:,} inserted, {total_dupes:,} duplicates skipped, {total_skipped:,} malformed{extra_msg}")
    emit_timing("pg-load", table, f"{time.monotonic() - _table_start:.1f}", total_loaded)
    return total_loaded


def _copy_batch_kind_30267_app_stacks(cur, batch: list[tuple]) -> int:
    # 12 columns: 5 base + 3 singleton TEXTs (name/description/title) + 3 TEXT[] at
    # indices 8,9,10 (platforms/h_tags/app_refs) + 1 JSONB at index 11 (extra_tags).
    return _copy_rows_via_stdin(
        cur, "events_kind_30267_app_stacks",
        ["event_id", "pubkey", "created_at", "d_tag", "content",
         "name", "description", "title",
         "platforms", "h_tags", "app_refs", "extra_tags"],
        batch,
        array_cols=frozenset({8, 9, 10}),
    )


def main():
    # Without this, stdout is block-buffered when piped (e.g. through tee),
    # so startup lines like "Connecting..." and "Loading..."
    # sit in the buffer for minutes until a progress line (with flush=True)
    # drains it. Line-buffered output flushes on every newline.
    sys.stdout.reconfigure(line_buffering=True)
    args = parse_args()
    t0 = time.monotonic()

    # Special mode: load lightning_provider_signers from YAML.
    # Bypasses --input/--kind validation; bypasses blocklist; uses its own loader.
    if args.load_lightning_signers is not None:
        if not os.path.isfile(args.load_lightning_signers):
            print(f"ERROR: --load-lightning-signers file not found: {args.load_lightning_signers}", file=sys.stderr)
            sys.exit(2)
        print(f"Connecting to {args.db_url.split('@')[1] if '@' in args.db_url else args.db_url}...")
        conn = psycopg2.connect(args.db_url)
        total = load_lightning_provider_signers(args.load_lightning_signers, conn)
        conn.close()
        elapsed = time.monotonic() - t0
        print(f"Done loading lightning_provider_signers in {elapsed:.1f}s. {total:,} mappings loaded.")
        return

    # Special mode: classify the loaded nutzap mint URLs (nostrology-data#9).
    if args.classify_nutzap_mints is not None:
        if not os.path.isfile(args.classify_nutzap_mints):
            print(f"ERROR: --classify-nutzap-mints file not found: {args.classify_nutzap_mints}", file=sys.stderr)
            sys.exit(2)
        print(f"Connecting to {args.db_url.split('@')[1] if '@' in args.db_url else args.db_url}...")
        conn = psycopg2.connect(args.db_url)
        total = classify_nutzap_mints(args.classify_nutzap_mints, conn)
        conn.close()
        elapsed = time.monotonic() - t0
        print(f"Done classifying nutzap mints in {elapsed:.1f}s. {total:,} mint URLs classified.")
        return

    # Validate input files BEFORE connecting to Postgres or running TRUNCATE.
    # Per-kind load functions TRUNCATE + DROP INDEX before opening files, so a
    # bad --input arg here used to nuke the table and leave it empty + de-indexed
    # before the FileNotFoundError fired (incident: kind-1 emptied 2026-04-19
    # by a test invocation).
    missing = [p for p in args.input if not os.path.isfile(p)]
    if missing:
        print(f"ERROR: --input file(s) not found:", file=sys.stderr)
        for p in missing:
            print(f"  {p}", file=sys.stderr)
        print(f"  Aborting before TRUNCATE — table state preserved.", file=sys.stderr)
        sys.exit(2)

    # Same incident class: a truncated/empty upstream export is a real file, so the
    # missing-file check above passes, but loading it under --truncate would empty a
    # populated table. When --min-rows is set, refuse before any TRUNCATE runs.
    # NOTE: for gzip inputs (Athena UNLOAD parts) this pre-count FULLY decompresses
    # every part in-process — a second decompress on top of the load pass below, so
    # it's a real CPU cost (minutes for a multi-GB kind), not a cheap line tally. That
    # re-read is what keeps the never-empty guard safe without staging NDJSON to disk;
    # the streaming S3 enhancement (GH issue) replaces it with Athena's OutputRows.
    if args.min_rows > 0:
        total_lines = 0
        for p in args.input:
            with _open_text(p) as f:
                for line in f:
                    if line.strip():
                        total_lines += 1
        if total_lines < args.min_rows:
            print(
                f"ERROR: input has {total_lines:,} non-empty line(s), below --min-rows="
                f"{args.min_rows:,}.",
                file=sys.stderr,
            )
            print("  Aborting before TRUNCATE — table state preserved.", file=sys.stderr)
            sys.exit(2)

    print(f"Connecting to {args.db_url.split('@')[1] if '@' in args.db_url else args.db_url}...")
    conn = psycopg2.connect(args.db_url)

    blocklist = None
    if not args.no_blocklist:
        blocklist = _load_blocklist()
        print(f"  Blocklist: {len(blocklist):,} pubkeys loaded (events from these will be skipped)")
    else:
        print("  Blocklist: disabled (--no-blocklist)")

    print(f"Loading kind-{args.kind} from {len(args.input)} file(s)...")
    for path in args.input:
        print(f"  {os.path.abspath(path)}")

    if args.kind == 1:
        total = load_kind_1_posts(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 0:
        total = load_kind_0_profiles(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 3:
        total = load_kind_3_follows(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url, refresh_mv=not args.no_refresh_mv)
    elif args.kind == 7:
        total = load_kind_7_reactions(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 6:
        total = load_kind_6_reposts(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 7376:
        total = load_kind_7376_nutzap_redemptions(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 9321:
        total = load_kind_9321_nutzaps(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 9735 and args.into is None:
        total = load_kind_9735_zaps(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 9735 and args.into == "zapstore":
        total = load_kind_9735_zaps_zapstore(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 10019:
        total = load_kind_10019_nutzap_configs(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 10002:
        total = load_kind_10002_relay_lists(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url, refresh_mv=not args.no_refresh_mv)
    elif args.kind == 30063:
        total = load_kind_30063_app_releases(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 30267:
        total = load_kind_30267_app_stacks(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    elif args.kind == 32267:
        total = load_kind_32267_app_catalog(args.input, conn, args.truncate, blocklist=blocklist, db_url=args.db_url)
    else:
        print(f"Error: unsupported (kind, into) combination: kind={args.kind} into={args.into}", file=sys.stderr)
        sys.exit(1)

    conn.close()
    elapsed = time.monotonic() - t0
    label = _derive_kind_label(args.kind, args.input)
    print(f"Done loading {label} in {elapsed:.1f}s. {total:,} events loaded into Postgres.")


if __name__ == "__main__":
    main()

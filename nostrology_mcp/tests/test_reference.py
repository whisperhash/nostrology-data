"""Reference-record spec tests: contract invariants (offline) + live-DB parity
gates against the page queries the records reproduce. Mirrors test_metric.py's
structure and conventions - placeholder discipline, declared-tables rule, the
kind-NNNNN (slug) sweep, and DSN-gated live verification."""

import json
import os
import re

import pytest

from nostrology_mcp.metric import ZAPSTORE_PUBKEY
from nostrology_mcp.reference import (
    APP_CATALOG,
    APP_RELEASES,
    LONG_TEXT_MAX_CHARS,
    PUBLISHER_PROFILE,
    RECORDS,
    ROW_CAP,
    lookup_record,
    registry_reference_entries,
)

ALL_RECORDS = [APP_CATALOG, PUBLISHER_PROFILE, APP_RELEASES]
RECORD_IDS = [r.key for r in ALL_RECORDS]

# The Mullvad app page from the design discussion - a curator republish with
# releases, catalog entry, and zaps; used by the live parity gates.
MULLVAD_APP_ID = "net.mullvad.mullvadvpn"
# Top native publisher (35 apps) with NO kind-0 profile row - the NULL-columns case.
NO_PROFILE_PUBLISHER = "4601a3c7a91925d9a2b1683538a820704ba5b35b67b87a59152dccfecd40802e"


# -- registry shape ---------------------------------------------------------------


def test_registry_holds_exactly_the_expected_records():
    assert set(RECORDS) == {
        "zapstore.app_catalog",
        "zapstore.publisher_profile",
        "zapstore.app_releases",
        "routstr.provider_profile",
        "routstr.provider_reviews",
        "routstr.catalog_versions",
        "routstr.model_catalog",
        "routstr.exclusions",
    }


def test_lookup_roundtrip_and_unknown():
    assert lookup_record("zapstore", "app_catalog") is APP_CATALOG
    with pytest.raises(KeyError) as exc:
        lookup_record("zapstore", "app_directory")
    # The rejection lists every legal record - the error is the schema.
    for key in RECORDS:
        assert key in str(exc.value)


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_record_identity_fields_are_populated(spec):
    assert spec.key == f"{spec.domain}.{spec.record}"
    assert spec.row_unit.startswith("one row per")
    assert spec.summary and spec.partition and spec.caveats
    assert spec.ordering_note and spec.narrow_hint
    assert spec.methodology == " ".join((spec.summary, spec.partition) + spec.caveats)


# -- SQL contract -----------------------------------------------------------------

_PLACEHOLDER = re.compile(r"%\((\w+)\)s")


def _sql_variants(spec):
    variants = [(spec.sql, "compact")]
    if spec.sql_app_scoped is not None:
        variants.append((spec.sql_app_scoped, "app_scoped"))
    return variants


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_sql_binds_values_never_interpolates_them(spec):
    """Every dynamic value is a %(name)s bind with a default in bind_params -
    the spec must be runnable AS-IS (the EXPLAIN gate depends on it), and no
    caller-supplied text can reach the SQL string."""
    for sql, label in _sql_variants(spec):
        placeholders = set(_PLACEHOLDER.findall(sql))
        assert placeholders <= set(spec.bind_params), (
            f"{spec.key} ({label}): placeholders {placeholders - set(spec.bind_params)} "
            "have no bind default"
        )
        assert ZAPSTORE_PUBKEY not in sql  # the curator key binds, never inlines


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_sql_is_row_capped_with_an_observable_total(spec):
    """Every variant must LIMIT to the bound row cap AND count the pre-LIMIT
    result set via the window function - the server strips __total_rows and
    drives the truncated: envelope line from it, so a variant without either
    half would truncate silently, the exact failure the cap design forbids."""
    for sql, label in _sql_variants(spec):
        assert "LIMIT %(row_cap)s" in sql, f"{spec.key} ({label}) is uncapped"
        assert "COUNT(*) OVER () AS __total_rows" in sql, (
            f"{spec.key} ({label}) cannot report its pre-cap total"
        )
        # LAST select-list item, positionally: the server strips row[:-1] and
        # reads row[-1], so a variant that selects __total_rows anywhere else
        # would silently drop a real column and report a data value as the
        # total - with the same row arity, so zip(strict=True) cannot catch it.
        assert re.search(r"AS __total_rows\nFROM ", sql), (
            f"{spec.key} ({label}): __total_rows is not the final select item"
        )
    assert spec.bind_params["row_cap"] == ROW_CAP


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_total_rows_is_not_part_of_the_column_contract(spec):
    """__total_rows is plumbing (stripped by the server), never a published
    column."""
    for cols in (spec.columns, spec.columns_app_scoped or ()):
        assert "__total_rows" not in [c.name for c in cols]


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_scope_rules_match_the_sql(spec):
    """The scope bind (%(publisher)s / %(provider)s) is in the SQL exactly when
    the record takes a scope parameter; app_filterable <-> an app-scoped variant
    exists and filters on %(app_id)s."""
    for bind in ("publisher", "provider"):
        present = f"%({bind})s" in spec.sql
        assert present == (spec.scope_bind == bind), f"{spec.key}: %({bind})s presence"
    if spec.app_filterable:
        assert spec.sql_app_scoped is not None and spec.columns_app_scoped is not None
        assert "%(app_id)s" in spec.sql_app_scoped
        assert "%(app_id)s" not in spec.sql  # compact scope never half-filters
    else:
        assert spec.sql_app_scoped is None and spec.columns_app_scoped is None


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_window_placeholders_only_where_windowable(spec):
    for sql, _ in _sql_variants(spec):
        if spec.windowable:
            assert "%(from_epoch)s" in sql and "%(to_epoch_excl)s" in sql
        else:
            assert "%(from_epoch)s" not in sql


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_sql_reads_only_its_declared_tables(spec):
    all_tables = {s.source_table for s in ALL_RECORDS} | {
        t for s in ALL_RECORDS for t in s.label_tables
    }
    allowed = {spec.source_table, *spec.label_tables}
    for sql, label in _sql_variants(spec):
        for t in all_tables - allowed:
            assert t not in sql, f"{spec.key} ({label}) reads undeclared table {t}"


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_ordering_is_deterministic(spec):
    """A capped listing without a total ORDER is nondeterministic pagination -
    the same call could return different row sets. Every variant must end in
    an ORDER BY before its LIMIT."""
    for sql, label in _sql_variants(spec):
        assert re.search(r"ORDER BY .+\nLIMIT %\(row_cap\)s", sql), (
            f"{spec.key} ({label}): no ORDER BY immediately before the cap"
        )


def test_app_scoped_variants_add_exactly_the_long_text_columns():
    """The app-scoped column contract is the compact one plus the declared
    long-text column(s) - nothing else may differ, or the two variants drift."""
    for spec, extra in ((APP_CATALOG, "description"), (APP_RELEASES, "release_notes")):
        compact = [c.name for c in spec.columns]
        full = [c.name for c in spec.columns_app_scoped]
        assert [n for n in full if n != extra] == compact
        (col,) = [c for c in spec.columns_app_scoped if c.name == extra]
        assert col.max_chars == LONG_TEXT_MAX_CHARS
        assert col.third_party


def test_profile_null_contract_uses_row_existence_cases():
    """publisher_profile is the one record whose population outlives its source
    events: NULL = no kind-0 (profile-metadata) row, '' = present but empty.
    The CASE WHEN pr.pubkey IS NOT NULL shape delivers that; a bare COALESCE
    would collapse the two."""
    assert "CASE WHEN pr.pubkey IS NOT NULL" in PUBLISHER_PROFILE.sql
    # And the same coalesce as the metric labels - one publisher_name definition.
    assert "COALESCE(NULLIF(pr.display_name, ''), NULLIF(pr.name, ''), '')" in PUBLISHER_PROFILE.sql


def test_catalog_and_releases_never_serve_null_text():
    """For records whose row IS the source event, a missing tag is
    'present-but-empty' ('' via COALESCE), never NULL - NULL is reserved for
    'source event absent', which cannot happen here."""
    for spec in (APP_CATALOG, APP_RELEASES):
        for sql, _ in _sql_variants(spec):
            assert "CASE WHEN" not in sql
            assert "COALESCE(" in sql


def test_no_icon_or_avatar_urls_are_served():
    """Locked design decision: icon/avatar URLs are auto-fetch vectors with no
    analytical value - the only page elements deliberately not reproducible."""
    for spec in ALL_RECORDS:
        for cols in (spec.columns, spec.columns_app_scoped or ()):
            names = [c.name for c in cols]
            assert "icon" not in " ".join(names)
            assert "picture" not in " ".join(names)
        for sql, _ in _sql_variants(spec):
            assert "icon" not in sql
            assert "picture" not in sql.replace("p.pubkey", "").replace("pr.pubkey", "")


# -- column / prose conventions ---------------------------------------------------


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_third_party_flags_cover_every_publisher_authored_column(spec):
    """Key/flag/timestamp columns are ours; every string a publisher wrote must
    be flagged so the renderer sanitizes it and the envelope discloses it."""
    ours = {
        "publisher_pubkey",
        "app_id",
        "event_id",
        "is_nostr_native",
        "is_zapstore_account",
        "catalog_updated_at",
        "profile_updated_at",
        "released_at",
    }
    for cols in (spec.columns, spec.columns_app_scoped or ()):
        for c in cols:
            if c.name in ours:
                assert not c.third_party, f"{spec.key}.{c.name} wrongly flagged"
            else:
                assert c.third_party, f"{spec.key}.{c.name} is publisher text, unflagged"


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_every_kind_mention_carries_its_nozzl_slug(spec):
    """Same convention as the metrics (test_metric.py's sweep): the slug must
    accompany the kind's FIRST mention in each consumer-visible string - the
    joined methodology (where a shared caveat may repeat a kind the summary
    already slugged), plus the standalone strings."""
    slugs = {
        0: "profile-metadata",
        5: "event-deletion-requests",
        9735: "zap-receipts",
        30063: "zapstore-app-releases",
        30267: "zapstore-app-stacks",
        32267: "zapstore-app-catalog",
        38421: "routstr-provider-announcements",
        38422: "routstr-analytics-snapshots",
        38423: "routstr-model-catalog",
        38425: "routstr-provider-reviews",
    }
    prose = [spec.methodology, spec.row_unit, spec.as_of_source]
    for cols in (spec.columns, spec.columns_app_scoped or ()):
        prose += [c.description for c in cols]
    if spec.snapshot_note:
        prose.append(spec.snapshot_note)
    for text in prose:
        for kind, slug in slugs.items():
            if f"kind-{kind}" in text:
                assert f"kind-{kind} ({slug})" in text, (
                    f"{spec.key}: kind-{kind} named without its slug ({slug})"
                )


@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_prose_is_plain_ascii(spec):
    """CLAUDE.md, Writing conventions: ASCII punctuation only, verified by byte check."""
    prose = [spec.summary, spec.partition, spec.row_unit, *spec.caveats]
    for cols in (spec.columns, spec.columns_app_scoped or ()):
        prose += [c.description for c in cols]
    for text in prose:
        assert text.isascii(), f"{spec.key}: non-ASCII in {text[:60]!r}"


def test_registry_entries_shape():
    entries = registry_reference_entries()
    assert [e["key"] for e in entries] == sorted(RECORDS)
    for e in entries:
        assert e["methodology"] == " ".join([e["summary"], e["partition"], *e["caveats"]])
        assert e["params"]["row_cap"] == ROW_CAP
        assert e["params"]["format"] == ["csv", "markdown"]
        # app_scope_only marks exactly the long-text additions.
        scoped = [c["name"] for c in e["columns"] if c["app_scope_only"]]
        if e["record"] == "app_catalog":
            assert scoped == ["description"]
        elif e["record"] == "app_releases":
            assert scoped == ["release_notes"]
        else:
            assert scoped == []
    # No physical schema details in the published document.
    blob = json.dumps(entries)
    assert "events_kind_" not in blob


def test_publisher_requirement_is_published():
    entries = {e["record"]: e for e in registry_reference_entries()}
    assert entries["app_catalog"]["params"]["publisher_pubkey"]["required"] is True
    assert entries["app_releases"]["params"]["publisher_pubkey"]["required"] is True
    assert entries["publisher_profile"]["params"]["publisher_pubkey"]["required"] is False
    assert entries["app_releases"]["params"]["window"]["accepted"] is True
    assert entries["app_catalog"]["params"]["window"]["accepted"] is False
    assert "app_id" not in entries["publisher_profile"]["params"]


# -- live-DB gates ----------------------------------------------------------------

_LIVE = pytest.mark.skipif(
    not os.environ.get("NOSTROLOGY_TEST_DSN"),
    reason="live-DB gate: set NOSTROLOGY_TEST_DSN (+ password via env/.pgpass)",
)


def _conn():
    import psycopg2

    return psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )


@_LIVE
@pytest.mark.parametrize("spec", ALL_RECORDS, ids=RECORD_IDS)
def test_sql_explains_against_live_schema(spec):
    conn = _conn()
    try:
        with conn.cursor() as cur:
            for sql, _ in _sql_variants(spec):
                cur.execute("EXPLAIN " + sql, spec.bind_params)
                assert cur.fetchall()
            cur.execute("EXPLAIN " + spec.as_of_sql)
            assert cur.fetchall()
    finally:
        conn.close()


@_LIVE
def test_live_app_catalog_matches_the_page_catalog_query():
    """App-scoped app_catalog must agree field-for-field with the page's
    catalogPromise (getZapstoreAppDetail): latest revision by created_at."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                APP_CATALOG.sql_app_scoped,
                {
                    **APP_CATALOG.bind_params,
                    "publisher": ZAPSTORE_PUBKEY,
                    "app_id": MULLVAD_APP_ID,
                },
            )
            rows = cur.fetchall()
            cur.execute(
                """
                SELECT name, summary, content, url, repository, created_at
                FROM events_kind_32267_app_catalog
                WHERE pubkey = %s AND d_tag = %s
                ORDER BY created_at DESC LIMIT 1
                """,
                (ZAPSTORE_PUBKEY, MULLVAD_APP_ID),
            )
            page = cur.fetchone()
    finally:
        conn.close()

    assert len(rows) == 1 and page is not None
    row = rows[0][:-1]  # strip __total_rows
    names = [c.name for c in APP_CATALOG.columns_app_scoped]
    got = dict(zip(names, row, strict=True))
    assert got["publisher_pubkey"].strip() == ZAPSTORE_PUBKEY
    assert got["app_id"] == MULLVAD_APP_ID
    assert got["app_name"] == (page[0] or "")
    assert got["summary"] == (page[1] or "")
    assert got["description"] == page[2]
    assert got["website_url"] == (page[3] or "")
    assert got["repository_url"] == (page[4] or "")
    assert got["is_nostr_native"] is False  # curator republish, not an own d-tag
    assert got["catalog_updated_at"] == page[5]


@_LIVE
def test_live_app_releases_match_the_page_releases_query_row_for_row():
    """App-scoped app_releases must reproduce getZapstoreAppReleases exactly:
    same events, same order (created_at DESC), same field values."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                APP_RELEASES.sql_app_scoped,
                {
                    **APP_RELEASES.bind_params,
                    "publisher": ZAPSTORE_PUBKEY,
                    "app_id": MULLVAD_APP_ID,
                },
            )
            ours = cur.fetchall()
            cur.execute(
                """
                SELECT event_id, created_at, version, d_tag, channel, url,
                       repo_url, commit_sha, content
                FROM events_kind_30063_app_releases
                WHERE pubkey = %s AND app_d_tag = %s
                ORDER BY created_at DESC
                """,
                (ZAPSTORE_PUBKEY, MULLVAD_APP_ID),
            )
            page = cur.fetchall()
    finally:
        conn.close()

    assert len(ours) == len(page) > 0
    names = [c.name for c in APP_RELEASES.columns_app_scoped]
    for our_row, page_row in zip(ours, page, strict=True):
        got = dict(zip(names, our_row[:-1], strict=True))
        (event_id, created_at, version, d_tag, channel, url, repo_url, commit_sha, content) = page_row
        assert got["event_id"] == event_id
        assert got["released_at"] == created_at
        assert got["version"] == (version or "")
        assert got["release_d_tag"] == d_tag
        assert got["channel"] == (channel or "")
        assert got["release_url"] == (url or "")
        assert got["repo_url"] == (repo_url or "")
        assert got["commit_sha"] == (commit_sha or "")
        assert got["release_notes"] == content


@_LIVE
def test_live_publisher_profile_population_and_null_contract():
    """Unfiltered publisher_profile = one row per distinct catalog publisher;
    a publisher with no kind-0 (profile-metadata) row carries NULL profile
    columns; the curator row is flagged."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(PUBLISHER_PROFILE.sql, PUBLISHER_PROFILE.bind_params)
            rows = cur.fetchall()
            cur.execute("SELECT COUNT(DISTINCT pubkey) FROM events_kind_32267_app_catalog")
            (n_publishers,) = cur.fetchone()
    finally:
        conn.close()

    assert len(rows) == n_publishers
    names = [c.name for c in PUBLISHER_PROFILE.columns]
    by_pubkey = {r[0].strip(): dict(zip(names, r[:-1], strict=True)) for r in rows}
    noprof = by_pubkey[NO_PROFILE_PUBLISHER]
    assert noprof["publisher_name"] is None
    assert noprof["about"] is None
    assert noprof["nip05"] is None
    assert noprof["profile_updated_at"] is None
    assert noprof["is_zapstore_account"] is False
    curator = by_pubkey[ZAPSTORE_PUBKEY]
    assert curator["is_zapstore_account"] is True
    assert curator["publisher_name"] is not None


@_LIVE
def test_live_row_cap_bites_on_the_curator_scope_and_reports_the_total():
    """The one live scope that exceeds the cap: the curator's publisher-scoped
    releases. Rows must stop at ROW_CAP while __total_rows carries the true
    count; ordering makes the kept prefix the newest rows."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                APP_RELEASES.sql,
                {**APP_RELEASES.bind_params, "publisher": ZAPSTORE_PUBKEY},
            )
            rows = cur.fetchall()
            cur.execute(
                "SELECT COUNT(*) FROM events_kind_30063_app_releases WHERE pubkey = %s",
                (ZAPSTORE_PUBKEY,),
            )
            (true_total,) = cur.fetchone()
    finally:
        conn.close()

    assert len(rows) == ROW_CAP
    assert all(int(r[-1]) == true_total for r in rows)
    assert true_total > ROW_CAP
    released = [r[4] for r in rows]  # released_at column
    assert released == sorted(released, reverse=True)


@_LIVE
def test_live_window_bounds_filter_released_at():
    """A from/to window on app_releases must reproduce a manual epoch filter."""
    from datetime import date, datetime, timezone

    frm, to = date(2026, 1, 1), date(2026, 6, 30)
    from_epoch = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
    to_excl = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp())
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                APP_RELEASES.sql,
                {
                    **APP_RELEASES.bind_params,
                    "publisher": NO_PROFILE_PUBLISHER,
                    "from_epoch": from_epoch,
                    "to_epoch_excl": to_excl,
                },
            )
            windowed = cur.fetchall()
            cur.execute(
                """
                SELECT COUNT(*) FROM events_kind_30063_app_releases
                WHERE pubkey = %s AND created_at >= %s AND created_at < %s
                """,
                (NO_PROFILE_PUBLISHER, from_epoch, to_excl),
            )
            (manual,) = cur.fetchone()
    finally:
        conn.close()

    assert len(windowed) == manual
    assert all(from_epoch <= r[4] < to_excl for r in windowed)
    assert frm <= to  # window sanity for the reader

"""Contract tests for the metric definitions - the invariants that make the SQL
trustworthy without a live DB (the live-DB path is covered by the optional
EXPLAIN/reconciliation gates below, mirroring the repo's pure-vs-integration split).

Structure: invariants that must hold for EVERY metric are parameterized over
METRICS.values(), so a third metric inherits them by existing rather than by
someone remembering to copy a test. Per-metric specifics stay in their own blocks.
"""

import os
import re

import pytest

from nostrology_mcp.metric import (
    APP_STACK_COUNT_CURRENT,
    APP_ZAPS_DAY,
    APP_ZAPS_TOTAL,
    ENTITY_FILTER_BINDS,
    METRIC_NAMES,
    METRICS,
    PUBLISHER_APP_COUNT_CURRENT,
    ROUTSTR_METRIC_SPECS,
    default_grain,
    grain_redirect,
    grains_for,
    ZAP_COUNT_DAY,
    ZAPPED_MSATS_DAY,
    NATIVE_RELEASE_COUNT_DAY,
    NEW_NATIVE_APP_COUNT_DAY,
    NEW_REPUBLISHED_APP_COUNT_DAY,
    REPUBLISHED_RELEASE_COUNT_DAY,
    NUTZAPS_METRIC_SPECS,
    ZAPSTORE_OWN_D_TAGS,
    ZAPSTORE_PUBKEY,
    lookup,
)

ALL_SPECS = sorted(METRICS.values(), key=lambda s: s.key)
SPEC_IDS = [s.key for s in ALL_SPECS]

# The original six single-table daily series - the invariants below that assume
# "one table, (utc_day, integer) columns, windowless ORDER BY 1" are THEIR
# contract; the leaderboard specs have their own contract block further down,
# and the Routstr family (a SECOND domain, served from pre-computed serving
# tables rather than event tables) has its own module, test_routstr.py.
LEADERBOARD_SPECS = [APP_ZAPS_DAY, APP_ZAPS_TOTAL, APP_STACK_COUNT_CURRENT, PUBLISHER_APP_COUNT_CURRENT]
ZAPSTORE_SPECS = [s for s in ALL_SPECS if s.domain == "zapstore"]
LEGACY_SPECS = [s for s in ZAPSTORE_SPECS if s not in LEADERBOARD_SPECS]
LEGACY_IDS = [s.key for s in LEGACY_SPECS]
LEADERBOARD_IDS = [s.key for s in LEADERBOARD_SPECS]
# The to_timestamp()-bucketing pin is an EVENT-TABLE contract: the Routstr day
# specs read DATE columns / ISO day strings the pipeline already bucketed.
DAY_SPECS = [s for s in ZAPSTORE_SPECS if s.grain == "day"]
DAY_IDS = [s.key for s in DAY_SPECS]
ROUTSTR_KEYS = {s.key for s in ROUTSTR_METRIC_SPECS}
NUTZAPS_KEYS = {s.key for s in NUTZAPS_METRIC_SPECS}


def test_registry_holds_exactly_the_expected_combinations():
    assert set(METRICS) == {
        "zapstore.new_native_app_count@day",
        "zapstore.new_republished_app_count@day",
        "zapstore.native_release_count@day",
        "zapstore.republished_release_count@day",
        "zapstore.zap_count@day",
        "zapstore.zapped_msats@day",
        "zapstore.app_zaps@day",
        "zapstore.app_zaps@total",
        "zapstore.app_stack_count@current",
        "zapstore.publisher_app_count@current",
    } | ROUTSTR_KEYS | NUTZAPS_KEYS
    assert NUTZAPS_KEYS == {
        "nutzaps.payment_activity@day",
        "nutzaps.payment_activity@total",
        "nutzaps.mint_leaderboard@total",
        "nutzaps.mint_activity@day",
        # day and week were withdrawn on review (per-mint rows summed to the paid
        # DAU on 87% of days) and made free by the operator's decision, 2026-09-30
        "nutzaps.mint_active_users@day",
        "nutzaps.mint_active_users@week",
        "nutzaps.mint_active_users@month",
    }
    assert ROUTSTR_KEYS == {
        "routstr.network_activity@day",
        "routstr.cumulative_revenue@day",
        "routstr.network_summary@current",
        "routstr.provider_leaderboard@current",
        "routstr.provider_activity@day",
        "routstr.provider_activity@total",
        "routstr.model_leaderboard@current",
        "routstr.model_activity@day",
        "routstr.model_activity@total",
        "routstr.catalog_size@day",
        "routstr.success_rate@current",
        "routstr.provider_facets@current",
    }


def test_metric_names_are_derived_from_the_registry():
    assert METRIC_NAMES == (
        "app_stack_count",
        "app_zaps",
        "catalog_size",
        "cumulative_revenue",
        "mint_active_users",
        "mint_activity",
        "mint_leaderboard",
        "model_activity",
        "model_leaderboard",
        "native_release_count",
        "network_activity",
        "network_summary",
        "new_native_app_count",
        "new_republished_app_count",
        "payment_activity",
        "provider_activity",
        "provider_facets",
        "provider_leaderboard",
        "publisher_app_count",
        "republished_release_count",
        "success_rate",
        "zap_count",
        "zapped_msats",
    )
    # A metric NAME is unique across domains - what makes resolve_domain's
    # "resolve on the name alone" rule sound.
    assert len(METRIC_NAMES) == len({(s.metric) for s in METRICS.values()})


def test_registry_keys_agree_with_their_specs():
    """A hand-typed key would let a spec be reachable under the wrong name."""
    for key, spec in METRICS.items():
        assert key == spec.key


def test_lookup_roundtrip_and_unknown():
    assert lookup("zapstore", "new_native_app_count", "day") is NEW_NATIVE_APP_COUNT_DAY
    assert lookup("zapstore", "new_republished_app_count", "day") is NEW_REPUBLISHED_APP_COUNT_DAY
    assert lookup("zapstore", "republished_release_count", "day") is REPUBLISHED_RELEASE_COUNT_DAY
    with pytest.raises(KeyError) as exc:
        lookup("zapstore", "new_native_app_count", "month")
    # The error must name the known combinations - agents recover from specific errors.
    assert "zapstore.new_native_app_count@day" in str(exc.value)
    assert "zapstore.new_republished_app_count@day" in str(exc.value)
    assert "zapstore.republished_release_count@day" in str(exc.value)
    assert lookup("zapstore", "native_release_count", "day") is NATIVE_RELEASE_COUNT_DAY


# -- invariants that hold for every metric ---------------------------------------


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_sql_binds_values_never_interpolates_them(spec):
    """The load-bearing safety property: constants reach the SQL only as bind
    parameters. If the curator pubkey ever appears in the SQL text, the
    no-model-text-reaches-SQL argument is broken at its foundation.

    Stated over the SPEC rather than over one hardcoded parameter set, because
    metrics now differ in what they bind: the partition metrics bind the curator
    pair, the zap metrics bind NOTHING (their population is the table itself).
    The invariant that must hold for every metric either way is that the set of
    %(name)s placeholders in the SQL and the keys of bind_params agree exactly -
    a placeholder with no param raises at execute; a param with no placeholder is
    dead weight that hides a dropped predicate."""
    assert ZAPSTORE_PUBKEY not in spec.sql
    # Strip the (legitimate) table name FIRST, then keep the full-strength bare
    # substring check. The d_tag "zapstore" is a substring of the table name
    # events_kind_9735_zaps_zapstore, which is the false positive that surfaced
    # when the zap metrics arrived - but weakening this to a quoted-form check
    # would let `LIKE 'zapstore%'` through, so strip rather than narrow.
    sql_without_table = spec.sql.replace(spec.source_table, "")
    for d_tag in ZAPSTORE_OWN_D_TAGS:
        assert d_tag not in sql_without_table
    placeholders = set(re.findall(r"%\((\w+)\)s", spec.sql))
    # epoch_inline specs carry two RUNTIME placeholders the server always binds
    # (None = unbounded); they are not spec constants, so they are legitimately
    # absent from bind_params.
    expected = set(spec.bind_params)
    if spec.window_mode == "epoch_inline":
        expected |= {"from_epoch", "to_epoch_excl"}
    elif spec.window_mode == "day_inline":
        expected |= {"from_day", "to_day"}
    assert placeholders == expected, (
        f"{spec.key}: SQL placeholders {placeholders} != expected {expected}"
    )


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_bind_params_are_not_shared_between_specs(spec):
    """MetricSpec is frozen but its bind dict is not; two specs aliasing one dict
    would let a future mutation on one silently rewrite the other's parameters."""
    others = [s for s in ALL_SPECS if s is not spec]
    assert all(spec.bind_params is not o.bind_params for o in others)
    # Whatever a spec binds must be the real constants, never a stale copy.
    # epoch_inline specs additionally carry their two None window bounds, and
    # filterable specs their None filter defaults, so every spec is runnable
    # as-is (fetch_metric(spec) must not KeyError).
    if spec.bind_params:
        expected: dict = {}
        if "curator" in spec.bind_params:
            expected.update({"curator": ZAPSTORE_PUBKEY, "own_d_tags": ZAPSTORE_OWN_D_TAGS})
        if spec.window_mode == "epoch_inline":
            expected.update({"from_epoch": None, "to_epoch_excl": None})
        for name in spec.entity_filters:
            expected[ENTITY_FILTER_BINDS[name]] = None
        if spec.row_cap is not None and spec.window_mode != "day_wrap":
            expected["row_cap"] = spec.row_cap  # a capped snapshot binds its own cap
        assert spec.bind_params == expected


@pytest.mark.parametrize("spec", DAY_SPECS, ids=DAY_IDS)
def test_sql_day_bucketing_is_utc_explicit(spec):
    """Pin the AT TIME ZONE 'UTC' form (sql/zapstore_native_releases_daily.sql's
    fix) - plain date_trunc on timestamptz resolves in the SESSION time zone and
    silently shifts day boundaries if the session is ever non-UTC. Both metrics
    must spell it the same way, or two series on the same axis would disagree
    about where a day ends under a non-UTC session."""
    assert "AT TIME ZONE 'UTC'" in spec.sql
    assert "date_trunc('day', to_timestamp(" in spec.sql
    assert "::date AS utc_day" in spec.sql


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_sql_reads_only_its_declared_tables(spec):
    """source_table + label_tables are declared, not parsed - so they can drift
    from the SQL unless pinned. The SQL must read the aggregation table and at
    most its declared label-join tables; the freshness probe's "my own table"
    guarantee below stays on source_table alone."""
    assert spec.source_table in spec.sql
    allowed = {spec.source_table, *spec.label_tables}
    all_tables = {s.source_table for s in ALL_SPECS} | {t for s in ALL_SPECS for t in s.label_tables}
    for t in all_tables - allowed:
        assert t not in spec.sql, f"{spec.key} reads undeclared table {t}"
    # The replaceable-catalog trap: 32267 is latest-only, so MIN(created_at) over
    # it returns a REPUBLICATION date. The legacy six exclude 32267 entirely; the
    # leaderboard specs may JOIN it for labels (and publisher_app_count counts
    # over it) but none may DATE from it - no MIN(created_at) anywhere in a
    # 32267-reading statement.
    if spec in LEGACY_SPECS:
        assert "32267" not in spec.sql
    elif "32267" in spec.sql:
        assert "MIN(created_at)" not in spec.sql, f"{spec.key} dates from a replaceable kind"


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_sql_has_no_hidden_now_anchor(spec):
    """No NOW()/INTERVAL anywhere: the full-history default is real (a consumer
    can trim, but cannot recover what we dropped), and a window reaches the SQL
    only as explicit UTC-day-derived binds - never a server-clock anchor."""
    assert "INTERVAL" not in spec.sql.upper()
    assert "NOW()" not in spec.sql.upper()
    if spec.key in ONE_ROW_AGGREGATES:
        # An aggregate with no GROUP BY returns exactly one row - nothing to order.
        assert "GROUP BY" not in spec.sql
    else:
        assert "ORDER BY" in spec.sql


# Specs whose statement is a bare aggregate (always exactly one row).
ONE_ROW_AGGREGATES = {"nutzaps.payment_activity@total"}


@pytest.mark.parametrize("spec", LEGACY_SPECS, ids=LEGACY_IDS)
def test_legacy_sql_is_windowless_and_ordered_by_day(spec):
    """The original six stay byte-stable: windowless (the day_wrap path wraps
    AROUND them, never edits them) and ordered by utc_day so the preamble's
    first/last row window is the real extent."""
    assert spec.sql.rstrip().endswith("ORDER BY 1")
    assert "%(from_" not in spec.sql and "%(to_" not in spec.sql
    assert spec.window_mode == "day_wrap"


@pytest.mark.parametrize("spec", LEGACY_SPECS, ids=LEGACY_IDS)
def test_column_contract_shape(spec):
    """utc_day first, then exactly one integer measure named for the metric - the
    property the CSV/markdown renderers and the preamble type line rely on."""
    assert [c.type for c in spec.columns] == ["date", "integer"]
    assert spec.columns[0].name == "utc_day"
    assert spec.columns[1].name == spec.metric
    # The measure column must be what the SQL actually aliases it to.
    assert f"AS {spec.metric}" in spec.sql


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_as_of_probe_reads_this_metrics_own_source_table(spec):
    """Per-spec, not global: a metric on another table with a shared probe would
    report an unrelated table's freshness as its own."""
    assert "MAX(" in spec.as_of_sql
    # freshness_table = as_of_table or source_table: the Routstr substrate specs
    # declare routstr_snapshot as their freshness source (written in the same
    # transaction), and that declaration is what this pins.
    assert spec.freshness_table in spec.as_of_sql
    if spec.domain == "zapstore":
        assert "MAX(created_at)" in spec.as_of_sql and spec.as_of_table is None
    # And NOT any other metric's freshness table - the exact bug the per-spec
    # probe replaced (a module-global probe was indistinguishable from correct
    # while every metric shared one table; the zap metrics are the first that
    # would have exposed it).
    for t in {s.freshness_table for s in ALL_SPECS} - {spec.freshness_table}:
        assert t not in spec.as_of_sql
    assert spec.as_of_source  # named in the preamble disclaimer


@pytest.mark.parametrize("spec", LEGACY_SPECS, ids=LEGACY_IDS)
def test_methodology_states_the_counting_unit_and_sparseness(spec):
    """Every methodology must answer "what is one row" and "are zeros present" -
    the two things a model gets wrong when it only reads column names."""
    m = spec.methodology
    assert "sparse" in m
    # Names the Nostr kind it is built from, DERIVED from the source table rather
    # than a bare "kind-" substring (which "kind-1" would satisfy). Chains into the
    # slug convention checked at the bottom of this module.
    kind = spec.source_table.split("_")[2]  # events_kind_<K>_...
    assert f"kind-{kind} (" in m, f"{spec.key}: methodology never names kind-{kind}"
    # ...and answers "what is one row" in the metric's own unit.
    assert any(
        phrase in m
        for phrase in (
            "apps, not release events",
            "releases, not distinct apps",
            "receipts, not distinct zappers",
            "Counts amounts, not zaps",
        )
    ), f"{spec.key}: methodology never says what one row counts"


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_metric_identity_fields_are_populated(spec):
    assert spec.domain in {"zapstore", "routstr", "nutzaps"}
    assert spec.grain in {"day", "total", "current"} or (
        # week and month buckets exist for the per-mint active-user series only
        spec.key in {"nutzaps.mint_active_users@week", "nutzaps.mint_active_users@month"}
    )
    assert spec.output in {"sparse", "dense", "entities"}
    if spec.domain == "zapstore":
        assert spec.unit in {"apps", "releases", "zaps", "msats", "zaps+msats", "stacks"}
        assert spec.source_table.startswith("events_kind_")
        assert spec.output != "dense"
    elif spec.domain == "nutzaps":
        assert spec.unit in {"payments+sats", "payments+sats+users", "users"}
        assert spec.source_table == "events_kind_9321_nutzaps"
        assert spec.output != "dense"
    else:
        assert spec.unit in {"msats", "msats+requests+tokens", "mixed", "models", "ratio", "providers"}
        assert spec.source_table.startswith("routstr_")
    assert spec.series_type in {"event", "snapshot"}
    assert spec.window_mode in {"day_wrap", "epoch_inline", "day_inline", "none"}


# -- the partition pairs (2 partitions x 2 units = 4 metrics) ----------------------

NATIVE_SPECS = [NEW_NATIVE_APP_COUNT_DAY, NATIVE_RELEASE_COUNT_DAY]
REPUBLISHED_SPECS = [NEW_REPUBLISHED_APP_COUNT_DAY, REPUBLISHED_RELEASE_COUNT_DAY]
APP_SPECS = [NEW_NATIVE_APP_COUNT_DAY, NEW_REPUBLISHED_APP_COUNT_DAY]
RELEASE_SPECS = [NATIVE_RELEASE_COUNT_DAY, REPUBLISHED_RELEASE_COUNT_DAY]


@pytest.mark.parametrize("spec", NATIVE_SPECS, ids=[s.key for s in NATIVE_SPECS])
def test_native_partition_rule_shape(spec):
    """native = (pubkey <> curator) OR (app_d_tag = ANY(own)) - the OR polarity is
    what re-includes Zapstore's own first-party apps; an AND here would be the
    classic inversion bug. Both native metrics must spell it identically, or the
    "bounded above" relationship between them silently stops holding."""
    assert "pubkey <> %(curator)s" in spec.sql
    assert "OR app_d_tag = ANY(%(own_d_tags)s)" in spec.sql


@pytest.mark.parametrize("spec", REPUBLISHED_SPECS, ids=[s.key for s in REPUBLISHED_SPECS])
def test_republished_partition_rule_shape(spec):
    """republished = (pubkey = curator) AND (app_d_tag <> ALL(own)) - the AND/ALL
    polarity is what EXCLUDES Zapstore's own apps from the republished bucket even
    though they share the curator key."""
    assert "pubkey = %(curator)s" in spec.sql
    assert "AND app_d_tag <> ALL(%(own_d_tags)s)" in spec.sql
    # Must NOT be the native predicate: `= ANY` here would invert the exemption.
    assert "= ANY(%(own_d_tags)s)" not in spec.sql


@pytest.mark.parametrize("spec", RELEASE_SPECS, ids=[s.key for s in RELEASE_SPECS])
def test_release_metrics_do_not_dedup(spec):
    """The releases unit counts every version: no per-app MIN, no per_app_first.
    A dedup sneaking in would silently turn a releases series into an apps series."""
    assert "MIN(created_at)" not in spec.sql
    assert "per_app_first" not in spec.sql


@pytest.mark.parametrize("spec", APP_SPECS, ids=[s.key for s in APP_SPECS])
def test_app_metrics_dedup_to_first_appearance(spec):
    """The apps unit dedupes to each app's first-ever release via the per_app_first
    CTE - MIN(created_at) grouped on the app identity (pubkey, app_d_tag), computed
    over ALL history before the partition filter. Filtering after the MIN is
    equivalent to filtering before it because the predicate reads only the GROUP BY
    keys - an app cannot straddle partitions."""
    assert "per_app_first" in spec.sql
    assert "MIN(created_at)" in spec.sql
    assert "GROUP BY pubkey, app_d_tag" in spec.sql
    assert spec.unit == "apps"


def test_the_two_release_metrics_are_the_two_bars_of_one_chart():
    """native_release_count + republished_release_count tile the source table, so
    they stack to the chart's total. Same counting unit, same day expression,
    complementary predicates - anything else and the bars would not sum."""
    n, r = NATIVE_RELEASE_COUNT_DAY, REPUBLISHED_RELEASE_COUNT_DAY
    assert n.unit == r.unit == "releases"
    assert n.sql.replace("native_release_count", "X") != r.sql.replace(
        "republished_release_count", "X"
    )  # they must differ in the predicate, not only in the alias
    assert "pubkey <> %(curator)s" in n.sql and "pubkey = %(curator)s" in r.sql


def test_the_two_app_metrics_are_the_two_lines_of_the_cumulative_charts():
    """new_native_app_count + new_republished_app_count tile per_app_first, so
    their cumulative sums are the native and github lines of nostrology-web's
    "Zapstore Apps" chart, their sum is the total line, and native/(native+
    republished) is the "Nostr-native Apps as % of Total" series - the derivability
    these four metrics exist to provide. Same unit, same dedup CTE, complementary
    predicates."""
    n, r = NEW_NATIVE_APP_COUNT_DAY, NEW_REPUBLISHED_APP_COUNT_DAY
    assert n.unit == r.unit == "apps"
    assert n.sql.replace("new_native_app_count", "X") != r.sql.replace(
        "new_republished_app_count", "X"
    )  # they must differ in the predicate, not only in the alias
    # Identical dedup CTE on both sides - byte-equal up to the SELECT that follows.
    cte = n.sql.split(")\nSELECT")[0]
    assert cte == r.sql.split(")\nSELECT")[0]
    assert "pubkey <> %(curator)s" in n.sql and "pubkey = %(curator)s" in r.sql


@pytest.mark.parametrize(
    "native_spec,republished_spec",
    [(NEW_NATIVE_APP_COUNT_DAY, NEW_REPUBLISHED_APP_COUNT_DAY),
     (NATIVE_RELEASE_COUNT_DAY, REPUBLISHED_RELEASE_COUNT_DAY)],
    ids=["apps", "releases"],
)
def test_partitions_are_exact_complements(native_spec, republished_spec):
    """De Morgan check on the predicate pair, spelled out because getting it wrong
    is silent: both metrics would still return plausible daily numbers.
    NOT((p <> c) OR (d = ANY(o)))  ==  (p = c) AND (d <> ALL(o))"""
    native, republished = native_spec.sql, republished_spec.sql
    assert ("pubkey <> %(curator)s" in native) and ("pubkey = %(curator)s" in republished)
    assert ("= ANY(%(own_d_tags)s)" in native) and ("<> ALL(%(own_d_tags)s)" in republished)
    assert (" OR " in native) and (" AND " in republished)


def test_apps_vs_releases_units_are_distinct_and_cross_referenced():
    """The asymmetry is intentional and is the thing a consumer most easily gets
    wrong (treating one as a subset or ratio of the other), so both the unit field
    and the methodology must say so."""
    assert NEW_NATIVE_APP_COUNT_DAY.unit == "apps"
    assert REPUBLISHED_RELEASE_COUNT_DAY.unit == "releases"
    # Apps metric dedupes to first appearance; releases metric does not dedupe.
    assert "MIN(created_at)" in NEW_NATIVE_APP_COUNT_DAY.sql
    assert "MIN(created_at)" not in REPUBLISHED_RELEASE_COUNT_DAY.sql
    assert "GROUP BY pubkey, app_d_tag" in NEW_NATIVE_APP_COUNT_DAY.sql
    # Each apps metric must point at its releases counterpart (and vice versa on
    # the native side), since confusing the two is the likeliest misread now that
    # both units exist over the SAME partition.
    assert "native_release_count" in NEW_NATIVE_APP_COUNT_DAY.methodology
    assert "new_native_app_count" in NATIVE_RELEASE_COUNT_DAY.methodology
    assert "republished_release_count" in NEW_REPUBLISHED_APP_COUNT_DAY.methodology


def test_republished_methodology_states_the_replaceability_caveat():
    """COUNT(*) over an addressable kind is NOT a raw event count: re-publishing the
    same version replaces the row. A consumer reading "releases" would otherwise
    assume raw events."""
    m = REPUBLISHED_RELEASE_COUNT_DAY.methodology
    assert "<package>@<version>" in m
    assert "replaces the earlier row" in m
    assert "pre-release" in m  # alpha/beta/rc are counted, matching the Zapstore client


def test_republished_methodology_states_the_created_at_provenance():
    """Empirically found, not assumed: some curator republishes carry the UPSTREAM
    release date in created_at, so the series' left edge is not Zapstore activity.
    Without this, a model would read 2015-2019 rows as Zapstore history - years
    before Nostr existed. Pinned by test_live_backdated_tail_is_still_present."""
    m = REPUBLISHED_RELEASE_COUNT_DAY.methodology
    assert "UPSTREAM" in m
    assert "2015-12-29" in m
    assert "NOT from when we ingested it" in m


# -- live-DB gates ---------------------------------------------------------------

_LIVE = pytest.mark.skipif(
    not os.environ.get("NOSTROLOGY_TEST_DSN"),
    reason="live-DB gate: set NOSTROLOGY_TEST_DSN (+ password via env/.pgpass)",
)


@_LIVE
@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_sql_explains_against_live_schema(spec):
    """Optional integration gate: EXPLAIN (never execute) each metric SQL against a
    real database so column/table drift is caught without moving data."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    # (ordering note: production runs the as_of probe before the metric SQL so READ
    # COMMITTED snapshot skew can only understate freshness - see db.fetch_metric.)
    params = dict(spec.bind_params)
    # Bind the window placeholders exactly as server.py does for a call without a window.
    if spec.window_mode == "epoch_inline":
        params.update({"from_epoch": None, "to_epoch_excl": None})
    elif spec.window_mode == "day_inline":
        params.update({"from_day": None, "to_day": None})
    try:
        with conn.cursor() as cur:
            cur.execute("EXPLAIN " + spec.sql, params or None)
            assert cur.fetchall()
            cur.execute("EXPLAIN " + spec.as_of_sql)
            assert cur.fetchall()
    finally:
        conn.close()


@_LIVE
def test_live_partitions_reconcile_to_the_whole_table():
    """The invariant no pure test can prove: the two partitions must tile the source
    table exactly - no row counted twice, none dropped. This is what would catch a
    NULL app_d_tag leaking out of BOTH predicates (NULL = ANY(...) and
    NULL <> ALL(...) are both NULL, so such a row would vanish from each side)."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)::bigint,
                       count(*) FILTER (
                           WHERE pubkey <> %(curator)s OR app_d_tag = ANY(%(own_d_tags)s)
                       )::bigint,
                       count(*) FILTER (
                           WHERE pubkey =  %(curator)s AND app_d_tag <> ALL(%(own_d_tags)s)
                       )::bigint,
                       count(*) FILTER (WHERE app_d_tag IS NULL)::bigint
                FROM events_kind_30063_app_releases
                """,
                REPUBLISHED_RELEASE_COUNT_DAY.bind_params,
            )
            total, native, republished, null_d = cur.fetchone()
    finally:
        conn.close()

    assert null_d == 0, f"{null_d} rows have NULL app_d_tag - both predicates drop them"
    assert native + republished == total, (
        f"partitions do not tile the table: {native} + {republished} != {total}"
    )
    assert republished > 0 and native > 0  # both partitions must be non-empty


@_LIVE
def test_live_republished_daily_sums_to_the_partition_total():
    """The daily series must account for every row in its partition: sum of the
    per-day counts == the partition's total. Catches a day-bucketing expression
    that drops rows (e.g. a NULL created_at or a failed cast)."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    spec = REPUBLISHED_RELEASE_COUNT_DAY
    try:
        with conn.cursor() as cur:
            cur.execute(spec.sql, spec.bind_params)
            rows = cur.fetchall()
            cur.execute(
                """
                SELECT count(*)::bigint FROM events_kind_30063_app_releases
                WHERE pubkey = %(curator)s AND app_d_tag <> ALL(%(own_d_tags)s)
                """,
                spec.bind_params,
            )
            (partition_total,) = cur.fetchone()
    finally:
        conn.close()

    assert rows, "republished partition is empty on live data"
    assert sum(r[1] for r in rows) == partition_total
    # The backdated tail the methodology warns about must actually be there: the
    # republished series reaches implausibly far back (upstream release dates)
    # while the native series starts when Nostr publishing really began. If this
    # contrast ever disappears, the created_at-provenance caveat is stale prose.
    assert rows[0][0].year <= 2016, f"republished series now starts {rows[0][0]}"
    # Sparse and ascending: no zero rows, strictly increasing days.
    days = [r[0] for r in rows]
    assert days == sorted(days) and len(set(days)) == len(days)
    assert all(r[1] > 0 for r in rows)


@_LIVE
def test_live_new_native_apps_never_exceed_native_releases():
    """The cross-metric invariant a consumer will reason with: an app's first release
    IS a release, so the deduped apps series is bounded above by the releases series
    on every day. Violated only if the two drift apart in partition or bucketing."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(NATIVE_RELEASE_COUNT_DAY.sql, NATIVE_RELEASE_COUNT_DAY.bind_params)
            releases = dict(cur.fetchall())
            cur.execute(NEW_NATIVE_APP_COUNT_DAY.sql, NEW_NATIVE_APP_COUNT_DAY.bind_params)
            apps = dict(cur.fetchall())
    finally:
        conn.close()

    assert apps and releases
    over = {d: (a, releases.get(d, 0)) for d, a in apps.items() if a > releases.get(d, 0)}
    assert not over, f"new apps exceed native releases on {over}"
    # Every day with a new app must exist in the releases series at all (sparseness
    # of one series must not hide rows the other has).
    assert set(apps) <= set(releases)


@_LIVE
def test_live_new_republished_apps_never_exceed_republished_releases():
    """Same bound as the native pair, republished side: an app's first release IS a
    release, so the deduped apps series is bounded above by the releases series on
    every day - including the backdated upstream tail, which both series share."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                REPUBLISHED_RELEASE_COUNT_DAY.sql, REPUBLISHED_RELEASE_COUNT_DAY.bind_params
            )
            releases = dict(cur.fetchall())
            cur.execute(
                NEW_REPUBLISHED_APP_COUNT_DAY.sql, NEW_REPUBLISHED_APP_COUNT_DAY.bind_params
            )
            apps = dict(cur.fetchall())
    finally:
        conn.close()

    assert apps and releases
    over = {d: (a, releases.get(d, 0)) for d, a in apps.items() if a > releases.get(d, 0)}
    assert not over, f"new apps exceed republished releases on {over}"
    assert set(apps) <= set(releases)
    # The backdated tail reaches this series too: a republished app's first
    # appearance is the MIN over rows that may carry upstream release dates, so the
    # left edge predates Nostr - the caveat's 2015-12-29 claim, checked live.
    first_day = min(apps)
    assert first_day.year <= 2016, f"republished apps series now starts {first_day}"


@_LIVE
def test_live_app_partitions_tile_all_apps():
    """The apps-unit half of the tiling invariant: summing the two new_*_app_count
    series must account for every distinct app in the table, exactly once. This is
    what makes cumsum(native) + cumsum(republished) a true total-catalog line."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(NEW_NATIVE_APP_COUNT_DAY.sql, NEW_NATIVE_APP_COUNT_DAY.bind_params)
            native_total = sum(r[1] for r in cur.fetchall())
            cur.execute(
                NEW_REPUBLISHED_APP_COUNT_DAY.sql, NEW_REPUBLISHED_APP_COUNT_DAY.bind_params
            )
            republished_total = sum(r[1] for r in cur.fetchall())
            cur.execute(
                "SELECT COUNT(*) FROM (SELECT 1 FROM events_kind_30063_app_releases"
                " GROUP BY pubkey, app_d_tag) apps"
            )
            (all_apps,) = cur.fetchone()
    finally:
        conn.close()

    assert native_total > 0 and republished_total > 0
    assert native_total + republished_total == all_apps, (
        f"app partitions do not tile: {native_total} + {republished_total} != {all_apps}"
    )


@_LIVE
def test_live_native_series_does_not_share_the_backdated_tail():
    """The other half of the contrast above: native releases carry real Nostr
    publication times, so the native series must NOT reach back before Nostr
    existed. This is what makes the tail attributable to republishing."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    spec = NEW_NATIVE_APP_COUNT_DAY
    try:
        with conn.cursor() as cur:
            cur.execute(spec.sql, spec.bind_params)
            rows = cur.fetchall()
    finally:
        conn.close()

    assert rows
    assert rows[0][0].year >= 2023, f"native series unexpectedly starts {rows[0][0]}"


@_LIVE
def test_live_zap_pair_reproduces_the_zapstore_zaps_chart_exactly():
    """THE reason these two metrics exist: the "Zapstore Zaps" chart must be
    derivable from them. Runs nostrology-web's getZapstoreZapsAggregateDaily
    statement in FULL (postgres.ts) - window CTE, dense day_series, LEFT JOIN
    fill, and the SUM(...) OVER (ORDER BY day) cumulative - then rebuilds exactly
    that output from the two sparse series and asserts they agree point for point,
    including the rounded cumulative SATS line the page actually renders.

    Reducing the chart SQL to its daily aggregate (an earlier version of this
    gate) would leave the cumulative arm - the whole reason zapped_msats exists -
    untested, and would hide a boundary-day divergence entirely: the chart's
    window is a TIMESTAMP cutoff while its calendar starts at a date_trunc, so its
    OLDEST day is partial. The reconstruction below therefore applies the same
    timestamp cutoff rather than a whole-day trim, which is what _ZAP_CHART_CAVEAT
    now tells consumers to do.

    Note the chart buckets with a plain date_trunc (session TZ) while we pin
    AT TIME ZONE 'UTC'; they agree only while the session is UTC, which this
    assertion also implicitly checks on the live cluster."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    # Verbatim from nostrology-web/src/lib/db/postgres.ts getZapstoreZapsAggregateDaily.
    chart_sql = """
        WITH window_zaps AS (
            SELECT created_at, COALESCE(amount_msats, 0) AS amount_msats
            FROM events_kind_9735_zaps_zapstore
            WHERE created_at >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '365 days'))::bigint
        ),
        day_series AS (
            SELECT generate_series(
                date_trunc('day', NOW() - INTERVAL '365 days'),
                date_trunc('day', NOW()),
                '1 day'::interval
            ) AS day
        ),
        daily AS (
            SELECT date_trunc('day', to_timestamp(created_at)) AS day,
                COUNT(*)::bigint          AS daily_count,
                SUM(amount_msats)::bigint AS daily_msats
            FROM window_zaps
            GROUP BY 1
        ),
        filled AS (
            SELECT ds.day,
                COALESCE(d.daily_count, 0) AS daily_count,
                COALESCE(d.daily_msats, 0) AS daily_msats
            FROM day_series ds
            LEFT JOIN daily d ON d.day = ds.day
        )
        SELECT
            EXTRACT(EPOCH FROM day)::bigint                      AS ts,
            daily_count::bigint                                  AS daily_count,
            SUM(daily_msats) OVER (ORDER BY day)::bigint         AS cumulative_msats
        FROM filled
        ORDER BY day
    """
    # Our two series, restricted by the SAME timestamp cutoff the chart uses, so
    # the partial boundary day is reconstructed the way the chart computes it.
    ours_sql = """
        SELECT date_trunc('day', to_timestamp(created_at) AT TIME ZONE 'UTC')::date AS utc_day,
               COUNT(*)::bigint,
               COALESCE(SUM(amount_msats), 0)::bigint
        FROM events_kind_9735_zaps_zapstore
        WHERE created_at >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '365 days'))::bigint
        GROUP BY 1 ORDER BY 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(chart_sql)
            chart = cur.fetchall()
            cur.execute(ours_sql)
            ours = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
            # Full-history series must be a strict superset of the windowed view.
            cur.execute(ZAP_COUNT_DAY.sql, ZAP_COUNT_DAY.bind_params)
            all_counts = dict(cur.fetchall())
            cur.execute(ZAPPED_MSATS_DAY.sql, ZAPPED_MSATS_DAY.bind_params)
            all_msats = dict(cur.fetchall())
    finally:
        conn.close()

    assert chart, "chart window is empty on live data"

    # Rebuild the chart's exact output (dense calendar + running sum + the page's
    # msats->sats rounding) from our series, and compare point for point.
    from datetime import datetime, timedelta, timezone as _tz

    first_day = datetime.fromtimestamp(chart[0][0], _tz.utc).date()
    running = 0
    rebuilt = []
    for i in range(len(chart)):
        day = first_day + timedelta(days=i)
        c, m = ours.get(day, (0, 0))
        running += m
        rebuilt.append((day, c, running))

    msats_to_sats = lambda ms: round(ms / 1000)  # noqa: E731 - mirrors the page's Math.round
    mismatches = [
        (r[0], (r[1], r[2]), (row[1], row[2]))
        for r, row in zip(rebuilt, chart)
        if (r[1], msats_to_sats(r[2])) != (row[1], msats_to_sats(row[2]))
    ]
    assert not mismatches, f"rebuilt chart diverges from the live chart SQL: {mismatches[:5]}"

    # ...and the sparse full-history series must contain every windowed day, so a
    # consumer trimming it themselves sees the same rows.
    assert set(ours) <= set(all_counts) and set(ours) <= set(all_msats)
    for d, (c, m) in ours.items():
        # Only the chart's OLDEST day may legitimately differ (partial coverage:
        # timestamp cutoff vs calendar day) - every other day must match exactly.
        if d != first_day:
            assert (all_counts[d], all_msats[d]) == (c, m), f"windowed vs full-history differ on {d}"


@_LIVE
def test_live_zap_series_share_their_days_and_stay_consistent():
    """count and sum are the two aggregates of ONE population: every day present
    in one must be present in the other, and a day with receipts must not report a
    negative or absurd amount. Divergence here would break mean-zap-size math."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(ZAP_COUNT_DAY.sql, ZAP_COUNT_DAY.bind_params)
            counts = dict(cur.fetchall())
            cur.execute(ZAPPED_MSATS_DAY.sql, ZAPPED_MSATS_DAY.bind_params)
            msats = dict(cur.fetchall())
    finally:
        conn.close()

    assert counts and msats
    assert set(counts) == set(msats), "the two zap series cover different days"
    assert all(v > 0 for v in counts.values())  # sparse: no zero-count rows
    assert all(v >= 0 for v in msats.values())  # a zero-amount day is legal, negative is not


@_LIVE
def test_live_fabricated_demo_receipt_is_still_the_series_left_edge():
    """The methodology tells consumers the first day is a fabricated 100k-sat
    receipt standing alone before a ~3-month gap. If the data ever changes (the
    row is purged, or real 2025 activity is backfilled), that prose becomes a lie
    that a model will repeat - so pin the shape the caveat describes."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(ZAP_COUNT_DAY.sql, ZAP_COUNT_DAY.bind_params)
            counts = cur.fetchall()
            cur.execute(ZAPPED_MSATS_DAY.sql, ZAPPED_MSATS_DAY.bind_params)
            msats = dict(cur.fetchall())
            cur.execute(
                """SELECT count(*) FROM events_kind_9735_zaps_zapstore
                   WHERE event_id LIKE '778150ee%'"""
            )
            (demo_present,) = cur.fetchone()
    finally:
        conn.close()

    assert demo_present == 1, "the audited demo receipt 778150ee... is no longer in the table"
    first_day, first_count = counts[0]
    assert first_count == 1, f"the left edge is no longer a lone receipt ({first_count})"
    assert msats[first_day] == 100_000_000, "the left-edge receipt is no longer the 100k-sat demo"
    # ...followed by the gap the caveat describes (months, not days).
    assert (counts[1][0] - first_day).days > 60, (
        f"the ~3-month gap after the demo receipt has closed ({counts[1][0] - first_day})"
    )


@_LIVE
def test_live_curator_share_still_matches_the_attribution_caveat():
    """The attribution caveat states a NUMBER ("20.9% of this series' all-time
    sats"), and an earlier version of these tests only checked that the string
    appeared in the prose - prose pinned against prose. This pins it against the
    data, so the day the share moves the caveat fails instead of quietly lying to
    every model that reads it.

    Tolerance is deliberately wide (+/- 3 points): the share drifts continuously
    as zaps arrive, and a gate that fails on normal drift gets muted. It catches
    what matters - a step change, or the claim rotting over months."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT sum(amount_msats),
                          sum(amount_msats) FILTER (WHERE recipient_pubkey = %(curator)s)
                   FROM events_kind_9735_zaps_zapstore""",
                {"curator": ZAPSTORE_PUBKEY},
            )
            total, curator = cur.fetchone()
    finally:
        conn.close()

    assert total and curator
    share = 100.0 * float(curator) / float(total)
    assert 17.9 <= share <= 23.9, (
        f"curator/indexer share is now {share:.1f}% of all-time sats - the "
        "attribution caveat still claims 20.9%, restate it (and the 'four fifths')"
    )


@_LIVE
def test_live_zap_amounts_are_whole_sats_so_the_conversion_note_holds():
    """The msats caveat says every amount is currently a whole number of sats, so
    per-day rounding happens to be exact today. That is a MEASURED claim about
    live data, not a schema guarantee - pin it, because the caveat's advice
    ("sum first, convert once") is what protects the day it stops being true."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT count(*), count(*) FILTER (WHERE amount_msats % 1000 <> 0),
                          count(*) FILTER (WHERE amount_msats IS NULL)
                   FROM events_kind_9735_zaps_zapstore"""
            )
            total, sub_sat, nulls = cur.fetchone()
    finally:
        conn.close()

    assert total > 0
    assert sub_sat == 0, (
        f"{sub_sat} receipts carry sub-satoshi amounts - the msats methodology's "
        "'every amount is a whole number of sats' claim is now stale"
    )
    assert nulls == 0, (
        f"{nulls} receipts have a NULL amount - the COALESCE path is now live, and "
        "the methodology's 'no NULLs present' measurement is stale"
    )


# -- the three-part methodology (the web/MCP single source) -----------------------


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_methodology_is_derived_from_the_three_parts(spec):
    """methodology is a PROPERTY joining summary + partition + caveats - never
    authored separately - so the MCP envelope and the web registry render the same
    authored words by construction."""
    assert spec.methodology == " ".join((spec.summary, spec.partition) + spec.caveats)


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_methodology_parts_stand_alone(spec):
    """The web renders each part separately (summary blurb, partition section,
    caveat bullets), so every part must be non-empty complete sentence(s) - a
    fragment that only reads correctly mid-join would render as a broken bullet."""
    parts = (spec.summary, spec.partition) + spec.caveats
    for p in parts:
        assert p and p[0].isupper() or p.startswith("'"), f"not sentence-initial: {p[:40]!r}"
        assert p.rstrip().endswith("."), f"not sentence-terminal: {p[-40:]!r}"
    assert len(spec.caveats) >= 2  # at least two substantive gotchas
    if spec.output == "sparse":
        assert "sparse" in spec.caveats[-1]  # the sparseness caveat closes the list
    elif spec.output == "dense":
        assert any("DENSE" in c or "dense" in c for c in spec.caveats)
    else:
        # entity outputs have no sparse/dense axis; the summary states the row
        # grain instead, and no caveat may CLAIM sparseness.
        assert "one row" in spec.summary.lower()
        assert not any(c.startswith("Series is sparse") for c in spec.caveats)


# -- the zaps pair (a SECOND source table, a SECOND population) -------------------

ZAP_SPECS = [ZAP_COUNT_DAY, ZAPPED_MSATS_DAY]
# NB: named PUBLISHING_SPECS, not RELEASE_SPECS - a module-level RELEASE_SPECS
# already exists above with a NARROWER meaning (the two *_release_count specs).
# Reusing the name shadowed it, and the older test only kept working because its
# @parametrize bound the list at import time; any reordering would have silently
# run it over the wrong specs.
PUBLISHING_SPECS = [s for s in LEGACY_SPECS if s not in ZAP_SPECS]


def test_zap_metrics_read_a_different_table_than_every_publishing_metric():
    """The structural claim behind the routing description: zaps are NOT the
    publishing partition sliced differently, they are an unrelated population. If
    they ever shared a table, the tool description's "different ROW population ...
    their totals never reconcile" would be a lie the model acts on."""
    for z in ZAP_SPECS:
        assert z.source_table == "events_kind_9735_zaps_zapstore"
        for r in PUBLISHING_SPECS:
            assert z.source_table != r.source_table


def test_zap_metrics_carry_no_bind_parameters():
    """Their population IS the table (the loader pre-filters to receipts bearing a
    32267 a-tag), so there is no predicate to bind - and nothing that could be
    interpolated. Pinned so a future edit cannot quietly add an inline literal."""
    for z in ZAP_SPECS:
        assert z.bind_params == {}
        # No BARE % either, not just no %(name)s. db.fetch_metric now passes
        # `bind_params or None` so an empty dict cannot trip psycopg2's formatter,
        # but a % in a parameter-free statement is still a smell worth refusing:
        # it means the statement wanted interpolation nobody declared.
        assert "%" not in z.sql


def test_zap_pair_shares_one_population_and_says_so():
    """count and sum over the SAME rows: same table, same (absent) predicate, same
    day expression. A consumer must be able to divide one by the other to get a
    mean zap size, so any divergence here is a silent correctness bug."""
    c, m = ZAP_COUNT_DAY, ZAPPED_MSATS_DAY
    assert c.source_table == m.source_table
    assert c.as_of_sql == m.as_of_sql
    assert c.partition == m.partition  # one authored copy, shared
    # Identical apart from the aggregate + alias.
    assert c.sql.replace("COUNT(*)::bigint AS zap_count", "X") == m.sql.replace(
        "COALESCE(SUM(amount_msats), 0)::bigint AS zapped_msats", "X"
    )
    # Each points at the other, since "how many zaps" vs "how many sats" is the
    # likeliest misroute between them.
    assert "zapstore.zapped_msats" in c.methodology
    assert "zapstore.zap_count" in m.methodology


def test_zapped_msats_states_the_unit_trap_loudly():
    """msats vs sats is a 1000x error - the single most damaging misread available
    in this registry - so the unit must be stated in the summary, the column
    description, AND a caveat that pins the sum-then-convert order."""
    m = ZAPPED_MSATS_DAY
    assert m.unit == "msats"
    assert "MILLI-satoshis" in m.summary and "divide by 1000" in m.summary
    assert "divide by 1000" in m.columns[1].description
    joined = " ".join(m.caveats)
    assert "SUM the msats first" in joined
    assert "never round per day" in joined


def test_zap_metrics_flag_the_fabricated_receipt():
    """The series' left edge is a fabricated event (audited 2026-07-27). A model
    reading the raw series would report it as real 2025 activity, so both metrics
    must carry the warning, its identifying prefix, and the exclusion advice."""
    for z in ZAP_SPECS:
        m = z.methodology
        assert "FABRICATED" in m
        assert "778150ee" in m
        assert "demo-zap-request" in m
        assert "exclude it from any all-time figure" in m


def test_zap_metrics_state_the_attribution_and_completeness_limits():
    """Two limits a consumer would otherwise assume away: the money is attributed
    to APPS (not developer earnings - ~22% goes to the curator/indexer), and the
    series is a slight undercount with a known, measured magnitude."""
    for z in ZAP_SPECS:
        m = z.methodology
        # The curator share, stated over THIS series' denominator (all-time), with
        # the audit's differently-scoped figure explicitly labelled as such - the
        # live gate below is what keeps the number itself honest.
        assert "20.9%" in m and "four fifths" in m
        assert "trailing-365-day window" in m  # names the other denominator
        assert "0.22%" in m and "undercount" in m


def test_zapped_msats_coalesces_nulls_like_the_chart():
    """amount_msats is nullable; the chart COALESCEs to 0. Dropping NULLs instead
    would silently under-sum, and the two surfaces would disagree."""
    assert "COALESCE(SUM(amount_msats), 0)" in ZAPPED_MSATS_DAY.sql
    assert "nullable" in ZAPPED_MSATS_DAY.methodology
    assert "counted as 0" in ZAPPED_MSATS_DAY.methodology


# -- the kind-slug convention ----------------------------------------------------


@pytest.mark.parametrize("spec", ALL_SPECS, ids=SPEC_IDS)
def test_every_kind_mention_carries_its_nozzl_slug(spec):
    """House rule: a bare kind number is unreadable to anyone not holding nozzl's
    kinds.toml in their head, so every Zapstore kind we name in text a consumer sees
    - the methodology and the freshness disclaimer - must be followed by the slug
    nozzl archives it under. Checked on FIRST mention per string; repeating the slug
    on every later mention in the same paragraph is noise, not clarity."""
    slugs = {
        "30063": "zapstore-app-releases",
        "32267": "zapstore-app-catalog",
        "9735": "zap-receipts",
        "38421": "routstr-provider-announcements",
        "38422": "routstr-analytics-snapshots",
        "38423": "routstr-model-catalog",
        "38425": "routstr-provider-reviews",
    }
    for text in (spec.methodology, spec.as_of_source, *(c.description for c in spec.columns)):
        for kind, slug in slugs.items():
            if f"kind-{kind}" in text:
                assert f"kind-{kind} ({slug})" in text, (
                    f"{spec.key}: kind-{kind} named without its slug ({slug})"
                )


# -- leaderboard specs: their own contract ----------------------------------------
# (design: docs/design/zapstore-leaderboard-mcp-design.md)

SNAPSHOT_SPECS = [APP_STACK_COUNT_CURRENT, PUBLISHER_APP_COUNT_CURRENT]


@pytest.mark.parametrize("spec", LEADERBOARD_SPECS, ids=LEADERBOARD_IDS)
def test_leaderboard_specs_declare_series_semantics_consistently(spec):
    """series_type / additive_over_time / grain / window_mode / snapshot_note move
    together or the envelope lies: a snapshot with a window mode, or one of these
    Zapstore leaderboard event grains claiming non-additivity, is a contract
    contradiction the type system cannot catch. (Not a registry-wide rule: an event
    grain is non-additive whenever one of its columns does not add - a distinct
    count, a running total - as on nutzaps.mint_active_users or
    routstr.cumulative_revenue; see MetricSpec.additive_over_time.)"""
    if spec.series_type == "snapshot":
        assert spec.additive_over_time is False
        assert spec.grain == "current"
        assert spec.window_mode == "none"
        assert spec.snapshot_note, "the state: line needs its parenthetical"
        assert "replaceable" in spec.snapshot_note
    else:
        assert spec.series_type == "event"
        assert spec.additive_over_time is True
        assert spec.grain in {"day", "total"}
        assert spec.window_mode in {"day_wrap", "epoch_inline"}
        assert spec.snapshot_note is None


@pytest.mark.parametrize("spec", LEADERBOARD_SPECS, ids=LEADERBOARD_IDS)
def test_leaderboard_label_columns_are_declared_third_party(spec):
    """Every label column must carry third_party=True (drives the sanitize pass +
    the untrusted: envelope line) and key/measure columns must NOT - marking a
    hex pubkey untrusted would dilute the one line consumers must take seriously."""
    by_name = {c.name: c for c in spec.columns}
    assert by_name["publisher_name"].third_party is True
    if "app_name" in by_name:
        assert by_name["app_name"].third_party is True
    for name in ("publisher_pubkey", "app_id", "utc_day", "zap_count", "zapped_msats",
                 "stack_count", "app_count", "is_nostr_native"):
        if name in by_name:
            assert by_name[name].third_party is False, f"{name} must not be marked third-party"


@pytest.mark.parametrize("spec", LEADERBOARD_SPECS, ids=LEADERBOARD_IDS)
def test_leaderboard_columns_are_produced_by_the_sql(spec):
    """Column contract == SQL output: every declared column name must be an alias
    or selected name in the statement."""
    for c in spec.columns:
        assert (f"AS {c.name}" in spec.sql) or (f"t.{c.name}" in spec.sql) or (
            c.name == "utc_day" and "utc_day" in spec.sql
        ), f"{spec.key}: column {c.name} not produced by the SQL"


def test_app_zaps_native_flag_polarity_matches_the_partition_pair():
    """is_nostr_native must be the SAME predicate as the native partition metrics
    ((pubkey <> curator) OR (d_tag = ANY(own))) - a drifted polarity here would
    make card 4 reproduce a different partition than the publishing metrics."""
    for spec in (APP_ZAPS_DAY, APP_ZAPS_TOTAL):
        assert "t.app_pubkey <> %(curator)s OR t.app_d_tag = ANY(%(own_d_tags)s)" in spec.sql
        assert spec.bind_params["curator"] == ZAPSTORE_PUBKEY
        assert spec.bind_params["own_d_tags"] == ZAPSTORE_OWN_D_TAGS


def test_publisher_app_count_uses_the_native_partition_rule():
    assert "pubkey <> %(curator)s OR d_tag = ANY(%(own_d_tags)s)" in PUBLISHER_APP_COUNT_CURRENT.sql
    assert "COUNT(DISTINCT d_tag)" in PUBLISHER_APP_COUNT_CURRENT.sql


def test_app_stack_count_mirrors_the_pages_public_stack_pipeline():
    """The three load-bearing shapes of postgres.ts getZapstoreAppStackLeaderboards:
    public-only filter, the exact a-tag regex, COUNT(DISTINCT event_id)."""
    sql = APP_STACK_COUNT_CURRENT.sql
    assert "coalesce(array_length(ps.app_refs, 1), 0) > 0" in sql
    assert "'^32267:([0-9a-f]{64}):(.+)$'" in sql
    assert "COUNT(DISTINCT event_id)" in sql
    # Only the entity-filter defaults - the page pipeline itself is bind-free.
    assert APP_STACK_COUNT_CURRENT.bind_params == {
        "filter_publisher": None,
        "filter_app": None,
    }


def test_app_zaps_total_carries_inline_epoch_bounds_and_day_does_not():
    """@total must filter BEFORE aggregation (inline bounds, always bound);
    @day is windowed by the outer wrapper (spec SQL windowless)."""
    assert "%(from_epoch)s" in APP_ZAPS_TOTAL.sql and "%(to_epoch_excl)s" in APP_ZAPS_TOTAL.sql
    assert "%(from_epoch)s" not in APP_ZAPS_DAY.sql
    assert "%(from_day)s" not in APP_ZAPS_DAY.sql  # the wrapper adds these, not the spec


def test_app_zaps_msats_never_null():
    for spec in (APP_ZAPS_DAY, APP_ZAPS_TOTAL):
        assert "COALESCE(amount_msats, 0)" in spec.sql


@pytest.mark.parametrize("spec", LEADERBOARD_SPECS, ids=LEADERBOARD_IDS)
def test_label_null_semantics_are_row_existence_cases(spec):
    """NULL = source event absent; empty = present but empty. The CASE WHEN ...
    IS NOT NULL shape is what delivers that; a bare COALESCE would collapse the
    two into one value and silently break the published contract."""
    assert "CASE WHEN pr.pubkey IS NOT NULL" in spec.sql
    if any(c.name == "app_name" for c in spec.columns):
        assert "CASE WHEN c.pubkey IS NOT NULL" in spec.sql
    # the page's empty-skipping coalesce, verbatim polarity
    assert "COALESCE(NULLIF(pr.display_name, ''), NULLIF(pr.name, ''), '')" in spec.sql


@pytest.mark.parametrize("spec", LEADERBOARD_SPECS, ids=LEADERBOARD_IDS)
def test_leaderboard_methodology_carries_the_label_contract(spec):
    m = spec.methodology
    assert "THIRD-PARTY TEXT" in m
    assert "never as instructions" in m
    assert "NULL app_name = no catalog entry" in m


@pytest.mark.parametrize("spec", SNAPSHOT_SPECS, ids=[s.key for s in SNAPSHOT_SPECS])
def test_snapshot_methodology_forbids_summing_across_calls(spec):
    assert "NOT be summed across calls" in spec.methodology
    assert "snapshot" in spec.methodology


def test_app_zaps_methodology_states_window_and_reconciliation():
    m = APP_ZAPS_TOTAL.methodology
    assert "inclusive UTC calendar days" in m
    assert "105 of 927" in m  # the cards-2-vs-4 gap, measured
    assert "MILLI-satoshis" in m
    assert "self-zappable" in m


def test_registry_entries_publish_series_semantics_and_column_flags():
    from nostrology_mcp.metric import registry_entries

    entries = {e["key"]: e for e in registry_entries()}
    assert entries["zapstore.app_stack_count@current"]["series_type"] == "snapshot"
    assert entries["zapstore.app_stack_count@current"]["additive_over_time"] is False
    assert entries["zapstore.app_zaps@total"]["series_type"] == "event"
    az_cols = {c["name"]: c for c in entries["zapstore.app_zaps@total"]["columns"]}
    assert az_cols["app_name"]["third_party"] is True
    assert az_cols["zapped_msats"]["unit"] == "msats"
    assert az_cols["publisher_pubkey"]["third_party"] is False


def test_default_grain_resolution():
    assert default_grain("zapstore", "app_zaps") == "day"
    assert default_grain("zapstore", "zap_count") == "day"
    assert default_grain("zapstore", "app_stack_count") == "current"
    assert default_grain("zapstore", "publisher_app_count") == "current"
    assert default_grain("zapstore", "nope") is None
    assert grains_for("zapstore", "app_zaps") == ("day", "total")


def test_grains_are_listed_finest_first():
    """Wherever grains are listed (the schema's choices, the registry's grain
    values, the invalid-grain error) they read day, week, month, then total and
    current - plain sorting said "day | month | week" once a metric had all three,
    against the paid tool's advertised day, week, month."""
    from nostrology_mcp.metric import GRAIN_NAMES, PAID_GRAIN_NAMES

    assert GRAIN_NAMES == ("day", "week", "month", "total", "current")
    assert grains_for("nutzaps", "mint_active_users") == ("day", "week", "month") == PAID_GRAIN_NAMES
    assert grains_for("nutzaps", "payment_activity") == ("day", "total")


def test_a_declared_default_grain_is_unique_and_overrides_the_rule():
    """is_default_grain marks at most ONE spec per metric, and only where
    default_grain()'s rule (current, else day, else the only grain) would pick a
    different one - a flag that restates the rule would read as a decision nobody
    took. Today exactly one metric declares one: nutzaps.mint_active_users, month.
    The paid registry keeps its own PAID_DEFAULT_GRAIN and never sets the flag."""
    from collections import defaultdict

    by_metric = defaultdict(list)
    for s in METRICS.values():
        by_metric[(s.domain, s.metric)].append(s)
    declared = {}
    for (domain, metric), specs in by_metric.items():
        flagged = [s.grain for s in specs if s.is_default_grain]
        assert len(flagged) <= 1, f"{domain}.{metric} declares {flagged} as its default"
        if flagged:
            grains = {s.grain for s in specs}
            rule = "current" if "current" in grains else ("day" if "day" in grains else None)
            assert len(specs) > 1 and rule not in (None, flagged[0]), f"{domain}.{metric}: the flag restates the rule"
            assert default_grain(domain, metric) == flagged[0]
            declared[(domain, metric)] = flagged[0]
    assert declared == {("nutzaps", "mint_active_users"): "month"}
    assert not any(s.is_default_grain for s in PAID_METRICS.values())


# -- the invalid-grain redirect ----------------------------------------------------
# Every grain a caller can ask for: the registry's own, plus the coarser periods a
# model reaches for - grain is a free string, so an off-list ask reaches the tool body.
_ASKABLE_GRAINS = ("day", "week", "month", "quarter", "year", "total", "current")
_METRIC_PAIRS = sorted({(s.domain, s.metric) for s in METRICS.values()})


def test_a_served_grain_or_an_unknown_metric_has_no_redirect():
    for domain, metric in _METRIC_PAIRS:
        for grain in grains_for(domain, metric):
            assert grain_redirect(domain, metric, grain) is None, f"{domain}.{metric}@{grain}"
    assert grain_redirect("zapstore", "nope", "total") is None


def test_every_redirect_names_only_metrics_and_grains_that_exist():
    """A redirect is an instruction the caller follows next, so it must never send
    them to a metric that does not exist, or to a grain it does not serve - in both
    shapes the clauses use: '<domain>.<metric> (grain g)' for another metric and
    'grain=g' for the metric itself."""
    named = re.compile(r"\b(zapstore|routstr|nutzaps)\.([a-z_]+)(?: \(grain ([a-z]+)\))?")
    shown = 0
    for domain, metric in _METRIC_PAIRS:
        for grain in _ASKABLE_GRAINS:
            text = grain_redirect(domain, metric, grain)
            if text is None:
                continue
            shown += 1
            # It is a clause appended after " - ": no capital, no full stop, ASCII.
            assert text == text.strip() and text[0].islower() and not text.endswith("."), text
            assert text.isascii(), text
            for d, m, g in named.findall(text):
                assert grains_for(d, m), f"{domain}.{metric}@{grain} names unknown {d}.{m}"
                assert not g or g in grains_for(d, m), f"{domain}.{metric}@{grain}: {d}.{m} does not serve {g}"
            for g in re.findall(r"\bgrain=([a-z]+)", text):
                assert g in grains_for(domain, metric), f"{domain}.{metric}@{grain}: grain={g} is not served"
    assert shown > 0


def test_a_derived_sum_clause_lands_only_on_a_series_flagged_additive():
    """The derived 'its day rows add up' trusts the day spec's whole-row flag, so it
    must never land on a series flagged additive_over_time: false. The flag is all
    the registry knows: a distinct count can still add across days when each entity
    falls on exactly one day (zapstore.new_native_app_count counts an app on its
    first-release day only), so no column test can tell an overlapping one apart.
    routstr.network_activity@day holds the overlapping kind (active_providers) under
    a flag that says true until nostrology-data#14 - hence its hand-written override,
    pinned grain by grain in test_the_redirect_for_a_known_ask so it cannot be
    trimmed back to the derived clause unnoticed. Per-column additivity would let
    this test see it; #14 proposes it."""
    from nostrology_mcp.metric import _GRAIN_REDIRECTS

    for domain, metric in _METRIC_PAIRS:
        for grain in _ASKABLE_GRAINS:
            text = grain_redirect(domain, metric, grain)
            if text is None or grain in _GRAIN_REDIRECTS.get((domain, metric), {}):
                continue
            if re.search(r"\bsum (them|the day rows|these day rows)\b", text):
                assert lookup(domain, metric, "day").additive_over_time, f"{domain}.{metric}@{grain}: summing a non-additive series"


def test_the_fallback_for_a_non_additive_day_series_claims_no_more_than_the_flag(monkeypatch):
    """No metric reaches the derived non-additive clause today (both Routstr levels
    have overrides); a future one might mix additive and distinct columns, so the
    clause must not say that nothing in its rows adds up."""
    from nostrology_mcp import metric as metric_mod

    trimmed = {k: v for k, v in metric_mod._GRAIN_REDIRECTS.items() if k != ("routstr", "catalog_size")}
    monkeypatch.setattr(metric_mod, "_GRAIN_REDIRECTS", trimmed)
    assert grain_redirect("routstr", "catalog_size", "month") == (
        "its rows are not additive as a whole (additive_over_time: false) - check "
        "what each column holds in its methodology before summing any"
    )


def test_redirect_overrides_only_name_grains_their_metric_does_not_serve():
    """An override for a served grain is dead text that can never be shown - the sign
    that a grain was added without retiring its redirect. It catches only the key for
    the grain that became served: giving routstr.network_activity a total grain must
    delete its WHOLE override, or week to year keep saying 'sum the day rows' where
    'call grain=total' would be the better answer."""
    from nostrology_mcp.metric import _GRAIN_REDIRECTS

    for (domain, metric), by_grain in _GRAIN_REDIRECTS.items():
        assert grains_for(domain, metric), f"an override for unknown {domain}.{metric}"
        dead = sorted(g for g in by_grain if g in grains_for(domain, metric))
        assert not dead, f"{domain}.{metric}: overrides for served grains {dead}"


def test_every_coarser_ask_gets_a_redirect_where_one_exists():
    """The ask the call log shows - grain=total on a day-only series, 11 times from
    model callers between 2026-08-01 and 2026-09-30 (10 of them one first-party client;
    our batteries' own probes aside) - never meets a bare list of grains again. Every
    metric with a day grain and no total answers total and every period; every
    metric with a windowed total grain answers every period."""
    for domain, metric in _METRIC_PAIRS:
        grains = grains_for(domain, metric)
        asks = ()
        if "day" in grains and "total" not in grains:
            asks = ("total", "week", "month", "quarter", "year")
        elif "total" in grains and lookup(domain, metric, "total").window_mode != "none":
            asks = ("week", "month", "quarter", "year")
        for grain in asks:
            if grain not in grains:
                assert grain_redirect(domain, metric, grain), f"{domain}.{metric}@{grain}"


@pytest.mark.parametrize(
    "domain,metric,grain,needle",
    [
        # The asks the call log shows, as they were made.
        ("zapstore", "new_native_app_count", "total", "its day rows add up - sum them over from/to"),
        ("zapstore", "zapped_msats", "total", "its day rows add up - sum them over from/to"),
        ("routstr", "network_activity", "total", "sum the day rows over from/to (active_providers does not"),
        # Every grain of that override, so trimming it to total cannot slip through.
        ("routstr", "network_activity", "week", "sum the day rows over from/to (active_providers does not"),
        ("routstr", "network_activity", "month", "sum the day rows over from/to (active_providers does not"),
        ("routstr", "network_activity", "quarter", "sum the day rows over from/to (active_providers does not"),
        ("routstr", "network_activity", "year", "sum the day rows over from/to (active_providers does not"),
        # The nutzaps battery's rejection probes (NZ81, NZ83, NZ82, NZ89).
        ("nutzaps", "payment_activity", "week", "for one week, call grain=total with from/to spanning it"),
        ("nutzaps", "payment_activity", "quarter", "for one quarter, call grain=total with from/to spanning it"),
        ("nutzaps", "mint_activity", "total", "call nutzaps.mint_leaderboard (grain total) with from/to"),
        ("nutzaps", "mint_leaderboard", "day", "call nutzaps.mint_activity (grain day)"),
        ("nutzaps", "mint_active_users", "total", "do not add across buckets or mints"),
        ("nutzaps", "mint_active_users", "quarter", "month is the coarsest bucket"),
        ("nutzaps", "mint_active_users", "year", "month is the coarsest bucket"),
        # The Routstr ledgers' weekly era: a window sums daily buckets only.
        ("routstr", "provider_activity", "month", "for a period before a provider reported daily"),
        ("routstr", "model_activity", "year", "use grain=day, whose weekly rows carry it"),
        # Sparse levels: the value on a day is the latest row on or before it.
        ("routstr", "cumulative_revenue", "total", "the value on a day is the latest row on or before it"),
        ("routstr", "cumulative_revenue", "month", "its last day's value minus the value the day before it starts"),
        ("routstr", "catalog_size", "month", "the size on a day is the latest row on or before it"),
        # Snapshots keep the clause they carried before 0.5.1, word for word.
        ("zapstore", "publisher_app_count", "day",
         "it is a current-state snapshot with no time axis, so its values are not additive across days"),
    ],
)
def test_the_redirect_for_a_known_ask(domain, metric, grain, needle):
    assert needle in grain_redirect(domain, metric, grain)


@pytest.mark.parametrize(
    "domain,metric,grain",
    [("zapstore", "zap_count", "current"), ("nutzaps", "payment_activity", "current"),
     ("nutzaps", "mint_active_users", "current"), ("routstr", "provider_activity", "current")],
)
def test_no_redirect_where_nothing_better_than_the_grains_can_be_said(domain, metric, grain):
    assert grain_redirect(domain, metric, grain) is None


# -- live gates for the leaderboard metrics ---------------------------------------


def _live_dsn():
    return os.environ["NOSTROLOGY_TEST_DSN"]


@_LIVE
def test_live_app_zaps_total_reconciles_with_the_zap_pair():
    """The all-time app_zaps@total rows must sum EXACTLY to the zap_count /
    zapped_msats series totals - one population, two projections; any gap means
    the label joins dropped or duplicated rows."""
    from nostrology_mcp.db import fetch_metric

    totals = fetch_metric(
        APP_ZAPS_TOTAL,
        dsn=_live_dsn(),
        params=None,
    ).rows
    zc = fetch_metric(METRICS["zapstore.zap_count@day"], dsn=_live_dsn()).rows
    zm = fetch_metric(METRICS["zapstore.zapped_msats@day"], dsn=_live_dsn()).rows
    assert sum(r[5] for r in totals) == sum(r[1] for r in zc)
    assert sum(r[6] for r in totals) == sum(r[1] for r in zm)
    # keys unique - the label join must not fan out
    keys = [(r[0], r[2]) for r in totals]
    assert len(keys) == len(set(keys))


@_LIVE
def test_live_app_zaps_day_rolls_up_to_total():
    from nostrology_mcp.db import fetch_metric

    day = fetch_metric(APP_ZAPS_DAY, dsn=_live_dsn()).rows
    tot = fetch_metric(
        APP_ZAPS_TOTAL,
        dsn=_live_dsn(),
        params={**APP_ZAPS_TOTAL.bind_params, "from_epoch": None, "to_epoch_excl": None},
    ).rows
    rolled = {}
    for r in day:
        k = (r[1], r[3])  # (publisher_pubkey, app_id)
        zc, zm = rolled.get(k, (0, 0))
        rolled[k] = (zc + r[6], zm + r[7])
    assert rolled == {(r[0], r[2]): (r[5], r[6]) for r in tot}


@_LIVE
def test_live_app_stack_count_matches_the_pages_query():
    """Run the page's own aggregation (postgres.ts getZapstoreAppStackLeaderboards
    reduced to counts) and compare row-for-row."""
    import psycopg2

    conn = psycopg2.connect(_live_dsn(), password=os.environ.get("NOSTROLOGY_PGPASSWORD"))
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH expanded AS (
                    SELECT ps.event_id, regexp_match(ref, '^32267:([0-9a-f]{64}):(.+)$') AS m
                    FROM events_kind_30267_app_stacks ps, unnest(ps.app_refs) AS ref
                    WHERE coalesce(array_length(ps.app_refs, 1), 0) > 0
                )
                SELECT m[1], m[2], COUNT(DISTINCT event_id)::bigint
                FROM expanded WHERE m IS NOT NULL GROUP BY 1, 2
            """)
            page = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    finally:
        conn.close()

    from nostrology_mcp.db import fetch_metric

    ours = fetch_metric(APP_STACK_COUNT_CURRENT, dsn=_live_dsn()).rows
    assert {(r[0], r[2]): r[4] for r in ours} == page


@_LIVE
def test_live_publisher_app_count_matches_the_pages_query():
    import psycopg2

    conn = psycopg2.connect(_live_dsn(), password=os.environ.get("NOSTROLOGY_PGPASSWORD"))
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pubkey, COUNT(DISTINCT d_tag)::bigint
                FROM events_kind_32267_app_catalog
                WHERE pubkey <> %(curator)s OR d_tag = ANY(%(own_d_tags)s)
                GROUP BY pubkey
                """,
                {"curator": ZAPSTORE_PUBKEY, "own_d_tags": ZAPSTORE_OWN_D_TAGS},
            )
            page = dict(cur.fetchall())
    finally:
        conn.close()

    from nostrology_mcp.db import fetch_metric

    ours = fetch_metric(PUBLISHER_APP_COUNT_CURRENT, dsn=_live_dsn()).rows
    assert {r[0]: r[2] for r in ours} == page


@_LIVE
def test_live_windowed_day_wrap_equals_python_side_trim():
    """The SQL-side window (wrapper) must equal trimming the full series in
    Python - two implementations of one definition."""
    from datetime import date as _date

    from nostrology_mcp.db import fetch_metric
    from nostrology_mcp.window import ResolvedWindow, windowed_day_sql

    spec = METRICS["zapstore.zap_count@day"]
    full = fetch_metric(spec, dsn=_live_dsn()).rows
    windowed = fetch_metric(
        spec,
        dsn=_live_dsn(),
        sql=windowed_day_sql(spec.sql),
        params={**spec.bind_params, "from_day": _date(2026, 7, 1), "to_day": _date(2026, 7, 31)},
    ).rows
    assert windowed == [r for r in full if _date(2026, 7, 1) <= r[0] <= _date(2026, 7, 31)]


def test_registry_entries_document_the_tool_parameters():
    """The registry is what humans (and browsing agents) read INSTEAD of the
    inputSchema - the from/to windowing and grain values must be discoverable
    there, not only in the tool schema."""
    from nostrology_mcp.metric import registry_entries

    entries = {e["key"]: e for e in registry_entries()}
    az = entries["zapstore.app_zaps@total"]["params"]
    assert az["grain"] == {"values": ["day", "total"], "default": "day", "note": "omit to get the default"}
    assert az["window"]["accepted"] is True
    assert "YYYY-MM-DD" in az["window"]["from"] and "YYYY-MM-DD" in az["window"]["to"]
    snap = entries["zapstore.app_stack_count@current"]["params"]
    assert snap["grain"]["values"] == ["current"] and snap["grain"]["default"] == "current"
    assert snap["window"]["accepted"] is False and "rejected" in snap["window"]["reason"]
    legacy = entries["zapstore.zap_count@day"]["params"]
    assert legacy["window"]["accepted"] is True  # the existing six accept from/to too
    for e in entries.values():
        assert e["params"]["format"] == ["csv", "markdown"]


@_LIVE
def test_live_entity_filter_selects_exactly_the_unfiltered_row():
    """A publisher+app filtered app_zaps@total must return exactly the one row
    the unfiltered call carries for that app - the filter is a row selector,
    never a different computation. Self-selecting: filters for the top row of
    the unfiltered ranking, so it never depends on one app staying zapped."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(APP_ZAPS_TOTAL.sql, APP_ZAPS_TOTAL.bind_params)
            unfiltered = cur.fetchall()
            assert unfiltered, "zaps table is empty - the gate cannot run"
            top = unfiltered[0]
            top_publisher, top_app = top[0].strip(), top[2]
            cur.execute(
                APP_ZAPS_TOTAL.sql,
                {
                    **APP_ZAPS_TOTAL.bind_params,
                    "filter_publisher": top_publisher,
                    "filter_app": top_app,
                },
            )
            filtered = cur.fetchall()
    finally:
        conn.close()

    assert filtered == [top]


# -- the PAID specs - their own contract block -------------------------------------
# Deliberately NOT folded into ALL_SPECS: the free-spec conventions above assume
# utc_day/day_wrap shapes the paid family does not share, and ALL_SPECS
# membership is exactly what the payment-bypass pin in test_server_nutzaps.py
# asserts the paid specs do NOT have.

from nostrology_mcp.metric import PAID_METRICS  # noqa: E402

PAID_SPECS = sorted(PAID_METRICS.values(), key=lambda s: s.key)
PAID_IDS = [s.key for s in PAID_SPECS]


@pytest.mark.parametrize("spec", PAID_SPECS, ids=PAID_IDS)
def test_paid_spec_conventions(spec):
    """The paid family's own invariants: priced, epoch_inline with its bound
    keys present, granularity bound to the spec's own grain, non-additive
    (distinct counts), bucket-keyed columns, and the freshness probe on the
    BASE event table (the aggregate is TRUNCATE+INSERTed, so MAX() over it
    reflects the rebuild, not the data)."""
    # Against cashu.price_sats(), NOT a literal: the registry's price and the
    # price the tool charges must be the same number by construction, so that
    # CASHU_PRICE_SATS cannot move one without the other.
    from nostrology_mcp import cashu

    assert spec.price_sats == cashu.price_sats()
    assert spec.domain == "nutzaps"
    assert spec.window_mode == "epoch_inline"
    assert set(spec.bind_params) == {"granularity", "from_epoch", "to_epoch_excl"}
    assert spec.bind_params["granularity"] == spec.grain
    assert spec.additive_over_time is False
    assert [c.name for c in spec.columns] == ["period_start", "unique_users"]
    assert spec.source_table == "active_user_counts_kind_9321_nutzaps"
    assert "events_kind_9321_nutzaps" in spec.as_of_sql
    # No value interpolation - binds only (the repo-wide SQL convention).
    assert "%(granularity)s" in spec.sql
    assert spec.grain not in spec.sql.replace("%(granularity)s", "")


@pytest.mark.parametrize("spec", PAID_SPECS, ids=PAID_IDS)
def test_paid_spec_registry_entry_carries_price_and_tool(spec):
    from nostrology_mcp.metric import registry_entries

    from nostrology_mcp import cashu

    (entry,) = [e for e in registry_entries() if e["key"] == spec.key]
    assert entry["price_sats"] == cashu.price_sats()
    assert entry["tool"] == "nostrology_query_nutzaps"
    assert entry["params"]["grain"]["default"] == "month"
    assert entry["params"]["grain"]["values"] == ["day", "week", "month"]


@_LIVE
@pytest.mark.parametrize("spec", PAID_SPECS, ids=PAID_IDS)
def test_paid_sql_explains_against_live_schema(spec):
    """Same drift gate as the free specs: EXPLAIN, never execute."""
    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute("EXPLAIN " + spec.sql, dict(spec.bind_params))
            assert cur.fetchall()
            cur.execute("EXPLAIN " + spec.as_of_sql)
            assert cur.fetchall()
    finally:
        conn.close()


def test_a_metrics_summary_does_not_vary_by_grain():
    """`summary` describes the METRIC, not the registry entry - because every
    surface that shows a metric (the /data card, the per-metric page) collapses
    its entries to the DEFAULT grain's and prints that one summary beside a
    label advertising all the grains. A grain-specific sentence there reads as
    "this metric only buckets by month" while the label next to it says
    month, day, week.

    Covers the free multi-grain metrics too (app_zaps @day/@total), which set
    this convention long before the paid family existed."""
    from collections import defaultdict

    by_metric = defaultdict(list)
    for spec in list(METRICS.values()) + list(PAID_METRICS.values()):
        by_metric[(spec.domain, spec.metric)].append(spec)

    multi = {k: v for k, v in by_metric.items() if len(v) > 1}
    assert multi, "no multi-grain metric left to check - this test has lost its subject"
    for (domain, metric), specs in multi.items():
        summaries = {s.summary for s in specs}
        assert len(summaries) == 1, (
            f"{domain}.{metric} has {len(summaries)} summaries across grains "
            f"{sorted(s.grain for s in specs)}; the catalog shows only one of them"
        )
        # Same collapse, same rule, applied per COLUMN NAME: grains may carry
        # different column SETS (app_zaps@day has utc_day, @total has no time
        # axis at all - the page surfaces that as "Columns per grain"), but a
        # column present in two grains must be DESCRIBED identically, because
        # /data's "Response columns" table publishes exactly one of those
        # descriptions under a header listing every grain.
        described = defaultdict(set)
        for spec in specs:
            for column in spec.columns:
                described[column.name].add(column.description)
        for name, descriptions in described.items():
            assert len(descriptions) == 1, (
                f"{domain}.{metric} describes column '{name}' differently per "
                f"grain: {sorted(descriptions)}; the catalog publishes one of them"
            )


@_LIVE
@pytest.mark.parametrize("spec", PAID_SPECS, ids=PAID_IDS)
def test_paid_grain_actually_has_rows(spec):
    """EXPLAIN proves the SQL matches the schema; it does NOT prove the grain is
    served. `granularity` is a bind, so a grain the loader never populated
    explains perfectly and returns an empty series - the tool would take 21 sats
    and answer with a header and no data. Every grain we advertise (and charge
    for) must therefore be shown to hold rows of the right shape.

    Executing rather than explaining is safe here: the aggregate table is three
    figures of rows, and this is the one assertion that cannot be made without
    reading them."""
    import datetime as dt

    import psycopg2

    conn = psycopg2.connect(
        os.environ["NOSTROLOGY_TEST_DSN"], password=os.environ.get("NOSTROLOGY_PGPASSWORD")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(spec.sql, dict(spec.bind_params))
            rows = cur.fetchall()
    finally:
        conn.close()

    assert rows, f"{spec.key} is advertised and priced but returns no rows"
    for period_start, unique_users in rows:
        # `is dt.date`, not isinstance: datetime subclasses date, and the
        # column contract published to callers says date.
        assert type(period_start) is dt.date
        assert isinstance(unique_users, int) and unique_users > 0  # sparse: zeros are omitted
    # Buckets are distinct and ordered - the contract the envelope's column
    # description states, and what a caller charting the series relies on.
    starts = [r[0] for r in rows]
    assert starts == sorted(starts)
    assert len(set(starts)) == len(starts)

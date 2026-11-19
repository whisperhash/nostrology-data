"""Unit tests for pipeline/kinds.py (kind -> table + MIN_ROWS maps + the class range law)."""

from dagster_code.pipeline.kinds import (
    LIGHTNING_MVS,
    RELAYS_MVS,
    kind_to_class,
    kind_to_table,
    min_rows_for,
)


def test_kind_to_class_range_law():
    # replaceable: 0, 3, and [10000,20000)
    assert kind_to_class(0) == "replaceable"
    assert kind_to_class(3) == "replaceable"
    assert kind_to_class(10002) == "replaceable"
    assert kind_to_class(10019) == "replaceable"
    assert kind_to_class(19999) == "replaceable"
    # addressable: [30000,40000)
    assert kind_to_class(30063) == "addressable"
    assert kind_to_class(32267) == "addressable"
    assert kind_to_class(39999) == "addressable"
    # regular: everything else integer (incl. 1, 9321, 9735)
    assert kind_to_class(1) == "regular"
    assert kind_to_class(9321) == "regular"
    assert kind_to_class(9735) == "regular"
    assert kind_to_class(40000) == "regular"
    # ephemeral [20000,30000) and non-integers -> None (no archived class)
    assert kind_to_class(20000) is None
    assert kind_to_class(29999) is None
    assert kind_to_class("nope") is None
    assert kind_to_class(None) is None
    # accepts str
    assert kind_to_class("10002") == "replaceable"


def test_kind_to_table():
    assert kind_to_table(10002) == "events_kind_10002_relay_lists"
    assert kind_to_table(9735) == "events_kind_9735_zaps"
    assert kind_to_table("9735") == "events_kind_9735_zaps"  # accepts str
    assert kind_to_table(9999) == ""  # unknown -> empty (caller skips the count)


def test_kind_to_table_zapstore_subset_distinct():
    # The 9735->zapstore subset writes a DIFFERENT table than the full 9735 load.
    assert kind_to_table(9735, "zapstore") == "events_kind_9735_zaps_zapstore"
    assert kind_to_table(9735, "zapstore") != kind_to_table(9735)
    # into=zapstore only applies to 9735.
    assert kind_to_table(10002, "zapstore") == "events_kind_10002_relay_lists"


def test_min_rows_for():
    assert min_rows_for(0) == 1806407
    assert min_rows_for(9735) == 2793747
    assert min_rows_for(10002) == 757084
    assert min_rows_for(9999) == 1  # unmeasured -> 1


def test_min_rows_for_zapstore_forced_to_one():
    # The zapstore subset is NOT gated by 9735's big floor (run.sh:732).
    assert min_rows_for(9735, "zapstore") == 1


def test_relays_mvs_are_the_three_leaderboard_views():
    assert RELAYS_MVS == (
        "relay_stats_mv",
        "relay_per_user_distribution_mv",
        "relay_tier_user_counts_mv",
    )


def test_lightning_mvs_are_the_leaderboard_and_daily_grain_views():
    # Order matters only for refresh logging; both must be present so lightning_mv
    # refreshes the leaderboard AND the trend-chart daily grain. See schema/init.sql.
    assert LIGHTNING_MVS == (
        "lightning_provider_stats_mv",
        "lightning_daily_provider_mv",
    )

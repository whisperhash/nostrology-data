"""Window resolution tests: the UTC-day algebra everything else composes with.
The properties pinned here are the ones two implementations (this server, the
web page once migrated) must agree on - inclusive both ends, half-open epoch
bounds, defaulted-to visibility."""

from datetime import date, datetime, timezone

import pytest

from nostrology_mcp.window import (
    ResolvedWindow,
    resolve_window,
    today_utc,
    windowed_day_sql,
)


def test_both_omitted_means_no_window():
    """None, not a degenerate all-time window: the caller must take the
    byte-identical legacy SQL path when nothing was asked for."""
    assert resolve_window(None, None) is None


def test_from_only_defaults_to_to_today():
    w = resolve_window("2026-07-04", None)
    assert w.from_day == date(2026, 7, 4)
    assert w.to_day == today_utc()
    assert w.to_defaulted is True


def test_to_only_leaves_from_unbounded():
    w = resolve_window(None, "2026-07-31")
    assert w.from_day is None
    assert w.to_day == date(2026, 7, 31)
    assert w.to_defaulted is False
    assert w.from_epoch is None


def test_epoch_bounds_are_utc_midnights_half_open():
    """Inclusive UTC days [from .. to] == created_at in [from 00:00Z, to+1 00:00Z).
    The +1-day exclusive upper bound IS the inclusivity of the to-day."""
    w = resolve_window("2026-07-04", "2026-08-02")
    assert w.from_epoch == int(datetime(2026, 7, 4, tzinfo=timezone.utc).timestamp())
    assert w.to_epoch_excl == int(datetime(2026, 8, 3, tzinfo=timezone.utc).timestamp())


def test_epoch_bounds_cross_month_and_year():
    w = resolve_window("2025-12-31", "2025-12-31")
    assert w.to_epoch_excl - w.from_epoch == 86400  # one full day


def test_single_day_window():
    w = resolve_window("2026-07-15", "2026-07-15")
    assert w.from_day == w.to_day == date(2026, 7, 15)


def test_from_after_to_rejected_with_both_values_named():
    with pytest.raises(ValueError) as exc:
        resolve_window("2026-09-01", "2026-07-01")
    msg = str(exc.value)
    assert "2026-09-01" in msg and "2026-07-01" in msg
    assert "from <= to" in msg


@pytest.mark.parametrize("bad", ["last-month", "2026-13-01", "2026-07-32", "yesterday", ""])
def test_unparseable_dates_rejected_with_the_expected_shape(bad):
    with pytest.raises(ValueError) as exc:
        resolve_window(bad, None)
    assert "YYYY-MM-DD" in str(exc.value)
    assert repr(bad) in str(exc.value)  # the ask is echoed so the caller sees what arrived


def test_echo_flags_a_defaulted_to():
    w = resolve_window("2026-07-04", None)
    assert "to defaulted to today - final day in progress" in w.echo()
    w2 = resolve_window("2026-07-04", "2026-08-02")
    assert "defaulted" not in w2.echo()
    assert w2.echo().startswith("2026-07-04..2026-08-02 (UTC days, inclusive")


def test_echo_names_series_start_when_from_unbounded():
    w = resolve_window(None, "2026-07-31")
    assert w.echo().startswith("series start..2026-07-31")


def test_windowed_day_sql_wraps_without_editing_the_inner_statement():
    inner = "SELECT 1 AS utc_day, 2 AS n\nORDER BY 1"
    wrapped = windowed_day_sql(inner)
    assert inner in wrapped  # the spec SQL survives byte-identical inside
    assert wrapped.count("%(from_day)s") == 2 and wrapped.count("%(to_day)s") == 2
    assert wrapped.rstrip().endswith("ORDER BY 1")
    # NULL-tolerant bounds: either side may be unbounded
    assert "IS NULL OR" in wrapped


def test_resolved_window_is_frozen():
    w = ResolvedWindow(from_day=None, to_day=date(2026, 8, 2), to_defaulted=False)
    with pytest.raises(Exception):
        w.to_day = date(2026, 8, 3)  # type: ignore[misc]

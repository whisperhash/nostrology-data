"""from/to window resolution - inclusive ISO UTC calendar days, resolved once here
so the SQL bind values, the envelope echo, and the validation errors cannot drift
from each other. Design: docs/design/zapstore-leaderboard-mcp-design.md sec 2-3.

The invariant everything else leans on: ALL bucketing and filtering is UTC-day
aligned. `from`/`to` are inclusive on both ends; the epoch bounds derived here are
half-open ([from 00:00:00Z, to+1day 00:00:00Z)) because created_at is a second
timestamp, and "inclusive of the whole to-day" IS the half-open next-midnight bound.

ValueErrors raised here are agent-facing: they must say what was wrong AND what
legal input looks like, because a one-turn self-correction is the difference
between a usable and a frustrating tool.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone


def today_utc() -> date:
    """The single source of "today" - UTC by construction, never the box TZ."""
    return datetime.now(timezone.utc).date()


@dataclass(frozen=True)
class ResolvedWindow:
    """A validated, fully-resolved window. from_day None = unbounded (series
    start); to_day is always concrete. to_defaulted drives the envelope's
    "final day in progress" flag - a caller must be able to tell whether the
    answer moves on re-ask."""

    from_day: date | None
    to_day: date
    to_defaulted: bool

    @property
    def from_epoch(self) -> int | None:
        if self.from_day is None:
            return None
        return int(datetime(self.from_day.year, self.from_day.month, self.from_day.day, tzinfo=timezone.utc).timestamp())

    @property
    def to_epoch_excl(self) -> int:
        nxt = self.to_day + timedelta(days=1)
        return int(datetime(nxt.year, nxt.month, nxt.day, tzinfo=timezone.utc).timestamp())

    def echo(self) -> str:
        """The envelope's resolved-range echo (design sec 3: always the RESOLVED
        range, flagging a defaulted `to` so the caller knows the answer moves)."""
        start = self.from_day.isoformat() if self.from_day else "series start"
        parts = [f"{start}..{self.to_day.isoformat()} (UTC days, inclusive"]
        if self.to_defaulted:
            parts.append("; to defaulted to today - final day in progress")
        parts.append(")")
        return "".join(parts)


def _parse_day(name: str, value: str) -> date:
    """YYYY-MM-DD. date.fromisoformat also tolerates the other ISO day spellings
    (compact 20260701, week dates 2026-W27-1) - accepted: each is an unambiguous
    calendar day and the envelope echoes the normalized form. Year 9999 is
    rejected: to_epoch_excl computes to_day + 1 day, and date.max + 1 raises
    OverflowError - which, uncaught, would skip the demand log and hand the
    caller a bare "date value out of range" (9999-12-31 is a common agent
    sentinel for "no upper bound"; the right spelling of that is omitting the
    parameter)."""
    try:
        parsed = date.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"'{name}' must be an ISO UTC calendar day (YYYY-MM-DD), got {value!r}"
        ) from exc
    if parsed.year > 9998:
        raise ValueError(
            f"'{name}' is out of range ({value!r}) - omit the parameter instead "
            "of passing a far-future sentinel"
        )
    return parsed


def resolve_window(from_str: str | None, to_str: str | None) -> ResolvedWindow | None:
    """None when no window was asked for (both omitted) - the caller then runs the
    metric's unmodified full-history SQL, which is the pre-window contract for the
    original six metrics and must stay byte-identical for them."""
    if from_str is None and to_str is None:
        return None
    from_day = _parse_day("from", from_str) if from_str is not None else None
    if to_str is not None:
        to_day, to_defaulted = _parse_day("to", to_str), False
    else:
        to_day, to_defaulted = today_utc(), True
    if from_day is not None and from_day > to_day:
        defaulted = " ('to' was omitted and defaulted to today UTC)" if to_defaulted else ""
        raise ValueError(
            f"'from' ({from_day.isoformat()}) is after 'to' ({to_day.isoformat()})"
            f"{defaulted} - the window is inclusive UTC days with from <= to"
        )
    return ResolvedWindow(from_day=from_day, to_day=to_day, to_defaulted=to_defaulted)


def capped_sql(sql: str, order_by: str = "1", window: bool = False) -> str:
    """Wrap a row-capped metric's SQL: the pre-LIMIT total rides along as the
    LAST column (`COUNT(*) OVER () AS __total_rows`, the reference records'
    contract - the server strips it and drives the `truncated:` line from it),
    the optional utc_day window filters column 0 exactly like windowed_day_sql,
    and `LIMIT %(row_cap)s` bounds the response. The spec SQL stays uncapped so
    the live gates and the pg contract tier read whole populations."""
    inner = sql.rstrip().rstrip(";")
    where = (
        "WHERE (%(from_day)s::date IS NULL OR w.utc_day >= %(from_day)s::date)\n"
        "  AND (%(to_day)s::date IS NULL OR w.utc_day <= %(to_day)s::date)\n"
        if window
        else ""
    )
    return (
        "SELECT w.*, COUNT(*) OVER () AS __total_rows FROM (\n"
        + inner
        + "\n) w\n"
        + where
        + f"ORDER BY {order_by}\n"
        + "LIMIT %(row_cap)s"
    )


def windowed_day_sql(sql: str, order_by: str = "1") -> str:
    """Wrap a day-grain metric's SQL with an outer utc_day filter. The spec SQL
    stays windowless (full-history default byte-identical, zero risk to the
    original six); this wrapper is applied only when a window was requested.
    Column 0 being utc_day is a pinned contract for every day-grain spec.

    `order_by` re-states the spec's OUTER ordering (spec.windowed_order_by):
    Postgres neither preserves a subquery's ORDER BY nor sorts stably, so
    without it the windowed call would lose the deterministic within-day
    tie-breakers the unwindowed call has."""
    inner = sql.rstrip().rstrip(";")
    return (
        "SELECT * FROM (\n"
        + inner
        + "\n) w\n"
        + "WHERE (%(from_day)s::date IS NULL OR w.utc_day >= %(from_day)s::date)\n"
        + "  AND (%(to_day)s::date IS NULL OR w.utc_day <= %(to_day)s::date)\n"
        + f"ORDER BY {order_by}"
    )

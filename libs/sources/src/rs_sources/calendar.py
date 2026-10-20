"""XNYS trading-calendar adapter over exchange_calendars.

Implements the rs_domain.TradingCalendar protocol. This is the only place
Eastern Time is handled; every datetime returned is tz-aware UTC (binding
S002 convention). Scheduled facts only - unscheduled halts and surprise
late opens are feed-observable, not calendar-knowable (ADR-0017).
"""

from datetime import date, datetime, timezone

import exchange_calendars as xcals

# Explicit bounds so the supported range never drifts with the wall clock
# (the library default is "today minus 20 years" to "today plus one year").
# Queries outside it raise ValueError rather than guess.
_FIRST_DAY = date(1990, 1, 1)
_LAST_DAY = date(2050, 12, 31)


class ExchangeCalendar:
    """TradingCalendar backed by exchange_calendars (XNYS by default).

    Early closes are part of the library's session data, so session_close
    carries the early-close instant with no special-casing.
    """

    def __init__(self, exchange: str = "XNYS") -> None:
        self._calendar = xcals.get_calendar(
            exchange, start=_FIRST_DAY.isoformat(), end=_LAST_DAY.isoformat()
        )

    def is_session(self, day: date) -> bool:
        return bool(self._calendar.is_session(day.isoformat()))

    def session_open(self, day: date) -> datetime:
        return self._calendar.session_open(day.isoformat()).to_pydatetime().astimezone(timezone.utc)

    def session_close(self, day: date) -> datetime:
        return self._calendar.session_close(day.isoformat()).to_pydatetime().astimezone(timezone.utc)

    def sessions_in_range(self, start: date, end: date) -> list[date]:
        sessions = self._calendar.sessions_in_range(start.isoformat(), end.isoformat())
        return [ts.date() for ts in sessions]

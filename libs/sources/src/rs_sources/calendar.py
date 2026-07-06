"""XNYS trading-calendar adapter over pandas-market-calendars.

Implements the rs_domain.TradingCalendar protocol. This is the only place
Eastern Time is handled; every datetime returned is tz-aware UTC (binding
S002 convention). Scheduled facts only — unscheduled halts and surprise
late opens are feed-observable, not calendar-knowable (ADR-0017).
"""

from datetime import date, datetime, timedelta, timezone
from functools import lru_cache

import pandas_market_calendars as mcal


class PandasMarketCalendar:
    """TradingCalendar backed by pandas_market_calendars (XNYS by default).

    schedule() covers half-days: market_close carries the early-close
    instant, so session_close needs no special-casing. Single-day schedules
    are memoized — the natural caller sequence (is_session → open → close)
    costs one schedule() build, not three.
    """

    # Generous fixed window per neighbor lookup: NYSE's longest modern
    # closure is about a week; an empty 30-day window means the query is
    # outside the calendar's supported range and raising beats guessing.
    _WINDOW = timedelta(days=30)

    def __init__(self, exchange: str = "XNYS") -> None:
        self._calendar = mcal.get_calendar(exchange)
        self._schedule_for = lru_cache(maxsize=128)(self._fetch_schedule)

    def is_session(self, day: date) -> bool:
        return not self._schedule_for(day).empty

    def session_open(self, day: date) -> datetime:
        return self._session_bound(day, "market_open")

    def session_close(self, day: date) -> datetime:
        return self._session_bound(day, "market_close")

    def next_session(self, day: date) -> date:
        schedule = self._calendar.schedule(
            start_date=day + timedelta(days=1), end_date=day + self._WINDOW
        )
        if schedule.empty:
            raise ValueError(f"no session within {self._WINDOW.days} days after {day}")
        return schedule.index[0].date()

    def previous_session(self, day: date) -> date:
        schedule = self._calendar.schedule(
            start_date=day - self._WINDOW, end_date=day - timedelta(days=1)
        )
        if schedule.empty:
            raise ValueError(f"no session within {self._WINDOW.days} days before {day}")
        return schedule.index[-1].date()

    def _fetch_schedule(self, day: date):
        return self._calendar.schedule(start_date=day, end_date=day)

    def _session_bound(self, day: date, column: str) -> datetime:
        schedule = self._schedule_for(day)
        if schedule.empty:
            raise ValueError(f"{day} is not a trading session")
        return schedule.iloc[0][column].to_pydatetime().astimezone(timezone.utc)

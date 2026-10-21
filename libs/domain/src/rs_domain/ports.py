"""Ports - the clock and calendar seams.

Implementations live outside rs_domain: the exchange_calendars adapter lives in
rs_sources.
"""

from collections.abc import Sequence
from datetime import date, datetime
from typing import Protocol


class Clock(Protocol):
    """Injected time. Domain code never reads ambient time (CI-enforced)."""

    def now(self) -> datetime:
        """Current instant, tz-aware UTC."""
        ...


class TradingCalendar(Protocol):
    """XNYS session facts. All returned datetimes are tz-aware UTC.

    Scheduled facts only: unscheduled halts and surprise late opens are
    feed-observable, not calendar-knowable (ADR-0017 staleness covers them).
    """

    def is_session(self, day: date) -> bool:
        ...

    def session_open(self, day: date) -> datetime:
        """Open instant for a session day; raises if `day` is not a session."""
        ...

    def session_close(self, day: date) -> datetime:
        """Close instant for a session day — early closes included; raises if not a session."""
        ...

    def sessions_in_range(self, start: date, end: date) -> Sequence[date]:
        """Session days from `start` to `end`, both inclusive, in order."""
        ...

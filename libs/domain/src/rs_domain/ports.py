"""Ports — the binding replay seams (ADR-0023) and the calendar seam (S002).

Implementations live outside rs_domain: live/recorded event sources come with
S003; the exchange_calendars adapter lives in rs_sources.
"""

from collections.abc import AsyncIterator, Sequence
from datetime import date, datetime
from typing import Protocol

from rs_domain.events import BarClosed
from rs_domain.models import Alert, Bar


class Clock(Protocol):
    """Injected time. Domain code never reads ambient time (CI-enforced)."""

    def now(self) -> datetime:
        """Current instant, tz-aware UTC."""
        ...


class EventSource(Protocol):
    """The ingress seam upstream of the Bus-1 fan-out (ADR-0005).

    Live adapter in production; recorded adapter for warm-up, backtest, and
    replay tests — interchangeable behind this port.
    """

    def events(self) -> AsyncIterator[BarClosed]:
        ...


class Detector(Protocol):
    """A detection rule, run behind the ADR-0008 error boundary (grilling #5).

    Stateless and pure: `decide` is a function of the session window alone —
    all of today's RTH bars, newest last — with no hidden state, so the same
    window always yields the same decision on replay (ADR-0023). It returns the
    rs_domain.Alert to announce, or None; Alert is the decision type, there is
    no separate decision model.
    """

    def decide(self, window: Sequence[Bar]) -> Alert | None:
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

"""The Clock implementation - the single allowlisted ambient-time call site.

Everything else in the domain receives a Clock (or a concrete event_time on
an event). scripts/check_clock_discipline.sh enforces this in CI.
"""

from datetime import UTC, datetime


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

from datetime import UTC, datetime, timedelta

import pytest


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        self._now += delta


@pytest.fixture
def clock() -> FixedClock:
    return FixedClock(datetime(2026, 10, 19, 15, 0, tzinfo=UTC))

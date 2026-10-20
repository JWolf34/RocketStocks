from datetime import UTC, datetime, timedelta

import time_machine

from rs_domain import Clock, SystemClock


def test_system_clock_returns_aware_utc():
    now = SystemClock().now()
    assert now.utcoffset() == timedelta(0)


@time_machine.travel(datetime(2026, 7, 6, 16, 0, tzinfo=UTC), tick=False)
def test_system_clock_is_time_machine_controllable():
    assert SystemClock().now() == datetime(2026, 7, 6, 16, 0, tzinfo=UTC)


def test_system_clock_satisfies_protocol():
    clock: Clock = SystemClock()
    assert isinstance(clock.now(), datetime)

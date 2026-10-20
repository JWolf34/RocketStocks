"""The shared Schwab budget: spacing across concurrent callers, in virtual time."""

import asyncio

import pytest

from rs_ingest.rate_limit import RateLimiter


class VirtualTime:
    def __init__(self) -> None:
        self.t = 0.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


def _limiter(per_minute: int, time: VirtualTime) -> RateLimiter:
    return RateLimiter(per_minute, now=time.now, sleep=time.sleep)


async def test_the_first_call_is_free_and_later_ones_are_spaced():
    time = VirtualTime()
    limiter = _limiter(120, time)  # one every half second

    for _ in range(4):
        await limiter.acquire()

    assert time.sleeps == pytest.approx([0.5, 0.5, 0.5])


async def test_time_spent_elsewhere_counts_toward_the_spacing():
    time = VirtualTime()
    limiter = _limiter(60, time)
    await limiter.acquire()
    time.t += 0.25  # the caller's own request took a quarter second

    await limiter.acquire()

    assert time.sleeps == pytest.approx([0.75])


async def test_an_idle_limiter_does_not_bank_a_burst():
    time = VirtualTime()
    limiter = _limiter(60, time)
    await limiter.acquire()
    time.t += 600  # idle for ten minutes

    await limiter.acquire()
    await limiter.acquire()

    assert time.sleeps == pytest.approx([1.0])


async def test_concurrent_callers_share_one_budget():
    time = VirtualTime()
    limiter = _limiter(60, time)

    await asyncio.gather(*(limiter.acquire() for _ in range(5)))

    assert time.t == pytest.approx(4.0)  # five calls, four one-second gaps


@pytest.mark.parametrize("calls", [0, -1])
def test_rejects_a_budget_below_one_call_a_minute(calls):
    with pytest.raises(ValueError, match="at least 1"):
        RateLimiter(calls)

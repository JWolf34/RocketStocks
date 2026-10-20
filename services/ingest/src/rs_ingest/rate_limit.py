"""The shared Schwab call budget: one limiter in `Dependencies`, awaited by
every job before each Schwab request, so concurrent jobs cannot together
exceed the account's rate limit."""

import asyncio
from collections.abc import Awaitable, Callable

SECONDS_PER_MINUTE = 60.0


def _loop_time() -> float:
    return asyncio.get_running_loop().time()


class RateLimiter:
    """Spaces calls at least 60 / `calls_per_minute` seconds apart, across concurrent callers.

    Spacing rather than a token bucket: a burst after idle time is the first
    thing that would trip Schwab's limit. `now` and `sleep` are injectable so
    tests run in virtual time.
    """

    def __init__(
        self,
        calls_per_minute: int,
        *,
        now: Callable[[], float] = _loop_time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if calls_per_minute < 1:
            raise ValueError(f"calls_per_minute must be at least 1, got {calls_per_minute}")
        self._interval = SECONDS_PER_MINUTE / calls_per_minute
        self._now = now
        self._sleep = sleep
        self._next_slot = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Return once the caller may make one request."""
        async with self._lock:
            delay = self._next_slot - self._now()
            if delay > 0:
                await self._sleep(delay)
            self._next_slot = self._now() + self._interval

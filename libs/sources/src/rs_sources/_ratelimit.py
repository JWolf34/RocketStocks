"""Minimum-spacing rate limiter shared by the HTTP adapters."""

import asyncio


class MinInterval:
    """Spaces calls at least `interval` seconds apart, across concurrent callers."""

    def __init__(self, interval: float) -> None:
        self._interval = interval
        self._next_slot = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            delay = self._next_slot - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_slot = loop.time() + self._interval

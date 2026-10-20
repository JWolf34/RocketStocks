import asyncio

from rs_sources._ratelimit import MinInterval


async def test_calls_are_spaced_by_the_interval_even_when_concurrent():
    limiter = MinInterval(0.05)
    loop = asyncio.get_running_loop()
    stamps: list[float] = []

    async def call():
        await limiter.wait()
        stamps.append(loop.time())

    await asyncio.gather(*(call() for _ in range(4)))

    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    assert all(gap >= 0.045 for gap in gaps)

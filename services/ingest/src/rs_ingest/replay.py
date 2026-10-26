"""Replay of the popularity-surge rule over every stored Popularity snapshot, both
origins, without holding the history.

The rule judges each snapshot on its own, as `popularity_surge.replay` does, so
reading the history in windows gives the same hits as one pass over all of it.
"""

from collections.abc import AsyncIterator
from datetime import datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_analysis import popularity_surge
from rs_analysis.popularity_surge import SurgeEvent
from rs_persistence import PopularityRepository

# A day is about 50,000 snapshots: small enough to hold, large enough to be few queries.
REPLAY_WINDOW = timedelta(days=1)


async def replayed_hits(
    session_factory: async_sessionmaker[AsyncSession], now: datetime
) -> AsyncIterator[SurgeEvent]:
    """Every hit the current rule makes over the stored snapshots up to `now`, oldest window first."""
    async with session_factory() as session:
        repo = PopularityRepository(session)
        window_start = await repo.earliest_polled_at()
        if window_start is None:
            return
        while window_start <= now:
            window_end = window_start + REPLAY_WINDOW
            hits = [
                event
                for snapshot in await repo.list_between(window_start, window_end)
                if (event := popularity_surge.evaluate_latest(snapshot)) is not None
            ]
            for event in hits:
                yield event
            window_start = window_end

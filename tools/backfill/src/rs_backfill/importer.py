"""Map v1 popularity rows to snapshots and insert them idempotently."""

from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from itertools import batched

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_backfill.dump import V1Row
from rs_backfill.zones import floor_bucket, naive_to_utc, resolve_zone
from rs_domain import PopularityOrigin, PopularitySnapshot
from rs_persistence import PopularityRepository

BATCH_SIZE = 5000


@dataclass
class ImportStats:
    parsed: int = 0
    inserted: int = 0
    rejected: Counter[str] = field(default_factory=Counter)

    @property
    def already_present(self) -> int:
        """Rows skipped because (polled_at, ticker) already existed: a live poll,
        an earlier import, or a duplicate inside the dump."""
        return self.parsed - sum(self.rejected.values()) - self.inserted


def to_snapshot(row: V1Row, zone_name: str) -> PopularitySnapshot | str:
    """The snapshot for one v1 row, or the reason it cannot be imported.

    `fetched_at` equals `polled_at`: v1 stored only the rounded boundary, and
    a derived value keeps reruns identical.
    """
    ticker = (row.ticker or "").strip().upper()
    if not ticker:
        return "missing ticker"
    if row.rank is None or row.mentions is None:
        return "missing rank or mentions"
    polled_at = naive_to_utc(row.at, resolve_zone(zone_name))
    if polled_at is None:
        return "wall-clock time does not exist in the zone"
    if floor_bucket(polled_at) != polled_at:
        return "not on a half-hour boundary"
    try:
        return PopularitySnapshot(
            polled_at=polled_at,
            ticker=ticker,
            rank=row.rank,
            mentions=row.mentions,
            upvotes=row.upvotes,
            rank_24h_ago=row.rank_24h_ago or None,
            mentions_24h_ago=row.mentions_24h_ago,
            fetched_at=polled_at,
            origin=PopularityOrigin.V1_IMPORT,
        )
    except ValidationError:
        return "value out of range"


async def import_rows(
    rows: Iterable[V1Row],
    zone_name: str,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    batch_size: int = BATCH_SIZE,
) -> ImportStats:
    """Insert in committed batches, so an interrupted run resumes by rerunning."""
    stats = ImportStats()

    def snapshots() -> Iterator[PopularitySnapshot]:
        for row in rows:
            stats.parsed += 1
            result = to_snapshot(row, zone_name)
            if isinstance(result, str):
                stats.rejected[result] += 1
            else:
                yield result

    for batch in batched(snapshots(), batch_size, strict=False):
        async with session_factory() as session:
            stats.inserted += await PopularityRepository(session).insert_many(batch)
            await session.commit()
    return stats

"""Options chain snapshots: the append-only writer and its restart-state read."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import ChainSnapshot, Dataset, SourceStatus, median_instant
from rs_persistence.models import chain_snapshot_table


class ChainSnapshotRepository:
    """Append-only writer for options_chain_snapshot (rev-005, S008). One
    ChainSnapshot persists as its payload rows, or - when it has none (a
    tombstone: failed/skipped fetch) - as a single payload-less membership row,
    so every (cycle, name) pair has rows either way (ADR-0030). Rows are
    immutable; there is no update path by design. Caller owns the commit."""

    # A snapshot older than this contributes no restart state: the stale guard
    # compares consecutive snapshots, and after days of silence "accept
    # anything" is the correct posture. Bounding by fetched_at also keeps the
    # restart query inside recent uncompressed chunks instead of scanning a
    # forward-only table that grows without retention.
    RESTART_STATE_HORIZON = timedelta(days=7)

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, snapshot: ChainSnapshot) -> int:
        """Persist one snapshot; returns rows written (>= 1)."""
        provenance = {
            "underlying": snapshot.underlying,
            "dataset": snapshot.dataset.value,
            "cycle_id": snapshot.cycle_id,
            "fetched_at": snapshot.fetched_at,
            "source_status": snapshot.source_status.value,
        }
        rows = [provenance | quote.model_dump() for quote in snapshot.rows] or [provenance]
        await self._session.execute(insert(chain_snapshot_table), rows)
        return len(rows)

    async def last_accepted_median_quote_times(
        self, dataset: Dataset, now: datetime
    ) -> dict[str, datetime]:
        """Stale-guard restart state (S008): per underlying, the median
        quote_time of the most recent *accepted* (ok/partial - never stale;
        tombstones carry no quote_time) snapshot within RESTART_STATE_HORIZON
        of `now`. `now` comes from the caller's Clock - no ambient time in
        persistence (ADR-0023). Median = rs_domain.median_instant, the same
        definition the scan loop applies to fresh responses."""
        t = chain_snapshot_table
        accepted = (
            (t.c.dataset == dataset.value)
            & t.c.source_status.in_([SourceStatus.OK.value, SourceStatus.PARTIAL.value])
            & t.c.quote_time.is_not(None)
            & (t.c.fetched_at >= now - self.RESTART_STATE_HORIZON)
        )
        latest = (
            select(t.c.underlying, func.max(t.c.cycle_id).label("cycle_id"))
            .where(accepted)
            .group_by(t.c.underlying)
            .subquery()
        )
        result = await self._session.execute(
            select(t.c.underlying, t.c.quote_time)
            .join(
                latest,
                (t.c.underlying == latest.c.underlying) & (t.c.cycle_id == latest.c.cycle_id),
            )
            .where(accepted)
        )
        times: dict[str, list[datetime]] = {}
        for underlying, quote_time in result:
            times.setdefault(underlying, []).append(quote_time.astimezone(UTC))
        medians = {}
        for underlying, values in times.items():
            median = median_instant(values)
            assert median is not None  # values is never empty by construction
            medians[underlying] = median
        return medians

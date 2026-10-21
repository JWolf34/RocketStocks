"""Options chain snapshots: the append-only writer."""

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import ChainSnapshot
from rs_persistence.models import chain_snapshot_table


class ChainSnapshotRepository:
    """Append-only writer for options_chain_snapshot (rev-005). One
    ChainSnapshot persists as its payload rows, or - when it has none (a
    tombstone: failed/skipped fetch) - as a single payload-less membership row,
    so every (cycle, name) pair has rows either way. Rows are
    immutable; there is no update path by design. Caller owns the commit."""

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

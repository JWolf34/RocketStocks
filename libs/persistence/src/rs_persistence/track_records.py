"""track_record: append-only writes of event-study outcomes."""

from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import TrackRecord
from rs_persistence.models import TrackRecordRow


class TrackRecordRepository:
    """Caller owns the commit. Reads go through rs_query."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, record: TrackRecord) -> int:
        """Append one study result; returns its id. Rows are never updated: a rerun
        inserts a new row and readers take the latest."""
        row = TrackRecordRow(**record.model_dump(mode="python"))
        self._session.add(row)
        await self._session.flush()
        return row.id

"""track_record reads."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import TrackRecord
from rs_persistence import TrackRecordRow
from rs_persistence.track_records import track_record_from_row


async def track_records(session: AsyncSession, trigger: str, rule_version: str) -> list[TrackRecord]:
    """A Trigger version's Track record, one row per horizon in horizon order; empty
    when the nightly job has not measured the version yet."""
    rows = await session.scalars(
        select(TrackRecordRow)
        .where(TrackRecordRow.trigger == trigger, TrackRecordRow.rule_version == rule_version)
        .order_by(TrackRecordRow.horizon)
    )
    return [track_record_from_row(row) for row in rows]

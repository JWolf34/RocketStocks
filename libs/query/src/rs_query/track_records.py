"""track_record reads."""

from datetime import UTC

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import TrackRecord, TrackRecordInput
from rs_persistence import TrackRecordRow

# The logged rate replaces the reconstructed one once this many real alerts are measured.
TRACK_RECORD_LOGGED_MIN_N = 30


def _to_track_record(row: TrackRecordRow) -> TrackRecord:
    return TrackRecord(
        alert_type=row.alert_type,
        rule_version=row.rule_version,
        computed_at=row.computed_at.astimezone(UTC),
        input=TrackRecordInput(row.input),
        n=row.n,
        n_unpriced=row.n_unpriced,
        hit_rate_1d=row.hit_rate_1d,
        hit_rate_5d=row.hit_rate_5d,
        hit_rate_10d=row.hit_rate_10d,
        median_excess_1d=row.median_excess_1d,
        median_excess_5d=row.median_excess_5d,
        median_excess_10d=row.median_excess_10d,
        survivorship_caveat=row.survivorship_caveat,
    )


async def _latest(
    session: AsyncSession, alert_type: str, rule_version: str, source: TrackRecordInput
) -> TrackRecord | None:
    row = await session.scalar(
        select(TrackRecordRow)
        .where(
            TrackRecordRow.alert_type == alert_type,
            TrackRecordRow.rule_version == rule_version,
            TrackRecordRow.input == source.value,
        )
        .order_by(TrackRecordRow.computed_at.desc(), TrackRecordRow.id.desc())
        .limit(1)
    )
    return None if row is None else _to_track_record(row)


async def current_track_record(
    session: AsyncSession, alert_type: str, rule_version: str
) -> TrackRecord | None:
    """The track record the bot should show for a rule version: the latest `logged`
    row once its N reaches TRACK_RECORD_LOGGED_MIN_N, otherwise the latest
    `reconstructed` row. None when there is nothing to show (a thin logged row is
    never shown on its own)."""
    logged = await _latest(session, alert_type, rule_version, TrackRecordInput.LOGGED)
    if logged is not None and logged.n >= TRACK_RECORD_LOGGED_MIN_N:
        return logged
    return await _latest(session, alert_type, rule_version, TrackRecordInput.RECONSTRUCTED)

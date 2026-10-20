"""import_verification: the recorded v1 timestamp-zone proof the import checks."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_persistence.models import ImportVerificationRow


@dataclass(frozen=True)
class ImportVerification:
    verified_at: datetime
    zone: str
    overlap_rows: int
    matched_rows: int
    match_rate: float
    overlap_start: datetime
    overlap_end: datetime


class ImportVerificationRepository:
    """Caller owns the commit."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(self, verification: ImportVerification) -> None:
        self._session.add(ImportVerificationRow(**vars(verification)))
        await self._session.flush()

    async def latest(self) -> ImportVerification | None:
        row = await self._session.scalar(
            select(ImportVerificationRow).order_by(ImportVerificationRow.id.desc()).limit(1)
        )
        if row is None:
            return None
        return ImportVerification(
            verified_at=row.verified_at.astimezone(UTC),
            zone=row.zone,
            overlap_rows=row.overlap_rows,
            matched_rows=row.matched_rows,
            match_rate=row.match_rate,
            overlap_start=row.overlap_start.astimezone(UTC),
            overlap_end=row.overlap_end.astimezone(UTC),
        )

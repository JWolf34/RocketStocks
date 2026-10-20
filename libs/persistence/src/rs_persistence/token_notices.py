"""schwab_token_notice: which token-expiry DMs were already sent for a token issue."""

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_persistence.models import SchwabTokenNoticeRow


class TokenNoticeRepository:
    """Caller owns the commit."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def sent_thresholds(self, issued_at: datetime) -> set[int]:
        rows = await self._session.scalars(
            select(SchwabTokenNoticeRow.threshold_hours).where(
                SchwabTokenNoticeRow.issued_at == issued_at
            )
        )
        return set(rows)

    async def record(self, issued_at: datetime, threshold_hours: int) -> None:
        await self._session.execute(
            insert(SchwabTokenNoticeRow)
            .values(issued_at=issued_at, threshold_hours=threshold_hours)
            .on_conflict_do_nothing()
        )

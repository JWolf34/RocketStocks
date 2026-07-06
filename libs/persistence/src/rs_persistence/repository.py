"""Minimal repository — only what the S002 round-trip test needs (spec non-goal:
nothing more). Converts between rs_domain.Bar and the StreamBar5m row."""

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import Bar
from rs_persistence.models import StreamBar5m


class BarRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, bar: Bar) -> None:
        self._session.add(
            StreamBar5m(
                symbol=bar.symbol,
                bucket=bar.bucket,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
            )
        )
        await self._session.flush()

    async def get(self, symbol: str, bucket: datetime) -> Bar | None:
        row = await self._session.scalar(
            select(StreamBar5m).where(StreamBar5m.symbol == symbol, StreamBar5m.bucket == bucket)
        )
        if row is None:
            return None
        return Bar(
            symbol=row.symbol,
            bucket=row.bucket,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
        )

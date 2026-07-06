"""Minimal repository — only what the S002 round-trip test needs (spec non-goal:
nothing more). Converts between rs_domain.Bar and the StreamBar5m row.

Column names deliberately mirror Bar's field names, so the conversions are
model_dump()-shaped rather than two hand-maintained field lists. Reads
normalize the bucket to UTC before validation (ADR-0004: coerce at the
boundary) — the engine already pins the session timezone, this is the
repository honoring the same contract explicitly.
"""

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import Bar
from rs_persistence.models import StreamBar5m

_FIELDS = ("symbol", "bucket", "open", "high", "low", "close", "volume")


class BarRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, bar: Bar) -> None:
        self._session.add(StreamBar5m(**bar.model_dump()))
        await self._session.flush()

    async def get(self, symbol: str, bucket: datetime) -> Bar | None:
        row = await self._session.scalar(
            select(StreamBar5m).where(StreamBar5m.symbol == symbol, StreamBar5m.bucket == bucket)
        )
        if row is None:
            return None
        fields = {name: getattr(row, name) for name in _FIELDS}
        fields["bucket"] = fields["bucket"].astimezone(timezone.utc)
        return Bar(**fields)

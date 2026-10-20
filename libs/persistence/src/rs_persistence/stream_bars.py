"""Detection-log bars: converts between rs_domain.Bar and the StreamBar5m row.

Column names deliberately mirror Bar's field names, so the conversions are
model_dump()-shaped rather than two hand-maintained field lists. Reads
normalize the bucket to UTC before validation (ADR-0004: coerce at the
boundary) - the engine already pins the session timezone, this is the
repository honoring the same contract explicitly.
"""

from datetime import UTC, date, datetime, timedelta

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
        return None if row is None else self._to_bar(row)

    async def list_for_day(self, symbol: str, day: date) -> list[Bar]:
        """Every bar for a symbol on one UTC calendar day, ordered by bucket. The
        engine pins the session TZ to UTC, so [start, start+1d) is a UTC-day slice
        - the shape compare_bars reconciles against a day of REST candles."""
        start = datetime(day.year, day.month, day.day, tzinfo=UTC)
        rows = (
            await self._session.scalars(
                select(StreamBar5m)
                .where(
                    StreamBar5m.symbol == symbol,
                    StreamBar5m.bucket >= start,
                    StreamBar5m.bucket < start + timedelta(days=1),
                )
                .order_by(StreamBar5m.bucket)
            )
        ).all()
        return [self._to_bar(row) for row in rows]

    async def list_range(self, symbol: str, start: datetime, end: datetime) -> list[Bar]:
        """Bars in the half-open [start, end), oldest-first - the serve read
        (S005). No row cap by design: cap/pagination are a Phase-3
        contract-review input (S005 spec)."""
        rows = (
            await self._session.scalars(
                select(StreamBar5m)
                .where(
                    StreamBar5m.symbol == symbol,
                    StreamBar5m.bucket >= start,
                    StreamBar5m.bucket < end,
                )
                .order_by(StreamBar5m.bucket)
            )
        ).all()
        return [self._to_bar(row) for row in rows]

    @staticmethod
    def _to_bar(row: StreamBar5m) -> Bar:
        fields = {name: getattr(row, name) for name in _FIELDS}
        fields["bucket"] = fields["bucket"].astimezone(UTC)
        return Bar(**fields)

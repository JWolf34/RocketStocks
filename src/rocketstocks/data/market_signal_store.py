"""Repository for the market_signals table."""
import datetime
import logging

from psycopg.types.json import Json

logger = logging.getLogger(__name__)

_TABLE = 'market_signals'
_FIELDS = [
    'ticker', 'detected_at', 'composite_score', 'price_z', 'vol_z',
    'pct_change', 'dominant_signal', 'rvol', 'status', 'confirmed_at',
    'alert_message_id', 'signal_data',
]
_ACTIVE_CUTOFF_HOURS = 8  # Signals older than 8 hours are eligible to expire


class MarketSignalRepository:
    """Async repository for tracking market signal events."""

    def __init__(self, db=None):
        self._db = db

    async def insert_signal(
        self,
        ticker: str,
        detected_at: datetime.datetime,
        composite_score: float,
        price_z: float | None,
        vol_z: float | None,
        pct_change: float | None,
        dominant_signal: str | None,
        rvol: float | None,
        signal_data: list | None = None,
    ) -> None:
        """Insert a new market signal record (ignores duplicates)."""
        await self._db.execute(
            """
            INSERT INTO market_signals
            (ticker, detected_at, composite_score, price_z, vol_z, pct_change,
             dominant_signal, rvol, signal_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, detected_at) DO NOTHING
            """,
            [
                ticker, detected_at,
                float(composite_score) if composite_score is not None else None,
                float(price_z) if price_z is not None else None,
                float(vol_z) if vol_z is not None else None,
                float(pct_change) if pct_change is not None else None,
                dominant_signal,
                float(rvol) if rvol is not None else None,
                Json(signal_data or []),
            ],
        )
        logger.debug(f"Inserted market signal for '{ticker}' at {detected_at}")

    async def get_active_signals(self) -> list[dict]:
        """Return pending, unexpired, today's signals."""
        now = datetime.datetime.utcnow()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + datetime.timedelta(days=1)
        rows = await self._db.execute(
            f"SELECT {', '.join(_FIELDS)} FROM {_TABLE} "
            "WHERE status = 'pending' AND detected_at >= %s AND detected_at < %s",
            [day_start, day_end],
        )
        return [dict(zip(_FIELDS, row)) for row in (rows or [])]

    async def get_signal_history(self, ticker: str) -> list[dict]:
        """Return observation snapshots (signal_data JSON array) for a ticker."""
        now = datetime.datetime.utcnow()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + datetime.timedelta(days=1)
        rows = await self._db.execute(
            "SELECT signal_data FROM market_signals "
            "WHERE ticker = %s AND status = 'pending' AND detected_at >= %s AND detected_at < %s "
            "ORDER BY detected_at ASC",
            [ticker, day_start, day_end],
        )
        observations = []
        for row in (rows or []):
            data = row[0]
            if isinstance(data, list):
                observations.extend(data)
        return observations

    async def mark_confirmed(self, ticker: str, detected_at: datetime.datetime) -> None:
        """Mark a signal as confirmed."""
        await self._db.execute(
            "UPDATE market_signals SET status = 'confirmed', confirmed_at = CURRENT_TIMESTAMP "
            "WHERE ticker = %s AND detected_at = %s",
            [ticker, detected_at],
        )
        logger.debug(f"Marked market signal confirmed for '{ticker}' at {detected_at}")

    async def expire_old_signals(self) -> None:
        """Mark pending signals older than the cutoff window as expired."""
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
        await self._db.execute(
            "UPDATE market_signals SET status = 'expired' "
            "WHERE status = 'pending' AND detected_at < %s",
            [cutoff],
        )
        logger.debug(f"Expired old market signals before {cutoff}")

    async def is_already_signaled(self, ticker: str) -> bool:
        """Return True if ticker has an active pending signal today."""
        now = datetime.datetime.utcnow()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + datetime.timedelta(days=1)
        row = await self._db.execute(
            "SELECT COUNT(*) FROM market_signals "
            "WHERE ticker = %s AND status = 'pending' AND detected_at >= %s AND detected_at < %s",
            [ticker, day_start, day_end],
            fetchone=True,
        )
        return (row[0] > 0) if row else False

    async def update_observation(
        self,
        ticker: str,
        detected_at: datetime.datetime,
        pct_change: float,
        composite_score: float,
        vol_z: float | None,
        price_z: float | None,
    ) -> None:
        """Atomically append a new observation snapshot to signal_data JSONB."""
        observation = {
            'ts': datetime.datetime.utcnow().isoformat(),
            'pct_change': float(pct_change) if pct_change is not None else None,
            'vol_z': float(vol_z) if vol_z is not None else None,
            'price_z': float(price_z) if price_z is not None else None,
            'composite': float(composite_score) if composite_score is not None else None,
        }
        async with self._db.transaction() as conn:
            cur = await conn.execute(
                "SELECT signal_data FROM market_signals WHERE ticker = %s AND detected_at = %s",
                [ticker, detected_at],
            )
            row = await cur.fetchone()
            if row is None:
                return
            existing = row[0] if isinstance(row[0], list) else []
            existing.append(observation)
            await conn.execute(
                "UPDATE market_signals SET signal_data = %s, composite_score = %s, "
                "pct_change = %s, vol_z = %s, price_z = %s "
                "WHERE ticker = %s AND detected_at = %s",
                [
                    Json(existing),
                    float(composite_score) if composite_score is not None else None,
                    float(pct_change) if pct_change is not None else None,
                    float(vol_z) if vol_z is not None else None,
                    float(price_z) if price_z is not None else None,
                    ticker, detected_at,
                ],
            )
        logger.debug(f"Updated observation for '{ticker}' at {detected_at}")

    async def get_latest_signal(self, ticker: str) -> dict | None:
        """Return the most recent pending signal for a ticker today."""
        now = datetime.datetime.utcnow()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + datetime.timedelta(days=1)
        row = await self._db.execute(
            f"SELECT {', '.join(_FIELDS)} FROM {_TABLE} "
            "WHERE ticker = %s AND status = 'pending' AND detected_at >= %s AND detected_at < %s "
            "ORDER BY detected_at DESC LIMIT 1",
            [ticker, day_start, day_end],
            fetchone=True,
        )
        if row is None:
            return None
        return dict(zip(_FIELDS, row))

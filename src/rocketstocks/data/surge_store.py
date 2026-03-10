"""Repository for the popularity_surges table."""
import datetime
import logging

logger = logging.getLogger(__name__)

_TABLE = 'popularity_surges'
_FIELDS = [
    'ticker', 'flagged_at', 'surge_types', 'current_rank',
    'mention_ratio', 'rank_change', 'price_at_flag',
    'alert_message_id', 'confirmed', 'confirmed_at', 'expired',
]
_ACTIVE_CUTOFF_HOURS = 24  # Surges older than 24 hours are eligible to expire


class SurgeRepository:
    """Async repository for tracking popularity surge events."""

    def __init__(self, db=None):
        self._db = db

    async def insert_surge(
        self,
        ticker: str,
        flagged_at: datetime.datetime,
        surge_types: str,
        current_rank: int | None,
        mention_ratio: float | None,
        rank_change: int | None,
        price_at_flag: float | None,
        alert_message_id: int | None = None,
    ) -> None:
        """Insert a new popularity surge record (ignores duplicates)."""
        await self._db.execute(
            """
            INSERT INTO popularity_surges
            (ticker, flagged_at, surge_types, current_rank, mention_ratio,
             rank_change, price_at_flag, alert_message_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, flagged_at) DO NOTHING
            """,
            [ticker, flagged_at, surge_types, current_rank,
             mention_ratio, rank_change, price_at_flag, alert_message_id],
        )
        logger.debug(f"Inserted surge for '{ticker}' at {flagged_at}")

    async def get_active_surges(self) -> list[dict]:
        """Return active (unconfirmed, unexpired, within cutoff) surges."""
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
        rows = await self._db.execute(
            f"SELECT {', '.join(_FIELDS)} FROM {_TABLE} "
            "WHERE confirmed = FALSE AND expired = FALSE AND flagged_at >= %s",
            [cutoff],
        )
        return [dict(zip(_FIELDS, row)) for row in (rows or [])]

    async def mark_confirmed(self, ticker: str, flagged_at: datetime.datetime) -> None:
        """Mark a surge as confirmed (price/volume followed)."""
        await self._db.execute(
            "UPDATE popularity_surges SET confirmed = TRUE, confirmed_at = CURRENT_TIMESTAMP "
            "WHERE ticker = %s AND flagged_at = %s",
            [ticker, flagged_at],
        )
        logger.debug(f"Marked surge confirmed for '{ticker}' at {flagged_at}")

    async def expire_old_surges(self) -> None:
        """Mark unconfirmed surges older than the cutoff window as expired."""
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
        await self._db.execute(
            "UPDATE popularity_surges SET expired = TRUE "
            "WHERE confirmed = FALSE AND expired = FALSE AND flagged_at < %s",
            [cutoff],
        )
        logger.debug(f"Expired old surges before {cutoff}")

    async def is_already_flagged(self, ticker: str) -> bool:
        """Return True if ticker has an active unconfirmed surge."""
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
        row = await self._db.execute(
            "SELECT COUNT(*) FROM popularity_surges "
            "WHERE ticker = %s AND confirmed = FALSE AND expired = FALSE "
            "AND flagged_at >= %s",
            [ticker, cutoff],
            fetchone=True,
        )
        return (row[0] > 0) if row else False

    async def update_alert_message_id(
        self,
        ticker: str,
        flagged_at: datetime.datetime,
        message_id: int,
    ) -> None:
        """Update the Discord message ID after a surge alert has been sent."""
        await self._db.execute(
            "UPDATE popularity_surges SET alert_message_id = %s "
            "WHERE ticker = %s AND flagged_at = %s",
            [message_id, ticker, flagged_at],
        )
        logger.debug(f"Updated alert_message_id={message_id} for '{ticker}' at {flagged_at}")

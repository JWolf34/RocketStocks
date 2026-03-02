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
    """Synchronous repository for tracking popularity surge events.

    All methods are synchronous. The cog wraps calls with asyncio.to_thread().
    """

    def __init__(self, db=None):
        self._db = db

    def insert_surge(
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
        sql = (
            f"INSERT INTO {_TABLE} "
            f"(ticker, flagged_at, surge_types, current_rank, mention_ratio, "
            f"rank_change, price_at_flag, alert_message_id) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            f"ON CONFLICT (ticker, flagged_at) DO NOTHING;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [
                ticker, flagged_at, surge_types, current_rank,
                mention_ratio, rank_change, price_at_flag, alert_message_id,
            ])
        logger.debug(f"Inserted surge for '{ticker}' at {flagged_at}")

    def get_active_surges(self) -> list[dict]:
        """Return active (unconfirmed, unexpired, within cutoff) surges."""
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
        sql = (
            f"SELECT {', '.join(_FIELDS)} FROM {_TABLE} "
            f"WHERE confirmed = FALSE AND expired = FALSE AND flagged_at >= %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [cutoff])
            rows = cur.fetchall()
        return [dict(zip(_FIELDS, row)) for row in rows]

    def mark_confirmed(self, ticker: str, flagged_at: datetime.datetime) -> None:
        """Mark a surge as confirmed (price/volume followed)."""
        sql = (
            f"UPDATE {_TABLE} SET confirmed = TRUE, confirmed_at = CURRENT_TIMESTAMP "
            f"WHERE ticker = %s AND flagged_at = %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [ticker, flagged_at])
        logger.debug(f"Marked surge confirmed for '{ticker}' at {flagged_at}")

    def expire_old_surges(self) -> None:
        """Mark unconfirmed surges older than the cutoff window as expired."""
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
        sql = (
            f"UPDATE {_TABLE} SET expired = TRUE "
            f"WHERE confirmed = FALSE AND expired = FALSE AND flagged_at < %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [cutoff])
        logger.debug(f"Expired old surges before {cutoff}")

    def is_already_flagged(self, ticker: str) -> bool:
        """Return True if ticker has an active unconfirmed surge."""
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
        sql = (
            f"SELECT COUNT(*) FROM {_TABLE} "
            f"WHERE ticker = %s AND confirmed = FALSE AND expired = FALSE "
            f"AND flagged_at >= %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [ticker, cutoff])
            count = cur.fetchone()[0]
        return count > 0

    def update_alert_message_id(
        self,
        ticker: str,
        flagged_at: datetime.datetime,
        message_id: int,
    ) -> None:
        """Update the Discord message ID after a surge alert has been sent."""
        sql = (
            f"UPDATE {_TABLE} SET alert_message_id = %s "
            f"WHERE ticker = %s AND flagged_at = %s;"
        )
        with self._db._cursor() as cur:
            cur.execute(sql, [message_id, ticker, flagged_at])
        logger.debug(f"Updated alert_message_id={message_id} for '{ticker}' at {flagged_at}")

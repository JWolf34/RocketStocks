"""Repository for the earnings_results table."""
import datetime
import logging

logger = logging.getLogger(__name__)

_TABLE = 'earnings_results'


class EarningsResultsRepository:
    """Async repository for tracking posted earnings results."""

    def __init__(self, db=None):
        self._db = db

    async def insert_result(
        self,
        date: datetime.date,
        ticker: str,
        eps_actual: float | None,
        eps_estimate: float | None,
        surprise_pct: float | None,
        source: str = 'yfinance',
    ) -> None:
        """Insert an earnings result record (ignores duplicates)."""
        await self._db.execute(
            """
            INSERT INTO earnings_results
            (date, ticker, eps_actual, eps_estimate, surprise_pct, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, ticker) DO NOTHING
            """,
            [date, ticker, eps_actual, eps_estimate, surprise_pct, source],
        )
        logger.debug(f"Inserted earnings result for '{ticker}' on {date}")

    async def get_posted_tickers_today(self, date: datetime.date) -> set[str]:
        """Return set of tickers that already have a result posted for *date*."""
        rows = await self._db.execute(
            "SELECT ticker FROM earnings_results WHERE date = %s",
            [date],
        )
        return {row[0] for row in (rows or [])}

    async def get_result(self, date: datetime.date, ticker: str) -> dict | None:
        """Return the stored result for *(date, ticker)*, or None if not found."""
        row = await self._db.execute(
            "SELECT eps_actual, eps_estimate, surprise_pct FROM earnings_results "
            "WHERE date = %s AND ticker = %s",
            [date, ticker],
            fetchone=True,
        )
        if not row:
            return None
        return {
            'eps_actual': row[0],
            'eps_estimate': row[1],
            'surprise_pct': row[2],
        }

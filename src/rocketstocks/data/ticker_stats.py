"""Repository for the ticker_stats table."""
import logging
from contextlib import contextmanager

import psycopg2.extras

logger = logging.getLogger(__name__)

_TABLE = 'ticker_stats'
_FIELDS = [
    'ticker', 'market_cap', 'classification',
    'volatility_20d', 'mean_return_20d', 'std_return_20d',
    'mean_return_60d', 'std_return_60d',
    'avg_rvol_20d', 'std_rvol_20d',
    'bb_upper', 'bb_lower', 'bb_mid',
    'updated_at',
]


class TickerStatsRepository:
    def __init__(self, db):
        self._db = db

    def upsert_stats(self, ticker: str, stats_dict: dict) -> None:
        """Insert or update ticker_stats row via ON CONFLICT DO UPDATE."""
        stats_dict = {k: v for k, v in stats_dict.items() if k != 'ticker'}
        cols = ['ticker'] + list(stats_dict.keys())
        vals = [ticker] + list(stats_dict.values())

        col_identifiers = ', '.join(cols)
        placeholders = ', '.join(['%s'] * len(vals))
        update_clause = ', '.join(
            f"{col} = EXCLUDED.{col}"
            for col in stats_dict.keys()
        )
        # Also update updated_at on conflict
        update_clause += ', updated_at = CURRENT_TIMESTAMP'

        sql = (
            f"INSERT INTO {_TABLE} ({col_identifiers}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT (ticker) DO UPDATE SET {update_clause};"
        )

        with self._db._cursor() as cur:
            cur.execute(sql, vals)
        logger.debug(f"Upserted ticker_stats for '{ticker}'")

    def get_stats(self, ticker: str) -> dict | None:
        """Return the stats row for *ticker* as a dict, or None if absent."""
        results = self._db.select(
            table=_TABLE,
            fields=_FIELDS,
            where_conditions=[('ticker', ticker)],
            fetchall=False,
        )
        if results is None:
            return None
        return dict(zip(_FIELDS, results))

    def get_classification(self, ticker: str) -> str:
        """Return the classification for *ticker*, defaulting to 'standard'."""
        result = self._db.select(
            table=_TABLE,
            fields=['classification'],
            where_conditions=[('ticker', ticker)],
            fetchall=False,
        )
        if result is None:
            return 'standard'
        return result[0]

    def get_all_classifications(self) -> dict[str, str]:
        """Return {ticker: classification} for all rows in ticker_stats."""
        results = self._db.select(
            table=_TABLE,
            fields=['ticker', 'classification'],
            fetchall=True,
        )
        if not results:
            return {}
        return {row[0]: row[1] for row in results}

    def get_all_stats(self) -> list[dict]:
        """Return all rows in ticker_stats as a list of dicts."""
        results = self._db.select(
            table=_TABLE,
            fields=_FIELDS,
            fetchall=True,
        )
        if not results:
            return []
        return [dict(zip(_FIELDS, row)) for row in results]

"""Repository for the ticker_stats table."""
import logging

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
_STATS_COLS = [
    'market_cap', 'classification',
    'volatility_20d', 'mean_return_20d', 'std_return_20d',
    'mean_return_60d', 'std_return_60d',
    'avg_rvol_20d', 'std_rvol_20d',
    'bb_upper', 'bb_lower', 'bb_mid',
]


class TickerStatsRepository:
    def __init__(self, db):
        self._db = db

    async def upsert_stats(self, ticker: str, stats_dict: dict) -> None:
        """Insert or update ticker_stats row via ON CONFLICT DO UPDATE."""
        vals = [ticker] + [stats_dict.get(col) for col in _STATS_COLS]
        await self._db.execute(
            """
            INSERT INTO ticker_stats (
                ticker, market_cap, classification,
                volatility_20d, mean_return_20d, std_return_20d,
                mean_return_60d, std_return_60d,
                avg_rvol_20d, std_rvol_20d,
                bb_upper, bb_lower, bb_mid
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
                market_cap      = EXCLUDED.market_cap,
                classification  = EXCLUDED.classification,
                volatility_20d  = EXCLUDED.volatility_20d,
                mean_return_20d = EXCLUDED.mean_return_20d,
                std_return_20d  = EXCLUDED.std_return_20d,
                mean_return_60d = EXCLUDED.mean_return_60d,
                std_return_60d  = EXCLUDED.std_return_60d,
                avg_rvol_20d    = EXCLUDED.avg_rvol_20d,
                std_rvol_20d    = EXCLUDED.std_rvol_20d,
                bb_upper        = EXCLUDED.bb_upper,
                bb_lower        = EXCLUDED.bb_lower,
                bb_mid          = EXCLUDED.bb_mid,
                updated_at      = CURRENT_TIMESTAMP
            """,
            vals,
        )
        logger.debug(f"Upserted ticker_stats for '{ticker}'")

    async def get_stats(self, ticker: str) -> dict | None:
        """Return the stats row for *ticker* as a dict, or None if absent."""
        row = await self._db.execute(
            "SELECT ticker, market_cap, classification, volatility_20d, mean_return_20d, "
            "std_return_20d, mean_return_60d, std_return_60d, avg_rvol_20d, std_rvol_20d, "
            "bb_upper, bb_lower, bb_mid, updated_at "
            "FROM ticker_stats WHERE ticker = %s",
            [ticker],
            fetchone=True,
        )
        if row is None:
            return None
        return dict(zip(_FIELDS, row))

    async def get_classification(self, ticker: str) -> str:
        """Return the classification for *ticker*, defaulting to 'standard'."""
        row = await self._db.execute(
            "SELECT classification FROM ticker_stats WHERE ticker = %s",
            [ticker],
            fetchone=True,
        )
        if row is None:
            return 'standard'
        return row[0]

    async def get_all_classifications(self) -> dict[str, str]:
        """Return {ticker: classification} for all rows in ticker_stats."""
        rows = await self._db.execute(
            "SELECT ticker, classification FROM ticker_stats"
        )
        if not rows:
            return {}
        return {row[0]: row[1] for row in rows}

    async def get_all_market_caps(self) -> dict[str, int | None]:
        """Return {ticker: market_cap} for all rows in ticker_stats."""
        rows = await self._db.execute("SELECT ticker, market_cap FROM ticker_stats")
        if not rows:
            return {}
        return {row[0]: row[1] for row in rows}

    async def get_all_stats(self) -> list[dict]:
        """Return all rows in ticker_stats as a list of dicts."""
        rows = await self._db.execute(
            "SELECT ticker, market_cap, classification, volatility_20d, mean_return_20d, "
            "std_return_20d, mean_return_60d, std_return_60d, avg_rvol_20d, std_rvol_20d, "
            "bb_upper, bb_lower, bb_mid, updated_at "
            "FROM ticker_stats"
        )
        if not rows:
            return []
        return [dict(zip(_FIELDS, row)) for row in rows]

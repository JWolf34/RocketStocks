"""Repository for the `popularity` table."""
import logging

import pandas as pd

from rocketstocks.data.clients.ape_wisdom import ApeWisdom

logger = logging.getLogger(__name__)

_POPULARITY_COLS = [
    'datetime', 'rank', 'ticker', 'name',
    'mentions', 'upvotes', 'rank_24h_ago', 'mentions_24h_ago',
]


class PopularityRepository:
    def __init__(self, db, ape_wisdom=None):
        self._db = db
        self._ape_wisdom = ape_wisdom or ApeWisdom()

    async def fetch_popularity(self, ticker: str = None) -> pd.DataFrame:
        """Return historical popularity for *ticker* (or all tickers) from database."""
        if ticker:
            logger.info(f"Retrieving historical popularity for {ticker} from database")
            rows = await self._db.execute(
                f"SELECT {', '.join(_POPULARITY_COLS)} FROM popularity "
                "WHERE ticker = %s ORDER BY datetime DESC",
                [ticker],
            )
        else:
            logger.info("Retrieving all historical popularity from database")
            rows = await self._db.execute(
                f"SELECT {', '.join(_POPULARITY_COLS)} FROM popularity ORDER BY datetime DESC"
            )
        return pd.DataFrame(rows or [], columns=_POPULARITY_COLS)

    async def insert_popularity(self, popular_stocks: pd.DataFrame) -> None:
        """Insert new rows into the popularity table."""
        logger.debug(f"Inserting new popularity data into database - {popular_stocks.shape[0]} rows")
        values = [tuple(row) for row in popular_stocks.values]
        cols = popular_stocks.columns.to_list()
        placeholders = ', '.join(['%s'] * len(cols))
        col_list = ', '.join(cols)
        await self._db.execute_batch(
            f"INSERT INTO popularity ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
            values,
        )

    def get_popular_stocks(self, filter_name='all stock subreddits', num_stocks=1000) -> pd.DataFrame:
        """Proxy for fetching popular stocks from Ape Wisdom client."""
        logger.info(f"Retrieving top {num_stocks} most popular stocks from database")
        return self._ape_wisdom.get_popular_stocks(filter_name=filter_name, num_stocks=num_stocks)

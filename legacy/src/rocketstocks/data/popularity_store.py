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

    async def fetch_popularity(
        self,
        ticker: str = None,
        limit: int = None,
        start_date=None,
        end_date=None,
    ) -> pd.DataFrame:
        """Return historical popularity for *ticker* (or all tickers) from database.

        Args:
            ticker: Filter to a single ticker; if None returns all tickers.
            limit: Cap the number of rows returned.
            start_date: Optional earliest date (inclusive).  Rows with
                ``datetime >= start_date`` are returned.
            end_date: Optional latest date (inclusive).  Rows with
                ``datetime < end_date + 1 day`` are returned.
        """
        import datetime as _dt

        conditions: list[str] = []
        params: list = []

        if ticker:
            logger.debug(f"Retrieving historical popularity for {ticker} from database")
            conditions.append("ticker = %s")
            params.append(ticker)
        else:
            logger.info("Retrieving all historical popularity from database")

        if start_date is not None:
            conditions.append("datetime >= %s")
            params.append(start_date)
        if end_date is not None:
            end_cutoff = (
                end_date + _dt.timedelta(days=1)
                if isinstance(end_date, _dt.date)
                else end_date
            )
            conditions.append("datetime < %s")
            params.append(end_cutoff)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"SELECT {', '.join(_POPULARITY_COLS)} FROM popularity {where} ORDER BY datetime DESC"
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)

        rows = await self._db.execute(query, params or None)
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

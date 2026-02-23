"""Repository for the `popularity` table."""
import logging

import pandas as pd

logger = logging.getLogger(__name__)


class PopularityRepository:
    def __init__(self, db):
        self._db = db

    def fetch_popularity(self, ticker: str = None) -> pd.DataFrame:
        """Return historical popularity for *ticker* (or all tickers) from database."""
        if ticker:
            logger.info(f"Retrieving historical popularity for {ticker} from database")
        else:
            logger.info("Retrieving all historical popularity from database")

        columns = self._db.get_table_columns('popularity')
        where_conditions = [('ticker', ticker)] if ticker else []

        results = self._db.select(
            table='popularity',
            fields=columns,
            where_conditions=where_conditions,
            order_by=('datetime', 'DESC'),
            fetchall=True,
        )
        return pd.DataFrame(results, columns=columns) if results else pd.DataFrame()

    def insert_popularity(self, popular_stocks: pd.DataFrame) -> None:
        """Insert new rows into the popularity table."""
        logger.debug(f"Inserting new popularity data into database - {popular_stocks.shape[0]} rows")
        values = [tuple(row) for row in popular_stocks.values]
        self._db.insert(
            table='popularity',
            fields=popular_stocks.columns.to_list(),
            values=values,
        )

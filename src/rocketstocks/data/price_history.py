"""Repository for daily and 5-minute price history tables."""
import datetime
import logging
import time

import pandas as pd

logger = logging.getLogger(__name__)


class PriceHistoryRepository:
    def __init__(self, db, schwab, tiingo=None, stooq=None):
        self._db = db
        self._schwab = schwab
        self._tiingo = tiingo
        self._stooq = stooq

    async def update_daily_price_history(self, tickers: list):
        """Update database with latest daily price data for all *tickers*."""
        logger.info("Updating daily price history for all tickers")
        start_time = time.time()
        num_tickers = len(tickers)
        for i, ticker in enumerate(tickers, 1):
            logger.info(f"Inserting daily price data for ticker {ticker}, {i}/{num_tickers}")
            await self.update_daily_price_history_by_ticker(ticker)
        elapsed = time.time() - start_time
        logger.info("Completed update to daily price history in database")
        logger.debug(f"Updating daily price history completed in {elapsed:.2f} seconds")

    async def update_daily_price_history_by_ticker(self, ticker: str):
        """Update database with latest daily price data for *ticker*."""
        result = self._db.select(
            table='daily_price_history',
            fields=['date'],
            where_conditions=[('ticker', ticker)],
            order_by=('date', 'DESC'),
            fetchall=False,
        )
        if not result:
            start_datetime = datetime.datetime(year=2000, month=1, day=1)
            logger.debug(
                f"No daily price history for {ticker} in database, "
                f"fetching from default date {start_datetime.date()}"
            )
        else:
            start_datetime = datetime.datetime.combine(result[0], datetime.time(0, 0, 0))
            logger.debug(f"Latest recorded daily price history for {ticker} is {start_datetime.date()}")

        price_history = await self._schwab.get_daily_price_history(ticker, start_datetime=start_datetime)

        if not price_history.empty:
            fields = price_history.columns.to_list()
            values = [tuple(row) for row in price_history.values]
            self._db.insert(table='daily_price_history', fields=fields, values=values)
        else:
            logger.warning(f"No daily price history found for ticker {ticker}")

    async def update_5m_price_history(self, tickers: list):
        """Update database with latest 5-minute price data for all *tickers*."""
        logger.info("Updating 5m price history for all tickers")
        start_time = time.time()
        num_tickers = len(tickers)
        for i, ticker in enumerate(tickers, 1):
            logger.info(f"Inserting 5m price data for ticker {ticker}, {i}/{num_tickers}")
            await self.update_5m_price_history_by_ticker(ticker)
        elapsed = time.time() - start_time
        logger.info("Completed update to 5m price history in database")
        logger.info(f"Updating 5m price history completed in {elapsed:.2f} seconds")

    async def update_5m_price_history_by_ticker(self, ticker: str):
        """Update database with latest 5-minute price data for *ticker*."""
        result = self._db.select(
            table='five_minute_price_history',
            fields=['datetime'],
            where_conditions=[('ticker', ticker)],
            order_by=('datetime', 'DESC'),
            fetchall=False,
        )
        if not result:
            start_datetime = datetime.datetime.now() - datetime.timedelta(days=365)
            logger.debug(f"No 5m price history for {ticker} in database, fetching from default date")
        else:
            start_datetime = result[0]
            logger.debug(f"Latest recorded 5m price history for {ticker} is {start_datetime.date()}")

        price_history = await self._schwab.get_5m_price_history(ticker, start_datetime=start_datetime)

        if not price_history.empty:
            fields = price_history.columns.to_list()
            values = [tuple(row) for row in price_history.values]
            self._db.insert(table='five_minute_price_history', fields=fields, values=values)
        else:
            logger.warning(f"No 5m price history found for ticker {ticker}")

    def load_delisted_price_history(self, ticker: str) -> int:
        """Fetch full historical OHLCV for a delisted ticker and store in daily_price_history.

        Tries Tiingo first; falls back to Stooq if Tiingo returns empty.
        Skips if data already exists for this ticker.
        Returns count of rows inserted (0 if skipped or no data found).
        """
        existing = self._db.select(
            table='daily_price_history',
            fields=['ticker'],
            where_conditions=[('ticker', ticker)],
            fetchall=False,
        )
        if existing:
            logger.debug(f"Price history already exists for '{ticker}', skipping")
            return 0

        today = datetime.date.today().strftime('%Y-%m-%d')
        df = pd.DataFrame()

        if self._tiingo is not None:
            df = self._tiingo.get_daily_price_history(ticker, '2000-01-01', today)

        if df.empty and self._stooq is not None:
            logger.debug(f"Tiingo returned empty for '{ticker}', trying Stooq")
            df = self._stooq.get_daily_price_history(ticker, '2000-01-01', today)

        if df.empty:
            logger.warning(f"No historical price data found for delisted ticker '{ticker}'")
            return 0

        fields = df.columns.to_list()
        values = [tuple(row) for row in df.values]
        self._db.insert(table='daily_price_history', fields=fields, values=values)
        logger.info(f"Inserted {len(df)} rows of price history for delisted ticker '{ticker}'")
        return len(df)

    def load_delisted_price_history_batch(self, limit: int = 50) -> int:
        """Load price history for up to *limit* delisted tickers with no existing data."""
        with self._db._cursor() as cur:
            cur.execute(
                """
                SELECT tickers.ticker FROM tickers
                LEFT JOIN daily_price_history ON tickers.ticker = daily_price_history.ticker
                WHERE tickers.delist_date IS NOT NULL AND daily_price_history.ticker IS NULL
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cur.fetchall()

        if not rows:
            logger.info("No delisted tickers queued for price history import")
            return 0

        total = 0
        for (ticker,) in rows:
            try:
                total += self.load_delisted_price_history(ticker)
            except Exception as exc:
                logger.warning(f"Failed to load price history for delisted ticker '{ticker}': {exc}")

        logger.info(f"Loaded price history for {total} delisted tickers in batch")
        return total

    def fetch_daily_price_history(
        self,
        ticker: str,
        start_date: datetime.date = None,
        end_date: datetime.date = None,
    ) -> pd.DataFrame:
        """Return daily price history for *ticker* from database."""
        logger.debug(f"Fetching daily price history for ticker '{ticker}' from database")
        where_conditions = [('ticker', ticker)]
        if start_date is not None:
            where_conditions.append(('date', '>', start_date))
        if end_date is not None:
            where_conditions.append(('date', '<', end_date))

        results = self._db.select(
            table='daily_price_history',
            fields=['ticker', 'open', 'high', 'low', 'close', 'volume', 'date'],
            where_conditions=where_conditions,
            fetchall=True,
        )
        if not results:
            logger.warning(f"No daily price history available for ticker '{ticker}'")
            return pd.DataFrame()

        columns = self._db.get_table_columns('daily_price_history')
        logger.debug(f"Returned {len(results)} row(s) for ticker '{ticker}'")
        return pd.DataFrame(results, columns=columns)

    def fetch_5m_price_history(
        self,
        ticker: str,
        start_datetime: datetime.datetime = None,
        end_datetime: datetime.datetime = None,
    ) -> pd.DataFrame:
        """Return 5-minute price history for *ticker* from database."""
        logger.debug(f"Fetching 5m price history for ticker '{ticker}' from database")
        where_conditions = [('ticker', ticker)]
        if start_datetime is not None:
            where_conditions.append(('datetime', '>=', start_datetime))
        if end_datetime is not None:
            where_conditions.append(('datetime', '<=', end_datetime))

        results = self._db.select(
            table='five_minute_price_history',
            fields=['ticker', 'open', 'high', 'low', 'close', 'volume', 'datetime'],
            where_conditions=where_conditions,
            fetchall=True,
        )
        if not results:
            logger.warning(f"No 5m price history available for ticker '{ticker}'")
            return pd.DataFrame()

        columns = self._db.get_table_columns('five_minute_price_history')
        logger.debug(f"Returned {len(results)} row(s) for ticker '{ticker}'")
        return pd.DataFrame(results, columns=columns)

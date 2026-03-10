"""Repository for daily and 5-minute price history tables."""
import datetime
import logging
import time

import pandas as pd

logger = logging.getLogger(__name__)

_DAILY_COLS = ['ticker', 'open', 'high', 'low', 'close', 'volume', 'date']
_5M_COLS = ['ticker', 'open', 'high', 'low', 'close', 'volume', 'datetime']


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
        row = await self._db.execute(
            "SELECT date FROM daily_price_history WHERE ticker = %s ORDER BY date DESC LIMIT 1",
            [ticker],
            fetchone=True,
        )
        if not row:
            start_datetime = datetime.datetime(year=2000, month=1, day=1)
            logger.debug(
                f"No daily price history for {ticker} in database, "
                f"fetching from default date {start_datetime.date()}"
            )
        else:
            start_datetime = datetime.datetime.combine(row[0], datetime.time(0, 0, 0))
            logger.debug(f"Latest recorded daily price history for {ticker} is {start_datetime.date()}")

        price_history = await self._schwab.get_daily_price_history(ticker, start_datetime=start_datetime)

        if not price_history.empty:
            fields = price_history.columns.to_list()
            values = [tuple(r) for r in price_history.values]
            placeholders = ', '.join(['%s'] * len(fields))
            col_list = ', '.join(fields)
            await self._db.execute_batch(
                f"INSERT INTO daily_price_history ({col_list}) VALUES ({placeholders}) "
                "ON CONFLICT DO NOTHING",
                values,
            )
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
        row = await self._db.execute(
            "SELECT datetime FROM five_minute_price_history WHERE ticker = %s "
            "ORDER BY datetime DESC LIMIT 1",
            [ticker],
            fetchone=True,
        )
        if not row:
            start_datetime = datetime.datetime.now() - datetime.timedelta(days=365)
            logger.debug(f"No 5m price history for {ticker} in database, fetching from default date")
        else:
            start_datetime = row[0]
            logger.debug(f"Latest recorded 5m price history for {ticker} is {start_datetime.date()}")

        price_history = await self._schwab.get_5m_price_history(ticker, start_datetime=start_datetime)

        if not price_history.empty:
            fields = price_history.columns.to_list()
            values = [tuple(r) for r in price_history.values]
            placeholders = ', '.join(['%s'] * len(fields))
            col_list = ', '.join(fields)
            await self._db.execute_batch(
                f"INSERT INTO five_minute_price_history ({col_list}) VALUES ({placeholders}) "
                "ON CONFLICT DO NOTHING",
                values,
            )
        else:
            logger.warning(f"No 5m price history found for ticker {ticker}")

    async def load_delisted_price_history(self, ticker: str) -> int:
        """Fetch full historical OHLCV for a delisted ticker and store in daily_price_history."""
        existing = await self._db.execute(
            "SELECT ticker FROM daily_price_history WHERE ticker = %s LIMIT 1",
            [ticker],
            fetchone=True,
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
        values = [tuple(r) for r in df.values]
        placeholders = ', '.join(['%s'] * len(fields))
        col_list = ', '.join(fields)
        await self._db.execute_batch(
            f"INSERT INTO daily_price_history ({col_list}) VALUES ({placeholders}) "
            "ON CONFLICT DO NOTHING",
            values,
        )
        logger.info(f"Inserted {len(df)} rows of price history for delisted ticker '{ticker}'")
        return len(df)

    async def load_delisted_price_history_batch(self, limit: int = 50) -> int:
        """Load price history for up to *limit* delisted tickers with no existing data."""
        rows = await self._db.execute(
            """
            SELECT tickers.ticker FROM tickers
            LEFT JOIN daily_price_history ON tickers.ticker = daily_price_history.ticker
            WHERE tickers.delist_date IS NOT NULL AND daily_price_history.ticker IS NULL
            LIMIT %s
            """,
            [limit],
        )
        if not rows:
            logger.info("No delisted tickers queued for price history import")
            return 0

        total = 0
        for (ticker,) in rows:
            try:
                total += await self.load_delisted_price_history(ticker)
            except Exception as exc:
                logger.warning(f"Failed to load price history for delisted ticker '{ticker}': {exc}")

        logger.info(f"Loaded price history for {total} delisted tickers in batch")
        return total

    async def fetch_daily_price_history(
        self,
        ticker: str,
        start_date: datetime.date = None,
        end_date: datetime.date = None,
    ) -> pd.DataFrame:
        """Return daily price history for *ticker* from database."""
        logger.debug(f"Fetching daily price history for ticker '{ticker}' from database")
        query = (
            "SELECT ticker, open, high, low, close, volume, date "
            "FROM daily_price_history WHERE ticker = %s"
        )
        params = [ticker]
        if start_date is not None:
            query += " AND date > %s"
            params.append(start_date)
        if end_date is not None:
            query += " AND date < %s"
            params.append(end_date)

        rows = await self._db.execute(query, params)
        if not rows:
            logger.warning(f"No daily price history available for ticker '{ticker}'")
            return pd.DataFrame()

        logger.debug(f"Returned {len(rows)} row(s) for ticker '{ticker}'")
        return pd.DataFrame(rows, columns=_DAILY_COLS)

    async def fetch_5m_price_history(
        self,
        ticker: str,
        start_datetime: datetime.datetime = None,
        end_datetime: datetime.datetime = None,
    ) -> pd.DataFrame:
        """Return 5-minute price history for *ticker* from database."""
        logger.debug(f"Fetching 5m price history for ticker '{ticker}' from database")
        query = (
            "SELECT ticker, open, high, low, close, volume, datetime "
            "FROM five_minute_price_history WHERE ticker = %s"
        )
        params = [ticker]
        if start_datetime is not None:
            query += " AND datetime >= %s"
            params.append(start_datetime)
        if end_datetime is not None:
            query += " AND datetime <= %s"
            params.append(end_datetime)

        rows = await self._db.execute(query, params)
        if not rows:
            logger.warning(f"No 5m price history available for ticker '{ticker}'")
            return pd.DataFrame()

        logger.debug(f"Returned {len(rows)} row(s) for ticker '{ticker}'")
        return pd.DataFrame(rows, columns=_5M_COLS)

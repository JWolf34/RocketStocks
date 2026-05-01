"""DataAPI — thin async facade over the RocketStocks data layer."""
import asyncio
import datetime
import logging

import pandas as pd

logger = logging.getLogger(__name__)


class DataNotAvailable(Exception):
    """Raised when db_only=True and no cached data exists for the request."""


def _to_date(value) -> datetime.date | None:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value))


def _to_str(value) -> str:
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value)


class _SyncWrapper:
    """Synchronous wrapper — each call uses asyncio.run."""

    def __init__(self, api: "DataAPI"):
        self._api = api

    def get_ticker_info(self, ticker: str) -> dict | None:
        return asyncio.run(self._api.get_ticker_info(ticker))

    def get_quote(self, ticker: str) -> dict:
        return asyncio.run(self._api.get_quote(ticker))

    def get_daily_history(
        self,
        ticker: str,
        start: str | datetime.date,
        end: str | datetime.date,
        force_live: bool = False,
        db_only: bool = False,
    ) -> pd.DataFrame:
        return asyncio.run(self._api.get_daily_history(ticker, start, end, force_live, db_only))


class DataAPI:
    """Async facade over all RocketStocks data repositories and clients.

    Consumers receive a StockData instance via build_data_api(); they should
    not construct DataAPI directly unless they already manage StockData.
    """

    def __init__(self, stock_data):
        self._sd = stock_data

    @property
    def sync(self) -> _SyncWrapper:
        return _SyncWrapper(self)

    # ------------------------------------------------------------------
    # Reference data
    # ------------------------------------------------------------------

    async def get_ticker_info(self, ticker: str) -> dict | None:
        """Return the tickers table row for *ticker*, or None if not found."""
        logger.debug(f"get_ticker_info({ticker!r})")
        return await self._sd.tickers.get_ticker_info(ticker)

    # ------------------------------------------------------------------
    # Quotes
    # ------------------------------------------------------------------

    async def get_quote(self, ticker: str) -> dict:
        """Return the latest Schwab quote for *ticker*."""
        logger.debug(f"get_quote({ticker!r})")
        return await self._sd.schwab.get_quote(ticker)

    # ------------------------------------------------------------------
    # Price history
    # ------------------------------------------------------------------

    async def get_daily_history(
        self,
        ticker: str,
        start: str | datetime.date,
        end: str | datetime.date,
        force_live: bool = False,
        db_only: bool = False,
    ) -> pd.DataFrame:
        """Return daily OHLCV for *ticker* between *start* and *end*.

        Default: DB-first with Tiingo → Stooq fallback on miss.
        force_live=True: skip DB entirely.
        db_only=True: DB only; raise DataNotAvailable on miss.
        """
        logger.debug(f"get_daily_history({ticker!r}, {start}, {end}, force_live={force_live}, db_only={db_only})")

        if not force_live:
            df = await self._sd.price_history.fetch_daily_price_history(
                ticker,
                start_date=_to_date(start),
                end_date=_to_date(end),
            )
            if not df.empty:
                return df
            if db_only:
                raise DataNotAvailable(
                    f"No cached daily history for {ticker!r} between {start} and {end}"
                )

        start_str = _to_str(start)
        end_str = _to_str(end)

        df = self._sd.tiingo.get_daily_price_history(ticker, start_str, end_str)
        if df.empty:
            logger.debug(f"Tiingo miss for {ticker!r}, falling back to Stooq")
            df = self._sd.stooq.get_daily_price_history(ticker, start_str, end_str)

        return df

"""DataAPI — thin async facade over the RocketStocks data layer."""
import asyncio
import datetime
import logging

import pandas as pd

from rocketstocks.api.batches import BatchAPI

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


def _to_datetime(value) -> datetime.datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time.min)
    return datetime.datetime.fromisoformat(str(value))


def _to_str(value) -> str:
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value)


class _SyncWrapper:
    """Synchronous wrapper — each call uses asyncio.run."""

    def __init__(self, api: "DataAPI"):
        self._api = api

    # Reference
    def get_ticker_info(self, ticker: str) -> dict | None:
        return asyncio.run(self._api.get_ticker_info(ticker))

    def get_ticker_infos(self, tickers: list[str]) -> list[dict | None]:
        return asyncio.run(self._api.get_ticker_infos(tickers))

    def validate_ticker(self, ticker: str) -> bool:
        return asyncio.run(self._api.validate_ticker(ticker))

    def get_ticker_stats(self, ticker: str) -> dict | None:
        return asyncio.run(self._api.get_ticker_stats(ticker))

    def get_ticker_stats_batch(self, tickers: list[str]) -> dict[str, dict | None]:
        return asyncio.run(self._api.get_ticker_stats_batch(tickers))

    # Quotes
    def get_quote(self, ticker: str) -> dict:
        return asyncio.run(self._api.get_quote(ticker))

    def get_quotes(self, tickers: list[str], fields=None) -> dict:
        return asyncio.run(self._api.get_quotes(tickers, fields))

    # Price history
    def get_daily_history(
        self,
        ticker: str,
        start: str | datetime.date,
        end: str | datetime.date,
        force_live: bool = False,
        db_only: bool = False,
    ) -> pd.DataFrame:
        return asyncio.run(self._api.get_daily_history(ticker, start, end, force_live, db_only))

    def get_daily_histories(
        self,
        tickers: list[str],
        start: str | datetime.date,
        end: str | datetime.date,
    ) -> dict[str, pd.DataFrame]:
        return asyncio.run(self._api.get_daily_histories(tickers, start, end))

    def get_5m_history(
        self,
        ticker: str,
        start: str | datetime.date | datetime.datetime,
        end: str | datetime.date | datetime.datetime,
    ) -> pd.DataFrame:
        return asyncio.run(self._api.get_5m_history(ticker, start, end))

    def get_5m_histories(
        self,
        tickers: list[str],
        start: str | datetime.date | datetime.datetime,
        end: str | datetime.date | datetime.datetime,
    ) -> dict[str, pd.DataFrame]:
        return asyncio.run(self._api.get_5m_histories(tickers, start, end))

    # Fundamentals
    def get_schwab_fundamentals(self, ticker: str) -> dict:
        return asyncio.run(self._api.get_schwab_fundamentals(ticker))

    def get_schwab_fundamentals_batch(self, tickers: list[str]) -> dict:
        return asyncio.run(self._api.get_schwab_fundamentals_batch(tickers))

    def get_financials(self, ticker: str) -> dict:
        return self._api.get_financials(ticker)

    def get_eps_history(self, ticker: str) -> pd.DataFrame:
        return asyncio.run(self._api.get_eps_history(ticker))

    def get_analyst_price_targets(self, ticker: str) -> dict | None:
        return self._api.get_analyst_price_targets(ticker)

    def get_recommendations(self, ticker: str) -> pd.DataFrame:
        return self._api.get_recommendations(ticker)

    def get_upgrades_downgrades(self, ticker: str) -> pd.DataFrame:
        return self._api.get_upgrades_downgrades(ticker)

    def get_float_data(self, ticker: str) -> dict:
        return self._api.get_float_data(ticker)

    def get_institutional_holders(self, ticker: str) -> pd.DataFrame:
        return self._api.get_institutional_holders(ticker)

    def get_major_holders(self, ticker: str) -> pd.DataFrame:
        return self._api.get_major_holders(ticker)

    def get_insider_transactions(self, ticker: str) -> pd.DataFrame:
        return self._api.get_insider_transactions(ticker)

    def get_insider_purchases(self, ticker: str) -> pd.DataFrame:
        return self._api.get_insider_purchases(ticker)

    # Earnings
    def get_next_earnings(self, ticker: str) -> dict | None:
        return asyncio.run(self._api.get_next_earnings(ticker))

    def get_earnings_calendar(self, date: datetime.date) -> pd.DataFrame:
        return asyncio.run(self._api.get_earnings_calendar(date))

    def get_eps_estimate(self, ticker: str, period: str = "quarter") -> pd.DataFrame:
        return self._api.get_eps_estimate(ticker, period)

    # Options & IV
    def get_options_chain(self, ticker: str) -> dict:
        return asyncio.run(self._api.get_options_chain(ticker))

    def get_options_chains_batch(self, tickers: list[str]) -> dict[str, dict]:
        return asyncio.run(self._api.get_options_chains_batch(tickers))

    def get_iv_history(self, ticker: str, days: int = 365) -> pd.DataFrame:
        return asyncio.run(self._api.get_iv_history(ticker, days))

    def get_latest_iv(self, ticker: str) -> float | None:
        return asyncio.run(self._api.get_latest_iv(ticker))

    # Sentiment & flow
    def get_popularity(self, ticker: str, start_date=None, end_date=None) -> pd.DataFrame:
        return asyncio.run(self._api.get_popularity(ticker, start_date, end_date))

    def get_popular_tickers(self, filter_name: str = "all stock subreddits", limit: int = 100) -> pd.DataFrame:
        return self._api.get_popular_tickers(filter_name, limit)

    def get_news(self, query: str, **kwargs) -> dict:
        return self._api.get_news(query, **kwargs)

    def get_recent_filings(self, ticker: str, latest: int = 10) -> pd.DataFrame:
        return asyncio.run(self._api.get_recent_filings(ticker, latest))

    def get_filing_link(self, ticker: str, filing: dict) -> str | None:
        return asyncio.run(self._api.get_filing_link(ticker, filing))

    def get_politician_trades(self, politician_id: str) -> pd.DataFrame:
        return self._api.get_politician_trades(politician_id)

    def get_all_politicians(self) -> list:
        return asyncio.run(self._api.get_all_politicians())

    # Movers
    def get_premarket_gainers(self) -> pd.DataFrame:
        return self._api.get_premarket_gainers()

    def get_intraday_gainers(self) -> pd.DataFrame:
        return self._api.get_intraday_gainers()

    def get_postmarket_gainers(self) -> pd.DataFrame:
        return self._api.get_postmarket_gainers()

    def get_unusual_volume_movers(self) -> pd.DataFrame:
        return self._api.get_unusual_volume_movers()

    def get_market_caps(self, limit: int = 10000) -> pd.DataFrame:
        return self._api.get_market_caps(limit)

    def get_schwab_movers(self, sort_order=None) -> dict:
        return asyncio.run(self._api.get_schwab_movers(sort_order))

    # Watchlists
    def get_watchlists(self, types: list[str] | None = None) -> list[str]:
        return asyncio.run(self._api.get_watchlists(types))

    def get_watchlist_tickers(self, watchlist_id: str) -> list[str]:
        return asyncio.run(self._api.get_watchlist_tickers(watchlist_id))


class DataAPI:
    """Async facade over all RocketStocks data repositories and clients.

    Consumers receive a StockData instance via build_data_api(); they should
    not construct DataAPI directly unless they already manage StockData.
    """

    def __init__(self, stock_data):
        self._sd = stock_data
        self.batches = BatchAPI(self)

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

    async def get_ticker_infos(self, tickers: list[str]) -> list[dict | None]:
        """Return ticker info dicts for all *tickers* concurrently."""
        logger.debug(f"get_ticker_infos({tickers})")
        return list(await asyncio.gather(*[self.get_ticker_info(t) for t in tickers]))

    async def validate_ticker(self, ticker: str) -> bool:
        """Return True if *ticker* exists in the database."""
        logger.debug(f"validate_ticker({ticker!r})")
        return await self._sd.tickers.validate_ticker(ticker)

    async def get_ticker_stats(self, ticker: str) -> dict | None:
        """Return ticker_stats row for *ticker*, or None if absent."""
        logger.debug(f"get_ticker_stats({ticker!r})")
        return await self._sd.ticker_stats.get_stats(ticker)

    async def get_ticker_stats_batch(self, tickers: list[str]) -> dict[str, dict | None]:
        """Return {ticker: stats_dict} for all *tickers* concurrently."""
        logger.debug(f"get_ticker_stats_batch({tickers})")
        results = await asyncio.gather(*[self.get_ticker_stats(t) for t in tickers])
        return dict(zip(tickers, results))

    # ------------------------------------------------------------------
    # Quotes
    # ------------------------------------------------------------------

    async def get_quote(self, ticker: str) -> dict:
        """Return the latest Schwab quote for *ticker*."""
        logger.debug(f"get_quote({ticker!r})")
        return await self._sd.schwab.get_quote(ticker)

    async def get_quotes(self, tickers: list[str], fields=None) -> dict:
        """Return Schwab quotes for all *tickers* in a single API call."""
        logger.debug(f"get_quotes({tickers})")
        return await self._sd.schwab.get_quotes(tickers, fields=fields)

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

    async def get_daily_histories(
        self,
        tickers: list[str],
        start: str | datetime.date,
        end: str | datetime.date,
    ) -> dict[str, pd.DataFrame]:
        """Return daily OHLCV for all *tickers* from the DB in a single query."""
        logger.debug(f"get_daily_histories({tickers})")
        return await self._sd.price_history.fetch_daily_price_history_batch(
            tickers,
            start_date=_to_date(start),
            end_date=_to_date(end),
        )

    async def get_5m_history(
        self,
        ticker: str,
        start: str | datetime.date | datetime.datetime,
        end: str | datetime.date | datetime.datetime,
    ) -> pd.DataFrame:
        """Return 5-minute OHLCV for *ticker* between *start* and *end*."""
        logger.debug(f"get_5m_history({ticker!r})")
        return await self._sd.price_history.fetch_5m_price_history(
            ticker,
            start_datetime=_to_datetime(start),
            end_datetime=_to_datetime(end),
        )

    async def get_5m_histories(
        self,
        tickers: list[str],
        start: str | datetime.date | datetime.datetime,
        end: str | datetime.date | datetime.datetime,
    ) -> dict[str, pd.DataFrame]:
        """Return 5-minute OHLCV for all *tickers* concurrently."""
        logger.debug(f"get_5m_histories({tickers})")
        results = await asyncio.gather(*[self.get_5m_history(t, start, end) for t in tickers])
        return dict(zip(tickers, results))

    # ------------------------------------------------------------------
    # Fundamentals & financials
    # ------------------------------------------------------------------

    async def get_schwab_fundamentals(self, ticker: str) -> dict:
        """Return Schwab fundamental data for *ticker*."""
        logger.debug(f"get_schwab_fundamentals({ticker!r})")
        return await self._sd.schwab.get_fundamentals([ticker])

    async def get_schwab_fundamentals_batch(self, tickers: list[str]) -> dict:
        """Return Schwab fundamental data for all *tickers* in a single call."""
        logger.debug(f"get_schwab_fundamentals_batch({tickers})")
        return await self._sd.schwab.get_fundamentals(tickers)

    def get_financials(self, ticker: str) -> dict:
        """Return yfinance financial statements for *ticker*.

        Keys: income_statement, quarterly_income_statement, balance_sheet,
        quarterly_balance_sheet, cash_flow, quarterly_cash_flow (each a DataFrame).
        """
        logger.debug(f"get_financials({ticker!r})")
        return self._sd.yfinance.get_financials(ticker)

    async def get_eps_history(self, ticker: str) -> pd.DataFrame:
        """Return historical EPS records for *ticker* from the DB."""
        logger.debug(f"get_eps_history({ticker!r})")
        return await self._sd.earnings.get_historical_earnings(ticker)

    def get_analyst_price_targets(self, ticker: str) -> dict | None:
        """Return analyst price targets for *ticker* from yfinance."""
        logger.debug(f"get_analyst_price_targets({ticker!r})")
        return self._sd.yfinance.get_analyst_price_targets(ticker)

    def get_recommendations(self, ticker: str) -> pd.DataFrame:
        """Return analyst recommendations summary for *ticker* from yfinance."""
        logger.debug(f"get_recommendations({ticker!r})")
        return self._sd.yfinance.get_recommendations_summary(ticker)

    def get_upgrades_downgrades(self, ticker: str) -> pd.DataFrame:
        """Return recent analyst upgrades/downgrades for *ticker* from yfinance."""
        logger.debug(f"get_upgrades_downgrades({ticker!r})")
        return self._sd.yfinance.get_upgrades_downgrades(ticker)

    def get_float_data(self, ticker: str) -> dict:
        """Return float and short interest data for *ticker* from yfinance."""
        logger.debug(f"get_float_data({ticker!r})")
        return self._sd.yfinance.get_float_data(ticker)

    def get_institutional_holders(self, ticker: str) -> pd.DataFrame:
        """Return institutional holders for *ticker* from yfinance."""
        logger.debug(f"get_institutional_holders({ticker!r})")
        return self._sd.yfinance.get_institutional_holders(ticker)

    def get_major_holders(self, ticker: str) -> pd.DataFrame:
        """Return major holders breakdown for *ticker* from yfinance."""
        logger.debug(f"get_major_holders({ticker!r})")
        return self._sd.yfinance.get_major_holders(ticker)

    def get_insider_transactions(self, ticker: str) -> pd.DataFrame:
        """Return insider transactions for *ticker* from yfinance."""
        logger.debug(f"get_insider_transactions({ticker!r})")
        return self._sd.yfinance.get_insider_transactions(ticker)

    def get_insider_purchases(self, ticker: str) -> pd.DataFrame:
        """Return insider purchases summary for *ticker* from yfinance."""
        logger.debug(f"get_insider_purchases({ticker!r})")
        return self._sd.yfinance.get_insider_purchases(ticker)

    # ------------------------------------------------------------------
    # Earnings calendar
    # ------------------------------------------------------------------

    async def get_next_earnings(self, ticker: str) -> dict | None:
        """Return the next upcoming earnings info for *ticker*, or None."""
        logger.debug(f"get_next_earnings({ticker!r})")
        return await self._sd.earnings.get_next_earnings_info(ticker)

    async def get_earnings_calendar(self, date: datetime.date) -> pd.DataFrame:
        """Return all earnings scheduled on *date*."""
        logger.debug(f"get_earnings_calendar({date})")
        return await self._sd.earnings.get_earnings_on_date(_to_date(date))

    def get_eps_estimate(self, ticker: str, period: str = "quarter") -> pd.DataFrame:
        """Return EPS estimate for *ticker*.

        period='quarter' returns the quarterly forecast; 'year' returns yearly.
        """
        logger.debug(f"get_eps_estimate({ticker!r}, period={period!r})")
        if period == "year":
            return self._sd.nasdaq.get_earnings_forecast_yearly(ticker)
        return self._sd.nasdaq.get_earnings_forecast_quarterly(ticker)

    # ------------------------------------------------------------------
    # Options & IV
    # ------------------------------------------------------------------

    async def get_options_chain(self, ticker: str) -> dict:
        """Return the Schwab options chain for *ticker*."""
        logger.debug(f"get_options_chain({ticker!r})")
        return await self._sd.schwab.get_options_chain(ticker)

    async def get_options_chains_batch(self, tickers: list[str]) -> dict[str, dict]:
        """Return options chains for all *tickers* concurrently."""
        logger.debug(f"get_options_chains_batch({tickers})")
        results = await asyncio.gather(
            *[self.get_options_chain(t) for t in tickers],
            return_exceptions=True,
        )
        return {
            ticker: result  # type: ignore[misc]
            for ticker, result in zip(tickers, results)
            if not isinstance(result, Exception)
        }

    async def get_iv_history(self, ticker: str, days: int = 365) -> pd.DataFrame:
        """Return the last *days* IV rows for *ticker* from the DB."""
        logger.debug(f"get_iv_history({ticker!r}, days={days})")
        return await self._sd.iv_history.get_iv_history(ticker, days)

    async def get_latest_iv(self, ticker: str) -> float | None:
        """Return the most recent root-level IV for *ticker*, or None."""
        logger.debug(f"get_latest_iv({ticker!r})")
        return await self._sd.iv_history.get_latest_iv(ticker)

    # ------------------------------------------------------------------
    # Sentiment & flow
    # ------------------------------------------------------------------

    async def get_popularity(
        self,
        ticker: str,
        start_date=None,
        end_date=None,
    ) -> pd.DataFrame:
        """Return historical ApeWisdom popularity for *ticker* from the DB."""
        logger.debug(f"get_popularity({ticker!r})")
        return await self._sd.popularity.fetch_popularity(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
        )

    def get_popular_tickers(
        self,
        filter_name: str = "all stock subreddits",
        limit: int = 100,
    ) -> pd.DataFrame:
        """Return live popular tickers from ApeWisdom."""
        logger.debug(f"get_popular_tickers(filter={filter_name!r}, limit={limit})")
        return self._sd.popularity_client.get_popular_stocks(
            filter_name=filter_name,
            num_stocks=limit,
        )

    def get_news(self, query: str, **kwargs) -> dict:
        """Return news articles matching *query* from NewsAPI."""
        logger.debug(f"get_news({query!r})")
        return self._sd.news.get_news(query, **kwargs)

    async def get_recent_filings(self, ticker: str, latest: int = 10) -> pd.DataFrame:
        """Return the *latest* most recent SEC filings for *ticker*."""
        logger.debug(f"get_recent_filings({ticker!r}, latest={latest})")
        return await self._sd.sec.get_recent_filings(ticker, latest=latest)

    async def get_filing_link(self, ticker: str, filing: dict) -> str | None:
        """Return the direct SEC EDGAR link for *filing*."""
        logger.debug(f"get_filing_link({ticker!r})")
        return await self._sd.sec.get_link_to_filing(ticker, filing)

    def get_politician_trades(self, politician_id: str) -> pd.DataFrame:
        """Return all trades by the politician identified by *politician_id*."""
        logger.debug(f"get_politician_trades({politician_id!r})")
        return self._sd.capitol_trades.trades(politician_id)

    async def get_all_politicians(self) -> list:
        """Return all politicians stored in the database."""
        logger.debug("get_all_politicians()")
        return await self._sd.capitol_trades.all_politicians()

    # ------------------------------------------------------------------
    # Movers
    # ------------------------------------------------------------------

    def get_premarket_gainers(self) -> pd.DataFrame:
        """Return pre-market gainers from TradingView."""
        logger.debug("get_premarket_gainers()")
        return self._sd.trading_view.get_premarket_gainers()

    def get_intraday_gainers(self) -> pd.DataFrame:
        """Return intraday gainers from TradingView."""
        logger.debug("get_intraday_gainers()")
        return self._sd.trading_view.get_intraday_gainers()

    def get_postmarket_gainers(self) -> pd.DataFrame:
        """Return post-market gainers from TradingView."""
        logger.debug("get_postmarket_gainers()")
        return self._sd.trading_view.get_postmarket_gainers()

    def get_unusual_volume_movers(self) -> pd.DataFrame:
        """Return unusual volume movers from TradingView."""
        logger.debug("get_unusual_volume_movers()")
        return self._sd.trading_view.get_unusual_volume_movers()

    def get_market_caps(self, limit: int = 10000) -> pd.DataFrame:
        """Return market caps for up to *limit* US stocks from TradingView."""
        logger.debug(f"get_market_caps(limit={limit})")
        return self._sd.trading_view.get_market_caps(limit)

    async def get_schwab_movers(self, sort_order=None) -> dict:
        """Return top equity movers from Schwab."""
        logger.debug("get_schwab_movers()")
        return await self._sd.schwab.get_movers(sort_order)

    # ------------------------------------------------------------------
    # Watchlists
    # ------------------------------------------------------------------

    async def get_watchlists(self, types: list[str] | None = None) -> list[str]:
        """Return watchlist identifiers. Defaults to named watchlists."""
        logger.debug(f"get_watchlists(types={types})")
        return await self._sd.watchlists.get_watchlists(watchlist_types=types)

    async def get_watchlist_tickers(self, watchlist_id: str) -> list[str]:
        """Return the ticker list for *watchlist_id*."""
        logger.debug(f"get_watchlist_tickers({watchlist_id!r})")
        return await self._sd.watchlists.get_watchlist_tickers(watchlist_id)

"""StockData — thin facade composing all data-layer repositories and clients.

All business logic lives in the individual repository modules.  Call sites
(bot cogs, scheduler) continue to use ``bot.stock_data.get_all_tickers()``
etc. without modification.
"""
import logging

from rocketstocks.data.db import Postgres
from rocketstocks.data.earnings import Earnings
from rocketstocks.data.financials import fetch_financials
from rocketstocks.data.popularity_store import PopularityRepository
from rocketstocks.data.price_history import PriceHistoryRepository
from rocketstocks.data.tickers import TickerRepository
from rocketstocks.data.watchlists import Watchlists
from rocketstocks.data.clients.nasdaq import Nasdaq
from rocketstocks.data.clients.news import News
from rocketstocks.data.clients.capitol_trades import CapitolTrades
from rocketstocks.data.clients.tradingview import TradingView
from rocketstocks.data.clients.ape_wisdom import ApeWisdom
from rocketstocks.data.clients.sec import SEC
from rocketstocks.data.clients.schwab import Schwab

logger = logging.getLogger(__name__)


class StockData:
    def __init__(self):

        # Clients
        self.db = Postgres()
        self.schwab = Schwab()
        self.nasdaq = Nasdaq()
        self.news = News()
        self.earnings = Earnings(nasdaq=self.nasdaq, db=self.db)
        self.capitol_trades = CapitolTrades(db=self.db)
        self.watchlists = Watchlists(self.db)
        self.trading_view = TradingView()
        self.popularity_client = ApeWisdom()
        self.sec = SEC(get_cik_fn=self.get_cik)

        # Repositories
        self._tickers = TickerRepository(db=self.db, nasdaq=self.nasdaq, sec=self.sec)
        self._price_history = PriceHistoryRepository(db=self.db, schwab=self.schwab)
        self._popularity = PopularityRepository(db=self.db)

        self._alert_tickers: dict = {}

    # ------------------------------------------------------------------
    # Alert ticker tracking
    # ------------------------------------------------------------------

    @property
    def alert_tickers(self) -> dict:
        return self._alert_tickers

    async def update_alert_tickers(self, tickers: list, source: str):
        """Update list of tickers to monitor for alerts."""
        self._alert_tickers[source] = tickers

    # ------------------------------------------------------------------
    # Ticker repository delegation
    # ------------------------------------------------------------------

    def update_tickers(self):
        return self._tickers.update_tickers()

    async def insert_tickers(self):
        return await self._tickers.insert_tickers()

    def get_ticker_info(self, ticker: str):
        return self._tickers.get_ticker_info(ticker)

    def get_all_ticker_info(self):
        return self._tickers.get_all_ticker_info()

    def get_all_tickers(self) -> list:
        return self._tickers.get_all_tickers()

    def get_all_tickers_by_market_cap(self, market_cap: float) -> list:
        return self._tickers.get_all_tickers_by_market_cap(market_cap)

    def get_all_tickers_by_sector(self, sector: str):
        return self._tickers.get_all_tickers_by_sector(sector)

    def get_cik(self, ticker: str):
        return self._tickers.get_cik(ticker)

    def get_market_cap(self, ticker: str):
        return self._tickers.get_market_cap(ticker)

    async def validate_ticker(self, ticker: str) -> bool:
        return await self._tickers.validate_ticker(ticker)

    async def parse_valid_tickers(self, ticker_string: str) -> tuple:
        return await self._tickers.parse_valid_tickers(ticker_string)

    # ------------------------------------------------------------------
    # Price history repository delegation
    # ------------------------------------------------------------------

    async def update_daily_price_history(self):
        tickers = self.get_all_tickers()
        return await self._price_history.update_daily_price_history(tickers)

    async def update_daily_price_history_by_ticker(self, ticker: str):
        return await self._price_history.update_daily_price_history_by_ticker(ticker)

    async def update_5m_price_history(self):
        tickers = self.get_all_tickers()
        return await self._price_history.update_5m_price_history(tickers)

    async def update_5m_price_history_by_ticker(self, ticker: str):
        return await self._price_history.update_5m_price_history_by_ticker(ticker)

    def fetch_daily_price_history(self, ticker, start_date=None, end_date=None):
        return self._price_history.fetch_daily_price_history(ticker, start_date, end_date)

    def fetch_5m_price_history(self, ticker, start_datetime=None, end_datetime=None):
        return self._price_history.fetch_5m_price_history(ticker, start_datetime, end_datetime)

    # ------------------------------------------------------------------
    # Financials (standalone function)
    # ------------------------------------------------------------------

    @staticmethod
    def fetch_financials(ticker: str) -> dict:
        return fetch_financials(ticker)

    # ------------------------------------------------------------------
    # Popularity repository delegation
    # ------------------------------------------------------------------

    def fetch_popularity(self, ticker=None):
        return self._popularity.fetch_popularity(ticker)

    def insert_popularity(self, popular_stocks):
        return self._popularity.insert_popularity(popular_stocks)

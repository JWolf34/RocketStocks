"""StockData — service container composing all data-layer repositories and clients.

Access pattern: ``stock_data.{client_or_repo}.{method}()``
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
        self.tickers = TickerRepository(db=self.db, nasdaq=self.nasdaq, sec=self.sec)
        self.price_history = PriceHistoryRepository(db=self.db, schwab=self.schwab)
        self.popularity = PopularityRepository(db=self.db)

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
    # CIK lookup — bound method reference used by SEC client at init time
    # ------------------------------------------------------------------

    def get_cik(self, ticker: str):
        return self.tickers.get_cik(ticker)

    # ------------------------------------------------------------------
    # Financials (standalone function wrapper)
    # ------------------------------------------------------------------

    @staticmethod
    def fetch_financials(ticker: str) -> dict:
        return fetch_financials(ticker)

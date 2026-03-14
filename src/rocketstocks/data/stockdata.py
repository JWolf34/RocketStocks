"""StockData — service container composing all data-layer repositories and clients.

Access pattern: ``stock_data.{client_or_repo}.{method}()``
"""
import logging

from rocketstocks.data.channel_config import ChannelConfigRepository
from rocketstocks.data.db import Postgres
from rocketstocks.data.earnings import Earnings
from rocketstocks.data.financials import fetch_financials
from rocketstocks.data.popularity_store import PopularityRepository
from rocketstocks.data.price_history import PriceHistoryRepository
from rocketstocks.data.ticker_stats import TickerStatsRepository
from rocketstocks.data.surge_store import SurgeRepository
from rocketstocks.data.market_signal_store import MarketSignalRepository
from rocketstocks.data.alert_roles import AlertRolesRepository
from rocketstocks.data.tickers import TickerRepository
from rocketstocks.data.watchlists import Watchlists
from rocketstocks.data.clients.nasdaq import Nasdaq
from rocketstocks.data.clients.news import News
from rocketstocks.data.clients.capitol_trades import CapitolTrades
from rocketstocks.data.clients.tradingview import TradingView
from rocketstocks.data.clients.ape_wisdom import ApeWisdom
from rocketstocks.data.clients.sec import SEC
from rocketstocks.data.clients.schwab import Schwab
from rocketstocks.data.clients.tiingo import Tiingo
from rocketstocks.data.clients.stooq import Stooq

logger = logging.getLogger(__name__)


class StockData:
    def __init__(self, db=None, schwab=None, nasdaq=None, news=None,
                 capitol_trades=None, watchlists=None, trading_view=None,
                 popularity_client=None, sec=None, tickers=None,
                 price_history=None, popularity=None, channel_config=None,
                 ticker_stats=None, surge_store=None, market_signal_store=None,
                 alert_roles=None, tiingo=None, stooq=None):

        # Clients
        self.db = db or Postgres()
        self.schwab = schwab or Schwab()
        self.nasdaq = nasdaq or Nasdaq()
        self.news = news or News()
        self.earnings = Earnings(nasdaq=self.nasdaq, db=self.db)
        self.capitol_trades = capitol_trades or CapitolTrades(db=self.db)
        self.watchlists = watchlists or Watchlists(self.db)
        self.trading_view = trading_view or TradingView()
        self.popularity_client = popularity_client or ApeWisdom()
        self.sec = sec or SEC(db=self.db)
        self.tiingo = tiingo or Tiingo()
        self.stooq = stooq or Stooq()

        # Repositories
        self.tickers = tickers or TickerRepository(db=self.db, nasdaq=self.nasdaq, sec=self.sec, tiingo=self.tiingo)
        self.price_history = price_history or PriceHistoryRepository(db=self.db, schwab=self.schwab, tiingo=self.tiingo, stooq=self.stooq)
        self.popularity = popularity or PopularityRepository(db=self.db)
        self.channel_config = channel_config or ChannelConfigRepository(db=self.db)
        self.ticker_stats = ticker_stats or TickerStatsRepository(db=self.db)
        self.surge_store = surge_store or SurgeRepository(db=self.db)
        self.market_signal_store = market_signal_store or MarketSignalRepository(db=self.db)
        self.alert_roles = alert_roles or AlertRolesRepository(db=self.db)

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
    # Financials (standalone function wrapper)
    # ------------------------------------------------------------------

    @staticmethod
    def fetch_financials(ticker: str) -> dict:
        return fetch_financials(ticker)

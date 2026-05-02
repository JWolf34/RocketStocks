"""Fixtures for rocketstocks.api tests."""
import datetime
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from rocketstocks.api.client import DataAPI


def _make_daily_df(ticker: str = "AAPL", rows: int = 5) -> pd.DataFrame:
    dates = [datetime.date(2026, 1, i + 1) for i in range(rows)]
    return pd.DataFrame({
        "ticker": ticker,
        "open": [150.0] * rows,
        "high": [155.0] * rows,
        "low": [148.0] * rows,
        "close": [152.0] * rows,
        "volume": [1_000_000] * rows,
        "date": dates,
    })


def _make_5m_df(ticker: str = "AAPL", rows: int = 10) -> pd.DataFrame:
    base = datetime.datetime(2026, 1, 2, 9, 30)
    datetimes = [base + datetime.timedelta(minutes=5 * i) for i in range(rows)]
    return pd.DataFrame({
        "ticker": ticker,
        "open": [150.0] * rows,
        "high": [151.0] * rows,
        "low": [149.0] * rows,
        "close": [150.5] * rows,
        "volume": [100_000] * rows,
        "datetime": datetimes,
    })


@pytest.fixture
def mock_stock_data():
    sd = MagicMock(name="StockData")
    sd.tickers = MagicMock(name="tickers")
    sd.schwab = MagicMock(name="schwab")
    sd.price_history = MagicMock(name="price_history")
    sd.tiingo = MagicMock(name="tiingo")
    sd.stooq = MagicMock(name="stooq")
    sd.ticker_stats = MagicMock(name="ticker_stats")
    sd.earnings = MagicMock(name="earnings")
    sd.nasdaq = MagicMock(name="nasdaq")
    sd.iv_history = MagicMock(name="iv_history")
    sd.popularity = MagicMock(name="popularity")
    sd.popularity_client = MagicMock(name="popularity_client")
    sd.news = MagicMock(name="news")
    sd.sec = MagicMock(name="sec")
    sd.capitol_trades = MagicMock(name="capitol_trades")
    sd.trading_view = MagicMock(name="trading_view")
    sd.watchlists = MagicMock(name="watchlists")
    sd.yfinance = MagicMock(name="yfinance")

    # Phase 1 defaults
    sd.tickers.get_ticker_info = AsyncMock(return_value={"ticker": "AAPL", "name": "Apple Inc."})
    sd.schwab.get_quote = AsyncMock(return_value={"lastPrice": 150.0})
    sd.price_history.fetch_daily_price_history = AsyncMock(return_value=_make_daily_df())
    sd.tiingo.get_daily_price_history = MagicMock(return_value=_make_daily_df())
    sd.stooq.get_daily_price_history = MagicMock(return_value=_make_daily_df())

    # Phase 2 defaults
    sd.tickers.validate_ticker = AsyncMock(return_value=True)
    sd.ticker_stats.get_stats = AsyncMock(return_value={"ticker": "AAPL", "market_cap": 3_000_000_000})
    sd.schwab.get_quotes = AsyncMock(return_value={"AAPL": {"lastPrice": 150.0}, "MSFT": {"lastPrice": 420.0}})
    sd.price_history.fetch_daily_price_history_batch = AsyncMock(
        return_value={"AAPL": _make_daily_df("AAPL"), "MSFT": _make_daily_df("MSFT")}
    )
    sd.price_history.fetch_5m_price_history = AsyncMock(return_value=_make_5m_df())
    sd.schwab.get_fundamentals = AsyncMock(return_value={"instruments": [{"fundamental": {"symbol": "AAPL"}}]})
    sd.schwab.get_options_chain = AsyncMock(return_value={"callExpDateMap": {}, "putExpDateMap": {}})
    sd.schwab.get_movers = AsyncMock(return_value={"screeners": []})
    sd.yfinance.get_financials = MagicMock(return_value={"income_statement": pd.DataFrame(), "balance_sheet": pd.DataFrame()})
    sd.yfinance.get_analyst_price_targets = MagicMock(return_value={"targetMeanPrice": 200.0})
    sd.yfinance.get_recommendations_summary = MagicMock(return_value=pd.DataFrame([{"period": "0m", "strongBuy": 10}]))
    sd.yfinance.get_upgrades_downgrades = MagicMock(return_value=pd.DataFrame([{"Action": "up"}]))
    sd.yfinance.get_float_data = MagicMock(return_value={"float_shares": 1_000_000})
    sd.yfinance.get_institutional_holders = MagicMock(return_value=pd.DataFrame([{"Holder": "Vanguard"}]))
    sd.yfinance.get_major_holders = MagicMock(return_value=pd.DataFrame([{"Value": "5%"}]))
    sd.yfinance.get_insider_transactions = MagicMock(return_value=pd.DataFrame([{"Insider": "CEO"}]))
    sd.yfinance.get_insider_purchases = MagicMock(return_value=pd.DataFrame([{"Insider": "CEO"}]))
    sd.earnings.get_next_earnings_info = AsyncMock(return_value={"ticker": "AAPL", "date": datetime.date(2026, 5, 1)})
    sd.earnings.get_earnings_on_date = AsyncMock(return_value=pd.DataFrame([{"ticker": "AAPL"}]))
    sd.earnings.get_historical_earnings = AsyncMock(return_value=pd.DataFrame([{"ticker": "AAPL", "eps": 1.5}]))
    sd.nasdaq.get_earnings_forecast_quarterly = MagicMock(return_value=pd.DataFrame([{"period": "Q1"}]))
    sd.nasdaq.get_earnings_forecast_yearly = MagicMock(return_value=pd.DataFrame([{"period": "2026"}]))
    sd.iv_history.get_iv_history = AsyncMock(return_value=pd.DataFrame([{"date": datetime.date(2026, 1, 1), "iv": 0.3}]))
    sd.iv_history.get_latest_iv = AsyncMock(return_value=0.3)
    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame([{"ticker": "GME", "mentions": 100}]))
    sd.popularity_client.get_popular_stocks = MagicMock(return_value=pd.DataFrame([{"ticker": "GME"}]))
    sd.news.get_news = MagicMock(return_value={"articles": [{"title": "Apple up"}]})
    sd.sec.get_recent_filings = AsyncMock(return_value=pd.DataFrame([{"filingDate": "2026-01-01"}]))
    sd.sec.get_link_to_filing = AsyncMock(return_value="https://sec.gov/Archives/edgar/...")
    sd.capitol_trades.trades = MagicMock(return_value=pd.DataFrame([{"ticker": "AAPL"}]))
    sd.capitol_trades.all_politicians = AsyncMock(return_value=[{"name": "Nancy Pelosi"}])
    sd.trading_view.get_premarket_gainers = MagicMock(return_value=pd.DataFrame([{"ticker": "AAPL"}]))
    sd.trading_view.get_intraday_gainers = MagicMock(return_value=pd.DataFrame([{"ticker": "AAPL"}]))
    sd.trading_view.get_postmarket_gainers = MagicMock(return_value=pd.DataFrame([{"ticker": "AAPL"}]))
    sd.trading_view.get_unusual_volume_movers = MagicMock(return_value=pd.DataFrame([{"ticker": "GME"}]))
    sd.trading_view.get_market_caps = MagicMock(return_value=pd.DataFrame([{"ticker": "AAPL", "market_cap": 3e12}]))
    sd.watchlists.get_watchlists = AsyncMock(return_value=["Tech", "Biotech"])
    sd.watchlists.get_watchlist_tickers = AsyncMock(return_value=["AAPL", "MSFT"])

    return sd


@pytest.fixture
def api(mock_stock_data) -> DataAPI:
    return DataAPI(mock_stock_data)


@pytest.fixture
def sample_daily_df() -> pd.DataFrame:
    return _make_daily_df()


@pytest.fixture
def sample_5m_df() -> pd.DataFrame:
    return _make_5m_df()

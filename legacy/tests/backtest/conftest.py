"""Shared fixtures for backtest tests."""
import datetime
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Async mock DB (overrides the sync mock_db from root conftest)
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    db = MagicMock(name='Postgres')
    db.execute = AsyncMock(return_value=None)
    db.execute_batch = AsyncMock(return_value=None)
    return db


@pytest.fixture
def mock_stock_data(daily_price_df):
    """StockData mock pre-configured for backtest tests."""
    sd = MagicMock(name='StockData')
    sd.tickers = MagicMock(name='tickers')
    sd.tickers.get_all_tickers = AsyncMock(return_value=['AAPL', 'MSFT', 'GME'])
    sd.tickers.get_all_ticker_info = AsyncMock(return_value=pd.DataFrame({
        'ticker': ['AAPL', 'MSFT', 'GME'],
        'sector': ['Technology', 'Technology', 'Consumer Cyclical'],
        'industry': ['Consumer Electronics', 'Software', 'Specialty Retail'],
        'exchange': ['NASDAQ', 'NASDAQ', 'NYSE'],
        'delist_date': [None, None, None],
    }))
    sd.ticker_stats = MagicMock(name='ticker_stats')
    sd.ticker_stats.get_all_stats = AsyncMock(return_value=[
        {'ticker': 'AAPL', 'classification': 'blue_chip', 'market_cap': 3_000_000_000_000, 'volatility_20d': 1.2},
        {'ticker': 'MSFT', 'classification': 'blue_chip', 'market_cap': 2_500_000_000_000, 'volatility_20d': 1.1},
        {'ticker': 'GME', 'classification': 'meme', 'market_cap': 10_000_000_000, 'volatility_20d': 6.5},
    ])
    sd.ticker_stats.get_all_classifications = AsyncMock(return_value={
        'AAPL': 'blue_chip', 'MSFT': 'blue_chip', 'GME': 'meme',
    })
    sd.popularity = MagicMock(name='popularity')
    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame(
        columns=['datetime', 'rank', 'ticker', 'name', 'mentions', 'upvotes',
                 'rank_24h_ago', 'mentions_24h_ago']
    ))
    sd.watchlists = MagicMock(name='watchlists')
    sd.watchlists.get_watchlist_tickers = AsyncMock(return_value=[])
    sd.watchlists.get_ticker_to_watchlist_map = AsyncMock(return_value={})
    sd.trading_view = MagicMock(name='trading_view')
    sd.trading_view.get_market_caps = MagicMock(return_value=pd.DataFrame(
        columns=['ticker', 'market_cap']
    ))
    sd.price_history = MagicMock(name='price_history')
    sd.price_history.fetch_daily_price_history = AsyncMock(return_value=daily_price_df)
    sd.price_history.fetch_5m_price_history = AsyncMock(return_value=pd.DataFrame())
    return sd


# ---------------------------------------------------------------------------
# Synthetic price DataFrames
# ---------------------------------------------------------------------------

def _make_daily_prices_lower(n: int = 100, seed: int = 42) -> pd.DataFrame:
    """Return a daily OHLCV DataFrame in the DB format (lowercase columns)."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range(end=datetime.date(2025, 3, 1), periods=n, freq='B')
    close = 100.0 + rng.standard_normal(n).cumsum()
    open_ = close - rng.uniform(0, 1, n)
    high = close + rng.uniform(0, 1, n)
    low = close - rng.uniform(0, 1, n)
    volume = rng.integers(500_000, 2_000_000, n).astype(float)
    return pd.DataFrame({
        'ticker': 'AAPL',
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
        'date': dates.date,
    })


@pytest.fixture
def daily_price_df():
    return _make_daily_prices_lower(100)


@pytest.fixture
def sample_results() -> list[dict]:
    """Synthetic backtest result dicts for stats tests."""
    return [
        {'ticker': 'AAPL', 'classification': 'blue_chip', 'sector': 'Technology',
         'return_pct': 5.0, 'sharpe_ratio': 0.8, 'max_drawdown': -3.0,
         'win_rate': 60.0, 'num_trades': 10, 'profit_factor': 1.5, 'error': None},
        {'ticker': 'MSFT', 'classification': 'blue_chip', 'sector': 'Technology',
         'return_pct': 8.0, 'sharpe_ratio': 1.2, 'max_drawdown': -2.0,
         'win_rate': 70.0, 'num_trades': 12, 'profit_factor': 1.8, 'error': None},
        {'ticker': 'GME', 'classification': 'meme', 'sector': 'Consumer Cyclical',
         'return_pct': -4.0, 'sharpe_ratio': -0.3, 'max_drawdown': -15.0,
         'win_rate': 40.0, 'num_trades': 5, 'profit_factor': 0.7, 'error': None},
        {'ticker': 'NVDA', 'classification': 'standard', 'sector': 'Technology',
         'return_pct': None, 'sharpe_ratio': None, 'max_drawdown': None,
         'win_rate': None, 'num_trades': None, 'profit_factor': None,
         'error': 'insufficient_data (15 bars)'},
    ]

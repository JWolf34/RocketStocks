"""Shared fixtures for the RocketStocks test suite."""
import datetime
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Database fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    """MagicMock of Postgres with a pre-configured cursor context manager."""
    db = MagicMock(name="Postgres")
    mock_cur = MagicMock(name="cursor")

    @contextmanager
    def _cursor():
        yield mock_cur

    db._cursor = _cursor
    db.get_table_columns.return_value = []
    db.select.return_value = []
    db.insert.return_value = None
    db.update.return_value = None
    db.delete.return_value = None
    return db


# ---------------------------------------------------------------------------
# StockData fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_stock_data():
    """MagicMock of StockData with all sub-objects as mocks."""
    sd = MagicMock(name="StockData")
    sd.db = MagicMock(name="db")
    sd.schwab = MagicMock(name="schwab")
    sd.nasdaq = MagicMock(name="nasdaq")
    sd.news = MagicMock(name="news")
    sd.earnings = MagicMock(name="earnings")
    sd.capitol_trades = MagicMock(name="capitol_trades")
    sd.watchlists = MagicMock(name="watchlists")
    sd.trading_view = MagicMock(name="trading_view")
    sd.popularity_client = MagicMock(name="popularity_client")
    sd.sec = MagicMock(name="sec")
    sd.tickers = MagicMock(name="tickers")
    sd.price_history = MagicMock(name="price_history")
    sd.popularity = MagicMock(name="popularity")
    sd.channel_config = MagicMock(name="channel_config")
    sd.alert_tickers = {}
    return sd


# ---------------------------------------------------------------------------
# Sample OHLCV DataFrame
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df():
    """Reusable sample OHLCV DataFrame for analysis/charting tests."""
    n = 60
    dates = pd.date_range(end=datetime.date.today(), periods=n, freq="B")
    import numpy as np
    rng = np.random.default_rng(42)
    close = 100.0 + rng.standard_normal(n).cumsum()
    open_ = close - rng.uniform(0, 1, n)
    high = close + rng.uniform(0, 1, n)
    low = close - rng.uniform(0, 1, n)
    volume = rng.integers(500_000, 2_000_000, n).astype(float)
    df = pd.DataFrame({
        "Open": open_,
        "High": high,
        "Low": low,
        "Close": close,
        "Volume": volume,
    }, index=dates)
    df.index.name = "Date"
    return df


# ---------------------------------------------------------------------------
# Environment patching
# ---------------------------------------------------------------------------

@pytest.fixture
def env_override(monkeypatch):
    """Patch os.getenv to return safe test values, preventing .env leakage."""
    env_values = {
        "DISCORD_TOKEN": "test-token",
        "POSTGRES_USER": "testuser",
        "POSTGRES_PASSWORD": "testpass",
        "POSTGRES_DB": "testdb",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "SCHWAB_API_KEY": "test-schwab-key",
        "SCHWAB_API_SECRET": "test-schwab-secret",
        "NEWS_API_KEY": "test-news-key",
        "TZ": "America/Chicago",
        "CONFIG_PATH": "/tmp/test_config.json",
    }
    monkeypatch.setattr("os.getenv", lambda key, *args: env_values.get(key, args[0] if args else None))
    return env_values


# ---------------------------------------------------------------------------
# Temp attachments path
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_attachments(tmp_path):
    """tmp_path based fixture for tests that write files."""
    attachments = tmp_path / "attachments"
    attachments.mkdir()
    return attachments

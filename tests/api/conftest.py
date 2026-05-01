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


@pytest.fixture
def mock_stock_data():
    sd = MagicMock(name="StockData")
    sd.tickers = MagicMock(name="tickers")
    sd.schwab = MagicMock(name="schwab")
    sd.price_history = MagicMock(name="price_history")
    sd.tiingo = MagicMock(name="tiingo")
    sd.stooq = MagicMock(name="stooq")

    sd.tickers.get_ticker_info = AsyncMock(return_value={"ticker": "AAPL", "name": "Apple Inc."})
    sd.schwab.get_quote = AsyncMock(return_value={"lastPrice": 150.0})
    sd.price_history.fetch_daily_price_history = AsyncMock(return_value=_make_daily_df())
    sd.tiingo.get_daily_price_history = MagicMock(return_value=_make_daily_df())
    sd.stooq.get_daily_price_history = MagicMock(return_value=_make_daily_df())
    return sd


@pytest.fixture
def api(mock_stock_data) -> DataAPI:
    return DataAPI(mock_stock_data)


@pytest.fixture
def sample_daily_df() -> pd.DataFrame:
    return _make_daily_df()

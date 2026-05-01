"""Tests for rocketstocks.api.client.DataAPI — Phase 1 methods."""
import datetime
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from rocketstocks.api.client import DataAPI, DataNotAvailable


class TestGetTickerInfo:
    async def test_returns_dict_from_repo(self, api, mock_stock_data):
        result = await api.get_ticker_info("AAPL")
        mock_stock_data.tickers.get_ticker_info.assert_called_once_with("AAPL")
        assert result["ticker"] == "AAPL"

    async def test_returns_none_when_not_found(self, api, mock_stock_data):
        mock_stock_data.tickers.get_ticker_info = AsyncMock(return_value=None)
        result = await api.get_ticker_info("ZZZZ")
        assert result is None


class TestGetQuote:
    async def test_calls_schwab_and_returns_data(self, api, mock_stock_data):
        result = await api.get_quote("AAPL")
        mock_stock_data.schwab.get_quote.assert_called_once_with("AAPL")
        assert "lastPrice" in result

    async def test_propagates_schwab_error(self, api, mock_stock_data):
        mock_stock_data.schwab.get_quote = AsyncMock(side_effect=RuntimeError("schwab down"))
        with pytest.raises(RuntimeError, match="schwab down"):
            await api.get_quote("AAPL")


class TestGetDailyHistory:
    async def test_db_first_returns_cached_data(self, api, mock_stock_data, sample_daily_df):
        mock_stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=sample_daily_df)
        result = await api.get_daily_history("AAPL", "2026-01-01", "2026-01-31")
        mock_stock_data.price_history.fetch_daily_price_history.assert_called_once()
        mock_stock_data.tiingo.get_daily_price_history.assert_not_called()
        assert len(result) == len(sample_daily_df)

    async def test_db_miss_falls_back_to_tiingo(self, api, mock_stock_data, sample_daily_df):
        mock_stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        mock_stock_data.tiingo.get_daily_price_history = MagicMock(return_value=sample_daily_df)
        result = await api.get_daily_history("AAPL", "2026-01-01", "2026-01-31")
        mock_stock_data.tiingo.get_daily_price_history.assert_called_once()
        mock_stock_data.stooq.get_daily_price_history.assert_not_called()
        assert not result.empty

    async def test_tiingo_miss_falls_back_to_stooq(self, api, mock_stock_data, sample_daily_df):
        mock_stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        mock_stock_data.tiingo.get_daily_price_history = MagicMock(return_value=pd.DataFrame())
        mock_stock_data.stooq.get_daily_price_history = MagicMock(return_value=sample_daily_df)
        result = await api.get_daily_history("AAPL", "2026-01-01", "2026-01-31")
        mock_stock_data.stooq.get_daily_price_history.assert_called_once()
        assert not result.empty

    async def test_force_live_skips_db(self, api, mock_stock_data, sample_daily_df):
        mock_stock_data.tiingo.get_daily_price_history = MagicMock(return_value=sample_daily_df)
        result = await api.get_daily_history("AAPL", "2026-01-01", "2026-01-31", force_live=True)
        mock_stock_data.price_history.fetch_daily_price_history.assert_not_called()
        mock_stock_data.tiingo.get_daily_price_history.assert_called_once()
        assert not result.empty

    async def test_db_only_raises_on_miss(self, api, mock_stock_data):
        mock_stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        with pytest.raises(DataNotAvailable):
            await api.get_daily_history("AAPL", "2026-01-01", "2026-01-31", db_only=True)

    async def test_db_only_does_not_call_live_sources(self, api, mock_stock_data):
        mock_stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        try:
            await api.get_daily_history("AAPL", "2026-01-01", "2026-01-31", db_only=True)
        except DataNotAvailable:
            pass
        mock_stock_data.tiingo.get_daily_price_history.assert_not_called()
        mock_stock_data.stooq.get_daily_price_history.assert_not_called()

    async def test_accepts_date_objects(self, api, mock_stock_data, sample_daily_df):
        mock_stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=sample_daily_df)
        start = datetime.date(2026, 1, 1)
        end = datetime.date(2026, 1, 31)
        result = await api.get_daily_history("AAPL", start, end)
        assert not result.empty


class TestSyncWrapper:
    def test_sync_get_ticker_info(self, api, mock_stock_data):
        result = api.sync.get_ticker_info("AAPL")
        assert result["ticker"] == "AAPL"

    def test_sync_get_quote(self, api, mock_stock_data):
        result = api.sync.get_quote("AAPL")
        assert "lastPrice" in result

    def test_sync_get_daily_history(self, api, mock_stock_data, sample_daily_df):
        result = api.sync.get_daily_history("AAPL", "2026-01-01", "2026-01-31")
        assert not result.empty

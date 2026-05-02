"""Tests for rocketstocks.api.batches.BatchAPI — Phase 3."""
import asyncio
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from rocketstocks.api.batches import (
    BatchAPI,
    FundamentalsSnapshot,
    OptionsSnapshot,
    PeerComparisonBatch,
    TickerBatch,
    _gather_fields,
)
from rocketstocks.api.client import DataAPI


# ---------------------------------------------------------------------------
# _gather_fields helper
# ---------------------------------------------------------------------------

class TestGatherFields:
    async def test_all_succeed_returns_empty_errors(self):
        async def _ok():
            return 42

        values, errors = await _gather_fields({"x": _ok(), "y": _ok()}, {"x": 0, "y": 0})
        assert values == {"x": 42, "y": 42}
        assert errors == {}

    async def test_one_failure_isolated(self):
        async def _ok():
            return "good"

        async def _bad():
            raise ValueError("boom")

        values, errors = await _gather_fields(
            {"good": _ok(), "bad": _bad()},
            {"good": None, "bad": "default"},
        )
        assert values["good"] == "good"
        assert values["bad"] == "default"
        assert "bad" in errors
        assert isinstance(errors["bad"], ValueError)

    async def test_all_fail_uses_all_defaults(self):
        async def _bad():
            raise RuntimeError("err")

        values, errors = await _gather_fields(
            {"a": _bad(), "b": _bad()},
            {"a": [], "b": {}},
        )
        assert values == {"a": [], "b": {}}
        assert len(errors) == 2


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def bundle_api(api):
    return api.batches


# ---------------------------------------------------------------------------
# TickerBatch (get_deep_dive)
# ---------------------------------------------------------------------------

class TestGetDeepDive:
    async def test_all_fields_populated_in_happy_path(self, api, mock_stock_data):
        bundle = await api.batches.get_deep_dive("AAPL")

        assert isinstance(bundle, TickerBatch)
        assert bundle.info is not None
        assert bundle.quote is not None
        assert bundle.errors == {}

    async def test_has_all_sixteen_non_error_fields(self, api, mock_stock_data):
        bundle = await api.batches.get_deep_dive("AAPL")
        field_names = [
            "info", "quote", "schwab_fundamentals", "yfinance_financials",
            "daily_history", "five_min_history", "options_chain", "iv_history",
            "eps_history", "next_earnings", "analyst_targets", "recommendations",
            "float_data", "insider_transactions", "popularity", "recent_filings",
        ]
        for name in field_names:
            assert hasattr(bundle, name), f"Missing field: {name}"

    async def test_isolates_yfinance_failure(self, api, mock_stock_data):
        mock_stock_data.yfinance.get_financials = MagicMock(side_effect=RuntimeError("yf down"))

        bundle = await api.batches.get_deep_dive("AAPL")

        assert "yfinance_financials" in bundle.errors
        assert isinstance(bundle.errors["yfinance_financials"], RuntimeError)
        assert bundle.yfinance_financials == {}

    async def test_other_fields_unaffected_by_single_failure(self, api, mock_stock_data):
        mock_stock_data.yfinance.get_financials = MagicMock(side_effect=RuntimeError("yf down"))

        bundle = await api.batches.get_deep_dive("AAPL")

        assert bundle.quote is not None
        assert bundle.info is not None
        assert bundle.options_chain is not None

    async def test_isolates_schwab_options_failure(self, api, mock_stock_data):
        mock_stock_data.schwab.get_options_chain = AsyncMock(side_effect=RuntimeError("rate limit"))

        bundle = await api.batches.get_deep_dive("AAPL")

        assert "options_chain" in bundle.errors
        assert bundle.options_chain == {}
        assert bundle.quote is not None

    async def test_five_min_history_is_none_on_failure(self, api, mock_stock_data):
        mock_stock_data.price_history.fetch_5m_price_history = AsyncMock(
            side_effect=RuntimeError("no intraday data")
        )

        bundle = await api.batches.get_deep_dive("AAPL")

        assert bundle.five_min_history is None
        assert "five_min_history" in bundle.errors

    async def test_history_days_passed_to_daily_history(self, api, mock_stock_data):
        mock_stock_data.price_history.fetch_daily_price_history = AsyncMock(
            return_value=pd.DataFrame()
        )
        mock_stock_data.tiingo.get_daily_price_history = MagicMock(return_value=pd.DataFrame())
        mock_stock_data.stooq.get_daily_price_history = MagicMock(return_value=pd.DataFrame())

        await api.batches.get_deep_dive("AAPL", history_days=30)

        call_args = mock_stock_data.price_history.fetch_daily_price_history.call_args
        start_date = call_args.kwargs.get("start_date") or call_args.args[1]
        today = datetime.date.today()
        delta = today - start_date
        assert 28 <= delta.days <= 32


# ---------------------------------------------------------------------------
# PeerComparisonBatch (get_peer_comparison)
# ---------------------------------------------------------------------------

class TestGetPeerComparison:
    async def test_includes_benchmarks_in_result(self, api, mock_stock_data):
        mock_stock_data.price_history.fetch_daily_price_history_batch = AsyncMock(
            return_value={"CRWV": pd.DataFrame(), "NVTS": pd.DataFrame(), "SPY": pd.DataFrame(), "XLK": pd.DataFrame()}
        )
        mock_stock_data.schwab.get_fundamentals = AsyncMock(return_value={})
        mock_stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        bundle = await api.batches.get_peer_comparison(
            ["CRWV", "NVTS"], benchmarks=["SPY", "XLK"]
        )

        assert isinstance(bundle, PeerComparisonBatch)
        assert set(bundle.daily_histories.keys()) == {"CRWV", "NVTS", "SPY", "XLK"}

    async def test_default_benchmark_is_spy(self, api, mock_stock_data):
        mock_stock_data.price_history.fetch_daily_price_history_batch = AsyncMock(
            return_value={"AAPL": pd.DataFrame(), "SPY": pd.DataFrame()}
        )
        mock_stock_data.schwab.get_fundamentals = AsyncMock(return_value={})
        mock_stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        bundle = await api.batches.get_peer_comparison(["AAPL"])

        call_args = mock_stock_data.price_history.fetch_daily_price_history_batch.call_args
        tickers_fetched = call_args.args[0] if call_args.args else call_args.kwargs["tickers"]
        assert "SPY" in tickers_fetched

    async def test_deduplicates_tickers_and_benchmarks(self, api, mock_stock_data):
        mock_stock_data.price_history.fetch_daily_price_history_batch = AsyncMock(return_value={})
        mock_stock_data.schwab.get_fundamentals = AsyncMock(return_value={})
        mock_stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        await api.batches.get_peer_comparison(["SPY", "AAPL"], benchmarks=["SPY"])

        call_args = mock_stock_data.price_history.fetch_daily_price_history_batch.call_args
        tickers_fetched = call_args.args[0] if call_args.args else call_args.kwargs["tickers"]
        assert tickers_fetched.count("SPY") == 1

    async def test_isolates_fundamentals_failure(self, api, mock_stock_data):
        mock_stock_data.price_history.fetch_daily_price_history_batch = AsyncMock(
            return_value={"AAPL": pd.DataFrame()}
        )
        mock_stock_data.schwab.get_fundamentals = AsyncMock(side_effect=RuntimeError("schwab err"))
        mock_stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        bundle = await api.batches.get_peer_comparison(["AAPL"])

        assert "schwab_fundamentals" in bundle.errors
        assert bundle.daily_histories is not None


# ---------------------------------------------------------------------------
# OptionsSnapshot (get_options_snapshot)
# ---------------------------------------------------------------------------

class TestGetOptionsSnapshot:
    async def test_shape_in_happy_path(self, api, mock_stock_data):
        bundle = await api.batches.get_options_snapshot("AAPL")

        assert isinstance(bundle, OptionsSnapshot)
        assert "callExpDateMap" in bundle.chain
        assert isinstance(bundle.iv_history, pd.DataFrame)
        assert bundle.errors == {}

    async def test_isolates_chain_failure(self, api, mock_stock_data):
        mock_stock_data.schwab.get_options_chain = AsyncMock(side_effect=RuntimeError("rate limit"))

        bundle = await api.batches.get_options_snapshot("AAPL")

        assert "chain" in bundle.errors
        assert bundle.chain == {}
        assert bundle.quote is not None

    async def test_ticker_stats_none_on_miss(self, api, mock_stock_data):
        mock_stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        bundle = await api.batches.get_options_snapshot("AAPL")

        assert bundle.ticker_stats is None
        assert bundle.errors == {}


# ---------------------------------------------------------------------------
# FundamentalsSnapshot (get_fundamentals_snapshot)
# ---------------------------------------------------------------------------

class TestGetFundamentalsSnapshot:
    async def test_shape_in_happy_path(self, api, mock_stock_data):
        bundle = await api.batches.get_fundamentals_snapshot("AAPL")

        assert isinstance(bundle, FundamentalsSnapshot)
        assert "income_statement" in bundle.financials
        assert isinstance(bundle.eps_history, pd.DataFrame)
        assert bundle.errors == {}

    async def test_isolates_yfinance_failure(self, api, mock_stock_data):
        mock_stock_data.yfinance.get_financials = MagicMock(side_effect=RuntimeError("yf down"))

        bundle = await api.batches.get_fundamentals_snapshot("AAPL")

        assert "financials" in bundle.errors
        assert bundle.financials == {}
        assert isinstance(bundle.eps_history, pd.DataFrame)

    async def test_all_six_fields_present(self, api, mock_stock_data):
        bundle = await api.batches.get_fundamentals_snapshot("AAPL")
        for name in ["financials", "eps_history", "analyst_targets", "recommendations",
                     "upgrades_downgrades", "insider_transactions"]:
            assert hasattr(bundle, name), f"Missing field: {name}"


# ---------------------------------------------------------------------------
# DataAPI.batches accessor
# ---------------------------------------------------------------------------

class TestBatchesAccessor:
    def test_batches_attr_is_batch_api(self, api):
        assert isinstance(api.batches, BatchAPI)

    def test_batches_attr_references_same_api(self, api):
        assert api.batches._api is api

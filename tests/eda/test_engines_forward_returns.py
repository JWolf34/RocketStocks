"""Tests for rocketstocks.eda.engines.forward_returns."""
import datetime
import math
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pandas as pd
import pytest

from rocketstocks.eda.engines.forward_returns import (
    run_forward_returns,
    HorizonResult,
    ForwardReturnResult,
    _compute_horizons,
    _accumulate_horizons,
    _horizons_from_accumulators,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _make_close_dict(tickers: list[str], n: int = 60, seed: int = 42) -> dict:
    rng = np.random.default_rng(seed)
    result = {}
    for ticker in tickers:
        dates = pd.date_range('2024-01-01', periods=n, freq='B')
        prices = 100 + rng.standard_normal(n).cumsum()
        prices = np.maximum(prices, 5.0)
        result[ticker] = pd.Series(prices, index=dates)
    return result


def _make_events(rows: list[dict], source_detail: str = 'mention_ratio>=3.0') -> pd.DataFrame:
    return pd.DataFrame([
        {
            'ticker': r['ticker'],
            'datetime': pd.Timestamp(r['datetime']),
            'signal_value': r.get('signal_value', 3.5),
            'source': 'sentiment',
            'source_detail': source_detail,
        }
        for r in rows
    ])


def _make_stock_data(close_dict: dict, timeframe: str = 'daily') -> MagicMock:
    """Build a mock StockData backed by a close_dict for testing."""
    sd = MagicMock()

    # fetch_bar_counts → db.execute returns [(ticker, count)] rows
    bar_rows = [(ticker, len(series)) for ticker, series in close_dict.items()]
    sd.db.execute = AsyncMock(return_value=bar_rows)

    # per-ticker price fetcher builds a minimal price DataFrame from the close series
    async def fetch_daily(ticker, start_date=None, end_date=None):
        if ticker not in close_dict:
            return pd.DataFrame()
        series = close_dict[ticker]
        dates = [ts.date() for ts in series.index]
        return pd.DataFrame({
            'ticker': ticker,
            'open': series.values,
            'high': series.values,
            'low': series.values,
            'close': series.values,
            'volume': 100_000,
            'date': dates,
        })

    async def fetch_5m(ticker, start_datetime=None, end_datetime=None):
        if ticker not in close_dict:
            return pd.DataFrame()
        series = close_dict[ticker]
        return pd.DataFrame({
            'ticker': ticker,
            'open': series.values,
            'high': series.values,
            'low': series.values,
            'close': series.values,
            'volume': 100_000,
            'datetime': series.index,
        })

    if timeframe == 'daily':
        sd.price_history.fetch_daily_price_history = AsyncMock(side_effect=fetch_daily)
        sd.price_history.fetch_5m_price_history = AsyncMock(return_value=pd.DataFrame())
    else:
        sd.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        sd.price_history.fetch_5m_price_history = AsyncMock(side_effect=fetch_5m)

    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())
    return sd


# ---------------------------------------------------------------------------
# Basic structure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_list_of_results():
    close_dict = _make_close_dict(['AAPL'])
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-10'}])
    sd = _make_stock_data(close_dict)
    results = await run_forward_returns(events, sd, timeframe='daily')
    assert isinstance(results, list)


@pytest.mark.asyncio
async def test_one_result_per_source_detail():
    close_dict = _make_close_dict(['AAPL'])
    events = pd.concat([
        _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-10'}], 'mention_ratio>=2.0'),
        _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-15'}], 'mention_ratio>=3.0'),
    ], ignore_index=True)
    sd = _make_stock_data(close_dict)
    results = await run_forward_returns(events, sd, timeframe='daily', stratify=False)
    assert len(results) == 2


@pytest.mark.asyncio
async def test_horizon_count_matches_daily_defaults():
    """Daily timeframe should have 6 horizon points."""
    close_dict = _make_close_dict(['AAPL'])
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-10'},
        {'ticker': 'AAPL', 'datetime': '2024-01-20'},
    ])
    sd = _make_stock_data(close_dict)
    results = await run_forward_returns(events, sd, timeframe='daily', stratify=False)
    assert len(results[0].horizons) == 6  # [1, 2, 3, 5, 10, 20]


@pytest.mark.asyncio
async def test_custom_horizons_used():
    close_dict = _make_close_dict(['AAPL'])
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-10'}])
    sd = _make_stock_data(close_dict)
    results = await run_forward_returns(events, sd, custom_horizons=[1, 5])
    assert len(results[0].horizons) == 2


@pytest.mark.asyncio
async def test_horizon_result_fields():
    close_dict = _make_close_dict(['AAPL'], n=50)
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-05'},
        {'ticker': 'AAPL', 'datetime': '2024-01-12'},
        {'ticker': 'AAPL', 'datetime': '2024-01-19'},
    ])
    sd = _make_stock_data(close_dict)
    results = await run_forward_returns(events, sd, timeframe='daily',
                                        custom_horizons=[1, 5], stratify=False)
    h = results[0].horizons[0]
    assert hasattr(h, 'mean_return')
    assert hasattr(h, 'win_rate')
    assert hasattr(h, 't_stat')
    assert hasattr(h, 'p_value')


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_events_returns_empty():
    sd = _make_stock_data({})
    results = await run_forward_returns(pd.DataFrame(), sd)
    assert results == []


@pytest.mark.asyncio
async def test_missing_ticker_in_price_data_handled():
    """Events for a ticker with no price data should be silently skipped."""
    close_dict = _make_close_dict(['GOOG'])
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-10'}])
    sd = _make_stock_data(close_dict)
    # AAPL not in close_dict; fetch_daily returns empty DataFrame
    results = await run_forward_returns(events, sd, stratify=False)
    assert isinstance(results, list)


@pytest.mark.asyncio
async def test_control_group_present():
    close_dict = _make_close_dict(['AAPL', 'GOOG'], n=50)
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-05'},
        {'ticker': 'AAPL', 'datetime': '2024-01-12'},
    ])
    sd = _make_stock_data(close_dict)
    results = await run_forward_returns(events, sd, n_control=20, stratify=False)
    # control is a list of HorizonResult; may have n_events=0 if no valid bars
    assert isinstance(results[0].control, list)


@pytest.mark.asyncio
async def test_forward_return_sign_with_rising_prices():
    """When price strictly rises, forward returns at all horizons should be positive."""
    dates = pd.date_range('2024-01-01', periods=50, freq='B')
    prices = pd.Series(np.linspace(100, 150, 50), index=dates)
    close_dict = {'AAPL': prices}

    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    sd = _make_stock_data(close_dict)
    results = await run_forward_returns(events, sd, custom_horizons=[1, 5, 10], stratify=False)

    for h in results[0].horizons:
        if h.n_events >= 1 and not math.isnan(h.mean_return):
            assert h.mean_return > 0, f"Expected positive return at horizon {h.horizon}"


# ---------------------------------------------------------------------------
# _compute_horizons (pure helper — unchanged; used by regime_analysis)
# ---------------------------------------------------------------------------

def test_compute_horizons_rising_prices():
    """Pure helper should still produce positive returns for rising prices."""
    dates = pd.date_range('2024-01-01', periods=30, freq='B')
    prices = pd.Series(np.linspace(100, 130, 30), index=dates)
    sorted_closes = {'AAPL': prices}

    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-03'}])
    results = _compute_horizons(events, sorted_closes, horizons=[1, 5], horizon_labels={})
    for h in results:
        if h.n_events >= 1 and not math.isnan(h.mean_return):
            assert h.mean_return > 0


def test_compute_horizons_missing_ticker():
    sorted_closes = {'GOOG': pd.Series([100.0, 101.0], index=pd.date_range('2024-01-01', periods=2))}
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-01'}])
    results = _compute_horizons(events, sorted_closes, horizons=[1], horizon_labels={})
    assert results[0].n_events == 0


# ---------------------------------------------------------------------------
# _accumulate_horizons
# ---------------------------------------------------------------------------

def test_accumulate_horizons_appends_returns():
    dates = pd.date_range('2024-01-01', periods=10, freq='B')
    prices = pd.Series(np.linspace(100, 110, 10), index=dates)
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-01'}])
    accumulators = {1: [], 2: []}
    _accumulate_horizons(events, prices, accumulators, horizons=[1, 2])
    assert len(accumulators[1]) == 1
    assert len(accumulators[2]) == 1
    assert accumulators[1][0] > 0  # rising prices


def test_accumulate_horizons_skips_missing_timestamp():
    dates = pd.date_range('2024-01-01', periods=5, freq='B')
    prices = pd.Series([100.0] * 5, index=dates)
    # Event date not in price index — loc will be past end
    events = _make_events([{'ticker': 'X', 'datetime': '2030-01-01'}])
    accumulators = {1: []}
    _accumulate_horizons(events, prices, accumulators, horizons=[1])
    assert accumulators[1] == []


# ---------------------------------------------------------------------------
# _horizons_from_accumulators
# ---------------------------------------------------------------------------

def test_horizons_from_accumulators_basic():
    accumulators = {1: [1.0, 2.0, 3.0], 5: [0.5, -0.5]}
    results = _horizons_from_accumulators(accumulators, horizons=[1, 5], horizon_labels={})
    assert len(results) == 2
    assert results[0].horizon == 1
    assert results[0].n_events == 3
    assert not math.isnan(results[0].mean_return)


def test_horizons_from_accumulators_too_few_events():
    accumulators = {1: [1.0]}  # n < 2
    results = _horizons_from_accumulators(accumulators, horizons=[1], horizon_labels={})
    assert results[0].n_events == 1
    assert math.isnan(results[0].mean_return)
    assert not results[0].significant

"""Tests for rocketstocks.eda.engines.forward_returns."""
import math

import numpy as np
import pandas as pd
import pytest

from rocketstocks.eda.engines.forward_returns import (
    run_forward_returns,
    HorizonResult,
    ForwardReturnResult,
)


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


# ---------------------------------------------------------------------------
# Basic structure
# ---------------------------------------------------------------------------

def test_returns_list_of_results():
    close_dict = _make_close_dict(['AAPL'])
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-10'}])
    results = run_forward_returns(events, close_dict, timeframe='daily')
    assert isinstance(results, list)


def test_one_result_per_source_detail():
    close_dict = _make_close_dict(['AAPL'])
    events = pd.concat([
        _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-10'}], 'mention_ratio>=2.0'),
        _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-15'}], 'mention_ratio>=3.0'),
    ], ignore_index=True)
    results = run_forward_returns(events, close_dict, timeframe='daily', stratify=False)
    assert len(results) == 2


def test_horizon_count_matches_daily_defaults():
    """Daily timeframe should have 6 horizon points."""
    close_dict = _make_close_dict(['AAPL'])
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-10'},
        {'ticker': 'AAPL', 'datetime': '2024-01-20'},
    ])
    results = run_forward_returns(events, close_dict, timeframe='daily', stratify=False)
    assert len(results[0].horizons) == 6  # [1, 2, 3, 5, 10, 20]


def test_custom_horizons_used():
    close_dict = _make_close_dict(['AAPL'])
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-10'}])
    results = run_forward_returns(events, close_dict, custom_horizons=[1, 5])
    assert len(results[0].horizons) == 2


def test_horizon_result_fields():
    close_dict = _make_close_dict(['AAPL'], n=50)
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-05'},
        {'ticker': 'AAPL', 'datetime': '2024-01-12'},
        {'ticker': 'AAPL', 'datetime': '2024-01-19'},
    ])
    results = run_forward_returns(events, close_dict, timeframe='daily',
                                  custom_horizons=[1, 5], stratify=False)
    h = results[0].horizons[0]
    assert hasattr(h, 'mean_return')
    assert hasattr(h, 'win_rate')
    assert hasattr(h, 't_stat')
    assert hasattr(h, 'p_value')


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_empty_events_returns_empty():
    close_dict = _make_close_dict(['AAPL'])
    results = run_forward_returns(pd.DataFrame(), close_dict)
    assert results == []


def test_empty_close_dict_returns_empty():
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-10'}])
    results = run_forward_returns(events, {})
    assert results == []


def test_missing_ticker_in_close_dict_handled():
    """Events for a ticker with no price data should be silently skipped."""
    close_dict = _make_close_dict(['GOOG'])
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-10'}])
    results = run_forward_returns(events, close_dict, stratify=False)
    # Should return results with n_events_total set but horizons may show n<2
    assert isinstance(results, list)


def test_control_group_present():
    close_dict = _make_close_dict(['AAPL', 'GOOG'], n=50)
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-05'},
        {'ticker': 'AAPL', 'datetime': '2024-01-12'},
    ])
    results = run_forward_returns(events, close_dict, n_control=20, stratify=False)
    assert results[0].control


def test_forward_return_sign_with_rising_prices():
    """When price strictly rises, forward returns at all horizons should be positive."""
    dates = pd.date_range('2024-01-01', periods=50, freq='B')
    prices = pd.Series(np.linspace(100, 150, 50), index=dates)
    close_dict = {'AAPL': prices}

    # Event at bar 5 (price = ~105)
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    results = run_forward_returns(events, close_dict, custom_horizons=[1, 5, 10], stratify=False)

    for h in results[0].horizons:
        if h.n_events >= 1 and not math.isnan(h.mean_return):
            assert h.mean_return > 0, f"Expected positive return at horizon {h.horizon}"

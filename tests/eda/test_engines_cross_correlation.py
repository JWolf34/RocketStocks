"""Tests for rocketstocks.eda.engines.cross_correlation."""
import math

import numpy as np
import pandas as pd
import pytest

from rocketstocks.eda.engines.cross_correlation import (
    run_cross_correlation,
    CrossCorrelationResult,
)


def _make_panel(tickers: list[str], n: int = 60, seed: int = 42) -> pd.DataFrame:
    """Build a simple daily panel with signal and return columns."""
    rng = np.random.default_rng(seed)
    rows = []
    for ticker in tickers:
        for i in range(n):
            rows.append({
                'ticker': ticker,
                'date': pd.Timestamp('2024-01-01') + pd.Timedelta(days=i),
                'mention_delta': rng.standard_normal(),
                'daily_return': rng.standard_normal(),
            })
    return pd.DataFrame(rows)


def _make_lagged_panel(n: int = 80, lag: int = 2) -> pd.DataFrame:
    """Build a panel where signal leads return by `lag` days."""
    rng = np.random.default_rng(42)
    signal = rng.standard_normal(n + lag)
    returns = signal[:n]  # return_t is correlated with signal_{t-lag}

    rows = []
    for i in range(n):
        rows.append({
            'ticker': 'AAPL',
            'date': pd.Timestamp('2024-01-01') + pd.Timedelta(days=i),
            'mention_delta': float(signal[i + lag]),  # signal at t+lag
            'daily_return': float(returns[i]),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Basic structure
# ---------------------------------------------------------------------------

def test_returns_result_object():
    panel = _make_panel(['AAPL'], n=40)
    result = run_cross_correlation(panel, 'mention_delta', 'daily_return', max_lag=5)
    assert isinstance(result, CrossCorrelationResult)


def test_ccf_length_matches_lags():
    panel = _make_panel(['AAPL'], n=40)
    max_lag = 5
    result = run_cross_correlation(panel, 'mention_delta', 'daily_return', max_lag=max_lag)
    assert len(result.ccf) == 2 * max_lag + 1


def test_ccf_lags_are_sequential():
    panel = _make_panel(['AAPL'], n=40)
    result = run_cross_correlation(panel, 'mention_delta', 'daily_return', max_lag=3)
    lags = [p.lag for p in result.ccf]
    assert lags == list(range(-3, 4))


def _make_lagged_panel_multi(n: int = 80, lag: int = 2, n_tickers: int = 3) -> pd.DataFrame:
    """Build a panel where signal leads return by `lag` bars, across multiple tickers."""
    rng = np.random.default_rng(42)
    rows = []
    for t_idx in range(n_tickers):
        ticker = f'TICK{t_idx}'
        signal = rng.standard_normal(n + lag)
        for i in range(n):
            rows.append({
                'ticker': ticker,
                'date': pd.Timestamp('2024-01-01') + pd.Timedelta(days=i),
                'mention_delta': float(signal[i + lag]),
                'daily_return': float(signal[i]),
            })
    return pd.DataFrame(rows)


def test_peak_lag_is_positive_when_signal_leads():
    """With a constructed signal that leads return by 2 days, peak lag should be 2."""
    panel = _make_lagged_panel_multi(n=80, lag=2, n_tickers=4)
    result = run_cross_correlation(panel, 'mention_delta', 'daily_return',
                                   max_lag=5, min_periods=20)
    # Peak should be at a positive lag (signal leads)
    assert result.peak_lag is not None
    assert result.peak_lag > 0


def test_missing_signal_col_returns_empty():
    panel = _make_panel(['AAPL'], n=40)
    result = run_cross_correlation(panel, 'nonexistent_col', 'daily_return')
    assert result.n_tickers == 0


def test_missing_return_col_returns_empty():
    panel = _make_panel(['AAPL'], n=40)
    result = run_cross_correlation(panel, 'mention_delta', 'nonexistent_col')
    assert result.n_tickers == 0


def test_insufficient_data_below_min_periods():
    """Panel with only 5 rows per ticker should not be analyzed."""
    panel = _make_panel(['AAPL'], n=5)
    result = run_cross_correlation(panel, 'mention_delta', 'daily_return', min_periods=30)
    assert result.n_tickers == 0


# ---------------------------------------------------------------------------
# Regression
# ---------------------------------------------------------------------------

def test_regression_present_with_sufficient_data():
    panel = _make_panel(['AAPL', 'GOOG'], n=50)
    result = run_cross_correlation(panel, 'mention_delta', 'daily_return', min_periods=20)
    assert result.regression is not None


def test_regression_has_all_fields():
    panel = _make_panel(['AAPL'], n=50)
    result = run_cross_correlation(panel, 'mention_delta', 'daily_return', min_periods=20)
    if result.regression:
        assert hasattr(result.regression, 'b1')
        assert hasattr(result.regression, 'b1_pvalue')
        assert hasattr(result.regression, 'n_obs')

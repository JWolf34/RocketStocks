"""Tests for rocketstocks.eda.engines.regime_analysis."""
import datetime

import numpy as np
import pandas as pd
import pytest

from rocketstocks.eda.engines.regime_analysis import (
    run_regime_analysis,
    RegimeAnalysisResult,
)


def _make_events(n: int = 30, source_detail: str = 'mention_ratio>=3.0') -> pd.DataFrame:
    rng = np.random.default_rng(42)
    rows = []
    for i in range(n):
        rows.append({
            'ticker': rng.choice(['AAPL', 'GOOG', 'TSLA']),
            'datetime': pd.Timestamp('2024-01-01') + pd.Timedelta(days=i * 3),
            'signal_value': float(rng.uniform(2, 6)),
            'source': 'sentiment',
            'source_detail': source_detail,
        })
    return pd.DataFrame(rows)


def _make_close_dict(tickers: list[str], n: int = 120, seed: int = 42) -> dict:
    rng = np.random.default_rng(seed)
    result = {}
    for ticker in tickers:
        dates = pd.date_range('2024-01-01', periods=n, freq='B')
        prices = 100 + rng.standard_normal(n).cumsum()
        prices = np.maximum(prices, 5.0)
        result[ticker] = pd.Series(prices, index=dates)
    return result


def _make_spy_df(n: int = 300) -> pd.DataFrame:
    """Build a SPY-like daily price DataFrame in DB format."""
    rng = np.random.default_rng(0)
    dates = pd.date_range('2023-01-01', periods=n, freq='B')
    close = 450 + rng.standard_normal(n).cumsum()
    close = np.maximum(close, 100.0)
    return pd.DataFrame({
        'ticker': 'SPY',
        'open': close - 1,
        'high': close + 2,
        'low': close - 2,
        'close': close,
        'volume': 50_000_000,
        'date': dates.date,
    })


# ---------------------------------------------------------------------------
# Basic structure
# ---------------------------------------------------------------------------

def test_returns_list():
    events = _make_events(n=20)
    close_dict = _make_close_dict(['AAPL', 'GOOG', 'TSLA'])
    spy_df = _make_spy_df()
    results = run_regime_analysis(events, close_dict, spy_df, timeframe='daily')
    assert isinstance(results, list)
    assert len(results) >= 1


def test_one_result_per_source_detail():
    events = pd.concat([
        _make_events(n=10, source_detail='mention_ratio>=2.0'),
        _make_events(n=10, source_detail='mention_ratio>=3.0'),
    ], ignore_index=True)
    close_dict = _make_close_dict(['AAPL', 'GOOG', 'TSLA'])
    spy_df = _make_spy_df()
    results = run_regime_analysis(events, close_dict, spy_df)
    assert len(results) == 2


def test_regime_slices_cover_all_regimes():
    """Result should have a slice for each MarketRegime value."""
    from rocketstocks.backtest.regime import MarketRegime
    events = _make_events(n=20)
    close_dict = _make_close_dict(['AAPL', 'GOOG', 'TSLA'])
    spy_df = _make_spy_df()
    results = run_regime_analysis(events, close_dict, spy_df)
    regime_names = {s.regime for s in results[0].regime_slices}
    for r in MarketRegime:
        assert r.value in regime_names


def test_empty_events_returns_empty():
    close_dict = _make_close_dict(['AAPL'])
    spy_df = _make_spy_df()
    results = run_regime_analysis(pd.DataFrame(), close_dict, spy_df)
    assert results == []


def test_empty_close_dict_returns_empty():
    events = _make_events(n=10)
    spy_df = _make_spy_df()
    results = run_regime_analysis(events, {}, spy_df)
    assert results == []


def test_missing_spy_data_does_not_crash():
    """Without SPY data, all events should be tagged as 'unknown' regime."""
    events = _make_events(n=15)
    close_dict = _make_close_dict(['AAPL', 'GOOG', 'TSLA'])
    results = run_regime_analysis(events, close_dict, pd.DataFrame())
    assert isinstance(results, list)
    # With no SPY data, all events land in the 'unknown' regime slice
    if results:
        unknown_slice = next(s for s in results[0].regime_slices if s.regime == 'unknown')
        assert unknown_slice.n_events == results[0].n_events_total


def test_breadth_fields_present():
    events = _make_events(n=20)
    close_dict = _make_close_dict(['AAPL', 'GOOG', 'TSLA'])
    spy_df = _make_spy_df()
    results = run_regime_analysis(events, close_dict, spy_df)
    r = results[0]
    assert hasattr(r, 'breadth_corr')
    assert hasattr(r, 'breadth_pvalue')
    assert hasattr(r, 'breadth_n')

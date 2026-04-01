"""Tests for rocketstocks.backtest.signal_decay."""
import datetime
import math

import numpy as np
import pandas as pd
import pytest

from rocketstocks.backtest.signal_decay import DecayPoint, compute_signal_decay, find_peak_horizon


def _make_price_df(ticker: str, n: int = 60, start: str = '2024-01-01',
                   seed: int = 0) -> pd.DataFrame:
    """Build a daily OHLCV DataFrame in backtesting.py format (DatetimeIndex, capitalized cols)."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range(start, periods=n, freq='B')
    close = 100 + rng.standard_normal(n).cumsum()
    close = np.maximum(close, 5.0)
    return pd.DataFrame({
        'Open': close - 0.5,
        'High': close + 1.0,
        'Low': close - 1.0,
        'Close': close,
        'Volume': 1_000_000,
    }, index=dates)


def _make_trade(ticker: str, entry_date: str, return_pct: float = 1.0) -> dict:
    return {
        'ticker': ticker,
        'entry_time': datetime.datetime.fromisoformat(entry_date),
        'return_pct': return_pct,
    }


# ---------------------------------------------------------------------------
# compute_signal_decay — basic structure
# ---------------------------------------------------------------------------

def test_compute_signal_decay_returns_list():
    df = _make_price_df('AAPL')
    trades = [_make_trade('AAPL', '2024-01-10')]
    result = compute_signal_decay(trades, price_data={'AAPL': df}, horizons=[1, 5])
    assert isinstance(result, list)


def test_compute_signal_decay_one_point_per_horizon():
    df = _make_price_df('AAPL')
    trades = [_make_trade('AAPL', '2024-01-10')]
    horizons = [1, 3, 5]
    result = compute_signal_decay(trades, price_data={'AAPL': df}, horizons=horizons)
    assert len(result) == len(horizons)


def test_compute_signal_decay_horizons_sorted():
    df = _make_price_df('AAPL')
    trades = [_make_trade('AAPL', '2024-01-10')]
    result = compute_signal_decay(trades, {'AAPL': df}, horizons=[10, 1, 5])
    assert [p.horizon for p in result] == [1, 5, 10]


def test_compute_signal_decay_returns_decay_points():
    df = _make_price_df('AAPL')
    trades = [_make_trade('AAPL', '2024-01-10')]
    result = compute_signal_decay(trades, {'AAPL': df}, horizons=[1])
    assert isinstance(result[0], DecayPoint)


# ---------------------------------------------------------------------------
# compute_signal_decay — insufficient data produces NaN points
# ---------------------------------------------------------------------------

def test_compute_signal_decay_single_trade_nan_stats():
    """A single trade yields n<2, so all stats should be NaN."""
    df = _make_price_df('AAPL', n=60)
    trades = [_make_trade('AAPL', '2024-01-10')]
    result = compute_signal_decay(trades, {'AAPL': df}, horizons=[5])
    pt = result[0]
    assert math.isnan(pt.mean_return)
    assert math.isnan(pt.t_stat)
    assert pt.significant is False


def test_compute_signal_decay_empty_trades_all_nan():
    df = _make_price_df('AAPL')
    result = compute_signal_decay([], {'AAPL': df}, horizons=[1, 5])
    assert all(math.isnan(p.mean_return) for p in result)


def test_compute_signal_decay_missing_ticker_skipped():
    """Trades for an unknown ticker should be skipped gracefully."""
    df = _make_price_df('AAPL')
    trades = [_make_trade('UNKNOWN', '2024-01-10')]
    result = compute_signal_decay(trades, {'AAPL': df}, horizons=[1])
    assert result[0].n_signals == 0


# ---------------------------------------------------------------------------
# compute_signal_decay — statistics with sufficient data
# ---------------------------------------------------------------------------

def test_compute_signal_decay_computes_win_rate():
    """With enough trades we should get a numeric win_rate."""
    n_trades = 10
    df = _make_price_df('AAPL', n=120)
    dates = pd.bdate_range('2024-01-10', periods=n_trades, freq='5B')
    trades = [_make_trade('AAPL', str(d.date())) for d in dates]
    result = compute_signal_decay(trades, {'AAPL': df}, horizons=[1])
    pt = result[0]
    if pt.n_signals >= 2:
        assert 0.0 <= pt.win_rate <= 100.0


def test_compute_signal_decay_n_signals_at_most_n_trades():
    n_trades = 5
    df = _make_price_df('AAPL', n=80)
    dates = pd.bdate_range('2024-01-10', periods=n_trades, freq='3B')
    trades = [_make_trade('AAPL', str(d.date())) for d in dates]
    result = compute_signal_decay(trades, {'AAPL': df}, horizons=[2])
    assert result[0].n_signals <= n_trades


def test_compute_signal_decay_default_horizons():
    """Calling without explicit horizons uses the module default."""
    df = _make_price_df('AAPL', n=120)
    dates = pd.bdate_range('2024-01-10', periods=8, freq='3B')
    trades = [_make_trade('AAPL', str(d.date())) for d in dates]
    result = compute_signal_decay(trades, {'AAPL': df})
    # Default is [1, 2, 3, 5, 10, 20]
    assert len(result) == 6


# ---------------------------------------------------------------------------
# find_peak_horizon
# ---------------------------------------------------------------------------

def test_find_peak_horizon_empty_returns_none():
    assert find_peak_horizon([]) is None


def test_find_peak_horizon_all_nan_returns_none():
    pts = [
        DecayPoint(h, float('nan'), float('nan'), float('nan'),
                   float('nan'), 0, float('nan'), float('nan'), False)
        for h in [1, 5, 10]
    ]
    assert find_peak_horizon(pts) is None


def _make_point(horizon: int, mean: float, significant: bool) -> DecayPoint:
    return DecayPoint(
        horizon=horizon, mean_return=mean, median_return=mean,
        std_return=1.0, win_rate=60.0, n_signals=10,
        t_stat=2.5 if significant else 1.0,
        p_value=0.01 if significant else 0.3,
        significant=significant,
    )


def test_find_peak_horizon_prefers_significant_points():
    """Should pick the significant horizon with the highest mean, ignoring non-significant."""
    pts = [
        _make_point(1, mean=5.0, significant=False),
        _make_point(5, mean=3.0, significant=True),   # significant, lower mean
        _make_point(10, mean=2.0, significant=True),
    ]
    # Among significant: horizon 5 has highest mean
    assert find_peak_horizon(pts) == 5


def test_find_peak_horizon_fallback_to_all_when_none_significant():
    pts = [
        _make_point(1, mean=2.0, significant=False),
        _make_point(5, mean=7.0, significant=False),  # highest overall
        _make_point(10, mean=4.0, significant=False),
    ]
    assert find_peak_horizon(pts) == 5


def test_find_peak_horizon_single_significant_point():
    pts = [_make_point(3, mean=4.0, significant=True)]
    assert find_peak_horizon(pts) == 3

"""Tests for AlertSignalStrategy."""
import datetime

import numpy as np
import pandas as pd
import pytest
from backtesting import Backtest

from rocketstocks.backtest.registry import get_strategy
from rocketstocks.backtest.strategies.alert_signal import AlertSignalStrategy


def _make_ohlcv(n: int = 120, seed: int = 42, spike_at: int | None = None) -> pd.DataFrame:
    """Return a capitalized OHLCV DataFrame for backtesting.py."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    close = 100.0 + rng.standard_normal(n).cumsum()

    if spike_at is not None and 0 < spike_at < n:
        close[spike_at] = close[spike_at - 1] * 1.15  # 15% spike

    df = pd.DataFrame({
        'Open': close - 0.5,
        'High': close + 1.0,
        'Low': close - 1.0,
        'Close': close,
        'Volume': rng.integers(1_000_000, 5_000_000, n).astype(float),
    }, index=dates)
    df.index.name = 'Date'
    return df


# ---------------------------------------------------------------------------
# Strategy is registered
# ---------------------------------------------------------------------------

def test_alert_signal_is_registered():
    assert get_strategy('alert_signal') is AlertSignalStrategy


# ---------------------------------------------------------------------------
# Strategy runs without error on realistic data
# ---------------------------------------------------------------------------

def test_alert_signal_runs_without_error():
    df = _make_ohlcv(n=120)
    bt = Backtest(df, AlertSignalStrategy, cash=10_000, commission=0.002,
                  exclusive_orders=True)
    stats = bt.run()
    assert stats is not None
    assert '# Trades' in stats.index


# ---------------------------------------------------------------------------
# Strategy needs 61 bars warmup before trading
# ---------------------------------------------------------------------------

def test_alert_signal_no_trades_before_warmup():
    df = _make_ohlcv(n=60)  # exactly 60 bars — strategy needs 61
    bt = Backtest(df, AlertSignalStrategy, cash=10_000, commission=0.002,
                  exclusive_orders=True)
    stats = bt.run()
    assert stats['# Trades'] == 0


# ---------------------------------------------------------------------------
# Hold bars parameter respected
# ---------------------------------------------------------------------------

def test_alert_signal_hold_bars_limits_exposure():
    df = _make_ohlcv(n=200, spike_at=70)
    bt = Backtest(df, AlertSignalStrategy, cash=10_000, commission=0.002,
                  exclusive_orders=True)
    stats = bt.run(hold_bars=1)
    # With hold_bars=1, exposure time should be low
    if stats['# Trades'] > 0:
        assert stats['Exposure Time [%]'] < 50.0


# ---------------------------------------------------------------------------
# Flat data should produce zero or few trades
# ---------------------------------------------------------------------------

def test_alert_signal_few_trades_on_flat_data():
    n = 200
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    df = pd.DataFrame({
        'Open': [100.0] * n,
        'High': [100.5] * n,
        'Low': [99.5] * n,
        'Close': [100.0] * n,
        'Volume': [1_000_000.0] * n,
    }, index=dates)
    df.index.name = 'Date'
    bt = Backtest(df, AlertSignalStrategy, cash=10_000, commission=0.002,
                  exclusive_orders=True)
    stats = bt.run()
    assert stats['# Trades'] == 0

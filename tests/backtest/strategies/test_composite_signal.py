"""Tests for CompositeSignalStrategy."""
import numpy as np
import pandas as pd
import pytest
from backtesting import Backtest

from rocketstocks.backtest.registry import get_strategy
from rocketstocks.backtest.strategies.composite_signal import CompositeSignalStrategy


def _make_ohlcv(n: int = 120, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    close = 100.0 + rng.standard_normal(n).cumsum()
    df = pd.DataFrame({
        'Open': close - 0.5,
        'High': close + 1.0,
        'Low': close - 1.0,
        'Close': close,
        'Volume': rng.integers(1_000_000, 5_000_000, n).astype(float),
    }, index=dates)
    df.index.name = 'Date'
    return df


def test_composite_signal_is_registered():
    assert get_strategy('composite_signal') is CompositeSignalStrategy


def test_composite_signal_runs_without_error():
    df = _make_ohlcv(n=120)
    bt = Backtest(df, CompositeSignalStrategy, cash=10_000, commission=0.002,
                  exclusive_orders=True)
    stats = bt.run()
    assert '# Trades' in stats.index


def test_composite_signal_no_trades_before_warmup():
    df = _make_ohlcv(n=60)
    bt = Backtest(df, CompositeSignalStrategy, cash=10_000, commission=0.002,
                  exclusive_orders=True)
    stats = bt.run()
    assert stats['# Trades'] == 0


def test_composite_signal_higher_threshold_fewer_trades():
    df = _make_ohlcv(n=200)
    bt = Backtest(df, CompositeSignalStrategy, cash=10_000, commission=0.002,
                  exclusive_orders=True)
    stats_low = bt.run(composite_threshold=1.5)
    stats_high = bt.run(composite_threshold=5.0)
    # Higher threshold → fewer or equal trades
    assert stats_high['# Trades'] <= stats_low['# Trades']

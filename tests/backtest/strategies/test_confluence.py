"""Tests for ConfluenceStrategy."""
import numpy as np
import pandas as pd
import pytest
from backtesting import Backtest

from rocketstocks.backtest.registry import get_strategy
from rocketstocks.backtest.strategies.confluence import ConfluenceStrategy


def _make_ohlcv(n: int = 200, seed: int = 99) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    close = 100.0 + rng.standard_normal(n).cumsum()
    df = pd.DataFrame({
        'Open': close - 0.5,
        'High': close + 1.0,
        'Low': close - 1.0,
        'Close': close,
        'Volume': rng.integers(500_000, 3_000_000, n).astype(float),
    }, index=dates)
    df.index.name = 'Date'
    return df


def test_confluence_is_registered():
    assert get_strategy('confluence') is ConfluenceStrategy


def test_confluence_runs_without_error():
    df = _make_ohlcv()
    bt = Backtest(df, ConfluenceStrategy, cash=10_000, commission=0.002,
                  exclusive_orders=True)
    stats = bt.run()
    assert '# Trades' in stats.index


def test_confluence_min_signals_4_fewer_trades_than_1():
    df = _make_ohlcv()
    bt = Backtest(df, ConfluenceStrategy, cash=10_000, commission=0.002,
                  exclusive_orders=True)
    stats_strict = bt.run(min_signals=4)
    stats_loose = bt.run(min_signals=1)
    assert stats_strict['# Trades'] <= stats_loose['# Trades']


def test_confluence_hold_bars_used():
    df = _make_ohlcv()
    bt = Backtest(df, ConfluenceStrategy, cash=10_000, commission=0.002,
                  exclusive_orders=True)
    stats_short = bt.run(hold_bars=1, min_signals=2)
    stats_long = bt.run(hold_bars=20, min_signals=2)
    # Longer hold period → more or equal exposure time
    if stats_short['# Trades'] > 0 and stats_long['# Trades'] > 0:
        assert stats_long['Exposure Time [%]'] >= stats_short['Exposure Time [%]']

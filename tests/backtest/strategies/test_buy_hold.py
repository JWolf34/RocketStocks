"""Tests for BuyHoldStrategy."""
import numpy as np
import pandas as pd
import pytest
from backtesting import Backtest

from rocketstocks.backtest.strategies.buy_hold import BuyHoldStrategy


def _make_price_df(n: int = 50, start_price: float = 100.0) -> pd.DataFrame:
    idx = pd.date_range('2025-01-02', periods=n, freq='B')
    prices = [start_price + i * 0.5 for i in range(n)]
    return pd.DataFrame({
        'Open': prices,
        'High': [p + 1 for p in prices],
        'Low': [p - 1 for p in prices],
        'Close': prices,
        'Volume': [1_000_000] * n,
    }, index=idx)


def test_buy_hold_buys_on_first_bar():
    df = _make_price_df(50)
    bt = Backtest(df, BuyHoldStrategy, cash=10_000, commission=0.0, finalize_trades=True)
    stats = bt.run()
    # Should have exactly 1 trade (buy on first bar, auto-closed at end)
    assert stats['# Trades'] == 1


def test_buy_hold_never_sells():
    df = _make_price_df(50)
    bt = Backtest(df, BuyHoldStrategy, cash=10_000, commission=0.0, finalize_trades=True)
    stats = bt.run()
    # Exposure should be nearly 100% (holds entire period)
    assert stats['Exposure Time [%]'] > 90


def test_buy_hold_rising_market_positive_return():
    df = _make_price_df(50, start_price=100.0)
    bt = Backtest(df, BuyHoldStrategy, cash=10_000, commission=0.0, finalize_trades=True)
    stats = bt.run()
    assert stats['Return [%]'] > 0

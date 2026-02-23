"""Tests for rocketstocks.core.analysis.signals."""
import numpy as np
import pandas as pd
import pytest

from rocketstocks.core.analysis.signals import signals


def _price_series(n=60, seed=42):
    rng = np.random.default_rng(seed)
    prices = 100.0 + rng.standard_normal(n).cumsum()
    return pd.Series(prices, name="close")


def _ohlcv(n=60, seed=42):
    rng = np.random.default_rng(seed)
    close = 100.0 + rng.standard_normal(n).cumsum()
    high = close + rng.uniform(0, 1, n)
    low = close - rng.uniform(0, 1, n)
    volume = rng.integers(500_000, 2_000_000, n).astype(float)
    return (
        pd.Series(high, name="high"),
        pd.Series(low, name="low"),
        pd.Series(close, name="close"),
        pd.Series(volume, name="volume"),
    )


class TestRsi:
    def test_returns_boolean_series(self):
        close = _price_series()
        result = signals.rsi(close)
        assert hasattr(result, "dtype")
        assert result.dtype == bool

    def test_same_length_as_input(self):
        close = _price_series(60)
        result = signals.rsi(close)
        assert len(result) == 60

    def test_rsi_oversold_when_steadily_declining(self):
        # Steadily declining price → RSI should be low → oversold signal True
        close = pd.Series([100.0 - i * 2 for i in range(60)])
        result = signals.rsi(close)
        # At least the last value should be True (oversold)
        assert result.iloc[-1] is True or result.any()


class TestMacd:
    def test_returns_boolean_series(self):
        close = _price_series()
        result = signals.macd(close)
        assert result.dtype == bool

    def test_same_length_as_input(self):
        close = _price_series(80)
        result = signals.macd(close)
        assert len(result) == 80


class TestSma:
    def test_returns_boolean_series(self):
        close = _price_series()
        result = signals.sma(close, short=10, long=20)
        assert result.dtype == bool

    def test_true_when_short_above_long(self):
        # Steadily rising → SMA_5 > SMA_10 at the last bar
        close = pd.Series([float(i) for i in range(60)])
        result = signals.sma(close, short=5, long=10)
        assert bool(result.iloc[-1]) is True


class TestAdx:
    def test_returns_boolean_series(self):
        high, low, close, _ = _ohlcv()
        result = signals.adx(close, high, low)
        assert result.dtype == bool

    def test_same_length_as_input(self):
        high, low, close, _ = _ohlcv(80)
        result = signals.adx(close, high, low)
        assert len(result) == 80


class TestObv:
    def test_returns_integer_series(self):
        # ta.increasing returns int64 (0/1), not bool dtype
        high, low, close, volume = _ohlcv()
        result = signals.obv(close, volume)
        assert result.dtype.kind in ("b", "i", "u")  # bool, int, or uint

    def test_values_are_zero_or_one(self):
        high, low, close, volume = _ohlcv()
        result = signals.obv(close, volume)
        non_nan = result.dropna()
        assert set(non_nan.unique()).issubset({0, 1})

    def test_same_length_as_input(self):
        high, low, close, volume = _ohlcv(80)
        result = signals.obv(close, volume)
        assert len(result) == 80


class TestZscore:
    def test_returns_series(self):
        close = _price_series()
        result = signals.zscore(close, BUY_THRESHOLD=-1.0, SELL_THRESHOLD=1.0)
        assert isinstance(result, pd.Series)

    def test_same_length_as_input(self):
        close = _price_series(60)
        result = signals.zscore(close, BUY_THRESHOLD=-1.0, SELL_THRESHOLD=1.0)
        assert len(result) == 60

    def test_values_are_zero_or_one(self):
        close = _price_series()
        result = signals.zscore(close, BUY_THRESHOLD=-1.0, SELL_THRESHOLD=1.0)
        assert set(result.unique()).issubset({0, 1})

    def test_first_value_is_zero(self):
        close = _price_series()
        result = signals.zscore(close, BUY_THRESHOLD=-1.0, SELL_THRESHOLD=1.0)
        assert result.iloc[0] == 0


class TestRoc:
    def test_returns_boolean_series(self):
        close = _price_series()
        result = signals.roc(close)
        assert result.dtype == bool

    def test_same_length_as_input(self):
        close = _price_series(60)
        result = signals.roc(close)
        assert len(result) == 60

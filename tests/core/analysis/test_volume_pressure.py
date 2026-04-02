"""Tests for rocketstocks.core.analysis.volume_pressure."""
import math

import numpy as np
import pandas as pd
import pytest

from rocketstocks.core.analysis.volume_pressure import compute_mfi, mfi_signal


def _make_series(values, name=None):
    return pd.Series(values, dtype=float, name=name)


def _ohlcv(n=50, up=True):
    """Generate synthetic OHLCV data.

    When up=True: typical price rises on high volume (buying pressure).
    When up=False: typical price falls on high volume (selling pressure).
    """
    rng = np.random.default_rng(42)
    if up:
        close = pd.Series(100.0 + np.arange(n) * 0.5 + rng.normal(0, 0.1, n))
        high = close + rng.uniform(0.1, 0.5, n)
        low = close - rng.uniform(0.1, 0.3, n)
        volume = pd.Series(np.ones(n) * 1_000_000 + rng.uniform(0, 100_000, n))
    else:
        close = pd.Series(100.0 - np.arange(n) * 0.5 + rng.normal(0, 0.1, n))
        low = close - rng.uniform(0.1, 0.5, n)
        high = close + rng.uniform(0.1, 0.3, n)
        volume = pd.Series(np.ones(n) * 1_000_000 + rng.uniform(0, 100_000, n))
    return high, low, close, volume


class TestComputeMfi:

    def test_returns_series(self):
        high, low, close, volume = _ohlcv(50)
        result = compute_mfi(high, low, close, volume)
        assert isinstance(result, pd.Series)

    def test_length_matches_input(self):
        high, low, close, volume = _ohlcv(50)
        result = compute_mfi(high, low, close, volume, period=14)
        assert len(result) == len(close)

    def test_values_within_0_100(self):
        high, low, close, volume = _ohlcv(100)
        result = compute_mfi(high, low, close, volume, period=14)
        valid = result.dropna()
        assert (valid >= -1e-9).all(), "MFI values must be >= 0"
        assert (valid <= 100 + 1e-9).all(), "MFI values must be <= 100"

    def test_nan_for_insufficient_data(self):
        # Only 5 bars — fewer than period + 1 = 15
        high, low, close, volume = _ohlcv(5)
        result = compute_mfi(high, low, close, volume, period=14)
        assert result.isna().all()

    def test_early_values_are_nan(self):
        # First `period` bars should be NaN (MFI not yet computable)
        high, low, close, volume = _ohlcv(60, up=True)
        result = compute_mfi(high, low, close, volume, period=14)
        # At least some of the first period values should be NaN
        assert result.iloc[:14].isna().any()

    def test_high_mfi_on_uptrend_with_high_volume(self):
        """Buying pressure: price rising on high volume → MFI should be above 50."""
        high, low, close, volume = _ohlcv(60, up=True)
        result = compute_mfi(high, low, close, volume, period=14)
        latest = result.dropna().iloc[-1]
        assert latest > 50, f"Expected MFI > 50 for uptrend, got {latest:.1f}"

    def test_low_mfi_on_downtrend_with_high_volume(self):
        """Selling pressure: price falling on high volume → MFI should be below 50."""
        high, low, close, volume = _ohlcv(60, up=False)
        result = compute_mfi(high, low, close, volume, period=14)
        latest = result.dropna().iloc[-1]
        assert latest < 50, f"Expected MFI < 50 for downtrend, got {latest:.1f}"

    def test_custom_period(self):
        high, low, close, volume = _ohlcv(80)
        result_14 = compute_mfi(high, low, close, volume, period=14)
        result_7 = compute_mfi(high, low, close, volume, period=7)
        # Different periods → different values (not guaranteed equal)
        valid_14 = result_14.dropna()
        valid_7 = result_7.dropna()
        assert len(valid_14) > 0
        assert len(valid_7) > 0
        # 7-period result should have more non-NaN values (shorter warmup)
        assert len(valid_7) >= len(valid_14)


class TestMfiSignal:

    def test_returns_float(self):
        high, low, close, volume = _ohlcv(50)
        result = mfi_signal(high, low, close, volume)
        assert isinstance(result, float)

    def test_returns_nan_for_insufficient_data(self):
        high, low, close, volume = _ohlcv(5)
        result = mfi_signal(high, low, close, volume, period=14)
        assert math.isnan(result)

    def test_value_within_0_100(self):
        high, low, close, volume = _ohlcv(50)
        result = mfi_signal(high, low, close, volume)
        assert 0 <= result <= 100

    def test_above_50_for_uptrend(self):
        high, low, close, volume = _ohlcv(60, up=True)
        result = mfi_signal(high, low, close, volume)
        assert result > 50

    def test_below_50_for_downtrend(self):
        high, low, close, volume = _ohlcv(60, up=False)
        result = mfi_signal(high, low, close, volume)
        assert result < 50

    def test_matches_last_value_of_compute_mfi(self):
        high, low, close, volume = _ohlcv(50)
        series = compute_mfi(high, low, close, volume)
        signal = mfi_signal(high, low, close, volume)
        expected = float(series.dropna().iloc[-1])
        assert signal == pytest.approx(expected)

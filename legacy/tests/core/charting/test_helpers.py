"""Tests for rocketstocks.core.charting.helpers."""
import numpy as np
import pandas as pd
import pytest

from rocketstocks.core.charting.helpers import (
    all_values_are_nan,
    format_millions,
    hline,
    recent_bars,
    recent_crossover,
    ta_ylim,
)


class TestAllValuesAreNan:
    def test_all_nan_returns_true(self):
        arr = np.array([np.nan, np.nan, np.nan])
        assert all_values_are_nan(arr) is True

    def test_one_non_nan_returns_false(self):
        arr = np.array([np.nan, 1.0, np.nan])
        assert all_values_are_nan(arr) is False

    def test_no_nan_returns_false(self):
        arr = np.array([1.0, 2.0, 3.0])
        assert all_values_are_nan(arr) is False


class TestRecentCrossover:
    def test_crossover_up(self):
        # indicator crosses above signal
        indicator = [1.0, 2.0, 3.0, 5.0]
        signal    = [4.0, 4.0, 4.0, 4.0]
        result = recent_crossover(indicator, signal)
        assert result == "UP"

    def test_crossover_down(self):
        indicator = [5.0, 4.0, 3.0, 1.0]
        signal    = [2.0, 2.0, 2.0, 2.0]
        result = recent_crossover(indicator, signal)
        assert result == "DOWN"

    def test_no_crossover_returns_none(self):
        indicator = [1.0, 1.0, 1.0, 1.0]
        signal    = [2.0, 2.0, 2.0, 2.0]
        result = recent_crossover(indicator, signal)
        assert result is None


class TestFormatMillions:
    def test_formats_correctly(self):
        # 1_500_000 → "1.5M"
        result = format_millions(1_500_000, 0)
        assert result == "1.5M"

    def test_small_value(self):
        result = format_millions(500_000, 0)
        assert "0.5M" in result


class TestRecentBars:
    def test_1y_returns_trading_days(self):
        df = pd.DataFrame(index=range(300))
        result = recent_bars(df, tf="1y")
        assert result > 0

    def test_all_returns_df_size(self):
        df = pd.DataFrame(index=range(500))
        result = recent_bars(df, tf="all")
        assert result == 500

    def test_6mo_returns_half_of_1y(self):
        df = pd.DataFrame(index=range(300))
        bars_1y = recent_bars(df, tf="1y")
        bars_6mo = recent_bars(df, tf="6mo")
        # 6mo should be roughly half of 1y (±2)
        assert abs(bars_6mo - bars_1y // 2) <= 2

    def test_unknown_tf_returns_all_rows(self):
        df = pd.DataFrame(index=range(123))
        result = recent_bars(df, tf="unknown_tf")
        assert result == 123


class TestTaYlim:
    def test_returns_tuple_of_two(self):
        s = pd.Series([1.0, 2.0, 3.0])
        result = ta_ylim(s, 0.1)
        assert len(result) == 2

    def test_lower_bound_less_than_upper(self):
        s = pd.Series([10.0, 20.0, 30.0])
        lo, hi = ta_ylim(s, 0.1)
        assert lo < hi

    def test_invalid_percent_uses_raw_minmax(self):
        s = pd.Series([5.0, 15.0])
        lo, hi = ta_ylim(s, percent=2.0)  # >1.0 → falls back
        assert lo == 5.0
        assert hi == 15.0

    def test_negative_series_lower_bound(self):
        s = pd.Series([-10.0, -5.0, -1.0])
        lo, hi = ta_ylim(s, 0.1)
        assert lo < hi


class TestHline:
    def test_correct_size(self):
        arr = hline(10, 42.0)
        assert len(arr) == 10

    def test_all_values_equal_value(self):
        arr = hline(5, 3.14)
        assert all(v == 3.14 for v in arr)

    def test_works_with_zero(self):
        arr = hline(4, 0)
        assert list(arr) == [0.0, 0.0, 0.0, 0.0]

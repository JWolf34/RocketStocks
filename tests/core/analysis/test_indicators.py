"""Tests for rocketstocks.core.analysis.indicators."""
import datetime
import math

import numpy as np
import pandas as pd
import pytest

from rocketstocks.core.analysis.indicators import indicators


def _make_5m_df(n_days=10, times_per_day=5):
    """Build a fake 5-minute OHLCV DataFrame with a 'datetime' column."""
    base = datetime.datetime(2024, 1, 2, 9, 30)
    rows = []
    for d in range(n_days):
        day_base = base + datetime.timedelta(days=d)
        for t in range(times_per_day):
            dt = day_base + datetime.timedelta(minutes=5 * t)
            volume = float(100_000 + d * 1000 + t * 100)
            rows.append({"datetime": dt, "volume": volume})
    return pd.DataFrame(rows)


class TestAvgVolAtTime:
    def test_returns_mean_for_matching_time(self):
        df = _make_5m_df(n_days=10, times_per_day=5)
        # Pick a time that exists in the data
        target_dt = datetime.datetime(2024, 1, 2, 9, 30)
        mean_vol, time = indicators.volume.avg_vol_at_time(df, periods=10, dt=target_dt)
        assert not math.isnan(mean_vol)
        assert mean_vol > 0

    def test_returns_nan_for_missing_time(self):
        df = _make_5m_df(n_days=10, times_per_day=5)
        # 15:00 never exists in the data
        target_dt = datetime.datetime(2024, 1, 2, 15, 0)
        mean_vol, time = indicators.volume.avg_vol_at_time(df, periods=10, dt=target_dt)
        assert math.isnan(mean_vol)

    def test_returns_time_object(self):
        df = _make_5m_df(n_days=10, times_per_day=5)
        target_dt = datetime.datetime(2024, 1, 2, 9, 30)
        _, time = indicators.volume.avg_vol_at_time(df, periods=10, dt=target_dt)
        assert isinstance(time, datetime.time)


class TestRvol:
    def test_rvol_greater_than_one_when_spike(self):
        df = _make_5m_df(n_days=10)
        avg = df["volume"].tail(10).mean()
        spike = avg * 5.0
        result = indicators.volume.rvol(df, periods=10, curr_volume=spike)
        assert result > 1.0

    def test_rvol_less_than_one_when_low(self):
        df = _make_5m_df(n_days=10)
        avg = df["volume"].tail(10).mean()
        low = avg * 0.1
        result = indicators.volume.rvol(df, periods=10, curr_volume=low)
        assert result < 1.0

    def test_rvol_approximately_one_when_equal(self):
        df = _make_5m_df(n_days=10)
        avg = df["volume"].tail(10).mean()
        result = indicators.volume.rvol(df, periods=10, curr_volume=avg)
        assert abs(result - 1.0) < 0.01


class TestRvolAtTime:
    def test_returns_float_for_valid_time(self):
        hist_df = _make_5m_df(n_days=10, times_per_day=5)
        today_df = _make_5m_df(n_days=1, times_per_day=5)
        target_dt = datetime.datetime(2024, 1, 2, 9, 30)
        result = indicators.volume.rvol_at_time(
            data=hist_df, today_data=today_df, periods=10, dt=target_dt
        )
        assert isinstance(result, float)

    def test_returns_nan_for_missing_time_in_today(self):
        hist_df = _make_5m_df(n_days=10, times_per_day=5)
        today_df = _make_5m_df(n_days=1, times_per_day=5)
        # Request a time not in today_df
        target_dt = datetime.datetime(2024, 1, 2, 15, 0)
        result = indicators.volume.rvol_at_time(
            data=hist_df, today_data=today_df, periods=10, dt=target_dt
        )
        assert math.isnan(result)

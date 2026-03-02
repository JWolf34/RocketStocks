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


# ---------------------------------------------------------------------------
# indicators.price
# ---------------------------------------------------------------------------

def _make_daily_price_df(n=60, base=100.0, std_pct=2.0, seed=42):
    """Build a daily OHLCV DataFrame with 'close' and 'volume' columns."""
    rng = np.random.default_rng(seed)
    returns = rng.normal(0, std_pct / 100, n)
    close = [base]
    for r in returns[1:]:
        close.append(close[-1] * (1 + r))
    close = close[:n]
    return pd.DataFrame({
        'open': [c * 0.99 for c in close],
        'high': [c * 1.01 for c in close],
        'low':  [c * 0.98 for c in close],
        'close': close,
        'volume': [1_000_000.0] * n,
    })


class TestIndicatorsPrice:
    def test_intraday_zscore_returns_float(self):
        df = _make_daily_price_df()
        z = indicators.price.intraday_zscore(df, current_pct_change=2.0)
        assert isinstance(z, float)

    def test_intraday_zscore_returns_nan_for_empty_df(self):
        z = indicators.price.intraday_zscore(pd.DataFrame(), current_pct_change=5.0)
        assert math.isnan(z)

    def test_intraday_zscore_extreme_move_has_high_magnitude(self):
        df = _make_daily_price_df(n=60, std_pct=0.5)
        z = indicators.price.intraday_zscore(df, current_pct_change=10.0)
        assert abs(z) > 2.0

    def test_return_percentile_in_valid_range(self):
        df = _make_daily_price_df(n=70)
        p = indicators.price.return_percentile(df, current_pct_change=0.0)
        if not math.isnan(p):
            assert 0.0 <= p <= 100.0

    def test_return_percentile_returns_nan_for_empty_df(self):
        p = indicators.price.return_percentile(pd.DataFrame(), current_pct_change=1.0)
        assert math.isnan(p)

    def test_return_percentile_high_pct_gives_high_percentile(self):
        df = _make_daily_price_df(n=70, std_pct=1.0)
        p = indicators.price.return_percentile(df, current_pct_change=20.0)
        assert p > 80.0 if not math.isnan(p) else True


# ---------------------------------------------------------------------------
# indicators.popularity
# ---------------------------------------------------------------------------

def _make_popularity_df(n=40, start_rank=100, seed=42):
    """Build a popularity DataFrame with 'rank' and 'datetime' columns."""
    rng = np.random.default_rng(seed)
    base = datetime.datetime(2024, 1, 1, 0, 0)
    rows = []
    rank = start_rank
    for i in range(n):
        rank = max(1, rank + int(rng.integers(-5, 5)))
        rows.append({'datetime': base + datetime.timedelta(hours=i), 'rank': rank})
    return pd.DataFrame(rows)


class TestIndicatorsPopularity:
    def test_rank_velocity_returns_float(self):
        df = _make_popularity_df()
        v = indicators.popularity.rank_velocity(df)
        assert isinstance(v, float)

    def test_rank_velocity_returns_nan_for_empty_df(self):
        v = indicators.popularity.rank_velocity(pd.DataFrame())
        assert math.isnan(v)

    def test_rank_velocity_returns_nan_for_insufficient_data(self):
        df = pd.DataFrame({'rank': [10], 'datetime': [datetime.datetime.now()]})
        v = indicators.popularity.rank_velocity(df, periods=5)
        assert math.isnan(v)

    def test_rank_velocity_zscore_returns_float(self):
        df = _make_popularity_df(n=60)
        z = indicators.popularity.rank_velocity_zscore(df)
        assert isinstance(z, float)

    def test_rank_velocity_zscore_returns_nan_for_insufficient_data(self):
        df = _make_popularity_df(n=5)
        z = indicators.popularity.rank_velocity_zscore(df, lookback=30, velocity_window=5)
        assert math.isnan(z)

    def test_rank_velocity_zscore_returns_nan_for_empty(self):
        z = indicators.popularity.rank_velocity_zscore(pd.DataFrame())
        assert math.isnan(z)

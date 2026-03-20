"""Tests for rocketstocks.backtest.data_prep."""
import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from rocketstocks.backtest.data_prep import (
    filter_regular_hours,
    prep_5m,
    prep_daily,
    prep_for_signals,
)

_ET = ZoneInfo('America/New_York')


def _make_daily_lower(n: int = 10) -> pd.DataFrame:
    dates = pd.date_range('2025-01-01', periods=n, freq='B').date
    return pd.DataFrame({
        'ticker': 'AAPL',
        'open': np.ones(n) * 100,
        'high': np.ones(n) * 101,
        'low': np.ones(n) * 99,
        'close': np.ones(n) * 100,
        'volume': np.ones(n) * 1_000_000,
        'date': dates,
    })


def _make_5m_lower(times: list[datetime.datetime]) -> pd.DataFrame:
    n = len(times)
    return pd.DataFrame({
        'ticker': 'AAPL',
        'open': np.ones(n) * 100,
        'high': np.ones(n) * 101,
        'low': np.ones(n) * 99,
        'close': np.ones(n) * 100,
        'volume': np.ones(n) * 10_000,
        'datetime': times,
    })


# ---------------------------------------------------------------------------
# prep_daily
# ---------------------------------------------------------------------------

def test_prep_daily_renames_columns():
    df = _make_daily_lower()
    result = prep_daily(df)
    assert set(result.columns) >= {'Open', 'High', 'Low', 'Close', 'Volume'}
    for col in ('open', 'high', 'low', 'close', 'volume'):
        assert col not in result.columns


def test_prep_daily_drops_ticker_column():
    df = _make_daily_lower()
    result = prep_daily(df)
    assert 'ticker' not in result.columns


def test_prep_daily_sets_datetimeindex():
    df = _make_daily_lower()
    result = prep_daily(df)
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.name == 'Date'


def test_prep_daily_sorts_ascending():
    df = _make_daily_lower(5)
    df = df.iloc[::-1].reset_index(drop=True)  # reverse order
    result = prep_daily(df)
    assert result.index.is_monotonic_increasing


def test_prep_daily_empty_input():
    df = pd.DataFrame()
    result = prep_daily(df)
    assert result.empty


def test_prep_daily_numeric_types():
    df = _make_daily_lower(5)
    df['close'] = df['close'].astype(object)  # make it object type
    result = prep_daily(df)
    assert result['Close'].dtype in (float, np.float64)


# ---------------------------------------------------------------------------
# prep_5m
# ---------------------------------------------------------------------------

def _et_times(hour_minute_pairs: list[tuple[int, int]]) -> list[datetime.datetime]:
    base = datetime.date(2025, 1, 2)
    return [
        datetime.datetime(base.year, base.month, base.day, h, m, tzinfo=_ET)
        for h, m in hour_minute_pairs
    ]


def test_prep_5m_renames_columns():
    times = _et_times([(9, 30), (9, 35), (9, 40)])
    df = _make_5m_lower(times)
    result = prep_5m(df)
    assert set(result.columns) >= {'Open', 'High', 'Low', 'Close', 'Volume'}


def test_prep_5m_sets_datetimeindex():
    times = _et_times([(9, 30), (9, 35)])
    df = _make_5m_lower(times)
    result = prep_5m(df)
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.name == 'Datetime'


def test_prep_5m_empty_input():
    result = prep_5m(pd.DataFrame())
    assert result.empty


# ---------------------------------------------------------------------------
# filter_regular_hours
# ---------------------------------------------------------------------------

def test_filter_regular_hours_excludes_premarket():
    times = _et_times([(8, 0), (9, 30), (10, 0)])
    df = prep_5m(_make_5m_lower(times))
    result = filter_regular_hours(df)
    # 8:00 AM bar should be excluded
    assert len(result) == 2


def test_filter_regular_hours_excludes_afterhours():
    times = _et_times([(15, 55), (16, 0), (17, 0)])
    df = prep_5m(_make_5m_lower(times))
    result = filter_regular_hours(df)
    # 16:00 and 17:00 bars excluded (< 16:00 is the rule)
    assert len(result) == 1


def test_filter_regular_hours_includes_930_open():
    times = _et_times([(9, 30), (12, 0), (15, 55)])
    df = prep_5m(_make_5m_lower(times))
    result = filter_regular_hours(df)
    assert len(result) == 3


def test_filter_regular_hours_empty_input():
    result = filter_regular_hours(pd.DataFrame())
    assert result.empty


def test_filter_regular_hours_naive_index():
    """Naive datetimes should be localized to ET before filtering."""
    times = [
        datetime.datetime(2025, 1, 2, 9, 30),   # 9:30 naive (treated as ET)
        datetime.datetime(2025, 1, 2, 8, 0),    # 8:00 naive
    ]
    df = _make_5m_lower(times)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.set_index('datetime').drop(columns=['ticker'])
    df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                             'close': 'Close', 'volume': 'Volume'})
    result = filter_regular_hours(df)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# prep_for_signals
# ---------------------------------------------------------------------------

def test_prep_for_signals_reverses_rename():
    df = _make_daily_lower(5)
    prepped = prep_daily(df)
    reversed_df = prep_for_signals(prepped)
    for col in ('open', 'high', 'low', 'close', 'volume'):
        assert col in reversed_df.columns
    for col in ('Open', 'High', 'Low', 'Close', 'Volume'):
        assert col not in reversed_df.columns

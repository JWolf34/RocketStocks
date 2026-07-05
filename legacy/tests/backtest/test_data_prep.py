"""Tests for rocketstocks.backtest.data_prep."""
import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from rocketstocks.backtest.data_prep import (
    enrich_5m_with_daily_context,
    filter_regular_hours,
    mark_regular_hours,
    merge_popularity,
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
# mark_regular_hours
# ---------------------------------------------------------------------------

def test_mark_regular_hours_adds_column():
    times = _et_times([(8, 0), (9, 30), (10, 0), (16, 0)])
    df = prep_5m(_make_5m_lower(times))
    result = mark_regular_hours(df)
    assert 'Is_Regular_Hours' in result.columns


def test_mark_regular_hours_preserves_all_rows():
    times = _et_times([(8, 0), (9, 30), (10, 0), (16, 0)])
    df = prep_5m(_make_5m_lower(times))
    result = mark_regular_hours(df)
    assert len(result) == len(df)


def test_mark_regular_hours_premarket_false():
    times = _et_times([(8, 0), (9, 30)])
    df = prep_5m(_make_5m_lower(times))
    result = mark_regular_hours(df)
    assert result['Is_Regular_Hours'].iloc[0] is False or result['Is_Regular_Hours'].iloc[0] == False


def test_mark_regular_hours_regular_true():
    times = _et_times([(9, 30), (12, 0), (15, 55)])
    df = prep_5m(_make_5m_lower(times))
    result = mark_regular_hours(df)
    assert result['Is_Regular_Hours'].all()


def test_mark_regular_hours_afterhours_false():
    times = _et_times([(15, 55), (16, 0), (17, 0)])
    df = prep_5m(_make_5m_lower(times))
    result = mark_regular_hours(df)
    # 16:00 and 17:00 are not regular
    assert result['Is_Regular_Hours'].iloc[0] == True
    assert result['Is_Regular_Hours'].iloc[1] == False
    assert result['Is_Regular_Hours'].iloc[2] == False


def test_mark_regular_hours_empty_input():
    result = mark_regular_hours(pd.DataFrame())
    assert result.empty


def test_mark_regular_hours_does_not_modify_original():
    times = _et_times([(9, 30), (10, 0)])
    df = prep_5m(_make_5m_lower(times))
    mark_regular_hours(df)
    assert 'Is_Regular_Hours' not in df.columns


# ---------------------------------------------------------------------------
# enrich_5m_with_daily_context
# ---------------------------------------------------------------------------

def _make_daily_prepped(n: int = 30, close_start: float = 100.0) -> pd.DataFrame:
    """Daily DataFrame in backtesting.py format (capitalised columns, DatetimeIndex)."""
    dates = pd.bdate_range('2025-01-01', periods=n)
    closes = [close_start + i for i in range(n)]
    return pd.DataFrame({
        'Open': closes,
        'High': [c + 1 for c in closes],
        'Low': [c - 1 for c in closes],
        'Close': closes,
        'Volume': [1_000_000] * n,
    }, index=dates)


def _make_5m_prepped(dates_times: list[datetime.datetime]) -> pd.DataFrame:
    """5m DataFrame in backtesting.py format (capitalised columns, DatetimeIndex)."""
    n = len(dates_times)
    closes = [100.0 + i * 0.1 for i in range(n)]
    return pd.DataFrame({
        'Open': closes,
        'High': [c + 0.5 for c in closes],
        'Low': [c - 0.5 for c in closes],
        'Close': closes,
        'Volume': [10_000] * n,
    }, index=pd.DatetimeIndex(dates_times, name='Datetime'))


def _et_dt(date: str, hour: int, minute: int) -> datetime.datetime:
    d = datetime.date.fromisoformat(date)
    return datetime.datetime(d.year, d.month, d.day, hour, minute, tzinfo=_ET)


def test_enrich_5m_adds_expected_columns():
    daily = _make_daily_prepped(30)
    times = [_et_dt('2025-02-03', 9, 30), _et_dt('2025-02-03', 9, 35)]
    df_5m = _make_5m_prepped(times)
    result = enrich_5m_with_daily_context(df_5m, daily)
    for col in ('Prev_Close', 'Daily_Vol_Mean', 'Daily_Vol_Std',
                'Daily_Return_Mean', 'Daily_Return_Std',
                'Cumulative_Volume', 'Intraday_Pct_Change'):
        assert col in result.columns, f'Missing column: {col}'


def test_enrich_5m_cumulative_volume_resets_per_day():
    daily = _make_daily_prepped(30)
    times = [
        _et_dt('2025-02-03', 9, 30),
        _et_dt('2025-02-03', 9, 35),
        _et_dt('2025-02-04', 9, 30),
        _et_dt('2025-02-04', 9, 35),
    ]
    df_5m = _make_5m_prepped(times)
    result = enrich_5m_with_daily_context(df_5m, daily)
    # First bar of each day: cumulative = Volume for that bar
    assert result['Cumulative_Volume'].iloc[0] == df_5m['Volume'].iloc[0]
    assert result['Cumulative_Volume'].iloc[2] == df_5m['Volume'].iloc[2]
    # Second bar of day: cumulative = sum of first two bars
    assert result['Cumulative_Volume'].iloc[1] == df_5m['Volume'].iloc[0] + df_5m['Volume'].iloc[1]


def test_enrich_5m_intraday_pct_change():
    daily = _make_daily_prepped(30, close_start=100.0)
    # daily.Close[-1] for Jan 31 will determine Prev_Close for Feb 3
    # With 30 bars starting 2025-01-01, the last bar ~2025-02-11
    # Use Feb 03 - find prev close from daily
    times = [_et_dt('2025-02-03', 9, 30)]
    closes_5m = [105.0]
    df_5m = pd.DataFrame({
        'Open': closes_5m, 'High': closes_5m, 'Low': closes_5m,
        'Close': closes_5m, 'Volume': [10_000],
    }, index=pd.DatetimeIndex(times, name='Datetime'))
    result = enrich_5m_with_daily_context(df_5m, daily)
    prev_close = float(result['Prev_Close'].iloc[0])
    expected_pct = (105.0 / prev_close - 1) * 100
    assert abs(result['Intraday_Pct_Change'].iloc[0] - expected_pct) < 1e-6


def test_enrich_5m_daily_vol_mean_uses_prior_day():
    """Daily_Vol_Mean on a 5m bar should match the rolling mean of prior-day data."""
    daily = _make_daily_prepped(25)
    # Use a bar in Feb so there are enough prior days
    times = [_et_dt('2025-02-03', 9, 30)]
    df_5m = _make_5m_prepped(times)
    result = enrich_5m_with_daily_context(df_5m, daily)
    # All daily volumes are 1_000_000 so mean should be 1_000_000
    assert abs(result['Daily_Vol_Mean'].iloc[0] - 1_000_000) < 1e-3


def test_enrich_5m_empty_5m():
    daily = _make_daily_prepped()
    result = enrich_5m_with_daily_context(pd.DataFrame(), daily)
    assert result.empty


def test_enrich_5m_empty_daily():
    times = [_et_dt('2025-02-03', 9, 30)]
    df_5m = _make_5m_prepped(times)
    result = enrich_5m_with_daily_context(df_5m, pd.DataFrame())
    # Returns copy of df_5m unchanged
    assert list(result.columns) == list(df_5m.columns)


# ---------------------------------------------------------------------------
# merge_popularity
# ---------------------------------------------------------------------------

def _make_price_5m(times: list[datetime.datetime]) -> pd.DataFrame:
    return _make_5m_prepped(times)


def _make_popularity_df(timestamps: list[datetime.datetime],
                        ranks: list[int],
                        mentions: list[int]) -> pd.DataFrame:
    n = len(timestamps)
    return pd.DataFrame({
        'datetime': pd.to_datetime(timestamps),
        'rank': ranks,
        'mentions': mentions,
        'rank_24h_ago': [r + 10 for r in ranks],
        'mentions_24h_ago': [m // 2 for m in mentions],
        'ticker': 'TEST',
        'name': 'Test',
        'upvotes': [0] * n,
    })


def test_merge_popularity_adds_columns():
    times = [_et_dt('2025-01-02', 9, 30), _et_dt('2025-01-02', 9, 35)]
    price_df = _make_price_5m(times)
    pop_time = [datetime.datetime(2025, 1, 2, 9, 0, tzinfo=_ET)]
    pop_df = _make_popularity_df(pop_time, ranks=[50], mentions=[200])
    result = merge_popularity(price_df, pop_df)
    for col in ('Rank', 'Mentions', 'Rank_24h_ago', 'Mentions_24h_ago'):
        assert col in result.columns


def test_merge_popularity_forward_fills():
    times = [
        _et_dt('2025-01-02', 9, 30),
        _et_dt('2025-01-02', 9, 35),
        _et_dt('2025-01-02', 10, 0),  # still within same popularity window
    ]
    price_df = _make_price_5m(times)
    pop_time = [datetime.datetime(2025, 1, 2, 9, 0, tzinfo=_ET)]
    pop_df = _make_popularity_df(pop_time, ranks=[42], mentions=[300])
    result = merge_popularity(price_df, pop_df)
    # All bars after pop_time should get rank=42
    assert all(result['Rank'] == 42)


def test_merge_popularity_empty_popularity_gives_nan():
    times = [_et_dt('2025-01-02', 9, 30)]
    price_df = _make_price_5m(times)
    result = merge_popularity(price_df, pd.DataFrame())
    assert result['Rank'].isna().all()


def test_merge_popularity_bars_before_first_reading_are_nan():
    times = [
        _et_dt('2025-01-02', 8, 0),   # pre-market — before any popularity data
        _et_dt('2025-01-02', 9, 30),  # after popularity reading
    ]
    price_df = _make_price_5m(times)
    # Popularity data arrives at 9:00 AM
    pop_time = [datetime.datetime(2025, 1, 2, 9, 0, tzinfo=_ET)]
    pop_df = _make_popularity_df(pop_time, ranks=[55], mentions=[100])
    result = merge_popularity(price_df, pop_df)
    assert pd.isna(result['Rank'].iloc[0])
    assert result['Rank'].iloc[1] == 55


def test_merge_popularity_preserves_original_columns():
    times = [_et_dt('2025-01-02', 9, 30)]
    price_df = _make_price_5m(times)
    pop_df = _make_popularity_df([datetime.datetime(2025, 1, 2, 9, 0, tzinfo=_ET)],
                                  ranks=[10], mentions=[500])
    result = merge_popularity(price_df, pop_df)
    for col in price_df.columns:
        assert col in result.columns


def test_merge_popularity_none_input():
    times = [_et_dt('2025-01-02', 9, 30)]
    price_df = _make_price_5m(times)
    result = merge_popularity(price_df, None)
    assert result['Rank'].isna().all()


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

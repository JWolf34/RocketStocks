"""Tests for rocketstocks.eda.events.base."""
import datetime
import pandas as pd
import pytest

from rocketstocks.eda.events.base import (
    _to_naive_utc,
    deduplicate_events,
    build_control_group,
    empty_events,
    validate_events,
    EVENT_COLS,
    _empty_control,
)


def _make_events(rows: list[dict]) -> pd.DataFrame:
    """Helper to create an events DataFrame from a list of dicts."""
    defaults = {'signal_value': 1.0, 'source': 'test'}
    full_rows = [{**defaults, **row} for row in rows]
    df = pd.DataFrame(full_rows)
    df['datetime'] = pd.to_datetime(df['datetime'])
    return df


def _make_close_dict(tickers: list[str], n: int = 30) -> dict:
    """Build a simple close_dict for testing."""
    import numpy as np
    rng = np.random.default_rng(42)
    close_dict = {}
    for ticker in tickers:
        dates = pd.date_range('2024-01-01', periods=n, freq='B')
        prices = 100 + rng.standard_normal(n).cumsum()
        close_dict[ticker] = pd.Series(prices, index=dates)
    return close_dict


def _make_bar_counts(tickers: list[str], n: int = 30) -> dict:
    """Build a bar_counts dict for testing."""
    return {ticker: n for ticker in tickers}


# ---------------------------------------------------------------------------
# empty_events / validate_events
# ---------------------------------------------------------------------------

def test_empty_events_has_required_cols():
    df = empty_events('test')
    for col in EVENT_COLS:
        assert col in df.columns


def test_empty_events_always_has_source_detail():
    """source_detail must be present even when source='' (empty string)."""
    df = empty_events('')
    assert 'source_detail' in df.columns


def test_empty_events_with_source_has_source_detail():
    df = empty_events('volume')
    assert 'source_detail' in df.columns


def test_validate_events_passes_valid():
    df = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    # Should not raise
    validate_events(df)


def test_validate_events_raises_on_missing_col():
    df = pd.DataFrame({'ticker': ['AAPL'], 'datetime': ['2024-01-05']})
    with pytest.raises(ValueError, match='missing required columns'):
        validate_events(df)


def test_validate_events_drops_nan_signal_value():
    """Rows with NaN signal_value should be silently removed."""
    df = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-05', 'signal_value': float('nan')},
        {'ticker': 'GOOG', 'datetime': '2024-01-05', 'signal_value': 2.0},
    ])
    result = validate_events(df)
    assert len(result) == 1
    assert result.iloc[0]['ticker'] == 'GOOG'


def test_validate_events_normalizes_datetime_to_naive():
    """tz-aware datetime column should be coerced to tz-naive."""
    import pytz
    df = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    df['datetime'] = df['datetime'].dt.tz_localize('UTC')
    result = validate_events(df)
    assert result['datetime'].dt.tz is None


def test_validate_events_empty_df_returns_empty():
    df = empty_events('test')  # has the required columns, zero rows
    result = validate_events(df)
    assert result.empty


# ---------------------------------------------------------------------------
# _to_naive_utc
# ---------------------------------------------------------------------------

def test_to_naive_utc_naive_unchanged():
    s = pd.to_datetime(pd.Series(['2024-01-01', '2024-06-01']))
    result = _to_naive_utc(s)
    assert result.dt.tz is None


def test_to_naive_utc_aware_stripped():
    s = pd.to_datetime(pd.Series(['2024-01-01', '2024-06-01'])).dt.tz_localize('UTC')
    result = _to_naive_utc(s)
    assert result.dt.tz is None


def test_to_naive_utc_string_series():
    s = pd.Series(['2024-01-01', '2024-06-01'])
    result = _to_naive_utc(s)
    assert pd.api.types.is_datetime64_any_dtype(result)
    assert result.dt.tz is None


# ---------------------------------------------------------------------------
# deduplicate_events
# ---------------------------------------------------------------------------

def test_dedup_removes_close_events():
    """Two events 1 day apart should be reduced to 1 (window=3)."""
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-05'},
        {'ticker': 'AAPL', 'datetime': '2024-01-06'},
    ])
    result = deduplicate_events(events, window_days=3)
    assert len(result) == 1


def test_dedup_keeps_events_outside_window():
    """Two events 5 days apart should both be kept (window=3)."""
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-01'},
        {'ticker': 'AAPL', 'datetime': '2024-01-10'},
    ])
    result = deduplicate_events(events, window_days=3)
    assert len(result) == 2


def test_dedup_independent_tickers():
    """Events on different tickers are independent — both should be kept."""
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-05'},
        {'ticker': 'GOOG', 'datetime': '2024-01-05'},
    ])
    result = deduplicate_events(events, window_days=3)
    assert len(result) == 2


def test_dedup_empty_returns_empty():
    result = deduplicate_events(pd.DataFrame(columns=list(EVENT_COLS)), window_days=3)
    assert result.empty


def test_dedup_preserves_first_event():
    """The FIRST event per window should be kept, not the last."""
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-01', 'signal_value': 2.0},
        {'ticker': 'AAPL', 'datetime': '2024-01-02', 'signal_value': 5.0},
    ])
    result = deduplicate_events(events, window_days=5)
    assert len(result) == 1
    assert result.iloc[0]['datetime'] == pd.Timestamp('2024-01-01')


# ---------------------------------------------------------------------------
# build_control_group
# ---------------------------------------------------------------------------

def test_control_group_source_is_control():
    bar_counts = _make_bar_counts(['AAPL', 'GOOG'], n=30)
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    control = build_control_group(events, bar_counts, n_samples=20)
    assert (control['source'] == 'control').all()


def test_control_group_respects_n_samples():
    bar_counts = _make_bar_counts(['AAPL'], n=10)
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-03'}])
    control = build_control_group(events, bar_counts, n_samples=5)
    assert len(control) <= 5


def test_control_group_empty_dict():
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    control = build_control_group(events, {})
    assert control.empty


def test_control_group_offsets_in_range():
    """Each _bar_offset must be in [0, bar_counts[ticker])."""
    bar_counts = _make_bar_counts(['AAPL', 'GOOG'], n=20)
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    control = build_control_group(events, bar_counts, n_samples=30)
    for _, row in control.iterrows():
        ticker = row['ticker']
        assert 0 <= row['_bar_offset'] < bar_counts[ticker]


def test_control_group_no_duplicate_offsets_per_ticker():
    """Sampled (ticker, offset) pairs should be unique."""
    bar_counts = _make_bar_counts(['AAPL'], n=50)
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    control = build_control_group(events, bar_counts, n_samples=30, seed=0)
    pairs = list(zip(control['ticker'], control['_bar_offset']))
    assert len(pairs) == len(set(pairs))


def test_control_group_signal_value_is_zero():
    bar_counts = _make_bar_counts(['AAPL'], n=20)
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    control = build_control_group(events, bar_counts, n_samples=10)
    assert (control['signal_value'] == 0.0).all()


def test_control_group_has_expected_columns():
    bar_counts = _make_bar_counts(['AAPL'], n=20)
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    control = build_control_group(events, bar_counts, n_samples=10)
    assert set(control.columns) >= {'ticker', '_bar_offset', 'signal_value', 'source'}


def test_control_group_empty_control_columns():
    """_empty_control() has the expected column structure."""
    df = _empty_control()
    assert list(df.columns) == ['ticker', '_bar_offset', 'signal_value', 'source']
    assert df.empty


def test_control_group_zero_bar_count_tickers_excluded():
    """Tickers with bar_count=0 should not appear in control output."""
    bar_counts = {'AAPL': 20, 'GOOG': 0}
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    control = build_control_group(events, bar_counts, n_samples=10)
    assert 'GOOG' not in control['ticker'].values


def test_control_group_deterministic_with_sorted_tickers():
    """Control group must be reproducible regardless of dict insertion order."""
    bar_counts_a = {'GOOG': 30, 'AAPL': 30}
    bar_counts_b = {'AAPL': 30, 'GOOG': 30}
    events = _make_events([{'ticker': 'AAPL', 'datetime': '2024-01-05'}])
    ctrl_a = build_control_group(events, bar_counts_a, n_samples=20, seed=0)
    ctrl_b = build_control_group(events, bar_counts_b, n_samples=20, seed=0)
    # Same seed + sorted tickers → same sample
    assert list(ctrl_a['ticker']) == list(ctrl_b['ticker'])
    assert list(ctrl_a['_bar_offset']) == list(ctrl_b['_bar_offset'])


# ---------------------------------------------------------------------------
# deduplicate_events — Timedelta / string window
# ---------------------------------------------------------------------------

def test_dedup_accepts_timedelta_window():
    """window_days can be a pd.Timedelta."""
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-01'},
        {'ticker': 'AAPL', 'datetime': '2024-01-02'},
    ])
    result = deduplicate_events(events, window_days=pd.Timedelta('5d'))
    assert len(result) == 1


def test_dedup_accepts_string_window():
    """window_days can be a Timedelta string such as '1d'."""
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-01'},
        {'ticker': 'AAPL', 'datetime': '2024-01-02'},
    ])
    result = deduplicate_events(events, window_days='5d')
    assert len(result) == 1


def test_dedup_short_string_window_keeps_more():
    """A '30min' window keeps events that are >30 min apart."""
    events = _make_events([
        {'ticker': 'AAPL', 'datetime': '2024-01-01 09:00:00'},
        {'ticker': 'AAPL', 'datetime': '2024-01-01 09:35:00'},  # > 30 min later
    ])
    result = deduplicate_events(events, window_days='30min')
    assert len(result) == 2

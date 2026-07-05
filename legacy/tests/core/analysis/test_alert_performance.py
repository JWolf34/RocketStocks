"""Tests for rocketstocks.core.analysis.alert_performance."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.analysis.alert_performance import (
    compute_surge_confidence,
    compute_signal_confidence,
    compute_price_outcome,
)


# ---------------------------------------------------------------------------
# compute_surge_confidence
# ---------------------------------------------------------------------------

def _make_surge_df(confirmed_list: list[bool], expired_list: list[bool]) -> pd.DataFrame:
    return pd.DataFrame({'confirmed': confirmed_list, 'expired': expired_list})


def test_surge_confidence_all_confirmed():
    df = _make_surge_df([True, True, True], [False, False, False])
    result = compute_surge_confidence(df)
    assert result['total'] == 3
    assert result['confirmed'] == 3
    assert result['expired'] == 0
    assert result['rate'] == pytest.approx(100.0)


def test_surge_confidence_all_expired():
    df = _make_surge_df([False, False], [True, True])
    result = compute_surge_confidence(df)
    assert result['confirmed'] == 0
    assert result['expired'] == 2
    assert result['rate'] == pytest.approx(0.0)


def test_surge_confidence_mixed():
    df = _make_surge_df([True, False, True, False], [False, True, False, True])
    result = compute_surge_confidence(df)
    assert result['confirmed'] == 2
    assert result['expired'] == 2
    assert result['rate'] == pytest.approx(50.0)


def test_surge_confidence_pending_excluded_from_rate():
    """Pending surges (neither confirmed nor expired) do not reduce the rate."""
    df = _make_surge_df([True, False, False], [False, False, False])
    result = compute_surge_confidence(df)
    assert result['pending'] == 2
    # Only 1 settled (confirmed) out of 1 settled total → 100%
    assert result['rate'] == pytest.approx(100.0)


def test_surge_confidence_empty_df():
    result = compute_surge_confidence(pd.DataFrame())
    assert result['total'] == 0
    assert result['rate'] is None


def test_surge_confidence_no_settled():
    """All surges still pending → rate is None."""
    df = _make_surge_df([False, False], [False, False])
    result = compute_surge_confidence(df)
    assert result['rate'] is None


# ---------------------------------------------------------------------------
# compute_signal_confidence
# ---------------------------------------------------------------------------

def _make_signal_df(statuses: list[str]) -> pd.DataFrame:
    return pd.DataFrame({'status': statuses})


def test_signal_confidence_all_confirmed():
    df = _make_signal_df(['confirmed', 'confirmed'])
    result = compute_signal_confidence(df)
    assert result['confirmed'] == 2
    assert result['rate'] == pytest.approx(100.0)


def test_signal_confidence_mixed():
    df = _make_signal_df(['confirmed', 'expired', 'confirmed', 'pending'])
    result = compute_signal_confidence(df)
    assert result['confirmed'] == 2
    assert result['expired'] == 1
    assert result['pending'] == 1
    assert result['rate'] == pytest.approx(2 / 3 * 100, abs=0.2)


def test_signal_confidence_empty():
    result = compute_signal_confidence(pd.DataFrame())
    assert result['rate'] is None


def test_signal_confidence_no_status_column():
    df = pd.DataFrame({'ticker': ['AAPL', 'GOOG']})
    result = compute_signal_confidence(df)
    # confirmed=0, expired=0 → rate is None
    assert result['rate'] is None


# ---------------------------------------------------------------------------
# compute_price_outcome
# ---------------------------------------------------------------------------

def _make_price_df(dates: list[datetime.date], closes: list[float]) -> pd.DataFrame:
    return pd.DataFrame({'date': dates, 'close': closes})


def _make_alert(ticker: str, date: datetime.date) -> dict:
    return {'ticker': ticker, 'date': date, 'alert_type': 'POPULARITY_SURGE', 'alert_data': {}}


def test_price_outcome_basic_positive():
    """Alert on day 0; T+1d price is higher → positive outcome."""
    base = datetime.date(2026, 1, 1)
    alerts = [_make_alert('GME', base)]
    price_history = {
        'GME': _make_price_df(
            [base, base + datetime.timedelta(days=1), base + datetime.timedelta(days=4)],
            [100.0, 110.0, 120.0],
        )
    }
    result = compute_price_outcome(alerts, price_history, horizons=[1, 4])
    per_alert = result['per_alert']
    assert len(per_alert) == 1
    assert per_alert[0]['pct_1d'] == pytest.approx(10.0)
    assert per_alert[0]['pct_4d'] == pytest.approx(20.0)


def test_price_outcome_negative():
    """Alert on day 0; T+1d price is lower → negative outcome."""
    base = datetime.date(2026, 1, 2)
    alerts = [_make_alert('AAPL', base)]
    price_history = {
        'AAPL': _make_price_df(
            [base, base + datetime.timedelta(days=1)],
            [200.0, 190.0],
        )
    }
    result = compute_price_outcome(alerts, price_history, horizons=[1])
    assert result['per_alert'][0]['pct_1d'] == pytest.approx(-5.0)


def test_price_outcome_missing_future_data():
    """No data after alert date → pct_Nd is None."""
    base = datetime.date(2026, 1, 5)
    alerts = [_make_alert('GME', base)]
    price_history = {'GME': _make_price_df([base], [50.0])}
    result = compute_price_outcome(alerts, price_history, horizons=[1, 4])
    assert result['per_alert'][0].get('pct_1d') is None
    assert result['per_alert'][0].get('pct_4d') is None


def test_price_outcome_missing_ticker():
    """Alert ticker not in price_history → skipped."""
    alerts = [_make_alert('UNKNOWN', datetime.date(2026, 1, 1))]
    result = compute_price_outcome(alerts, {}, horizons=[1])
    assert result['per_alert'] == []


def test_price_outcome_aggregate_stats():
    """Aggregate mean and positive_rate are computed across multiple alerts."""
    base = datetime.date(2026, 1, 1)
    alerts = [_make_alert('GME', base), _make_alert('GME', base + datetime.timedelta(days=1))]
    price_history = {
        'GME': _make_price_df(
            [base, base + datetime.timedelta(days=1), base + datetime.timedelta(days=2)],
            [100.0, 110.0, 105.0],
        )
    }
    result = compute_price_outcome(alerts, price_history, horizons=[1])
    agg = result['aggregate']['pct_1d']
    # Alert 1 at day 0: next day close 110 → +10%
    # Alert 2 at day 1: next day close 105 → -4.76%
    assert agg['count'] == 2
    assert agg['positive_rate'] == pytest.approx(50.0)


def test_price_outcome_empty_alerts():
    result = compute_price_outcome([], {}, horizons=[1, 4])
    assert result['per_alert'] == []
    assert result['aggregate']['pct_1d']['count'] == 0

"""Tests for rocketstocks.core.content.reports.alert_history_report."""
import datetime

import pytest

from rocketstocks.core.content.models import AlertHistoryData, EmbedSpec
from rocketstocks.core.content.reports.alert_history_report import AlertHistory


def _make_alert(
    ticker: str = "GME",
    alert_type: str = "POPULARITY_SURGE",
    date: datetime.date | None = None,
    alert_data: dict | None = None,
    pct_1d: float | None = None,
    pct_4d: float | None = None,
) -> dict:
    return {
        'ticker': ticker,
        'alert_type': alert_type,
        'date': date or datetime.date(2026, 1, 15),
        'alert_data': alert_data or {},
        'pct_1d': pct_1d,
        'pct_4d': pct_4d,
    }


def test_alert_history_returns_embed_spec():
    data = AlertHistoryData(ticker='GME', alerts=[_make_alert()], count=1)
    result = AlertHistory(data).build()
    assert isinstance(result, EmbedSpec)


def test_alert_history_title_includes_ticker():
    data = AlertHistoryData(ticker='TSLA', alerts=[_make_alert(ticker='TSLA')], count=1)
    result = AlertHistory(data).build()
    assert "TSLA" in result.title


def test_alert_history_empty_alerts_message():
    data = AlertHistoryData(ticker='GME', alerts=[], count=0)
    result = AlertHistory(data).build()
    assert "No alerts" in result.description


def test_alert_history_shows_outcome_data():
    alert = _make_alert(pct_1d=5.25, pct_4d=-1.5)
    data = AlertHistoryData(ticker='GME', alerts=[alert], count=1)
    result = AlertHistory(data).build()
    field_values = " ".join(f.value for f in result.fields)
    assert "+5.25%" in field_values
    assert "-1.50%" in field_values


def test_alert_history_shows_popularity_surge_details():
    alert = _make_alert(
        alert_type='POPULARITY_SURGE',
        alert_data={'current_rank': 42, 'mention_ratio': 4.5},
    )
    data = AlertHistoryData(ticker='GME', alerts=[alert], count=1)
    result = AlertHistory(data).build()
    field_values = " ".join(f.value for f in result.fields)
    assert "rank #42" in field_values
    assert "4.5x" in field_values


def test_alert_history_shows_momentum_confirmation_pct():
    alert = _make_alert(
        alert_type='MOMENTUM_CONFIRMATION',
        alert_data={'price_change_since_flag': 2.3},
    )
    data = AlertHistoryData(ticker='GME', alerts=[alert], count=1)
    result = AlertHistory(data).build()
    field_values = " ".join(f.value for f in result.fields)
    assert "since flag" in field_values


def test_alert_history_footer_shows_count():
    alerts = [_make_alert(date=datetime.date(2026, 1, i + 1)) for i in range(5)]
    data = AlertHistoryData(ticker='GME', alerts=alerts, count=10)
    result = AlertHistory(data).build()
    assert "5 of 10" in result.footer


def test_alert_history_multiple_alerts_produce_fields():
    alerts = [_make_alert(date=datetime.date(2026, 1, i + 1)) for i in range(3)]
    data = AlertHistoryData(ticker='GME', alerts=alerts, count=3)
    result = AlertHistory(data).build()
    assert len(result.fields) >= 1

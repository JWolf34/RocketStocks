"""Tests for rocketstocks.core.content.reports.alert_stats_report."""
import pytest

from rocketstocks.core.content.models import AlertStatsData, EmbedSpec
from rocketstocks.core.content.reports.alert_stats_report import AlertStats


def _make_data(
    period_label: str = "Last 7 Days",
    surge_confidence: dict | None = None,
    signal_confidence: dict | None = None,
    price_outcomes: dict | None = None,
    alert_counts: dict | None = None,
) -> AlertStatsData:
    return AlertStatsData(
        period_label=period_label,
        surge_confidence=surge_confidence or {'total': 0, 'confirmed': 0, 'expired': 0, 'pending': 0, 'rate': None},
        signal_confidence=signal_confidence or {'total': 0, 'confirmed': 0, 'expired': 0, 'pending': 0, 'rate': None},
        price_outcomes=price_outcomes or {'per_alert': [], 'aggregate': {}},
        alert_counts=alert_counts or {},
    )


def test_alert_stats_returns_embed_spec():
    data = _make_data()
    result = AlertStats(data).build()
    assert isinstance(result, EmbedSpec)


def test_alert_stats_no_data_shows_no_data_message():
    data = _make_data()
    result = AlertStats(data).build()
    assert "No alert data" in result.description


def test_alert_stats_title_includes_period():
    data = _make_data(period_label="Today")
    result = AlertStats(data).build()
    assert "Today" in result.title


def test_alert_stats_shows_surge_confidence_rate():
    data = _make_data(
        surge_confidence={'total': 10, 'confirmed': 7, 'expired': 3, 'pending': 0, 'rate': 70.0},
    )
    result = AlertStats(data).build()
    field_values = [f.value for f in result.fields]
    assert any("70.0%" in v for v in field_values)


def test_alert_stats_shows_signal_confidence_rate():
    data = _make_data(
        signal_confidence={'total': 5, 'confirmed': 3, 'expired': 2, 'pending': 0, 'rate': 60.0},
    )
    result = AlertStats(data).build()
    field_values = [f.value for f in result.fields]
    assert any("60.0%" in v for v in field_values)


def test_alert_stats_shows_alert_counts():
    data = _make_data(alert_counts={'POPULARITY_SURGE': 3, 'MOMENTUM_CONFIRMATION': 1})
    result = AlertStats(data).build()
    field_names = [f.name for f in result.fields]
    assert any("Alerts Fired" in n for n in field_names)


def test_alert_stats_shows_price_outcomes():
    outcomes = {
        'per_alert': [],
        'aggregate': {
            'pct_1d': {'mean': 2.5, 'positive_rate': 65.0, 'count': 10},
            'pct_4d': {'mean': -0.5, 'positive_rate': 40.0, 'count': 8},
        },
    }
    data = _make_data(price_outcomes=outcomes)
    result = AlertStats(data).build()
    field_names = [f.name for f in result.fields]
    assert any("Price Outcome" in n for n in field_names)


def test_alert_stats_none_rate_shows_na():
    data = _make_data(
        surge_confidence={'total': 2, 'confirmed': 0, 'expired': 0, 'pending': 2, 'rate': None},
    )
    result = AlertStats(data).build()
    # When all are pending, no surge confidence field appears (total > 0 is set)
    # but rate should display "n/a"
    field_values = [f.value for f in result.fields]
    assert any("n/a" in v for v in field_values)

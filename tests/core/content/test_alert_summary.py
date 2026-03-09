"""Tests for rocketstocks.core.content.reports.alert_summary."""
import datetime
import pytest

from rocketstocks.core.content.models import AlertSummaryData, COLOR_INDIGO
from rocketstocks.core.content.reports.alert_summary import (
    AlertSummary,
    EMBED_CHAR_BUDGET,
    _build_field_value,
    _format_line,
)


def _make_alert(ticker="AAPL", alert_type="WATCHLIST_ALERT", alert_data=None):
    return {
        'date': datetime.date.today(),
        'ticker': ticker,
        'alert_type': alert_type,
        'messageid': 123,
        'alert_data': alert_data or {},
    }


def _make_summary(alerts, label="test label"):
    data = AlertSummaryData(
        since_dt=datetime.datetime(2026, 3, 8, 14, 30),
        label=label,
        alerts=alerts,
    )
    return AlertSummary(data=data)


class TestBuildFieldValue:
    def test_joins_lines_normally(self):
        lines = ["• AAPL  +1.2%", "• TSLA  +2.3%"]
        result = _build_field_value(lines)
        assert "• AAPL" in result
        assert "• TSLA" in result

    def test_truncates_at_limit(self):
        lines = ["x" * 100] * 20
        result = _build_field_value(lines, limit=500)
        assert "… +" in result
        assert len(result) <= 510  # small buffer for the truncation line

    def test_single_line_no_truncation(self):
        lines = ["• AAPL  +1.2%  z: 2.1"]
        result = _build_field_value(lines)
        assert result == lines[0]

    def test_empty_lines(self):
        assert _build_field_value([]) == ""


class TestFormatLine:
    def test_market_mover(self):
        data = {'pct_change': 4.2, 'composite_score': 0.87, 'dominant_signal': 'BULL'}
        line = _format_line("NVDA", data, "MARKET_MOVER")
        assert "NVDA" in line
        assert "+4.2%" in line
        assert "0.87" in line
        assert "BULL" in line

    def test_market_alert(self):
        data = {'pct_change': -1.5, 'composite_score': 0.55, 'dominant_signal': 'BEAR'}
        line = _format_line("SPY", data, "MARKET_ALERT")
        assert "SPY" in line
        assert "-1.5%" in line

    def test_watchlist_alert(self):
        data = {'pct_change': 1.2, 'zscore': 2.1}
        line = _format_line("AAPL", data, "WATCHLIST_ALERT")
        assert "AAPL" in line
        assert "+1.2%" in line
        assert "z: 2.1" in line

    def test_earnings_alert(self):
        data = {'pct_change': -3.0, 'zscore': -1.5}
        line = _format_line("MSFT", data, "EARNINGS_ALERT")
        assert "MSFT" in line
        assert "-3.0%" in line
        assert "z: -1.5" in line

    def test_popularity_surge(self):
        data = {'current_rank': 5, 'rank_change': 120, 'mention_ratio': 4.2}
        line = _format_line("GME", data, "POPULARITY_SURGE")
        assert "GME" in line
        assert "▲120" in line
        assert "×4.2" in line

    def test_momentum_confirmation(self):
        data = {'price_change_since_flag': 2.1}
        line = _format_line("TSLA", data, "MOMENTUM_CONFIRMATION")
        assert "TSLA" in line
        assert "+2.1%" in line
        assert "since flag" in line

    def test_missing_fields_handled_gracefully(self):
        line = _format_line("UNKNOWN", {}, "WATCHLIST_ALERT")
        assert "UNKNOWN" in line
        assert "n/a" in line

    def test_unknown_alert_type_fallback(self):
        line = _format_line("XYZ", {}, "SOME_FUTURE_TYPE")
        assert "XYZ" in line


class TestAlertSummaryBuild:
    def test_empty_alerts_returns_no_alerts_message(self):
        summary = _make_summary([])
        spec = summary.build()
        assert "No alerts found" in spec.description
        assert spec.color == COLOR_INDIGO
        assert spec.timestamp is True

    def test_title_includes_label(self):
        summary = _make_summary([], label="since last close (Mar 07)")
        spec = summary.build()
        assert "since last close (Mar 07)" in spec.title

    def test_groups_by_alert_type(self):
        alerts = [
            _make_alert("AAPL", "WATCHLIST_ALERT", {'pct_change': 1.0, 'zscore': 2.0}),
            _make_alert("MSFT", "WATCHLIST_ALERT", {'pct_change': 2.0, 'zscore': 1.5}),
            _make_alert("NVDA", "MARKET_MOVER", {'pct_change': 4.0, 'composite_score': 0.9, 'dominant_signal': 'BULL'}),
        ]
        spec = _make_summary(alerts).build()
        field_names = [f.name for f in spec.fields]
        assert any("Watchlist Alerts" in n for n in field_names)
        assert any("Market Movers" in n for n in field_names)

    def test_field_name_includes_count(self):
        alerts = [
            _make_alert("AAPL", "WATCHLIST_ALERT"),
            _make_alert("MSFT", "WATCHLIST_ALERT"),
        ]
        spec = _make_summary(alerts).build()
        field_names = [f.name for f in spec.fields]
        assert any("(2)" in n for n in field_names)

    def test_skips_empty_alert_types(self):
        alerts = [_make_alert("AAPL", "MARKET_MOVER", {'pct_change': 1.0})]
        spec = _make_summary(alerts).build()
        field_names = [f.name for f in spec.fields]
        assert not any("Watchlist Alerts" in n for n in field_names)

    def test_all_known_alert_types_rendered(self):
        alerts = [
            _make_alert("T1", "MARKET_MOVER", {'pct_change': 1.0, 'composite_score': 0.8, 'dominant_signal': 'BULL'}),
            _make_alert("T2", "MARKET_ALERT", {'pct_change': 1.0, 'composite_score': 0.7}),
            _make_alert("T3", "WATCHLIST_ALERT", {'pct_change': 1.0, 'zscore': 1.5}),
            _make_alert("T4", "EARNINGS_ALERT", {'pct_change': -2.0, 'zscore': -1.0}),
            _make_alert("T5", "POPULARITY_SURGE", {'rank_change': 50, 'mention_ratio': 3.0}),
            _make_alert("T6", "MOMENTUM_CONFIRMATION", {'price_change_since_flag': 1.5}),
        ]
        spec = _make_summary(alerts).build()
        assert len(spec.fields) == 6

    def test_embed_budget_truncation(self):
        # Create alerts distributed across all 6 types so each type produces a
        # large field; together they should exceed EMBED_CHAR_BUDGET.
        types = [
            ("MARKET_MOVER",          {'pct_change': 1.0, 'composite_score': 0.8, 'dominant_signal': 'BULL'}),
            ("MARKET_ALERT",          {'pct_change': 1.0, 'composite_score': 0.7, 'dominant_signal': 'BULL'}),
            ("WATCHLIST_ALERT",       {'pct_change': 1.0, 'zscore': 2.0}),
            ("EARNINGS_ALERT",        {'pct_change': -2.0, 'zscore': -1.5}),
            ("POPULARITY_SURGE",      {'rank_change': 50, 'mention_ratio': 3.0}),
            ("MOMENTUM_CONFIRMATION", {'price_change_since_flag': 1.5}),
        ]
        alerts = []
        # 500 alerts per type → each type's field value will hit the 1000-char
        # truncation; 6 large fields will exceed EMBED_CHAR_BUDGET (5500).
        for alert_type, alert_data in types:
            for i in range(500):
                alerts.append(_make_alert(f"T{i:04d}", alert_type, alert_data))
        spec = _make_summary(alerts).build()
        field_names = [f.name for f in spec.fields]
        assert any("Truncated" in n for n in field_names)

    def test_field_value_within_1000_chars(self):
        # Many alerts in one type should not exceed 1000 chars in field value
        alerts = [
            _make_alert(f"T{i:04d}", "WATCHLIST_ALERT", {'pct_change': 1.0, 'zscore': 2.0})
            for i in range(100)
        ]
        spec = _make_summary(alerts).build()
        for f in spec.fields:
            if "Truncated" not in f.name:
                assert len(f.value) <= 1010  # small buffer for truncation line

    def test_color_is_indigo(self):
        spec = _make_summary([_make_alert()]).build()
        assert spec.color == COLOR_INDIGO

    def test_timestamp_true(self):
        spec = _make_summary([_make_alert()]).build()
        assert spec.timestamp is True

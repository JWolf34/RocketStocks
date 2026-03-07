"""Tests for rocketstocks.data.discord_state.DiscordState."""
import json
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from rocketstocks.data.discord_state import DiscordState


def _make(db=None):
    return DiscordState(db=db or MagicMock())


class TestGetScreenerMessageId:
    def test_returns_id_when_found(self):
        db = MagicMock()
        db.select.return_value = ("123456",)
        ds = _make(db)
        result = ds.get_screener_message_id("GAINER")
        assert result == "123456"

    def test_returns_none_when_not_found(self):
        db = MagicMock()
        db.select.return_value = None
        ds = _make(db)
        result = ds.get_screener_message_id("GAINER")
        assert result is None

    def test_uses_report_suffix_in_where(self):
        db = MagicMock()
        db.select.return_value = None
        ds = _make(db)
        ds.get_screener_message_id("GAINER")
        call_kwargs = db.select.call_args[1]
        where = call_kwargs["where_conditions"]
        assert any("GAINER_REPORT" in str(v) for _, v in where)


class TestUpdateScreenerMessageId:
    def test_calls_db_update(self):
        db = MagicMock()
        ds = _make(db)
        ds.update_screener_message_id("999", "GAINER")
        db.update.assert_called_once()
        call_kwargs = db.update.call_args[1]
        assert call_kwargs["table"] == "reports"


class TestInsertScreenerMessageId:
    def test_calls_db_insert(self):
        db = MagicMock()
        db.get_table_columns.return_value = ["type", "messageid"]
        ds = _make(db)
        ds.insert_screener_message_id("999", "GAINER")
        db.insert.assert_called_once()
        call_kwargs = db.insert.call_args[1]
        assert call_kwargs["table"] == "reports"


class TestAlertMessageId:
    @pytest.mark.asyncio
    async def test_get_alert_message_id_returns_id(self):
        db = MagicMock()
        db.select.return_value = ("777",)
        ds = _make(db)
        result = await ds.get_alert_message_id("2024-01-01", "AAPL", "VOLUME_MOVER")
        assert result == "777"

    @pytest.mark.asyncio
    async def test_get_alert_message_id_returns_none_when_missing(self):
        db = MagicMock()
        db.select.return_value = None
        ds = _make(db)
        assert await ds.get_alert_message_id("2024-01-01", "AAPL", "VOLUME_MOVER") is None

    @pytest.mark.asyncio
    async def test_get_alert_message_data_deserializes_string(self):
        db = MagicMock()
        payload = {"pct_change": 5.0}
        db.select.return_value = (json.dumps(payload),)
        ds = _make(db)
        result = await ds.get_alert_message_data("2024-01-01", "AAPL", "VOLUME_MOVER")
        # Returns the raw stored value (caller deserializes)
        assert json.dumps(payload) in result or result == json.dumps(payload)

    @pytest.mark.asyncio
    async def test_insert_alert_message_id_calls_db_insert(self):
        db = MagicMock()
        db.get_table_columns.return_value = ["date", "ticker", "alert_type", "messageid", "alert_data"]
        ds = _make(db)
        await ds.insert_alert_message_id("2024-01-01", "AAPL", "VOLUME_MOVER", "888", {"pct_change": 5.0})
        db.insert.assert_called_once()
        call_kwargs = db.insert.call_args[1]
        assert call_kwargs["table"] == "alerts"

    @pytest.mark.asyncio
    async def test_update_alert_message_data_calls_db_update(self):
        db = MagicMock()
        ds = _make(db)
        await ds.update_alert_message_data("2024-01-01", "AAPL", "VOLUME_MOVER", "888", {"pct_change": 6.0})
        db.update.assert_called_once()
        call_kwargs = db.update.call_args[1]
        assert call_kwargs["table"] == "alerts"


class TestGetRecentAlertsForTicker:
    def test_returns_empty_list_when_no_rows(self):
        db = MagicMock()
        db.select.return_value = []
        ds = _make(db)
        result = ds.get_recent_alerts_for_ticker("AAPL")
        assert result == []

    def test_returns_empty_list_when_db_returns_none(self):
        db = MagicMock()
        db.select.return_value = None
        ds = _make(db)
        result = ds.get_recent_alerts_for_ticker("AAPL")
        assert result == []

    def test_returns_rows_for_ticker_today(self):
        import datetime
        today = datetime.date.today()
        db = MagicMock()
        db.select.return_value = [
            (today, 'EARNINGS_MOVER', '111'),
            (today, 'WATCHLIST_MOVER', '222'),
        ]
        ds = _make(db)
        result = ds.get_recent_alerts_for_ticker("AAPL")
        assert len(result) == 2
        assert result[0] == (today, 'EARNINGS_MOVER', '111')

    def test_queries_correct_table_and_fields(self):
        db = MagicMock()
        db.select.return_value = []
        ds = _make(db)
        ds.get_recent_alerts_for_ticker("TSLA")
        call_kwargs = db.select.call_args[1]
        assert call_kwargs['table'] == 'alerts'
        assert 'date' in call_kwargs['fields']
        assert 'alert_type' in call_kwargs['fields']
        assert 'messageid' in call_kwargs['fields']

    def test_where_conditions_include_ticker_and_today(self):
        import datetime
        db = MagicMock()
        db.select.return_value = []
        ds = _make(db)
        ds.get_recent_alerts_for_ticker("TSLA")
        call_kwargs = db.select.call_args[1]
        where = call_kwargs['where_conditions']
        tickers_in_where = [v for _, v in where if v == 'TSLA']
        dates_in_where = [v for _, v in where if v == datetime.date.today()]
        assert tickers_in_where, "ticker not found in where_conditions"
        assert dates_in_where, "today's date not found in where_conditions"


class TestVolumeMessageId:
    def test_get_returns_id(self):
        db = MagicMock()
        db.select.return_value = ("555",)
        ds = _make(db)
        assert ds.get_volume_message_id() == "555"

    def test_get_returns_none_when_missing(self):
        db = MagicMock()
        db.select.return_value = None
        ds = _make(db)
        assert ds.get_volume_message_id() is None

    def test_update_calls_db_update(self):
        db = MagicMock()
        ds = _make(db)
        ds.update_volume_message_id("444")
        db.update.assert_called_once()

"""Tests for rocketstocks.data.discord_state.DiscordState."""
import datetime
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


class TestInsertAlertMessageIdFields:
    @pytest.mark.asyncio
    async def test_uses_explicit_fields_not_db_columns(self):
        """insert_alert_message_id must NOT call get_table_columns (created_at should use DB DEFAULT)."""
        db = MagicMock()
        ds = _make(db)
        await ds.insert_alert_message_id("2024-01-01", "AAPL", "WATCHLIST_ALERT", "888", {"pct_change": 5.0})
        db.get_table_columns.assert_not_called()
        call_kwargs = db.insert.call_args[1]
        assert call_kwargs['fields'] == ['date', 'ticker', 'alert_type', 'messageid', 'alert_data']


class TestGetAlertsSince:
    def _row(self, ticker, alert_type, alert_data, created_at=None):
        return (
            datetime.date.today(),
            ticker,
            alert_type,
            111,
            json.dumps(alert_data),
            created_at,
        )

    def test_returns_all_rows_when_midnight_time(self):
        db = MagicMock()
        db.select.return_value = [
            self._row("AAPL", "WATCHLIST_ALERT", {"pct_change": 1.0}),
            self._row("TSLA", "MARKET_MOVER", {"pct_change": 2.0}),
        ]
        ds = _make(db)
        since = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        result = ds.get_alerts_since(since)
        assert len(result) == 2

    def test_filters_by_created_at_when_time_specified(self):
        db = MagicMock()
        cutoff = datetime.datetime(2026, 3, 8, 14, 30)
        early = datetime.datetime(2026, 3, 8, 13, 0)
        late = datetime.datetime(2026, 3, 8, 15, 0)
        db.select.return_value = [
            self._row("AAPL", "WATCHLIST_ALERT", {}, created_at=early),
            self._row("TSLA", "WATCHLIST_ALERT", {}, created_at=late),
        ]
        ds = _make(db)
        result = ds.get_alerts_since(cutoff)
        tickers = [r['ticker'] for r in result]
        assert "TSLA" in tickers
        assert "AAPL" not in tickers

    def test_null_created_at_always_included_when_time_specified(self):
        db = MagicMock()
        cutoff = datetime.datetime(2026, 3, 8, 14, 30)
        db.select.return_value = [
            self._row("AAPL", "WATCHLIST_ALERT", {}, created_at=None),
        ]
        ds = _make(db)
        result = ds.get_alerts_since(cutoff)
        assert len(result) == 1
        assert result[0]['ticker'] == "AAPL"

    def test_deserializes_alert_data_from_string(self):
        db = MagicMock()
        db.select.return_value = [
            self._row("AAPL", "WATCHLIST_ALERT", {"pct_change": 3.5}),
        ]
        ds = _make(db)
        since = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        result = ds.get_alerts_since(since)
        assert result[0]['alert_data'] == {"pct_change": 3.5}

    def test_returns_empty_list_when_no_rows(self):
        db = MagicMock()
        db.select.return_value = []
        ds = _make(db)
        result = ds.get_alerts_since(datetime.datetime(2026, 3, 8))
        assert result == []

    def test_returns_empty_list_when_db_returns_none(self):
        db = MagicMock()
        db.select.return_value = None
        ds = _make(db)
        result = ds.get_alerts_since(datetime.datetime(2026, 3, 8))
        assert result == []

    def test_queries_correct_table_with_date_filter(self):
        db = MagicMock()
        db.select.return_value = []
        ds = _make(db)
        since = datetime.datetime(2026, 3, 5)
        ds.get_alerts_since(since)
        call_kwargs = db.select.call_args[1]
        assert call_kwargs['table'] == 'alerts'
        where = call_kwargs['where_conditions']
        assert any(v == since.date() for _, op, v in where)

    def test_result_dict_has_expected_keys(self):
        db = MagicMock()
        db.select.return_value = [
            self._row("AAPL", "WATCHLIST_ALERT", {"pct_change": 1.0}),
        ]
        ds = _make(db)
        result = ds.get_alerts_since(datetime.datetime(2026, 3, 8))
        assert set(result[0].keys()) == {'date', 'ticker', 'alert_type', 'messageid', 'alert_data'}


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

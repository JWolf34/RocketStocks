"""Tests for rocketstocks.data.discord_state.DiscordState."""
import datetime
import json
from unittest.mock import AsyncMock, MagicMock

from psycopg.types.json import Json

import pytest

from rocketstocks.data.discord_state import DiscordState


def _make(db=None):
    if db is None:
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
    return DiscordState(db=db)


class TestGetScreenerMessageId:
    async def test_returns_id_when_found(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=("123456",))
        ds = _make(db)
        result = await ds.get_screener_message_id("GAINER")
        assert result == "123456"

    async def test_returns_none_when_not_found(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        result = await ds.get_screener_message_id("GAINER")
        assert result is None

    async def test_uses_report_suffix_in_query(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.get_screener_message_id("GAINER")
        db.execute.assert_called_once()
        call_args = db.execute.call_args
        # Second positional arg is the params list
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get('params', call_args[0][1])
        assert any("GAINER_REPORT" in str(p) for p in params)


class TestUpdateScreenerMessageId:
    async def test_calls_db_execute(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.update_screener_message_id("999", "GAINER")
        db.execute.assert_called_once()

    async def test_sql_targets_reports_table(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.update_screener_message_id("999", "GAINER")
        sql = db.execute.call_args[0][0]
        assert "reports" in sql.lower()

    async def test_passes_message_id_and_type_in_params(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.update_screener_message_id("999", "GAINER")
        params = db.execute.call_args[0][1]
        assert "999" in params
        assert "GAINER_REPORT" in params


class TestInsertScreenerMessageId:
    async def test_calls_db_execute(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.insert_screener_message_id("999", "GAINER")
        db.execute.assert_called_once()

    async def test_sql_contains_insert_into_reports(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.insert_screener_message_id("999", "GAINER")
        sql = db.execute.call_args[0][0]
        assert "INSERT INTO reports" in sql

    async def test_passes_type_and_message_id_in_params(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.insert_screener_message_id("999", "GAINER")
        params = db.execute.call_args[0][1]
        assert "999" in params
        assert "GAINER_REPORT" in params

    async def test_no_get_table_columns_call(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.insert_screener_message_id("999", "GAINER")
        db.get_table_columns.assert_not_called()


class TestAlertMessageId:
    async def test_get_alert_message_id_returns_id(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=("777",))
        ds = _make(db)
        result = await ds.get_alert_message_id("2024-01-01", "AAPL", "VOLUME_MOVER")
        assert result == "777"

    async def test_get_alert_message_id_returns_none_when_missing(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        assert await ds.get_alert_message_id("2024-01-01", "AAPL", "VOLUME_MOVER") is None

    async def test_get_alert_message_data_returns_dict_directly(self):
        db = MagicMock()
        payload = {"pct_change": 5.0}
        db.execute = AsyncMock(return_value=(payload,))
        ds = _make(db)
        result = await ds.get_alert_message_data("2024-01-01", "AAPL", "VOLUME_MOVER")
        assert result == {"pct_change": 5.0}

    async def test_get_alert_message_data_returns_none_when_missing(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        result = await ds.get_alert_message_data("2024-01-01", "AAPL", "VOLUME_MOVER")
        assert result is None

    async def test_insert_alert_message_id_calls_db_execute(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.insert_alert_message_id("2024-01-01", "AAPL", "VOLUME_MOVER", "888", {"pct_change": 5.0})
        db.execute.assert_called_once()

    async def test_insert_alert_message_id_sql_targets_alerts(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.insert_alert_message_id("2024-01-01", "AAPL", "VOLUME_MOVER", "888", {"pct_change": 5.0})
        sql = db.execute.call_args[0][0]
        assert "alerts" in sql.lower()

    async def test_update_alert_message_data_calls_db_execute(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.update_alert_message_data("2024-01-01", "AAPL", "VOLUME_MOVER", "888", {"pct_change": 6.0})
        db.execute.assert_called_once()

    async def test_update_alert_message_data_sql_targets_alerts(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.update_alert_message_data("2024-01-01", "AAPL", "VOLUME_MOVER", "888", {"pct_change": 6.0})
        sql = db.execute.call_args[0][0]
        assert "alerts" in sql.lower()

    async def test_update_alert_message_data_passes_alert_data_as_is(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        payload = {"pct_change": 6.0}
        await ds.update_alert_message_data("2024-01-01", "AAPL", "VOLUME_MOVER", "888", payload)
        params = db.execute.call_args[0][1]
        # alert_data is wrapped in Json() for JSONB native handling, not json.dumps'd
        assert any(isinstance(p, Json) and p.obj == payload for p in params)


class TestGetRecentAlertsForTicker:
    async def test_returns_empty_list_when_no_rows(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[])
        ds = _make(db)
        result = await ds.get_recent_alerts_for_ticker("AAPL")
        assert result == []

    async def test_returns_empty_list_when_db_returns_none(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        result = await ds.get_recent_alerts_for_ticker("AAPL")
        assert result == []

    async def test_returns_rows_for_ticker_today(self):
        today = datetime.date.today()
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            (today, 'EARNINGS_MOVER', '111'),
            (today, 'WATCHLIST_MOVER', '222'),
        ])
        ds = _make(db)
        result = await ds.get_recent_alerts_for_ticker("AAPL")
        assert len(result) == 2
        assert result[0] == (today, 'EARNINGS_MOVER', '111')

    async def test_queries_alerts_table_with_ticker_and_today(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[])
        ds = _make(db)
        await ds.get_recent_alerts_for_ticker("TSLA")
        db.execute.assert_called_once()
        sql = db.execute.call_args[0][0]
        params = db.execute.call_args[0][1]
        assert "alerts" in sql.lower()
        assert "TSLA" in params
        assert datetime.date.today() in params


class TestInsertAlertMessageIdFields:
    async def test_sql_contains_all_required_columns(self):
        """insert_alert_message_id must INSERT all five fields."""
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.insert_alert_message_id("2024-01-01", "AAPL", "WATCHLIST_ALERT", "888", {"pct_change": 5.0})
        sql = db.execute.call_args[0][0]
        assert "date" in sql
        assert "ticker" in sql
        assert "alert_type" in sql
        assert "messageid" in sql
        assert "alert_data" in sql

    async def test_no_get_table_columns_call(self):
        """insert_alert_message_id must NOT call get_table_columns."""
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.insert_alert_message_id("2024-01-01", "AAPL", "WATCHLIST_ALERT", "888", {"pct_change": 5.0})
        db.get_table_columns.assert_not_called()

    async def test_alert_data_passed_as_dict_not_json_string(self):
        """alert_data must be passed as-is (JSONB native), not serialized via json.dumps."""
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        payload = {"pct_change": 5.0}
        await ds.insert_alert_message_id("2024-01-01", "AAPL", "WATCHLIST_ALERT", "888", payload)
        params = db.execute.call_args[0][1]
        # alert_data is wrapped in Json() for JSONB native handling, not json.dumps'd
        assert any(isinstance(p, Json) and p.obj == payload for p in params)
        # Confirm no JSON string was smuggled in
        assert not any(isinstance(p, str) and '"pct_change"' in p for p in params)


class TestGetAlertsSince:
    def _row(self, ticker, alert_type, alert_data, created_at=None):
        return (
            datetime.date.today(),
            ticker,
            alert_type,
            111,
            alert_data,  # dict (JSONB native) — no json.dumps
            created_at,
        )

    async def test_returns_all_rows_when_midnight_time(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            self._row("AAPL", "WATCHLIST_ALERT", {"pct_change": 1.0}),
            self._row("TSLA", "MARKET_MOVER", {"pct_change": 2.0}),
        ])
        ds = _make(db)
        since = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        result = await ds.get_alerts_since(since)
        assert len(result) == 2

    async def test_filters_by_created_at_when_time_specified(self):
        db = MagicMock()
        cutoff = datetime.datetime(2026, 3, 8, 14, 30)
        early = datetime.datetime(2026, 3, 8, 13, 0)
        late = datetime.datetime(2026, 3, 8, 15, 0)
        db.execute = AsyncMock(return_value=[
            self._row("AAPL", "WATCHLIST_ALERT", {}, created_at=early),
            self._row("TSLA", "WATCHLIST_ALERT", {}, created_at=late),
        ])
        ds = _make(db)
        result = await ds.get_alerts_since(cutoff)
        tickers = [r['ticker'] for r in result]
        assert "TSLA" in tickers
        assert "AAPL" not in tickers

    async def test_filters_correctly_with_timezone_aware_created_at(self):
        """Chicago-aware created_at timestamps must be normalized to UTC before comparison."""
        db = MagicMock()
        chicago = datetime.timezone(datetime.timedelta(hours=-5))
        cutoff = datetime.datetime(2026, 3, 8, 14, 30)  # naive UTC: 9:30 AM ET
        # 9:30 AM Chicago == 14:30 UTC → exactly at cutoff, should be included
        at_open = datetime.datetime(2026, 3, 8, 9, 30, tzinfo=chicago)
        # 9:29 AM Chicago == 14:29 UTC → before cutoff, should be excluded
        before_open = datetime.datetime(2026, 3, 8, 9, 29, tzinfo=chicago)
        db.execute = AsyncMock(return_value=[
            self._row("TSLA", "WATCHLIST_ALERT", {}, created_at=at_open),
            self._row("AAPL", "WATCHLIST_ALERT", {}, created_at=before_open),
        ])
        ds = _make(db)
        result = await ds.get_alerts_since(cutoff)
        tickers = [r['ticker'] for r in result]
        assert "TSLA" in tickers
        assert "AAPL" not in tickers

    async def test_null_created_at_always_included_when_time_specified(self):
        db = MagicMock()
        cutoff = datetime.datetime(2026, 3, 8, 14, 30)
        db.execute = AsyncMock(return_value=[
            self._row("AAPL", "WATCHLIST_ALERT", {}, created_at=None),
        ])
        ds = _make(db)
        result = await ds.get_alerts_since(cutoff)
        assert len(result) == 1
        assert result[0]['ticker'] == "AAPL"

    async def test_alert_data_dict_passed_through_as_is(self):
        """JSONB alert_data (dict) must be returned directly without json.loads."""
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            self._row("AAPL", "WATCHLIST_ALERT", {"pct_change": 3.5}),
        ])
        ds = _make(db)
        since = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        result = await ds.get_alerts_since(since)
        assert result[0]['alert_data'] == {"pct_change": 3.5}

    async def test_alert_data_string_fallback_deserialized(self):
        """alert_data that arrives as a JSON string should still be deserialized."""
        db = MagicMock()
        payload = {"pct_change": 3.5}
        db.execute = AsyncMock(return_value=[
            (
                datetime.date.today(),
                "AAPL",
                "WATCHLIST_ALERT",
                111,
                json.dumps(payload),  # string fallback
                None,
            )
        ])
        ds = _make(db)
        since = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        result = await ds.get_alerts_since(since)
        assert result[0]['alert_data'] == {"pct_change": 3.5}

    async def test_returns_empty_list_when_no_rows(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[])
        ds = _make(db)
        result = await ds.get_alerts_since(datetime.datetime(2026, 3, 8))
        assert result == []

    async def test_returns_empty_list_when_db_returns_none(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        result = await ds.get_alerts_since(datetime.datetime(2026, 3, 8))
        assert result == []

    async def test_queries_with_date_filter(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[])
        ds = _make(db)
        since = datetime.datetime(2026, 3, 5)
        await ds.get_alerts_since(since)
        db.execute.assert_called_once()
        sql = db.execute.call_args[0][0]
        params = db.execute.call_args[0][1]
        assert "alerts" in sql.lower()
        assert since.date() in params

    async def test_result_dict_has_expected_keys(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            self._row("AAPL", "WATCHLIST_ALERT", {"pct_change": 1.0}),
        ])
        ds = _make(db)
        result = await ds.get_alerts_since(datetime.datetime(2026, 3, 8))
        assert set(result[0].keys()) == {'date', 'ticker', 'alert_type', 'messageid', 'alert_data'}


class TestVolumeMessageId:
    async def test_get_returns_id(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=("555",))
        ds = _make(db)
        assert await ds.get_volume_message_id() == "555"

    async def test_get_returns_none_when_missing(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        assert await ds.get_volume_message_id() is None

    async def test_update_calls_db_execute(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.update_volume_message_id("444")
        db.execute.assert_called_once()

    async def test_update_sql_targets_unusual_volume_report(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ds = _make(db)
        await ds.update_volume_message_id("444")
        params = db.execute.call_args[0][1]
        assert "UNUSUAL_VOLUME_REPORT" in params
        assert "444" in params

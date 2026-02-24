"""Tests for rocketstocks.bot.senders.alert_sender."""
import datetime
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


_TZ = datetime.timezone.utc
_TODAY = datetime.date.today()


def _make_alert(ticker="AAPL", alert_type="VOLUME_MOVER", build_text="alert text"):
    alert = MagicMock()
    alert.ticker = ticker
    alert.alert_type = alert_type
    alert.build_alert.return_value = build_text
    # Simulate an alert that hasn't implemented build_embed_spec — forces plain-text fallback
    alert.build_embed_spec.side_effect = NotImplementedError
    alert.alert_data = {"pct_change": 5.0}
    alert.override_and_edit.return_value = False
    return alert


def _make_channel(sent_id=777):
    channel = AsyncMock()
    sent_msg = AsyncMock()
    sent_msg.id = sent_id
    channel.send.return_value = sent_msg
    return channel, sent_msg


def _make_dstate(message_id=None, alert_data=None):
    dstate = MagicMock()
    dstate.get_alert_message_id.return_value = message_id
    dstate.get_alert_message_data.return_value = alert_data or json.dumps({"pct_change": 3.0})
    return dstate


def _patch_date_utils():
    """Return a context manager that patches date_utils.timezone() with UTC."""
    mock_du = MagicMock()
    mock_du.timezone.return_value = _TZ
    return patch("rocketstocks.bot.senders.alert_sender.date_utils", mock_du)


class TestSendAlertNewAlert:
    async def test_sends_new_message_when_no_existing(self):
        from rocketstocks.bot.senders.alert_sender import send_alert
        channel, sent_msg = _make_channel()
        alert = _make_alert()
        dstate = _make_dstate(message_id=None)

        with _patch_date_utils():
            result = await send_alert(alert, channel, dstate)

        channel.send.assert_awaited_once()
        dstate.insert_alert_message_id.assert_called_once()
        assert result is sent_msg

    async def test_inserts_with_correct_fields(self):
        from rocketstocks.bot.senders.alert_sender import send_alert
        channel, sent_msg = _make_channel(sent_id=555)
        alert = _make_alert(ticker="TSLA", alert_type="WATCHLIST_MOVER")
        dstate = _make_dstate(message_id=None)

        with _patch_date_utils():
            await send_alert(alert, channel, dstate)

        call_kwargs = dstate.insert_alert_message_id.call_args[1]
        assert call_kwargs["ticker"] == "TSLA"
        assert call_kwargs["alert_type"] == "WATCHLIST_MOVER"
        assert call_kwargs["message_id"] == 555


class TestSendAlertExistingNoOverride:
    async def test_returns_none_when_no_override(self):
        from rocketstocks.bot.senders.alert_sender import send_alert
        channel, _ = _make_channel()
        alert = _make_alert()
        alert.override_and_edit.return_value = False
        dstate = _make_dstate(message_id=123)

        with _patch_date_utils():
            result = await send_alert(alert, channel, dstate)

        channel.send.assert_not_awaited()
        assert result is None


class TestSendAlertExistingWithOverride:
    async def test_sends_update_when_override(self):
        from rocketstocks.bot.senders.alert_sender import send_alert
        channel, sent_msg = _make_channel()
        alert = _make_alert()
        alert.override_and_edit.return_value = True
        dstate = _make_dstate(message_id=123)

        prev_message = AsyncMock()
        prev_message.created_at = MagicMock()
        tz_aware = MagicMock()
        tz_aware.strftime.return_value = "10:30 AM"
        tz_aware.tzname.return_value = "UTC"
        prev_message.created_at.astimezone.return_value = tz_aware
        prev_message.jump_url = "https://discord.com/channels/123/456/123"
        channel.fetch_message.return_value = prev_message

        with _patch_date_utils():
            result = await send_alert(alert, channel, dstate)

        channel.send.assert_awaited_once()
        dstate.update_alert_message_data.assert_called_once()
        assert result is sent_msg

    async def test_update_message_contains_previous_link(self):
        from rocketstocks.bot.senders.alert_sender import send_alert
        channel, _ = _make_channel()
        alert = _make_alert()
        alert.override_and_edit.return_value = True
        dstate = _make_dstate(message_id=123)

        prev_message = AsyncMock()
        prev_message.created_at = MagicMock()
        tz_aware = MagicMock()
        tz_aware.strftime.return_value = "10:30 AM"
        tz_aware.tzname.return_value = "UTC"
        prev_message.created_at.astimezone.return_value = tz_aware
        prev_message.jump_url = "https://discord.com/jump/to/prev"
        channel.fetch_message.return_value = prev_message

        with _patch_date_utils():
            await send_alert(alert, channel, dstate)

        # In plain-text fallback path, message is the first positional argument
        sent_args, sent_kwargs = channel.send.call_args
        sent_text = sent_args[0] if sent_args else ""
        assert "https://discord.com/jump/to/prev" in sent_text

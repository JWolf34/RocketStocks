"""Tests for rocketstocks.bot.senders.alert_sender."""
import datetime
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rocketstocks.core.content.models import COLOR_GREEN, EmbedSpec


_TZ = datetime.timezone.utc
_TODAY = datetime.date.today()


def _make_alert(ticker="AAPL", alert_type="VOLUME_MOVER"):
    alert = MagicMock()
    alert.ticker = ticker
    alert.alert_type = alert_type
    alert.build.return_value = EmbedSpec(
        title=f"🚨 {alert_type}: {ticker}",
        description="Alert description",
        color=COLOR_GREEN,
    )
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
    dstate.get_alert_message_id = AsyncMock(return_value=message_id)
    dstate.get_alert_message_data = AsyncMock(return_value=alert_data or json.dumps({"pct_change": 3.0}))
    dstate.insert_alert_message_id = AsyncMock()
    dstate.update_alert_message_data = AsyncMock()
    return dstate


def _patch_date_utils():
    """Return a context manager that patches timezone() with UTC."""
    return patch("rocketstocks.bot.senders.alert_sender.timezone", return_value=_TZ)


class TestSendAlertNewAlert:
    async def test_sends_new_message_when_no_existing(self):
        from rocketstocks.bot.senders.alert_sender import send_alert
        channel, sent_msg = _make_channel()
        alert = _make_alert()
        dstate = _make_dstate(message_id=None)

        with _patch_date_utils():
            result = await send_alert(alert, channel, dstate)

        channel.send.assert_awaited_once()
        call_kwargs = channel.send.call_args.kwargs
        assert "embed" in call_kwargs
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
        call_kwargs = channel.send.call_args.kwargs
        assert "embed" in call_kwargs
        dstate.update_alert_message_data.assert_called_once()
        assert result is sent_msg

    async def test_update_embed_contains_previous_link(self):
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

        sent_kwargs = channel.send.call_args.kwargs
        sent_embed = sent_kwargs["embed"]
        assert "https://discord.com/jump/to/prev" in sent_embed.description


class TestSendAlertMomentumConfirmation:
    async def test_surge_link_without_duration_when_no_flagged_at(self):
        """Test that surge link is added without duration if surge_flagged_at is None."""
        from rocketstocks.bot.senders.alert_sender import send_alert
        channel, sent_msg = _make_channel()
        
        # Create a momentum alert without surge_flagged_at
        alert = _make_alert(ticker="AAPL", alert_type="MOMENTUM_CONFIRMATION")
        alert.data = MagicMock()
        alert.data.surge_alert_message_id = 123456789
        alert.data.surge_flagged_at = None
        
        dstate = _make_dstate(message_id=None)
        channel.guild.id = 9999
        channel.id = 8888

        with _patch_date_utils():
            await send_alert(alert, channel, dstate)

        sent_kwargs = channel.send.call_args.kwargs
        sent_embed = sent_kwargs["embed"]
        assert "[📡 View original surge alert]" in sent_embed.description
        assert "ago)" not in sent_embed.description  # No duration text

    async def test_surge_link_with_duration_when_flagged_at_provided(self):
        """Test that surge link includes duration when surge_flagged_at is provided."""
        from rocketstocks.bot.senders.alert_sender import send_alert
        channel, sent_msg = _make_channel()
        
        # Create a momentum alert with surge_flagged_at
        alert = _make_alert(ticker="AAPL", alert_type="MOMENTUM_CONFIRMATION")
        alert.data = MagicMock()
        alert.data.surge_alert_message_id = 123456789
        alert.data.surge_flagged_at = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=2)
        
        dstate = _make_dstate(message_id=None)
        channel.guild.id = 9999
        channel.id = 8888

        def mock_format_duration(dt):
            return "2 hours ago"

        with _patch_date_utils():
            with patch("rocketstocks.bot.senders.alert_sender.format_duration_since", side_effect=mock_format_duration):
                await send_alert(alert, channel, dstate)

        sent_kwargs = channel.send.call_args.kwargs
        sent_embed = sent_kwargs["embed"]
        assert "[📡 View original surge alert (2 hours ago)]" in sent_embed.description

    async def test_surge_link_without_message_id_not_added(self):
        """Test that surge link is not added if surge_alert_message_id is None."""
        from rocketstocks.bot.senders.alert_sender import send_alert
        channel, sent_msg = _make_channel()
        
        # Create a momentum alert without surge message ID
        alert = _make_alert(ticker="AAPL", alert_type="MOMENTUM_CONFIRMATION")
        alert.data = MagicMock()
        alert.data.surge_alert_message_id = None
        alert.data.surge_flagged_at = datetime.datetime.now(tz=datetime.timezone.utc)
        
        dstate = _make_dstate(message_id=None)
        channel.guild.id = 9999
        channel.id = 8888

        with _patch_date_utils():
            await send_alert(alert, channel, dstate)

        sent_kwargs = channel.send.call_args.kwargs
        sent_embed = sent_kwargs["embed"]
        assert "[📡 View original surge alert" not in sent_embed.description

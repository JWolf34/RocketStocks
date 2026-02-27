"""Tests for rocketstocks.bot.senders.report_sender."""
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rocketstocks.core.content.models import COLOR_BLUE, EmbedSpec


def _make_channel():
    channel = AsyncMock()
    sent_msg = AsyncMock()
    sent_msg.id = 12345
    channel.send.return_value = sent_msg
    return channel, sent_msg


def _make_content_plaintext(report_text="report body", screener_type="GAINER"):
    """Content mock that falls back to plaintext (no embed support)."""
    content = MagicMock()
    content.build_report.return_value = report_text
    content.screener_type = screener_type
    content.build_embed_spec.side_effect = NotImplementedError
    return content


def _make_content_embed(screener_type="GAINER"):
    """Content mock with embed support."""
    content = MagicMock()
    content.screener_type = screener_type
    content.build_embed_spec.return_value = EmbedSpec(
        title="Test Title",
        description="Test description",
        color=COLOR_BLUE,
    )
    return content


def _make_dstate(message_id=None):
    dstate = MagicMock()
    dstate.get_screener_message_id.return_value = message_id
    return dstate


class TestSendReport:
    @pytest.mark.asyncio
    async def test_sends_to_channel_when_public_plaintext(self):
        from rocketstocks.bot.senders.report_sender import send_report
        channel, sent_msg = _make_channel()
        content = _make_content_plaintext()
        result = await send_report(content, channel, visibility="public")
        channel.send.assert_awaited_once()
        assert result is sent_msg

    @pytest.mark.asyncio
    async def test_sends_to_channel_when_public_embed(self):
        from rocketstocks.bot.senders.report_sender import send_report
        channel, sent_msg = _make_channel()
        content = _make_content_embed()
        result = await send_report(content, channel, visibility="public")
        channel.send.assert_awaited_once()
        call_kwargs = channel.send.call_args.kwargs
        assert "embed" in call_kwargs
        assert result is sent_msg

    @pytest.mark.asyncio
    async def test_sends_to_dm_when_private(self):
        from rocketstocks.bot.senders.report_sender import send_report
        channel, _ = _make_channel()
        content = _make_content_plaintext()
        interaction = AsyncMock()
        interaction.user = AsyncMock()
        dm_msg = AsyncMock()
        interaction.user.send.return_value = dm_msg
        result = await send_report(content, channel, interaction=interaction, visibility="private")
        interaction.user.send.assert_awaited_once()
        channel.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_appends_double_newline_to_report_plaintext(self):
        from rocketstocks.bot.senders.report_sender import send_report
        channel, _ = _make_channel()
        content = _make_content_plaintext("hello")
        await send_report(content, channel)
        sent_text = channel.send.call_args[0][0]
        assert sent_text.endswith("\n\n")

    @pytest.mark.asyncio
    async def test_embed_path_does_not_call_build_report(self):
        from rocketstocks.bot.senders.report_sender import send_report
        channel, _ = _make_channel()
        content = _make_content_embed()
        await send_report(content, channel)
        content.build_report.assert_not_called()


class TestSendScreener:
    @pytest.mark.asyncio
    async def test_inserts_new_message_when_no_existing(self):
        from rocketstocks.bot.senders.report_sender import send_screener
        channel, sent_msg = _make_channel()
        content = _make_content_plaintext(screener_type="gainer")
        dstate = _make_dstate(message_id=None)

        mock_du = MagicMock()
        mock_du.timezone.return_value = datetime.timezone.utc
        with patch("rocketstocks.bot.senders.report_sender.date_utils", mock_du):
            result = await send_screener(content, channel, dstate)

        channel.send.assert_awaited_once()
        dstate.insert_screener_message_id.assert_called_once()
        assert result is sent_msg

    @pytest.mark.asyncio
    async def test_inserts_new_embed_when_no_existing(self):
        from rocketstocks.bot.senders.report_sender import send_screener
        channel, sent_msg = _make_channel()
        content = _make_content_embed(screener_type="gainer")
        dstate = _make_dstate(message_id=None)

        mock_du = MagicMock()
        mock_du.timezone.return_value = datetime.timezone.utc
        with patch("rocketstocks.bot.senders.report_sender.date_utils", mock_du):
            result = await send_screener(content, channel, dstate)

        channel.send.assert_awaited_once()
        call_kwargs = channel.send.call_args.kwargs
        assert "embed" in call_kwargs
        dstate.insert_screener_message_id.assert_called_once()
        assert result is sent_msg

    @pytest.mark.asyncio
    async def test_edits_in_place_for_same_day_message(self):
        from rocketstocks.bot.senders.report_sender import send_screener
        today = datetime.datetime.now()
        channel, _ = _make_channel()
        content = _make_content_plaintext(screener_type="gainer")
        dstate = _make_dstate(message_id=999)

        existing_msg = AsyncMock()
        existing_msg.created_at = MagicMock()
        existing_msg.created_at.astimezone.return_value.date.return_value = today.date()
        channel.fetch_message.return_value = existing_msg

        with patch("rocketstocks.bot.senders.report_sender.date_utils") as mock_du:
            mock_du.timezone.return_value = datetime.timezone.utc
            mock_today = MagicMock()
            mock_today.date.return_value = today.date()
            mock_du.timezone.return_value = datetime.timezone.utc

            with patch("rocketstocks.bot.senders.report_sender.datetime") as mock_dt:
                mock_dt.datetime.now.return_value = mock_today

                result = await send_screener(content, channel, dstate)

        existing_msg.edit.assert_awaited_once()
        assert result is None

    @pytest.mark.asyncio
    async def test_edits_embed_in_place_for_same_day_message(self):
        from rocketstocks.bot.senders.report_sender import send_screener
        today = datetime.datetime.now()
        channel, _ = _make_channel()
        content = _make_content_embed(screener_type="gainer")
        dstate = _make_dstate(message_id=999)

        existing_msg = AsyncMock()
        existing_msg.created_at = MagicMock()
        existing_msg.created_at.astimezone.return_value.date.return_value = today.date()
        channel.fetch_message.return_value = existing_msg

        with patch("rocketstocks.bot.senders.report_sender.date_utils") as mock_du:
            mock_du.timezone.return_value = datetime.timezone.utc
            mock_today = MagicMock()
            mock_today.date.return_value = today.date()
            with patch("rocketstocks.bot.senders.report_sender.datetime") as mock_dt:
                mock_dt.datetime.now.return_value = mock_today
                result = await send_screener(content, channel, dstate)

        existing_msg.edit.assert_awaited_once()
        call_kwargs = existing_msg.edit.call_args.kwargs
        assert "embed" in call_kwargs
        assert result is None

    @pytest.mark.asyncio
    async def test_sends_new_message_for_stale_existing(self):
        from rocketstocks.bot.senders.report_sender import send_screener
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)

        channel, sent_msg = _make_channel()
        content = _make_content_plaintext(screener_type="gainer")
        dstate = _make_dstate(message_id=888)

        existing_msg = AsyncMock()
        existing_msg.created_at = MagicMock()
        existing_msg.created_at.astimezone.return_value.date.return_value = yesterday
        channel.fetch_message.return_value = existing_msg

        with patch("rocketstocks.bot.senders.report_sender.date_utils") as mock_du:
            mock_du.timezone.return_value = datetime.timezone.utc
            with patch("rocketstocks.bot.senders.report_sender.datetime") as mock_dt:
                mock_dt.datetime.now.return_value.date.return_value = today
                result = await send_screener(content, channel, dstate)

        channel.send.assert_awaited_once()
        dstate.update_screener_message_id.assert_called_once()

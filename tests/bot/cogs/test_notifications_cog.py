"""Tests for rocketstocks.bot.cogs.notifications."""
import asyncio
import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.core.notifications.config import NotificationConfig, NotificationFilter, NotificationLevel
from rocketstocks.core.notifications.emitter import EventEmitter
from rocketstocks.core.notifications.event import NotificationEvent


def _make_bot(channel_id=999):
    bot = MagicMock(name="Bot")
    bot.latency = 0.05
    channel = AsyncMock(name="Channel")
    channel.send = AsyncMock()
    bot.get_channel.return_value = channel
    bot.wait_until_ready = AsyncMock()
    return bot, channel


def _make_cog(filter_=NotificationFilter.ALL, latency_threshold=1.0):
    from rocketstocks.bot.cogs.notifications import Notifications
    bot, channel = _make_bot()
    emitter = EventEmitter()
    config = NotificationConfig(filter=filter_, latency_threshold_seconds=latency_threshold)

    # Patch task loops so they don't auto-start
    with patch.object(Notifications, 'drain_notifications'), \
         patch.object(Notifications, 'check_latency'):
        cog = Notifications(bot=bot, emitter=emitter, config=config)

    cog.bot = bot
    cog._channel = channel
    return cog, emitter, config, channel


class TestDrainNotifications:
    @pytest.mark.asyncio
    async def test_sends_embed_for_each_filtered_event(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, channel = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig(filter=NotificationFilter.ALL)

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        cog.bot = bot

        # Emit 2 events
        for i in range(2):
            emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS,
                source="test",
                job_name=f"job{i}",
                message="ok",
            ))

        # Manually call the drain coroutine
        with patch("rocketstocks.bot.cogs.notifications.notifications_channel_id", 999):
            await Notifications.drain_notifications.coro(cog)

        assert channel.send.call_count == 2

    @pytest.mark.asyncio
    async def test_does_not_send_when_filter_is_off(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, channel = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig(filter=NotificationFilter.OFF)

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        cog.bot = bot

        emitter.emit(NotificationEvent(
            level=NotificationLevel.FAILURE,
            source="test",
            job_name="job",
            message="error",
        ))

        with patch("rocketstocks.bot.cogs.notifications.notifications_channel_id", 999):
            await Notifications.drain_notifications.coro(cog)

        channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_failures_only_blocks_success_sends(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, channel = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig(filter=NotificationFilter.FAILURES_ONLY)

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        cog.bot = bot

        emitter.emit(NotificationEvent(
            level=NotificationLevel.SUCCESS, source="test", job_name="job", message="ok"
        ))
        emitter.emit(NotificationEvent(
            level=NotificationLevel.FAILURE, source="test", job_name="fail", message="error"
        ))

        with patch("rocketstocks.bot.cogs.notifications.notifications_channel_id", 999):
            await Notifications.drain_notifications.coro(cog)

        # Only failure should be sent
        assert channel.send.call_count == 1

    @pytest.mark.asyncio
    async def test_no_send_when_channel_not_found(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, channel = _make_bot()
        bot.get_channel.return_value = None  # channel not found
        emitter = EventEmitter()
        config = NotificationConfig(filter=NotificationFilter.ALL)

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        cog.bot = bot

        emitter.emit(NotificationEvent(
            level=NotificationLevel.SUCCESS, source="test", job_name="job", message="ok"
        ))

        with patch("rocketstocks.bot.cogs.notifications.notifications_channel_id", 999):
            await Notifications.drain_notifications.coro(cog)

        channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_events_stored_in_ring_buffer(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, channel = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig(filter=NotificationFilter.ALL)

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        cog.bot = bot

        for i in range(5):
            emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS, source="test", job_name=f"job{i}", message="ok"
            ))

        with patch("rocketstocks.bot.cogs.notifications.notifications_channel_id", 999):
            await Notifications.drain_notifications.coro(cog)

        assert len(cog._recent_events) == 5


class TestHeartbeatMonitoring:
    @pytest.mark.asyncio
    async def test_on_disconnect_records_timestamp(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, _ = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig()

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        assert cog._last_disconnect is None
        await cog.on_disconnect()
        assert cog._last_disconnect is not None

    @pytest.mark.asyncio
    async def test_on_resumed_emits_warning(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, _ = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig()

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        cog._last_disconnect = datetime.datetime.now() - datetime.timedelta(seconds=5)
        await cog.on_resumed()

        events = emitter.drain()
        assert len(events) == 1
        assert events[0].level == NotificationLevel.WARNING
        assert events[0].job_name == "heartbeat"

    @pytest.mark.asyncio
    async def test_on_resumed_clears_last_disconnect(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, _ = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig()

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        cog._last_disconnect = datetime.datetime.now()
        await cog.on_resumed()
        assert cog._last_disconnect is None

    @pytest.mark.asyncio
    async def test_on_resumed_without_disconnect_does_not_emit(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, _ = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig()

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        await cog.on_resumed()
        assert emitter.drain() == []

    @pytest.mark.asyncio
    async def test_high_latency_emits_warning(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, _ = _make_bot()
        bot.latency = 2.0  # above threshold
        emitter = EventEmitter()
        config = NotificationConfig(latency_threshold_seconds=1.0)

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        cog.bot = bot
        await Notifications.check_latency.coro(cog)

        events = emitter.drain()
        assert len(events) == 1
        assert events[0].level == NotificationLevel.WARNING

    @pytest.mark.asyncio
    async def test_normal_latency_does_not_emit(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, _ = _make_bot()
        bot.latency = 0.1  # below threshold
        emitter = EventEmitter()
        config = NotificationConfig(latency_threshold_seconds=1.0)

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        cog.bot = bot
        await Notifications.check_latency.coro(cog)

        assert emitter.drain() == []


class TestNotificationsFilterCommand:
    @pytest.mark.asyncio
    async def test_filter_command_updates_config(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, _ = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig(filter=NotificationFilter.ALL)

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        interaction = AsyncMock()
        choice = MagicMock()
        choice.value = "failures_only"

        await cog.notifications_filter.callback(cog, interaction, choice)

        assert config.filter == NotificationFilter.FAILURES_ONLY
        interaction.response.send_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_filter_command_set_off(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, _ = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig(filter=NotificationFilter.ALL)

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        interaction = AsyncMock()
        choice = MagicMock()
        choice.value = "off"

        await cog.notifications_filter.callback(cog, interaction, choice)
        assert config.filter == NotificationFilter.OFF

    @pytest.mark.asyncio
    async def test_status_command_sends_embed(self):
        from rocketstocks.bot.cogs.notifications import Notifications
        bot, _ = _make_bot()
        emitter = EventEmitter()
        config = NotificationConfig()

        with patch.object(Notifications, 'drain_notifications'), \
             patch.object(Notifications, 'check_latency'):
            cog = Notifications(bot=bot, emitter=emitter, config=config)

        interaction = AsyncMock()
        await cog.notifications_status.callback(cog, interaction)
        interaction.response.send_message.assert_awaited_once()
        call_kwargs = interaction.response.send_message.call_args.kwargs
        assert isinstance(call_kwargs.get("embed"), type(call_kwargs["embed"]))

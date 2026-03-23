"""Tests for rocketstocks.bot.cogs.config."""
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from rocketstocks.data.channel_config import REPORTS, ALERTS, SCREENERS, NOTIFICATIONS, ALL_CONFIG_TYPES


def _make_bot(guild_id=123456):
    bot = MagicMock(name="Bot")
    bot.stock_data = MagicMock(name="StockData")
    bot.stock_data.channel_config = MagicMock(name="ChannelConfigRepository")
    bot.stock_data.channel_config.get_all_for_guild = AsyncMock(return_value={})
    bot.stock_data.channel_config.get_unconfigured_guilds = AsyncMock(return_value=[])
    bot.stock_data.channel_config.upsert_channel = AsyncMock()
    bot.stock_data.bot_settings = MagicMock(name="BotSettingsRepository")
    bot.stock_data.bot_settings.get = AsyncMock(return_value=None)
    bot.stock_data.bot_settings.set = AsyncMock()
    bot.notification_config = MagicMock(name="NotificationConfig")
    guild = MagicMock(name="Guild")
    guild.id = guild_id
    guild.name = "Test Guild"
    bot.guilds = [guild]
    bot.get_guild.return_value = guild
    bot.get_channel.return_value = MagicMock(name="TextChannel", mention="#channel")
    return bot, guild


def _make_cog(bot=None):
    from rocketstocks.bot.cogs.config import Config
    if bot is None:
        bot, _ = _make_bot()
    return Config(bot=bot), bot


def _make_interaction(guild_id=123456):
    interaction = AsyncMock(name="Interaction")
    interaction.guild_id = guild_id
    interaction.response = AsyncMock()
    interaction.followup = AsyncMock()
    return interaction


def _make_guild(guild_id=123456):
    """Return a mock Guild with get_channel and text_channels support."""
    guild = MagicMock(name="Guild")
    guild.id = guild_id
    guild.name = "Test Guild"
    guild.text_channels = []
    return guild


class TestServerSetupCommand:
    @pytest.mark.asyncio
    async def test_setup_sends_channel_view(self):
        from rocketstocks.bot.cogs.config import ChannelSetupView
        cog, _ = _make_cog()
        interaction = _make_interaction()

        await cog.server_setup.callback(cog, interaction)

        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.call_args.kwargs
        assert isinstance(kwargs.get("view"), ChannelSetupView)
        assert kwargs.get("ephemeral") is True

    @pytest.mark.asyncio
    async def test_setup_sends_bot_settings_view_as_followup(self):
        from rocketstocks.bot.cogs.config import BotSettingsView
        cog, _ = _make_cog()
        interaction = _make_interaction()

        await cog.server_setup.callback(cog, interaction)

        interaction.followup.send.assert_awaited_once()
        kwargs = interaction.followup.send.call_args.kwargs
        assert isinstance(kwargs.get("view"), BotSettingsView)
        assert kwargs.get("ephemeral") is True


class TestServerStatusCommand:
    def _make_bot_with_roles(self, guild_id=123456):
        bot, guild = _make_bot(guild_id)
        bot.stock_data.alert_roles = MagicMock()
        bot.stock_data.alert_roles.get_all_for_guild = AsyncMock(return_value={})
        return bot, guild

    @pytest.mark.asyncio
    async def test_shows_mentions_for_configured_channels(self):
        bot, guild = self._make_bot_with_roles()
        cog, _ = _make_cog(bot)
        interaction = _make_interaction()
        interaction.guild = guild

        bot.stock_data.channel_config.get_all_for_guild.return_value = {
            ct: 100 + i for i, ct in enumerate(ALL_CONFIG_TYPES)
        }
        mock_channel = MagicMock()
        mock_channel.mention = "#test"
        bot.get_channel.return_value = mock_channel

        await cog.server_status.callback(cog, interaction)

        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.call_args.kwargs
        embed = kwargs.get("embed")
        assert embed is not None

    @pytest.mark.asyncio
    async def test_shows_not_configured_for_missing_channels(self):
        bot, guild = self._make_bot_with_roles()
        cog, _ = _make_cog(bot)
        interaction = _make_interaction()
        interaction.guild = guild

        # Only reports configured
        bot.stock_data.channel_config.get_all_for_guild.return_value = {REPORTS: 111}
        mock_channel = MagicMock()
        mock_channel.mention = "#reports"
        bot.get_channel.return_value = mock_channel

        await cog.server_status.callback(cog, interaction)

        kwargs = interaction.response.send_message.call_args.kwargs
        embed = kwargs.get("embed")
        assert "Not configured" in embed.description

    @pytest.mark.asyncio
    async def test_shows_channel_and_role_sections(self):
        """server status embed includes Channel Configuration, Alert Subscription Roles, Bot Settings."""
        bot, guild = self._make_bot_with_roles()
        cog, _ = _make_cog(bot)
        interaction = _make_interaction()
        interaction.guild = guild

        bot.stock_data.channel_config.get_all_for_guild.return_value = {}

        await cog.server_status.callback(cog, interaction)

        kwargs = interaction.response.send_message.call_args.kwargs
        embed = kwargs.get("embed")
        assert "Channel Configuration" in embed.description
        assert "Alert Subscription Roles" in embed.description
        assert "Bot Settings" in embed.description

    @pytest.mark.asyncio
    async def test_shows_bot_settings_env_override_badge(self, monkeypatch):
        """When TZ and NOTIFICATION_FILTER are in env, status shows [ENV override]."""
        bot, guild = self._make_bot_with_roles()
        cog, _ = _make_cog(bot)
        interaction = _make_interaction()
        interaction.guild = guild

        monkeypatch.setenv("TZ", "UTC")
        monkeypatch.setenv("NOTIFICATION_FILTER", "off")

        await cog.server_status.callback(cog, interaction)

        kwargs = interaction.response.send_message.call_args.kwargs
        embed = kwargs.get("embed")
        assert "[ENV override]" in embed.description

    @pytest.mark.asyncio
    async def test_shows_bot_settings_from_db(self, monkeypatch):
        """When no ENV override, status reads tz/filter from DB."""
        bot, guild = self._make_bot_with_roles()
        bot.stock_data.bot_settings.get = AsyncMock(side_effect=lambda key: {
            "tz": "Europe/London",
            "notification_filter": "failures_only",
        }.get(key))
        cog, _ = _make_cog(bot)
        interaction = _make_interaction()
        interaction.guild = guild

        monkeypatch.delenv("TZ", raising=False)
        monkeypatch.delenv("NOTIFICATION_FILTER", raising=False)

        await cog.server_status.callback(cog, interaction)

        kwargs = interaction.response.send_message.call_args.kwargs
        embed = kwargs.get("embed")
        assert "Europe/London" in embed.description

    @pytest.mark.asyncio
    async def test_green_when_all_configured(self):
        """Color is green only when all channels AND all roles are configured."""
        from rocketstocks.bot.views.subscription_views import ALERT_ROLE_DEFS
        bot, guild = self._make_bot_with_roles()
        all_role_keys = [k for k, _ in ALERT_ROLE_DEFS]
        bot.stock_data.alert_roles.get_all_for_guild.return_value = {
            k: i + 100 for i, k in enumerate(all_role_keys)
        }
        cog, _ = _make_cog(bot)
        interaction = _make_interaction()
        interaction.guild = guild
        interaction.guild.get_role = MagicMock(side_effect=lambda rid: MagicMock(mention=f"<@&{rid}>"))

        bot.stock_data.channel_config.get_all_for_guild.return_value = {
            ct: 100 + i for i, ct in enumerate(ALL_CONFIG_TYPES)
        }
        mock_channel = MagicMock()
        mock_channel.mention = "#test"
        bot.get_channel.return_value = mock_channel

        await cog.server_status.callback(cog, interaction)

        kwargs = interaction.response.send_message.call_args.kwargs
        embed = kwargs.get("embed")
        assert embed.color == discord.Color.green()


class TestOnGuildJoin:
    @pytest.mark.asyncio
    async def test_sends_setup_prompt_to_priority_channel(self):
        from rocketstocks.bot.cogs.config import Config, ChannelSetupView, BotSettingsView

        bot, guild = _make_bot()
        cog = Config(bot=bot)

        # Create a writable channel named "bot-commands"
        ch = AsyncMock(spec=["name", "permissions_for", "send"])
        ch.name = "bot-commands"
        me_perms = MagicMock()
        me_perms.send_messages = True
        ch.permissions_for.return_value = me_perms
        ch.send = AsyncMock()

        guild.text_channels = [ch]
        guild.me = MagicMock()

        await cog.on_guild_join(guild)

        assert ch.send.await_count == 2
        first_view = ch.send.call_args_list[0].kwargs.get("view")
        assert isinstance(first_view, ChannelSetupView)
        second_view = ch.send.call_args_list[1].kwargs.get("view")
        assert isinstance(second_view, BotSettingsView)

    @pytest.mark.asyncio
    async def test_falls_back_to_first_writable_channel(self):
        from rocketstocks.bot.cogs.config import Config, ChannelSetupView

        bot, guild = _make_bot()
        cog = Config(bot=bot)

        ch = AsyncMock(spec=["name", "permissions_for", "send"])
        ch.name = "random-channel"
        me_perms = MagicMock()
        me_perms.send_messages = True
        ch.permissions_for.return_value = me_perms
        ch.send = AsyncMock()

        guild.text_channels = [ch]
        guild.me = MagicMock()

        await cog.on_guild_join(guild)

        assert ch.send.await_count >= 1
        first_view = ch.send.call_args_list[0].kwargs.get("view")
        assert isinstance(first_view, ChannelSetupView)

    @pytest.mark.asyncio
    async def test_dms_owner_when_no_writable_channels(self):
        from rocketstocks.bot.cogs.config import Config, ChannelSetupView

        bot, guild = _make_bot()
        cog = Config(bot=bot)

        guild.text_channels = []
        guild.me = MagicMock()

        dm_channel = AsyncMock()
        dm_channel.send = AsyncMock()
        guild.owner.create_dm = AsyncMock(return_value=dm_channel)

        await cog.on_guild_join(guild)

        assert dm_channel.send.await_count >= 1
        first_view = dm_channel.send.call_args_list[0].kwargs.get("view")
        assert isinstance(first_view, ChannelSetupView)


class TestOnReady:
    @pytest.mark.asyncio
    async def test_prompts_unconfigured_guilds(self):
        from rocketstocks.bot.cogs.config import Config, ChannelSetupView

        bot, guild = _make_bot()
        cog = Config(bot=bot)

        bot.stock_data.channel_config.get_unconfigured_guilds.return_value = [guild.id]

        ch = AsyncMock(spec=["name", "permissions_for", "send"])
        ch.name = "general"
        me_perms = MagicMock()
        me_perms.send_messages = True
        ch.permissions_for.return_value = me_perms
        ch.send = AsyncMock()
        guild.text_channels = [ch]
        guild.me = MagicMock()

        await cog.on_ready()

        assert ch.send.await_count >= 1
        first_view = ch.send.call_args_list[0].kwargs.get("view")
        assert isinstance(first_view, ChannelSetupView)

    @pytest.mark.asyncio
    async def test_skips_fully_configured_guilds(self):
        from rocketstocks.bot.cogs.config import Config

        bot, guild = _make_bot()
        cog = Config(bot=bot)

        # All guilds are configured
        bot.stock_data.channel_config.get_unconfigured_guilds.return_value = []

        ch = AsyncMock(spec=["name", "permissions_for", "send"])
        ch.send = AsyncMock()
        guild.text_channels = [ch]

        await cog.on_ready()

        ch.send.assert_not_called()


class TestChannelSetupView:
    @pytest.mark.asyncio
    async def test_has_five_select_items(self):
        from rocketstocks.bot.cogs.config import ChannelSetupView, _ChannelTypeSelect
        bot, guild = _make_bot()
        view = ChannelSetupView(bot, guild.id, {})

        selects = [c for c in view.children if isinstance(c, _ChannelTypeSelect)]
        assert len(selects) == 4

    @pytest.mark.asyncio
    async def test_pre_populates_default_values_from_current_config(self):
        from rocketstocks.bot.cogs.config import ChannelSetupView, _ChannelTypeSelect
        bot, guild = _make_bot()
        current_config = {REPORTS: 111, ALERTS: 222}
        view = ChannelSetupView(bot, guild.id, current_config)

        reports_select = next(c for c in view.children
                               if isinstance(c, _ChannelTypeSelect) and c.config_type == REPORTS)
        # Should have a default value set
        assert len(reports_select.default_values) == 1
        assert reports_select.default_values[0].id == 111

    @pytest.mark.asyncio
    async def test_callback_saves_channel_and_edits_message(self):
        from rocketstocks.bot.cogs.config import ChannelSetupView, _ChannelTypeSelect
        bot, guild = _make_bot()
        view = ChannelSetupView(bot, guild.id, {})

        reports_select = next(c for c in view.children
                               if isinstance(c, _ChannelTypeSelect) and c.config_type == REPORTS)

        mock_channel = MagicMock()
        mock_channel.id = 999
        reports_select._values = [mock_channel]

        interaction = _make_interaction(guild.id)
        interaction.response = AsyncMock()

        await reports_select.callback(interaction)

        bot.stock_data.channel_config.upsert_channel.assert_awaited_once_with(
            guild.id, REPORTS, 999
        )
        interaction.response.edit_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_callback_updates_current_state(self):
        from rocketstocks.bot.cogs.config import ChannelSetupView, _ChannelTypeSelect
        bot, guild = _make_bot()
        view = ChannelSetupView(bot, guild.id, {})

        alerts_select = next(c for c in view.children
                              if isinstance(c, _ChannelTypeSelect) and c.config_type == ALERTS)

        mock_channel = MagicMock()
        mock_channel.id = 777
        alerts_select._values = [mock_channel]

        interaction = _make_interaction(guild.id)
        interaction.response = AsyncMock()

        await alerts_select.callback(interaction)

        assert view._current[ALERTS] == 777

    @pytest.mark.asyncio
    async def test_callback_db_error_sends_ephemeral_and_returns(self):
        """If upsert_channel raises, an ephemeral error is sent and edit_message is NOT called."""
        from rocketstocks.bot.cogs.config import ChannelSetupView, _ChannelTypeSelect
        bot, guild = _make_bot()
        bot.stock_data.channel_config.upsert_channel = AsyncMock(side_effect=Exception("DB down"))
        view = ChannelSetupView(bot, guild.id, {})

        reports_select = next(c for c in view.children
                               if isinstance(c, _ChannelTypeSelect) and c.config_type == REPORTS)

        mock_channel = MagicMock()
        mock_channel.id = 999
        reports_select._values = [mock_channel]

        interaction = _make_interaction(guild.id)
        interaction.response = AsyncMock()

        await reports_select.callback(interaction)

        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.call_args.kwargs
        assert kwargs.get("ephemeral") is True
        interaction.response.edit_message.assert_not_awaited()


class TestBotSettingsView:
    @pytest.mark.asyncio
    async def test_tz_select_updates_internal_state(self):
        from rocketstocks.bot.cogs.config import BotSettingsView
        bot, guild = _make_bot()
        view = BotSettingsView(bot, guild.id, "America/Chicago", "all")
        view._tz_select._values = ["UTC"]

        interaction = _make_interaction()
        interaction.response = AsyncMock()

        await view._on_tz_select(interaction)

        assert view._tz == "UTC"
        interaction.response.defer.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_filter_select_updates_internal_state(self):
        from rocketstocks.bot.cogs.config import BotSettingsView
        bot, guild = _make_bot()
        view = BotSettingsView(bot, guild.id, "America/Chicago", "all")
        view._filter_select._values = ["failures_only"]

        interaction = _make_interaction()
        interaction.response = AsyncMock()

        await view._on_filter_select(interaction)

        assert view._filter == "failures_only"

    @pytest.mark.asyncio
    async def test_save_persists_to_db_and_configures_runtime(self, monkeypatch):
        from rocketstocks.bot.cogs.config import BotSettingsView
        from rocketstocks.core.notifications.config import NotificationFilter
        bot, guild = _make_bot()
        bot.stock_data.channel_config.get_all_for_guild = AsyncMock(return_value={})

        monkeypatch.delenv("TZ", raising=False)
        monkeypatch.delenv("NOTIFICATION_FILTER", raising=False)

        view = BotSettingsView(bot, guild.id, "America/Chicago", "all")
        view._tz = "UTC"
        view._filter = "failures_only"

        interaction = _make_interaction(guild.id)
        interaction.guild = guild
        interaction.response = AsyncMock()

        with patch("rocketstocks.bot.cogs.config.configure_tz") as mock_configure_tz:
            await view._on_save(interaction)

        bot.stock_data.bot_settings.set.assert_any_await("tz", "UTC")
        bot.stock_data.bot_settings.set.assert_any_await("notification_filter", "failures_only")
        mock_configure_tz.assert_called_once_with("UTC")
        assert bot.notification_config.filter == NotificationFilter.FAILURES_ONLY
        interaction.response.edit_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_save_skips_db_when_env_overrides_present(self, monkeypatch):
        from rocketstocks.bot.cogs.config import BotSettingsView
        bot, guild = _make_bot()

        monkeypatch.setenv("TZ", "America/Chicago")
        monkeypatch.setenv("NOTIFICATION_FILTER", "all")

        view = BotSettingsView(bot, guild.id, "America/Chicago", "all")

        interaction = _make_interaction(guild.id)
        interaction.guild = guild
        interaction.response = AsyncMock()
        bot.stock_data.channel_config.get_all_for_guild = AsyncMock(return_value={})

        with patch("rocketstocks.bot.cogs.config.configure_tz") as mock_configure_tz:
            await view._on_save(interaction)

        bot.stock_data.bot_settings.set.assert_not_awaited()
        mock_configure_tz.assert_not_called()

    @pytest.mark.asyncio
    async def test_tz_select_disabled_when_env_override(self, monkeypatch):
        from rocketstocks.bot.cogs.config import BotSettingsView
        bot, guild = _make_bot()
        monkeypatch.setenv("TZ", "America/Chicago")

        view = BotSettingsView(bot, guild.id, "America/Chicago", "all")

        assert view._tz_select.disabled is True

    @pytest.mark.asyncio
    async def test_filter_select_disabled_when_env_override(self, monkeypatch):
        from rocketstocks.bot.cogs.config import BotSettingsView
        bot, guild = _make_bot()
        monkeypatch.setenv("NOTIFICATION_FILTER", "all")

        view = BotSettingsView(bot, guild.id, "America/Chicago", "all")

        assert view._filter_select.disabled is True

    @pytest.mark.asyncio
    async def test_save_triggers_subscriptions_when_channels_fully_configured(self, monkeypatch):
        from rocketstocks.bot.cogs.config import BotSettingsView
        bot, guild = _make_bot()
        bot.stock_data.channel_config.get_all_for_guild = AsyncMock(
            return_value={ct: 100 + i for i, ct in enumerate(ALL_CONFIG_TYPES)}
        )

        monkeypatch.delenv("TZ", raising=False)
        monkeypatch.delenv("NOTIFICATION_FILTER", raising=False)

        view = BotSettingsView(bot, guild.id, "America/Chicago", "all")

        interaction = _make_interaction(guild.id)
        interaction.guild = guild
        interaction.response = AsyncMock()

        with patch("rocketstocks.bot.cogs.config.configure_tz"):
            with patch("rocketstocks.bot.cogs.config.setup_alert_subscriptions", new_callable=AsyncMock) as mock_subs:
                await view._on_save(interaction)

        mock_subs.assert_awaited_once_with(bot, guild)


class TestSetupAlertSubscriptions:
    def _make_bot(self):
        bot = MagicMock(name="Bot")
        bot.stock_data.alert_roles.upsert = AsyncMock()
        bot.stock_data.channel_config.get_channel_id = AsyncMock(return_value=None)
        bot.get_channel = MagicMock(return_value=None)
        return bot

    @pytest.mark.asyncio
    async def test_setup_alert_subscriptions_creates_roles_and_posts_panel(self):
        from rocketstocks.bot.cogs.config import setup_alert_subscriptions
        from rocketstocks.bot.views.subscription_views import ALERT_ROLE_DEFS

        bot = self._make_bot()
        guild = MagicMock(spec=discord.Guild)
        guild.id = 123
        guild.roles = []
        mock_role = MagicMock(spec=discord.Role)
        mock_role.id = 500
        guild.create_role = AsyncMock(return_value=mock_role)

        mock_channel = AsyncMock(spec=discord.TextChannel)
        mock_channel.send = AsyncMock()
        bot.stock_data.channel_config.get_channel_id = AsyncMock(return_value=999)
        bot.get_channel = MagicMock(return_value=mock_channel)

        with patch("rocketstocks.bot.cogs.config.discord.utils.get", return_value=None):
            await setup_alert_subscriptions(bot, guild)

        assert guild.create_role.call_count == len(ALERT_ROLE_DEFS)
        assert bot.stock_data.alert_roles.upsert.call_count == len(ALERT_ROLE_DEFS)
        mock_channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_setup_alert_subscriptions_reuses_existing_roles(self):
        from rocketstocks.bot.cogs.config import setup_alert_subscriptions
        from rocketstocks.bot.views.subscription_views import ALERT_ROLE_DEFS

        bot = self._make_bot()
        guild = MagicMock(spec=discord.Guild)
        guild.id = 123
        existing_role = MagicMock(spec=discord.Role)
        existing_role.id = 42
        guild.create_role = AsyncMock()

        mock_channel = AsyncMock(spec=discord.TextChannel)
        mock_channel.send = AsyncMock()
        bot.stock_data.channel_config.get_channel_id = AsyncMock(return_value=1)
        bot.get_channel = MagicMock(return_value=mock_channel)

        with patch("rocketstocks.bot.cogs.config.discord.utils.get", return_value=existing_role):
            await setup_alert_subscriptions(bot, guild)

        guild.create_role.assert_not_called()
        assert bot.stock_data.alert_roles.upsert.call_count == len(ALERT_ROLE_DEFS)

    @pytest.mark.asyncio
    async def test_setup_alert_subscriptions_skips_panel_when_no_alerts_channel(self):
        from rocketstocks.bot.cogs.config import setup_alert_subscriptions

        bot = self._make_bot()
        guild = MagicMock(spec=discord.Guild)
        guild.id = 123
        guild.create_role = AsyncMock(return_value=MagicMock(id=1))

        with patch("rocketstocks.bot.cogs.config.discord.utils.get", return_value=None):
            await setup_alert_subscriptions(bot, guild)

        bot.get_channel.assert_not_called()

"""Tests for rocketstocks.bot.cogs.config."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from rocketstocks.data.channel_config import REPORTS, ALERTS, SCREENERS, CHARTS, NOTIFICATIONS, ALL_CONFIG_TYPES


def _make_bot(guild_id=123456):
    bot = MagicMock(name="Bot")
    bot.stock_data = MagicMock(name="StockData")
    bot.stock_data.channel_config = MagicMock(name="ChannelConfigRepository")
    bot.stock_data.channel_config.get_all_for_guild = AsyncMock(return_value={})
    bot.stock_data.channel_config.get_unconfigured_guilds = AsyncMock(return_value=[])
    bot.stock_data.channel_config.upsert_channel = AsyncMock()
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


class TestSetupCommand:
    @pytest.mark.asyncio
    async def test_setup_opens_modal(self):
        cog, _ = _make_cog()
        interaction = _make_interaction()

        await cog.setup.callback(cog, interaction)

        from rocketstocks.bot.cogs.config import SetupModal
        interaction.response.send_modal.assert_awaited_once()
        modal = interaction.response.send_modal.call_args.args[0]
        assert isinstance(modal, SetupModal)


class TestSetupStatusCommand:
    @pytest.mark.asyncio
    async def test_shows_mentions_for_configured_channels(self):
        cog, bot = _make_cog()
        interaction = _make_interaction()

        bot.stock_data.channel_config.get_all_for_guild.return_value = {
            ct: 100 + i for i, ct in enumerate(ALL_CONFIG_TYPES)
        }
        mock_channel = MagicMock()
        mock_channel.mention = "#test"
        bot.get_channel.return_value = mock_channel

        await cog.setup_status.callback(cog, interaction)

        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.call_args.kwargs
        embed = kwargs.get("embed")
        assert embed is not None

    @pytest.mark.asyncio
    async def test_shows_not_configured_for_missing_channels(self):
        cog, bot = _make_cog()
        interaction = _make_interaction()

        # Only reports configured
        bot.stock_data.channel_config.get_all_for_guild.return_value = {REPORTS: 111}
        mock_channel = MagicMock()
        mock_channel.mention = "#reports"
        bot.get_channel.return_value = mock_channel

        await cog.setup_status.callback(cog, interaction)

        kwargs = interaction.response.send_message.call_args.kwargs
        embed = kwargs.get("embed")
        assert "Not configured" in embed.description


class TestOnGuildJoin:
    @pytest.mark.asyncio
    async def test_sends_setup_prompt_to_priority_channel(self):
        from rocketstocks.bot.cogs.config import Config, SetupView

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

        ch.send.assert_awaited_once()
        embed = ch.send.call_args.kwargs.get("embed")
        assert embed is not None
        view = ch.send.call_args.kwargs.get("view")
        assert isinstance(view, SetupView)

    @pytest.mark.asyncio
    async def test_falls_back_to_first_writable_channel(self):
        from rocketstocks.bot.cogs.config import Config, SetupView

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

        ch.send.assert_awaited_once()
        view = ch.send.call_args.kwargs.get("view")
        assert isinstance(view, SetupView)

    @pytest.mark.asyncio
    async def test_dms_owner_when_no_writable_channels(self):
        from rocketstocks.bot.cogs.config import Config, SetupView

        bot, guild = _make_bot()
        cog = Config(bot=bot)

        guild.text_channels = []
        guild.me = MagicMock()

        dm_channel = AsyncMock()
        dm_channel.send = AsyncMock()
        guild.owner.create_dm = AsyncMock(return_value=dm_channel)

        await cog.on_guild_join(guild)

        dm_channel.send.assert_awaited_once()
        view = dm_channel.send.call_args.kwargs.get("view")
        assert isinstance(view, SetupView)


class TestOnReady:
    @pytest.mark.asyncio
    async def test_prompts_unconfigured_guilds(self):
        from rocketstocks.bot.cogs.config import Config, SetupView

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

        ch.send.assert_awaited_once()
        view = ch.send.call_args.kwargs.get("view")
        assert isinstance(view, SetupView)

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


class TestParseChannel:
    def setup_method(self):
        from rocketstocks.bot.cogs.config import _parse_channel
        self._parse_channel = _parse_channel
        self.guild = _make_guild()

    def _make_text_channel(self, channel_id, name="test"):
        ch = MagicMock(spec=discord.TextChannel)
        ch.id = channel_id
        ch.name = name
        return ch

    def test_empty_string_returns_none(self):
        assert self._parse_channel("", self.guild) is None

    def test_whitespace_returns_none(self):
        assert self._parse_channel("   ", self.guild) is None

    def test_mention_format_returns_text_channel(self):
        ch = self._make_text_channel(111)
        self.guild.get_channel.return_value = ch
        result = self._parse_channel("<#111>", self.guild)
        assert result is ch
        self.guild.get_channel.assert_called_once_with(111)

    def test_mention_format_unknown_id_returns_none(self):
        self.guild.get_channel.return_value = None
        assert self._parse_channel("<#999>", self.guild) is None

    def test_mention_format_non_text_channel_returns_none(self):
        # Returns a VoiceChannel instead of TextChannel
        ch = MagicMock(spec=discord.VoiceChannel)
        self.guild.get_channel.return_value = ch
        assert self._parse_channel("<#111>", self.guild) is None

    def test_raw_digit_id_returns_text_channel(self):
        ch = self._make_text_channel(222)
        self.guild.get_channel.return_value = ch
        result = self._parse_channel("222", self.guild)
        assert result is ch

    def test_raw_digit_id_non_text_channel_returns_none(self):
        ch = MagicMock(spec=discord.VoiceChannel)
        self.guild.get_channel.return_value = ch
        assert self._parse_channel("333", self.guild) is None

    def test_channel_name_without_hash_matches(self):
        ch = self._make_text_channel(444, name="reports")
        self.guild.text_channels = [ch]
        result = self._parse_channel("reports", self.guild)
        assert result is ch

    def test_channel_name_with_hash_prefix_matches(self):
        ch = self._make_text_channel(555, name="alerts")
        self.guild.text_channels = [ch]
        result = self._parse_channel("#alerts", self.guild)
        assert result is ch

    def test_channel_name_not_found_returns_none(self):
        self.guild.text_channels = []
        assert self._parse_channel("nonexistent", self.guild) is None


class TestSetupModal:
    def _make_modal(self):
        cog, bot = _make_cog()
        from rocketstocks.bot.cogs.config import SetupModal
        modal = SetupModal(cog)
        return modal, cog, bot

    def _make_text_channel(self, channel_id, name="ch"):
        ch = MagicMock(spec=discord.TextChannel)
        ch.id = channel_id
        ch.name = name
        ch.mention = f"<#{channel_id}>"
        return ch

    @pytest.mark.asyncio
    async def test_on_submit_valid_channels_upserts_and_confirms(self):
        modal, cog, bot = self._make_modal()
        interaction = _make_interaction()

        channels = {
            REPORTS:       self._make_text_channel(111, "reports"),
            ALERTS:        self._make_text_channel(222, "alerts"),
            SCREENERS:     self._make_text_channel(333, "screeners"),
            CHARTS:        self._make_text_channel(444, "charts"),
            NOTIFICATIONS: self._make_text_channel(555, "notifications"),
        }
        interaction.guild = _make_guild()
        interaction.guild.get_channel.side_effect = lambda cid: {
            111: channels[REPORTS],
            222: channels[ALERTS],
            333: channels[SCREENERS],
            444: channels[CHARTS],
            555: channels[NOTIFICATIONS],
        }.get(cid)

        modal.reports._value = "<#111>"
        modal.alerts._value = "<#222>"
        modal.screeners._value = "<#333>"
        modal.charts._value = "<#444>"
        modal.notifications._value = "<#555>"

        with patch("rocketstocks.bot.cogs.config.setup_alert_subscriptions", new_callable=AsyncMock):
            await modal.on_submit(interaction)

        assert bot.stock_data.channel_config.upsert_channel.call_count == 5
        interaction.followup.send.assert_awaited_once()
        kwargs = interaction.followup.send.call_args.kwargs
        embed = kwargs.get("embed")
        assert embed is not None
        assert embed.color == discord.Color.green()

    @pytest.mark.asyncio
    async def test_on_submit_partial_channels_upserts_only_resolved(self):
        modal, cog, bot = self._make_modal()
        interaction = _make_interaction()

        ch_reports = self._make_text_channel(111, "reports")
        ch_alerts = self._make_text_channel(222, "alerts")
        interaction.guild = _make_guild()
        interaction.guild.get_channel.side_effect = lambda cid: {
            111: ch_reports,
            222: ch_alerts,
        }.get(cid)
        interaction.guild.text_channels = []

        modal.reports._value = "<#111>"
        modal.alerts._value = "<#222>"
        modal.screeners._value = ""
        modal.charts._value = ""
        modal.notifications._value = ""

        with patch("rocketstocks.bot.cogs.config.setup_alert_subscriptions", new_callable=AsyncMock):
            await modal.on_submit(interaction)

        assert bot.stock_data.channel_config.upsert_channel.call_count == 2

    @pytest.mark.asyncio
    async def test_on_submit_no_valid_channels_sends_error(self):
        modal, cog, bot = self._make_modal()
        interaction = _make_interaction()
        interaction.guild = _make_guild()
        interaction.guild.get_channel.return_value = None
        interaction.guild.text_channels = []

        modal.reports._value = ""
        modal.alerts._value = ""
        modal.screeners._value = ""
        modal.charts._value = ""
        modal.notifications._value = ""

        await modal.on_submit(interaction)

        bot.stock_data.channel_config.upsert_channel.assert_not_called()
        interaction.followup.send.assert_awaited_once()
        args = interaction.followup.send.call_args.args
        assert "No valid channels" in args[0]

    @pytest.mark.asyncio
    async def test_on_error_sends_followup_message(self):
        modal, _, _ = self._make_modal()
        interaction = _make_interaction()

        await modal.on_error(interaction, ValueError("boom"))

        interaction.followup.send.assert_awaited_once()
        args = interaction.followup.send.call_args.args
        assert "unexpected error" in args[0].lower()


class TestSetupView:
    # View.__init__ calls asyncio.get_running_loop(), so all tests must be async.

    @pytest.mark.asyncio
    async def test_view_timeout_is_none(self):
        cog, _ = _make_cog()
        from rocketstocks.bot.cogs.config import SetupView
        view = SetupView(cog)
        assert view.timeout is None

    @pytest.mark.asyncio
    async def test_button_opens_setup_modal(self):
        from rocketstocks.bot.cogs.config import SetupModal, SetupView
        cog, _ = _make_cog()
        view = SetupView(cog)
        interaction = _make_interaction()

        # button.callback is a _ViewCallback — call it with just the interaction
        await view.configure_channels.callback(interaction)

        interaction.response.send_modal.assert_awaited_once()
        modal = interaction.response.send_modal.call_args.args[0]
        assert isinstance(modal, SetupModal)

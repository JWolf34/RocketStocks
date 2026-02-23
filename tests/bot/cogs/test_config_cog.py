"""Tests for rocketstocks.bot.cogs.config."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.data.channel_config import REPORTS, ALERTS, SCREENERS, CHARTS, NOTIFICATIONS, ALL_CONFIG_TYPES


def _make_bot(guild_id=123456):
    bot = MagicMock(name="Bot")
    bot.stock_data = MagicMock(name="StockData")
    bot.stock_data.channel_config = MagicMock(name="ChannelConfigRepository")
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


class TestSetupCommand:
    @pytest.mark.asyncio
    async def test_setup_with_channels_upserts_and_confirms(self):
        cog, bot = _make_cog()
        interaction = _make_interaction()

        reports_ch = MagicMock(spec=["id", "mention"])
        reports_ch.id = 111
        reports_ch.mention = "#reports"
        alerts_ch = MagicMock(spec=["id", "mention"])
        alerts_ch.id = 222
        alerts_ch.mention = "#alerts"

        await cog.setup.callback(cog, interaction, reports=reports_ch, alerts=alerts_ch)

        # Should upsert for each provided channel
        calls = bot.stock_data.channel_config.upsert_channel.call_args_list
        assert len(calls) == 2
        assert any(c.args == (123456, REPORTS, 111) for c in calls)
        assert any(c.args == (123456, ALERTS, 222) for c in calls)

        # Should respond with a green embed
        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.call_args.kwargs
        assert kwargs.get("ephemeral") is True
        embed = kwargs.get("embed")
        assert embed is not None

    @pytest.mark.asyncio
    async def test_setup_with_no_args_sends_error(self):
        cog, bot = _make_cog()
        interaction = _make_interaction()

        await cog.setup.callback(cog, interaction)

        interaction.response.send_message.assert_awaited_once()
        msg = interaction.response.send_message.call_args.args[0]
        assert "at least one" in msg.lower() or "/setup" in msg
        bot.stock_data.channel_config.upsert_channel.assert_not_called()

    @pytest.mark.asyncio
    async def test_setup_with_all_channels(self):
        cog, bot = _make_cog()
        interaction = _make_interaction()

        channels = {}
        for i, ct in enumerate(ALL_CONFIG_TYPES):
            ch = MagicMock(spec=["id", "mention"])
            ch.id = 100 + i
            ch.mention = f"#{ct}"
            channels[ct] = ch

        await cog.setup.callback(
            cog, interaction,
            reports=channels[REPORTS],
            alerts=channels[ALERTS],
            screeners=channels[SCREENERS],
            charts=channels[CHARTS],
            notifications=channels[NOTIFICATIONS],
        )

        assert bot.stock_data.channel_config.upsert_channel.call_count == 5


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
        from rocketstocks.bot.cogs.config import Config

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

    @pytest.mark.asyncio
    async def test_falls_back_to_first_writable_channel(self):
        from rocketstocks.bot.cogs.config import Config

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

    @pytest.mark.asyncio
    async def test_dms_owner_when_no_writable_channels(self):
        from rocketstocks.bot.cogs.config import Config

        bot, guild = _make_bot()
        cog = Config(bot=bot)

        guild.text_channels = []
        guild.me = MagicMock()

        dm_channel = AsyncMock()
        dm_channel.send = AsyncMock()
        guild.owner.create_dm = AsyncMock(return_value=dm_channel)

        await cog.on_guild_join(guild)

        dm_channel.send.assert_awaited_once()


class TestOnReady:
    @pytest.mark.asyncio
    async def test_prompts_unconfigured_guilds(self):
        from rocketstocks.bot.cogs.config import Config

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

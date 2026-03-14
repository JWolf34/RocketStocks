"""Tests for rocketstocks.bot.cogs.subscriptions."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from rocketstocks.bot.cogs.subscriptions import Subscriptions, setup_alert_subscriptions
from rocketstocks.bot.views.subscription_views import ALERT_ROLE_DEFS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bot(guild_roles: dict | None = None):
    bot = MagicMock(name="Bot")
    bot.stock_data.alert_roles.get_all_for_guild = AsyncMock(return_value=guild_roles or {})
    bot.stock_data.alert_roles.get_role_ids = AsyncMock(return_value=[])
    bot.stock_data.alert_roles.upsert = AsyncMock()
    bot.stock_data.channel_config.get_channel_id = AsyncMock(return_value=None)
    bot.get_channel = MagicMock(return_value=None)
    bot.get_cog = MagicMock(return_value=None)
    return bot


def _make_cog(guild_roles: dict | None = None):
    bot = _make_bot(guild_roles)
    cog = Subscriptions(bot)
    return cog, bot


def _make_interaction(guild_id: int = 123, user_role_ids: list[int] | None = None):
    interaction = MagicMock(spec=discord.Interaction)
    interaction.guild_id = guild_id
    interaction.guild = MagicMock(spec=discord.Guild)
    interaction.guild.id = guild_id
    roles = []
    for rid in (user_role_ids or []):
        role = MagicMock(spec=discord.Role)
        role.id = rid
        roles.append(role)
    interaction.user = MagicMock(spec=discord.Member)
    interaction.user.roles = roles
    interaction.response.send_message = AsyncMock()
    return interaction


# ---------------------------------------------------------------------------
# _send_subscription_select
# ---------------------------------------------------------------------------

async def test_send_subscription_select_sends_ephemeral_message():
    cog, bot = _make_cog()
    interaction = _make_interaction()
    with patch('rocketstocks.bot.cogs.subscriptions.AlertSubscriptionSelect'), \
         patch('rocketstocks.bot.cogs.subscriptions.AlertSubscriptionView'):
        await cog._send_subscription_select(interaction)
    interaction.response.send_message.assert_called_once()
    assert interaction.response.send_message.call_args.kwargs.get('ephemeral') is True


async def test_send_subscription_select_fetches_guild_roles():
    guild_roles = {'earnings_mover': 100}
    cog, bot = _make_cog(guild_roles)
    interaction = _make_interaction(guild_id=999)
    with patch('rocketstocks.bot.cogs.subscriptions.AlertSubscriptionSelect'), \
         patch('rocketstocks.bot.cogs.subscriptions.AlertSubscriptionView'):
        await cog._send_subscription_select(interaction)
    bot.stock_data.alert_roles.get_all_for_guild.assert_called_once_with(999)


# ---------------------------------------------------------------------------
# /subscriptions status
# ---------------------------------------------------------------------------

async def test_subscriptions_status_shows_configured_roles():
    guild_roles = {'earnings_mover': 101, 'all_alerts': 202}
    cog, bot = _make_cog(guild_roles)
    interaction = _make_interaction(guild_id=111)
    interaction.guild.get_role = MagicMock(side_effect=lambda rid: MagicMock(mention=f"<@&{rid}>"))
    interaction.response.send_message = AsyncMock()

    await cog.subscriptions_status.callback(cog, interaction)

    interaction.response.send_message.assert_called_once()
    kwargs = interaction.response.send_message.call_args.kwargs
    assert kwargs.get('ephemeral') is True
    embed = kwargs.get('embed')
    assert embed is not None
    assert '✅' in embed.description
    assert '❌' in embed.description  # unconfigured roles


async def test_subscriptions_status_green_when_all_configured():
    all_keys = [k for k, _ in ALERT_ROLE_DEFS]
    guild_roles = {k: i + 10 for i, k in enumerate(all_keys)}
    cog, bot = _make_cog(guild_roles)
    interaction = _make_interaction(guild_id=111)
    interaction.guild.get_role = MagicMock(side_effect=lambda rid: MagicMock(mention=f"<@&{rid}>"))
    interaction.response.send_message = AsyncMock()

    await cog.subscriptions_status.callback(cog, interaction)

    embed = interaction.response.send_message.call_args.kwargs.get('embed')
    assert embed.colour == discord.Colour.green()


# ---------------------------------------------------------------------------
# /alert-subscriptions
# ---------------------------------------------------------------------------

async def test_alert_subscriptions_delegates_to_send_subscription_select():
    cog, bot = _make_cog()
    interaction = _make_interaction()
    cog._send_subscription_select = AsyncMock()

    await cog.alert_subscriptions.callback(cog, interaction)

    cog._send_subscription_select.assert_called_once_with(interaction)


# ---------------------------------------------------------------------------
# setup_alert_subscriptions
# ---------------------------------------------------------------------------

async def test_setup_alert_subscriptions_creates_roles_and_posts_panel():
    bot = _make_bot()
    guild = MagicMock(spec=discord.Guild)
    guild.id = 123
    guild.roles = []
    mock_role = MagicMock(spec=discord.Role)
    mock_role.id = 500
    guild.create_role = AsyncMock(return_value=mock_role)
    discord.utils.get = MagicMock(return_value=None)  # no existing roles

    mock_channel = AsyncMock(spec=discord.TextChannel)
    mock_channel.send = AsyncMock()
    bot.stock_data.channel_config.get_channel_id = AsyncMock(return_value=999)
    bot.get_channel = MagicMock(return_value=mock_channel)

    with patch('rocketstocks.bot.cogs.subscriptions.discord.utils.get', return_value=None):
        await setup_alert_subscriptions(bot, guild)

    assert guild.create_role.call_count == len(ALERT_ROLE_DEFS)
    assert bot.stock_data.alert_roles.upsert.call_count == len(ALERT_ROLE_DEFS)
    mock_channel.send.assert_called_once()


async def test_setup_alert_subscriptions_reuses_existing_roles():
    bot = _make_bot()
    guild = MagicMock(spec=discord.Guild)
    guild.id = 123
    existing_role = MagicMock(spec=discord.Role)
    existing_role.id = 42
    guild.create_role = AsyncMock()

    mock_channel = AsyncMock(spec=discord.TextChannel)
    mock_channel.send = AsyncMock()
    bot.stock_data.channel_config.get_channel_id = AsyncMock(return_value=1)
    bot.get_channel = MagicMock(return_value=mock_channel)

    # All roles already exist
    with patch('rocketstocks.bot.cogs.subscriptions.discord.utils.get', return_value=existing_role):
        await setup_alert_subscriptions(bot, guild)

    guild.create_role.assert_not_called()
    assert bot.stock_data.alert_roles.upsert.call_count == len(ALERT_ROLE_DEFS)


async def test_setup_alert_subscriptions_skips_panel_when_no_alerts_channel():
    bot = _make_bot()
    guild = MagicMock(spec=discord.Guild)
    guild.id = 123
    guild.create_role = AsyncMock(return_value=MagicMock(id=1))

    with patch('rocketstocks.bot.cogs.subscriptions.discord.utils.get', return_value=None):
        await setup_alert_subscriptions(bot, guild)

    # No channel → channel.send never called
    bot.get_channel.assert_not_called()

"""Tests for rocketstocks.bot.views.subscription_views."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from rocketstocks.bot.views.subscription_views import (
    ALERT_ROLE_DEFS,
    AlertSubscriptionSelect,
    AlertSubscriptionView,
    ManageSubscriptionsButton,
    SubscriptionEntryView,
)


def _make_guild_roles(keys: list[str]) -> dict[str, int]:
    """Build a guild_roles mapping for the given keys."""
    return {key: i + 100 for i, key in enumerate(keys)}


# ---------------------------------------------------------------------------
# ALERT_ROLE_DEFS
# ---------------------------------------------------------------------------

def test_alert_role_defs_has_seven_entries():
    assert len(ALERT_ROLE_DEFS) == 7


def test_alert_role_defs_includes_all_alerts():
    keys = [k for k, _ in ALERT_ROLE_DEFS]
    assert 'all_alerts' in keys


def test_alert_role_defs_includes_volume_accumulation():
    keys = [k for k, _ in ALERT_ROLE_DEFS]
    assert 'volume_accumulation' in keys


def test_alert_role_defs_includes_breakout():
    keys = [k for k, _ in ALERT_ROLE_DEFS]
    assert 'breakout' in keys


def test_alert_role_defs_has_no_market_mover_keys():
    keys = [k for k, _ in ALERT_ROLE_DEFS]
    market_keys = [k for k in keys if k.startswith('market_mover_')]
    assert market_keys == []


# ---------------------------------------------------------------------------
# AlertSubscriptionSelect — option building
# ---------------------------------------------------------------------------

def test_select_only_includes_roles_in_guild_roles():
    guild_roles = _make_guild_roles(['earnings_mover', 'all_alerts'])
    select = AlertSubscriptionSelect(guild_roles=guild_roles, member_role_ids=set())
    option_values = {opt.value for opt in select.options}
    assert option_values == {'earnings_mover', 'all_alerts'}


def test_select_marks_held_roles_as_default():
    guild_roles = _make_guild_roles(['earnings_mover', 'all_alerts'])
    # Member holds the 'earnings_mover' role (id=100)
    member_role_ids = {100}
    select = AlertSubscriptionSelect(guild_roles=guild_roles, member_role_ids=member_role_ids)
    defaulted = {opt.value for opt in select.options if opt.default}
    assert defaulted == {'earnings_mover'}


def test_select_no_defaults_when_member_holds_no_roles():
    guild_roles = _make_guild_roles(['earnings_mover', 'all_alerts'])
    select = AlertSubscriptionSelect(guild_roles=guild_roles, member_role_ids=set())
    defaulted = [opt for opt in select.options if opt.default]
    assert defaulted == []


def test_select_shows_placeholder_when_no_guild_roles():
    select = AlertSubscriptionSelect(guild_roles={}, member_role_ids=set())
    assert len(select.options) == 1
    assert select.options[0].value == '_none'


# ---------------------------------------------------------------------------
# AlertSubscriptionSelect — callback: add/remove logic
# ---------------------------------------------------------------------------

async def _run_callback(select, selected_values: list[str]):
    """Helper to invoke select.callback with mocked interaction."""
    guild = MagicMock(spec=discord.Guild)
    member = MagicMock(spec=discord.Member)
    member.roles = []  # overridden by select._member_role_ids internally
    member.add_roles = AsyncMock()
    member.remove_roles = AsyncMock()

    mock_role = MagicMock(spec=discord.Role)
    guild.get_role = MagicMock(return_value=mock_role)

    interaction = MagicMock(spec=discord.Interaction)
    interaction.guild = guild
    interaction.user = member
    interaction.response.edit_message = AsyncMock()

    # Simulate discord setting .values on the select (_values is the backing attribute)
    select._values = selected_values
    await select.callback(interaction)
    return interaction, member, mock_role


async def test_callback_adds_newly_selected_role():
    guild_roles = _make_guild_roles(['earnings_mover'])
    select = AlertSubscriptionSelect(guild_roles=guild_roles, member_role_ids=set())
    interaction, member, mock_role = await _run_callback(select, ['earnings_mover'])
    member.add_roles.assert_called_once_with(mock_role)
    member.remove_roles.assert_not_called()


async def test_callback_removes_deselected_role():
    guild_roles = _make_guild_roles(['earnings_mover'])
    # Member currently holds earnings_mover (role_id=100)
    select = AlertSubscriptionSelect(guild_roles=guild_roles, member_role_ids={100})
    interaction, member, mock_role = await _run_callback(select, [])
    member.remove_roles.assert_called_once_with(mock_role)
    member.add_roles.assert_not_called()


async def test_callback_no_change_when_selection_unchanged():
    guild_roles = _make_guild_roles(['earnings_mover'])
    # Member holds earnings_mover; selects it again → no change
    select = AlertSubscriptionSelect(guild_roles=guild_roles, member_role_ids={100})
    interaction, member, mock_role = await _run_callback(select, ['earnings_mover'])
    member.add_roles.assert_not_called()
    member.remove_roles.assert_not_called()


async def test_callback_ignores_none_placeholder():
    guild_roles = {}
    select = AlertSubscriptionSelect(guild_roles=guild_roles, member_role_ids=set())
    interaction, member, _ = await _run_callback(select, ['_none'])
    member.add_roles.assert_not_called()
    member.remove_roles.assert_not_called()


async def test_callback_edits_message_with_summary():
    guild_roles = _make_guild_roles(['earnings_mover'])
    select = AlertSubscriptionSelect(guild_roles=guild_roles, member_role_ids=set())
    interaction, _, _ = await _run_callback(select, ['earnings_mover'])
    interaction.response.edit_message.assert_called_once()
    content_arg = interaction.response.edit_message.call_args.kwargs.get('content', '')
    assert 'Earnings Mover' in content_arg or 'updated' in content_arg.lower()


# ---------------------------------------------------------------------------
# SubscriptionEntryView / ManageSubscriptionsButton
# ---------------------------------------------------------------------------

async def test_subscription_entry_view_has_no_timeout():
    view = SubscriptionEntryView()
    assert view.timeout is None


async def test_subscription_entry_view_contains_manage_button():
    view = SubscriptionEntryView()
    buttons = [item for item in view.children if isinstance(item, ManageSubscriptionsButton)]
    assert len(buttons) == 1


def test_manage_subscriptions_button_has_correct_custom_id():
    btn = ManageSubscriptionsButton()
    assert btn.custom_id == "manage_subscriptions"


async def test_manage_subscriptions_button_delegates_to_cog():
    btn = ManageSubscriptionsButton()
    cog = MagicMock()
    cog._send_subscription_select = AsyncMock()
    client = MagicMock()
    client.get_cog = MagicMock(return_value=cog)
    interaction = MagicMock(spec=discord.Interaction)
    interaction.client = client
    interaction.response = MagicMock()

    await btn.callback(interaction)
    cog._send_subscription_select.assert_called_once_with(interaction)


async def test_manage_subscriptions_button_handles_missing_cog():
    btn = ManageSubscriptionsButton()
    client = MagicMock()
    client.get_cog = MagicMock(return_value=None)
    interaction = MagicMock(spec=discord.Interaction)
    interaction.client = client
    interaction.response.send_message = AsyncMock()

    await btn.callback(interaction)
    interaction.response.send_message.assert_called_once()
    assert interaction.response.send_message.call_args.kwargs.get('ephemeral') is True

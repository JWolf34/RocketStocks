"""Tests for rocketstocks.bot.views.paper_trading_views."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from rocketstocks.bot.views.paper_trading_views import ConfirmResetView, TradeConfirmView


# ---------------------------------------------------------------------------
# TradeConfirmView
# ---------------------------------------------------------------------------

async def test_trade_confirm_view_buy_timeout_is_60():
    view = TradeConfirmView(side="BUY")
    assert view.timeout == 60


async def test_trade_confirm_view_sell_timeout_is_60():
    view = TradeConfirmView(side="SELL")
    assert view.timeout == 60


async def test_trade_confirm_view_initial_state():
    view = TradeConfirmView(side="BUY")
    assert view.confirmed is None


async def test_trade_confirm_view_has_two_buttons():
    view = TradeConfirmView(side="BUY")
    assert len(view.children) == 2


async def test_trade_confirm_view_confirm_sets_true():
    view = TradeConfirmView(side="BUY")
    interaction = MagicMock()
    interaction.response.defer = AsyncMock()
    await view._on_confirm(interaction)
    assert view.confirmed is True


async def test_trade_confirm_view_cancel_sets_false():
    view = TradeConfirmView(side="BUY")
    interaction = MagicMock()
    interaction.response.defer = AsyncMock()
    await view._on_cancel(interaction)
    assert view.confirmed is False


async def test_trade_confirm_view_timeout_sets_none():
    view = TradeConfirmView(side="BUY")
    view.confirmed = True  # set it first
    await view.on_timeout()
    assert view.confirmed is None


# ---------------------------------------------------------------------------
# ConfirmResetView
# ---------------------------------------------------------------------------

async def test_confirm_reset_view_timeout_is_30():
    view = ConfirmResetView()
    assert view.timeout == 30


async def test_confirm_reset_view_initial_state():
    view = ConfirmResetView()
    assert view.confirmed is None


async def test_confirm_reset_view_has_two_buttons():
    view = ConfirmResetView()
    assert len(view.children) == 2


async def test_confirm_reset_confirm_button():
    view = ConfirmResetView()
    interaction = MagicMock()
    interaction.response.defer = AsyncMock()
    button = MagicMock()
    # discord.ui.button-decorated methods take (self, interaction, button)
    await ConfirmResetView.confirm(view, interaction, button)
    assert view.confirmed is True


async def test_confirm_reset_cancel_button():
    view = ConfirmResetView()
    interaction = MagicMock()
    interaction.response.defer = AsyncMock()
    button = MagicMock()
    await ConfirmResetView.cancel(view, interaction, button)
    assert view.confirmed is False


async def test_confirm_reset_timeout_sets_none():
    view = ConfirmResetView()
    view.confirmed = True
    await view.on_timeout()
    assert view.confirmed is None

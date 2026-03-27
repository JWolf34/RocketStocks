"""Tests for rocketstocks.bot.cogs.paper_trading.PaperTrading."""
import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.bot.cogs.paper_trading import PaperTrading


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_bot():
    bot = MagicMock(name="Bot")
    bot.emitter = MagicMock()
    bot.emitter.emit = MagicMock()
    return bot


def _make_stock_data():
    sd = MagicMock(name="StockData")
    sd.paper_trading.get_portfolio = AsyncMock(return_value={'guild_id': 1, 'user_id': 2, 'cash': 10000.0})
    sd.paper_trading.create_portfolio = AsyncMock()
    sd.paper_trading.reset_portfolio = AsyncMock()
    sd.paper_trading.get_positions = AsyncMock(return_value=[])
    sd.paper_trading.get_position = AsyncMock(return_value=None)
    sd.paper_trading.get_pending_orders = AsyncMock(return_value=[])
    sd.paper_trading.get_all_pending_orders = AsyncMock(return_value=[])
    sd.paper_trading.get_transactions = AsyncMock(return_value=[])
    sd.paper_trading.execute_buy = AsyncMock()
    sd.paper_trading.execute_sell = AsyncMock()
    sd.paper_trading.queue_buy_order = AsyncMock()
    sd.paper_trading.queue_sell_order = AsyncMock()
    sd.paper_trading.cancel_buy_order = AsyncMock(return_value=True)
    sd.paper_trading.cancel_sell_order = AsyncMock(return_value=True)
    sd.paper_trading.mark_order_executed = AsyncMock()
    sd.paper_trading.get_all_portfolios = AsyncMock(return_value=[])
    sd.paper_trading.get_distinct_guild_ids = AsyncMock(return_value=[])
    sd.paper_trading.insert_snapshot = AsyncMock()
    sd.tickers.validate_ticker = AsyncMock(return_value=True)
    sd.tickers.get_all_tickers = AsyncMock(return_value=['AAPL', 'TSLA'])
    sd.tickers.get_ticker_info = AsyncMock(return_value={'name': 'Apple Inc.'})
    sd.schwab.get_quote = AsyncMock(return_value={'quote': {'lastPrice': 150.0}})
    sd.channel_config.get_channel_id = AsyncMock(return_value=None)
    sd.paper_trading.get_snapshots = AsyncMock(return_value=[])
    return sd


def _make_cog():
    bot = _make_bot()
    sd = _make_stock_data()
    with (
        patch.object(PaperTrading, "execute_pending_orders"),
        patch.object(PaperTrading, "daily_snapshot"),
    ):
        cog = PaperTrading(bot=bot, stock_data=sd)
    return cog, bot, sd


def _make_interaction(guild_id=1001, user_id=2001, display_name="TestUser"):
    interaction = MagicMock(name="Interaction")
    interaction.guild_id = guild_id
    interaction.user.id = user_id
    interaction.user.display_name = display_name
    interaction.response.defer = AsyncMock()
    interaction.followup.send = AsyncMock()
    interaction.edit_original_response = AsyncMock()
    return interaction


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

def test_cog_construction_starts_tasks():
    bot = _make_bot()
    sd = _make_stock_data()
    mock_pending = MagicMock()
    mock_snapshot = MagicMock()
    with (
        patch.object(PaperTrading, "execute_pending_orders", mock_pending),
        patch.object(PaperTrading, "daily_snapshot", mock_snapshot),
    ):
        cog = PaperTrading(bot=bot, stock_data=sd)
    mock_pending.start.assert_called_once()
    mock_snapshot.start.assert_called_once()


# ---------------------------------------------------------------------------
# _execute_pending_orders_impl — not intraday
# ---------------------------------------------------------------------------

async def test_execute_pending_orders_skips_when_not_intraday():
    cog, _, sd = _make_cog()
    with patch.object(cog.mutils, "in_intraday", return_value=False):
        await cog._execute_pending_orders_impl()
    sd.paper_trading.get_all_pending_orders.assert_not_called()


async def test_execute_pending_orders_no_pending():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_all_pending_orders.return_value = []
    with (
        patch.object(cog.mutils, "in_intraday", return_value=True),
        patch.object(cog.mutils, "_refresh_schedule_if_needed"),
    ):
        await cog._execute_pending_orders_impl()
    sd.paper_trading.execute_buy.assert_not_called()
    sd.paper_trading.execute_sell.assert_not_called()


async def test_execute_pending_buy_order():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_all_pending_orders.return_value = [
        {'id': 1, 'guild_id': 1001, 'user_id': 2001, 'ticker': 'AAPL', 'side': 'BUY', 'shares': 10}
    ]
    with (
        patch.object(cog.mutils, "in_intraday", return_value=True),
        patch.object(cog.mutils, "_refresh_schedule_if_needed"),
        patch.object(cog.mutils, "get_current_price", return_value=155.0),
    ):
        await cog._execute_pending_orders_impl()
    sd.paper_trading.execute_buy.assert_called_once_with(1001, 2001, 'AAPL', 10, 155.0)
    sd.paper_trading.mark_order_executed.assert_called_once_with(1, 155.0)


async def test_execute_pending_sell_order():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_all_pending_orders.return_value = [
        {'id': 2, 'guild_id': 1001, 'user_id': 2001, 'ticker': 'TSLA', 'side': 'SELL', 'shares': 5}
    ]
    with (
        patch.object(cog.mutils, "in_intraday", return_value=True),
        patch.object(cog.mutils, "_refresh_schedule_if_needed"),
        patch.object(cog.mutils, "get_current_price", return_value=200.0),
    ):
        await cog._execute_pending_orders_impl()
    sd.paper_trading.execute_sell.assert_called_once_with(1001, 2001, 'TSLA', 5, 200.0)


async def test_execute_pending_skips_zero_price():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_all_pending_orders.return_value = [
        {'id': 1, 'guild_id': 1001, 'user_id': 2001, 'ticker': 'AAPL', 'side': 'BUY', 'shares': 10}
    ]
    with (
        patch.object(cog.mutils, "in_intraday", return_value=True),
        patch.object(cog.mutils, "_refresh_schedule_if_needed"),
        patch.object(cog.mutils, "get_current_price", return_value=0.0),
    ):
        await cog._execute_pending_orders_impl()
    sd.paper_trading.execute_buy.assert_not_called()


# ---------------------------------------------------------------------------
# _daily_snapshot_impl
# ---------------------------------------------------------------------------

async def test_daily_snapshot_skips_non_market_day():
    cog, _, sd = _make_cog()
    with patch.object(cog.mutils, "market_open_today", return_value=False):
        await cog._daily_snapshot_impl()
    sd.paper_trading.get_distinct_guild_ids.assert_not_called()


async def test_daily_snapshot_creates_snapshots():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_distinct_guild_ids.return_value = [1001]
    sd.paper_trading.get_all_portfolios.return_value = [
        {'user_id': 2001, 'cash': 5000.0}
    ]
    sd.paper_trading.get_positions.return_value = [
        {'ticker': 'AAPL', 'shares': 10, 'avg_cost_basis': 100.0}
    ]
    with (
        patch.object(cog.mutils, "market_open_today", return_value=True),
        patch.object(cog.mutils, "get_current_price", return_value=110.0),
    ):
        await cog._daily_snapshot_impl()
    sd.paper_trading.insert_snapshot.assert_called_once()
    call_kwargs = sd.paper_trading.insert_snapshot.call_args
    assert call_kwargs[1]['cash'] == 5000.0
    assert call_kwargs[1]['positions_value'] == pytest.approx(1100.0)  # 10 * 110.0
    assert call_kwargs[1]['portfolio_value'] == pytest.approx(6100.0)


# ---------------------------------------------------------------------------
# /trade buy
# ---------------------------------------------------------------------------

async def test_trade_buy_invalid_shares():
    cog, _, sd = _make_cog()
    interaction = _make_interaction()
    await cog.trade_buy.callback(cog, interaction, ticker="AAPL", shares=0)
    interaction.followup.send.assert_called_once()
    msg = interaction.followup.send.call_args[0][0]
    assert "greater than 0" in msg


async def test_trade_buy_invalid_ticker():
    cog, _, sd = _make_cog()
    sd.tickers.validate_ticker.return_value = False
    interaction = _make_interaction()
    await cog.trade_buy.callback(cog, interaction, ticker="FAKE", shares=10)
    msg = interaction.followup.send.call_args[0][0]
    assert "Unknown ticker" in msg


async def test_trade_buy_insufficient_cash():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_portfolio.return_value = {'guild_id': 1001, 'user_id': 2001, 'cash': 100.0}
    with patch.object(cog.mutils, "get_current_price", return_value=150.0):
        interaction = _make_interaction()
        await cog.trade_buy.callback(cog, interaction, ticker="AAPL", shares=10)
    msg = interaction.followup.send.call_args[0][0]
    assert "Insufficient cash" in msg


async def test_trade_buy_executes_intraday():
    cog, _, sd = _make_cog()
    from rocketstocks.bot.views.paper_trading_views import TradeConfirmView

    async def mock_view_wait(view):
        view.confirmed = True

    with (
        patch.object(cog.mutils, "get_current_price", return_value=150.0),
        patch.object(cog.mutils, "in_intraday", return_value=True),
        patch.object(cog.mutils, "_refresh_schedule_if_needed"),
        patch("rocketstocks.bot.cogs.paper_trading.TradeConfirmView") as MockView,
    ):
        mock_view = MagicMock()
        mock_view.confirmed = True
        mock_view.wait = AsyncMock()
        MockView.return_value = mock_view

        interaction = _make_interaction()
        interaction.followup.send = AsyncMock()
        interaction.edit_original_response = AsyncMock()

        await cog.trade_buy.callback(cog, interaction, ticker="AAPL", shares=10)

    sd.paper_trading.execute_buy.assert_called_once_with(1001, 2001, 'AAPL', 10, 150.0)


async def test_trade_buy_queues_off_hours():
    cog, _, sd = _make_cog()

    with (
        patch.object(cog.mutils, "get_current_price", return_value=150.0),
        patch.object(cog.mutils, "in_intraday", return_value=False),
        patch.object(cog.mutils, "_refresh_schedule_if_needed"),
        patch("rocketstocks.bot.cogs.paper_trading.TradeConfirmView") as MockView,
    ):
        mock_view = MagicMock()
        mock_view.confirmed = True
        mock_view.wait = AsyncMock()
        MockView.return_value = mock_view

        interaction = _make_interaction()
        interaction.followup.send = AsyncMock()
        interaction.edit_original_response = AsyncMock()

        await cog.trade_buy.callback(cog, interaction, ticker="AAPL", shares=10)

    sd.paper_trading.queue_buy_order.assert_called_once()
    sd.paper_trading.execute_buy.assert_not_called()


async def test_trade_buy_cancelled():
    cog, _, sd = _make_cog()

    with (
        patch.object(cog.mutils, "get_current_price", return_value=150.0),
        patch.object(cog.mutils, "_refresh_schedule_if_needed"),
        patch("rocketstocks.bot.cogs.paper_trading.TradeConfirmView") as MockView,
    ):
        mock_view = MagicMock()
        mock_view.confirmed = False
        mock_view.wait = AsyncMock()
        MockView.return_value = mock_view

        interaction = _make_interaction()
        interaction.followup.send = AsyncMock()
        interaction.edit_original_response = AsyncMock()

        await cog.trade_buy.callback(cog, interaction, ticker="AAPL", shares=10)

    sd.paper_trading.execute_buy.assert_not_called()
    interaction.edit_original_response.assert_called_once()
    msg = interaction.edit_original_response.call_args[1].get('content', '')
    assert "cancelled" in msg.lower()


# ---------------------------------------------------------------------------
# /trade sell
# ---------------------------------------------------------------------------

async def test_trade_sell_no_position():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_position.return_value = None
    interaction = _make_interaction()
    await cog.trade_sell.callback(cog, interaction, ticker="AAPL", shares=5)
    msg = interaction.followup.send.call_args[0][0]
    assert "don't own" in msg


async def test_trade_sell_exceeds_owned_shares():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_position.return_value = {'shares': 3, 'avg_cost_basis': 100.0}
    interaction = _make_interaction()
    await cog.trade_sell.callback(cog, interaction, ticker="AAPL", shares=10)
    msg = interaction.followup.send.call_args[0][0]
    assert "only own" in msg


async def test_trade_sell_executes_intraday():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_position.return_value = {'shares': 10, 'avg_cost_basis': 100.0}

    with (
        patch.object(cog.mutils, "get_current_price", return_value=200.0),
        patch.object(cog.mutils, "in_intraday", return_value=True),
        patch.object(cog.mutils, "_refresh_schedule_if_needed"),
        patch("rocketstocks.bot.cogs.paper_trading.TradeConfirmView") as MockView,
    ):
        mock_view = MagicMock()
        mock_view.confirmed = True
        mock_view.wait = AsyncMock()
        MockView.return_value = mock_view

        interaction = _make_interaction()
        interaction.followup.send = AsyncMock()
        interaction.edit_original_response = AsyncMock()

        await cog.trade_sell.callback(cog, interaction, ticker="AAPL", shares=5)

    sd.paper_trading.execute_sell.assert_called_once_with(1001, 2001, 'AAPL', 5, 200.0)


# ---------------------------------------------------------------------------
# /trade portfolio
# ---------------------------------------------------------------------------

async def test_trade_portfolio_sends_embed():
    cog, _, sd = _make_cog()
    interaction = _make_interaction()
    with patch.object(cog.mutils, "_refresh_schedule_if_needed"):
        await cog.trade_portfolio.callback(cog, interaction, user=None)
    interaction.followup.send.assert_called_once()


async def test_trade_portfolio_views_other_user():
    cog, _, sd = _make_cog()
    other = MagicMock()
    other.id = 9999
    other.display_name = "OtherUser"
    interaction = _make_interaction()
    with patch.object(cog.mutils, "_refresh_schedule_if_needed"):
        await cog.trade_portfolio.callback(cog, interaction, user=other)
    sd.paper_trading.get_portfolio.assert_called_with(interaction.guild_id, 9999)


# ---------------------------------------------------------------------------
# /trade history
# ---------------------------------------------------------------------------

async def test_trade_history_sends_embed():
    cog, _, sd = _make_cog()
    interaction = _make_interaction()
    await cog.trade_history.callback(cog, interaction, user=None)
    interaction.followup.send.assert_called_once()


async def test_trade_history_fetches_correct_user():
    cog, _, sd = _make_cog()
    interaction = _make_interaction(user_id=2001)
    await cog.trade_history.callback(cog, interaction, user=None)
    sd.paper_trading.get_transactions.assert_called_with(interaction.guild_id, 2001)


# ---------------------------------------------------------------------------
# /trade cancel
# ---------------------------------------------------------------------------

async def test_trade_cancel_no_id_shows_pending():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_pending_orders.return_value = [
        {'id': 1, 'side': 'BUY', 'shares': 10, 'ticker': 'AAPL', 'quoted_price': 150.0}
    ]
    interaction = _make_interaction()
    await cog.trade_cancel.callback(cog, interaction, order_id=None)
    msg = interaction.followup.send.call_args[0][0]
    assert "Pending Orders" in msg


async def test_trade_cancel_no_pending_no_id():
    cog, _, sd = _make_cog()
    interaction = _make_interaction()
    await cog.trade_cancel.callback(cog, interaction, order_id=None)
    msg = interaction.followup.send.call_args[0][0]
    assert "no pending orders" in msg.lower()


async def test_trade_cancel_buy_order():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_pending_orders.return_value = [
        {'id': 5, 'side': 'BUY', 'shares': 10, 'ticker': 'AAPL', 'quoted_price': 150.0}
    ]
    interaction = _make_interaction()
    await cog.trade_cancel.callback(cog, interaction, order_id=5)
    sd.paper_trading.cancel_buy_order.assert_called_once_with(5, 1001, 2001)


async def test_trade_cancel_sell_order():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_pending_orders.return_value = [
        {'id': 6, 'side': 'SELL', 'shares': 5, 'ticker': 'TSLA', 'quoted_price': 200.0}
    ]
    interaction = _make_interaction()
    await cog.trade_cancel.callback(cog, interaction, order_id=6)
    sd.paper_trading.cancel_sell_order.assert_called_once_with(6, 1001, 2001)


async def test_trade_cancel_order_not_found():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_pending_orders.return_value = []
    interaction = _make_interaction()
    await cog.trade_cancel.callback(cog, interaction, order_id=99)
    msg = interaction.followup.send.call_args[0][0]
    assert "not found" in msg.lower()


# ---------------------------------------------------------------------------
# /trade reset
# ---------------------------------------------------------------------------

async def test_trade_reset_confirmed():
    cog, _, sd = _make_cog()
    with patch("rocketstocks.bot.cogs.paper_trading.ConfirmResetView") as MockView:
        mock_view = MagicMock()
        mock_view.confirmed = True
        mock_view.wait = AsyncMock()
        MockView.return_value = mock_view

        interaction = _make_interaction()
        interaction.followup.send = AsyncMock()
        interaction.edit_original_response = AsyncMock()
        await cog.trade_reset.callback(cog, interaction)

    sd.paper_trading.reset_portfolio.assert_called_once_with(1001, 2001)
    msg = interaction.edit_original_response.call_args[1].get('content', '')
    assert "$10,000" in msg


async def test_trade_reset_cancelled():
    cog, _, sd = _make_cog()
    with patch("rocketstocks.bot.cogs.paper_trading.ConfirmResetView") as MockView:
        mock_view = MagicMock()
        mock_view.confirmed = False
        mock_view.wait = AsyncMock()
        MockView.return_value = mock_view

        interaction = _make_interaction()
        interaction.followup.send = AsyncMock()
        interaction.edit_original_response = AsyncMock()
        await cog.trade_reset.callback(cog, interaction)

    sd.paper_trading.reset_portfolio.assert_not_called()


async def test_trade_reset_timeout():
    cog, _, sd = _make_cog()
    with patch("rocketstocks.bot.cogs.paper_trading.ConfirmResetView") as MockView:
        mock_view = MagicMock()
        mock_view.confirmed = None
        mock_view.wait = AsyncMock()
        MockView.return_value = mock_view

        interaction = _make_interaction()
        interaction.followup.send = AsyncMock()
        interaction.edit_original_response = AsyncMock()
        await cog.trade_reset.callback(cog, interaction)

    sd.paper_trading.reset_portfolio.assert_not_called()
    msg = interaction.edit_original_response.call_args[1].get('content', '')
    assert "timed out" in msg.lower()


# ---------------------------------------------------------------------------
# _post_trade_announcement
# ---------------------------------------------------------------------------

async def test_post_announcement_sends_to_configured_channel():
    cog, bot, sd = _make_cog()
    sd.channel_config = MagicMock()
    sd.channel_config.get_channel_id = AsyncMock(return_value=9999)
    mock_channel = MagicMock()
    mock_channel.send = AsyncMock()
    bot.get_channel = MagicMock(return_value=mock_channel)

    from rocketstocks.core.content.models import TradeAnnouncementData
    data = TradeAnnouncementData(
        user_name="Alice", ticker="AAPL", ticker_name="Apple Inc.",
        side="BUY", shares=10, price=150.0, total=1500.0, was_queued=False
    )
    await cog._post_trade_announcement(1001, data)
    mock_channel.send.assert_called_once()


async def test_post_announcement_skips_when_no_channel_configured():
    cog, bot, sd = _make_cog()
    sd.channel_config = MagicMock()
    sd.channel_config.get_channel_id = AsyncMock(return_value=None)
    bot.get_channel = MagicMock()

    from rocketstocks.core.content.models import TradeAnnouncementData
    data = TradeAnnouncementData(
        user_name="Alice", ticker="AAPL", ticker_name="Apple Inc.",
        side="BUY", shares=10, price=150.0, total=1500.0, was_queued=False
    )
    await cog._post_trade_announcement(1001, data)
    bot.get_channel.assert_not_called()


async def test_post_announcement_skips_when_channel_not_in_cache():
    cog, bot, sd = _make_cog()
    sd.channel_config = MagicMock()
    sd.channel_config.get_channel_id = AsyncMock(return_value=9999)
    bot.get_channel = MagicMock(return_value=None)

    from rocketstocks.core.content.models import TradeAnnouncementData
    data = TradeAnnouncementData(
        user_name="Alice", ticker="AAPL", ticker_name="Apple Inc.",
        side="BUY", shares=10, price=150.0, total=1500.0, was_queued=False
    )
    # should not raise
    await cog._post_trade_announcement(1001, data)


async def test_buy_posts_announcement_after_execution():
    cog, bot, sd = _make_cog()
    sd.channel_config = MagicMock()
    sd.channel_config.get_channel_id = AsyncMock(return_value=None)  # no channel → no-op

    with (
        patch.object(cog.mutils, "get_current_price", return_value=150.0),
        patch.object(cog.mutils, "in_intraday", return_value=True),
        patch.object(cog.mutils, "_refresh_schedule_if_needed"),
        patch("rocketstocks.bot.cogs.paper_trading.TradeConfirmView") as MockView,
        patch.object(cog, "_post_trade_announcement", new=AsyncMock()) as mock_announce,
    ):
        mock_view = MagicMock()
        mock_view.confirmed = True
        mock_view.wait = AsyncMock()
        MockView.return_value = mock_view

        interaction = _make_interaction()
        interaction.followup.send = AsyncMock()
        interaction.edit_original_response = AsyncMock()

        await cog.trade_buy.callback(cog, interaction, ticker="AAPL", shares=10)

    mock_announce.assert_called_once()
    call_kwargs = mock_announce.call_args[0]
    assert call_kwargs[0] == 1001  # guild_id
    assert call_kwargs[1].side == "BUY"


async def test_sell_posts_announcement_after_execution():
    cog, bot, sd = _make_cog()
    sd.channel_config = MagicMock()
    sd.channel_config.get_channel_id = AsyncMock(return_value=None)
    sd.paper_trading.get_position.return_value = {'shares': 10, 'avg_cost_basis': 100.0}

    with (
        patch.object(cog.mutils, "get_current_price", return_value=200.0),
        patch.object(cog.mutils, "in_intraday", return_value=True),
        patch.object(cog.mutils, "_refresh_schedule_if_needed"),
        patch("rocketstocks.bot.cogs.paper_trading.TradeConfirmView") as MockView,
        patch.object(cog, "_post_trade_announcement", new=AsyncMock()) as mock_announce,
    ):
        mock_view = MagicMock()
        mock_view.confirmed = True
        mock_view.wait = AsyncMock()
        MockView.return_value = mock_view

        interaction = _make_interaction()
        interaction.followup.send = AsyncMock()
        interaction.edit_original_response = AsyncMock()

        await cog.trade_sell.callback(cog, interaction, ticker="AAPL", shares=5)

    mock_announce.assert_called_once()
    call_kwargs = mock_announce.call_args[0]
    assert call_kwargs[1].side == "SELL"


# ---------------------------------------------------------------------------
# /trade leaderboard
# ---------------------------------------------------------------------------

async def test_trade_leaderboard_no_portfolios():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_all_portfolios.return_value = []
    interaction = _make_interaction()
    await cog.trade_leaderboard.callback(cog, interaction)
    msg = interaction.followup.send.call_args[0][0]
    assert "No portfolios" in msg


async def test_trade_leaderboard_sends_embed():
    cog, bot, sd = _make_cog()
    sd.paper_trading.get_all_portfolios.return_value = [
        {'user_id': 2001, 'cash': 5000.0},
        {'user_id': 2002, 'cash': 8000.0},
    ]
    sd.paper_trading.get_positions.return_value = []

    guild = MagicMock()
    guild.name = "TestGuild"
    guild.get_member.return_value = None
    interaction = _make_interaction()
    interaction.guild = guild

    with patch.object(cog.mutils, "_refresh_schedule_if_needed"):
        await cog.trade_leaderboard.callback(cog, interaction)

    # Should send an embed (not a plain string)
    call_kwargs = interaction.followup.send.call_args[1]
    assert 'embed' in call_kwargs


async def test_trade_leaderboard_uses_member_display_name():
    cog, bot, sd = _make_cog()
    sd.paper_trading.get_all_portfolios.return_value = [
        {'user_id': 2001, 'cash': 9000.0},
    ]
    sd.paper_trading.get_positions.return_value = []

    member = MagicMock()
    member.display_name = "GuildMember"
    guild = MagicMock()
    guild.name = "G"
    guild.get_member.return_value = member

    interaction = _make_interaction()
    interaction.guild = guild

    with patch.object(cog.mutils, "_refresh_schedule_if_needed"):
        await cog.trade_leaderboard.callback(cog, interaction)

    interaction.followup.send.assert_called_once()


async def test_trade_leaderboard_falls_back_user_id_when_no_member():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_all_portfolios.return_value = [
        {'user_id': 9999, 'cash': 10000.0},
    ]
    sd.paper_trading.get_positions.return_value = []

    guild = MagicMock()
    guild.name = "G"
    guild.get_member.return_value = None  # member not cached

    interaction = _make_interaction()
    interaction.guild = guild

    with patch.object(cog.mutils, "_refresh_schedule_if_needed"):
        await cog.trade_leaderboard.callback(cog, interaction)

    interaction.followup.send.assert_called_once()


# ---------------------------------------------------------------------------
# /trade performance
# ---------------------------------------------------------------------------

async def test_trade_performance_sends_embed():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_snapshots = AsyncMock(return_value=[])

    with patch.object(cog.mutils, "_refresh_schedule_if_needed"):
        interaction = _make_interaction()
        await cog.trade_performance.callback(cog, interaction, days=7, user=None)

    interaction.followup.send.assert_called_once()


async def test_trade_performance_clamps_days_max():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_snapshots = AsyncMock(return_value=[])

    with patch.object(cog.mutils, "_refresh_schedule_if_needed"):
        interaction = _make_interaction()
        await cog.trade_performance.callback(cog, interaction, days=999, user=None)

    # Passes without error (days clamped to 30)
    interaction.followup.send.assert_called_once()


async def test_trade_performance_clamps_days_min():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_snapshots = AsyncMock(return_value=[])

    with patch.object(cog.mutils, "_refresh_schedule_if_needed"):
        interaction = _make_interaction()
        await cog.trade_performance.callback(cog, interaction, days=0, user=None)

    interaction.followup.send.assert_called_once()


async def test_trade_performance_fetches_correct_user():
    cog, _, sd = _make_cog()
    sd.paper_trading.get_snapshots = AsyncMock(return_value=[])

    other = MagicMock()
    other.id = 5555
    other.display_name = "OtherUser"

    with patch.object(cog.mutils, "_refresh_schedule_if_needed"):
        interaction = _make_interaction()
        await cog.trade_performance.callback(cog, interaction, days=7, user=other)

    sd.paper_trading.get_portfolio.assert_called_with(interaction.guild_id, 5555)


async def test_trade_performance_with_snapshots():
    import datetime as _dt
    cog, _, sd = _make_cog()
    snaps = [
        {'snapshot_date': _dt.date(2026, 3, 25), 'portfolio_value': 10500.0,
         'cash': 500.0, 'positions_value': 10000.0},
    ]
    sd.paper_trading.get_snapshots = AsyncMock(return_value=snaps)

    with patch.object(cog.mutils, "_refresh_schedule_if_needed"):
        interaction = _make_interaction()
        await cog.trade_performance.callback(cog, interaction, days=7, user=None)

    call_kwargs = interaction.followup.send.call_args[1]
    assert 'embed' in call_kwargs

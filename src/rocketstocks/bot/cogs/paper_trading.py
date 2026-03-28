"""Paper trading cog — virtual portfolio commands for Discord users."""
import datetime
import logging
import time
import traceback as tb

import discord
from discord import app_commands
from discord.ext import commands, tasks

from rocketstocks.data.stockdata import StockData
from rocketstocks.data.channel_config import TRADE
from rocketstocks.core.utils.market import MarketUtils
from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.notifications.event import NotificationEvent
from rocketstocks.core.analysis.paper_trading import (
    calculate_gain_loss,
    calculate_position_value,
    calculate_portfolio_total,
    calculate_total_gain_loss,
    evaluate_weekly_awards,
)
from rocketstocks.core.content.models import (
    LeaderboardEntry,
    LeaderboardViewData,
    PerformanceViewData,
    PortfolioPosition,
    PortfolioViewData,
    TradeAnnouncementData,
    TradeConfirmationData,
    TradeHistoryData,
    TradeQuoteData,
    WeeklyRoundupData,
)
from rocketstocks.core.content.reports.leaderboard import Leaderboard
from rocketstocks.core.content.reports.trade_announcement import TradeAnnouncement
from rocketstocks.core.content.reports.trade_quote import TradeQuote
from rocketstocks.core.content.reports.trade_confirmation import TradeConfirmation
from rocketstocks.core.content.reports.portfolio_view import PortfolioView
from rocketstocks.core.content.reports.trade_history import TradeHistory
from rocketstocks.core.content.reports.performance_view import PerformanceView
from rocketstocks.core.content.reports.weekly_roundup import WeeklyRoundup
from rocketstocks.bot.senders.embed_utils import spec_to_embed
from rocketstocks.bot.views.paper_trading_views import ConfirmResetView, TradeConfirmView

logger = logging.getLogger(__name__)

_STARTING_CASH = 10000.0
_MAX_SHARES_PER_ORDER = 10_000
_SNAPSHOT_HOUR_UTC = 21
_SNAPSHOT_MINUTE_UTC = 5
_MAX_PERFORMANCE_DAYS = 30
_ROUNDUP_HOUR_UTC = 18   # noon CT Sunday
_ROUNDUP_WEEKDAY = 6     # Sunday


class PaperTrading(commands.Cog):
    """Virtual portfolio trading for Discord users."""

    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.mutils = MarketUtils()

        self.execute_pending_orders.start()
        self.daily_snapshot.start()
        self.weekly_roundup.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    # -------------------------------------------------------------------------
    # Task runner helper
    # -------------------------------------------------------------------------

    async def _run_task(self, name: str, coro) -> None:
        """Run *coro*, emit SUCCESS/FAILURE notification. Never re-raises."""
        _start = time.monotonic()
        try:
            await coro
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS,
                source=__name__,
                job_name=name,
                message="Task completed successfully",
                elapsed_seconds=time.monotonic() - _start,
            ))
        except Exception as exc:
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source=__name__,
                job_name=name,
                message=str(exc),
                traceback=tb.format_exc(),
                elapsed_seconds=time.monotonic() - _start,
            ))

    # -------------------------------------------------------------------------
    # Task loops
    # -------------------------------------------------------------------------

    @tasks.loop(minutes=1)
    async def execute_pending_orders(self):
        """Execute queued orders at market open, every minute while intraday."""
        await self._run_task("execute_pending_orders", self._execute_pending_orders_impl())

    async def _execute_pending_orders_impl(self):
        self.mutils._refresh_schedule_if_needed()
        if not self.mutils.in_intraday():
            return

        pending = await self.stock_data.paper_trading.get_all_pending_orders()
        if not pending:
            return

        logger.info(f"Executing {len(pending)} pending paper trading orders at market open")
        for order in pending:
            try:
                quote = await self.stock_data.schwab.get_quote(order['ticker'])
                price = self.mutils.get_current_price(quote) if quote else 0.0
                if price <= 0:
                    logger.warning(f"Skipping order {order['id']}: could not get price for {order['ticker']}")
                    continue
                if order['side'] == 'BUY':
                    executed = await self.stock_data.paper_trading.execute_pending_buy(
                        order['guild_id'], order['user_id'], order['ticker'],
                        order['shares'], order['quoted_price'], price,
                    )
                    if executed:
                        await self.stock_data.paper_trading.mark_order_executed(order['id'], price)
                        logger.debug(
                            f"Executed queued BUY {order['shares']}x{order['ticker']} "
                            f"@ {price} (order {order['id']})"
                        )
                    else:
                        await self.stock_data.paper_trading.mark_order_cancelled(order['id'])
                        await self._notify_order_cancelled(order, price)
                        logger.info(
                            f"Cancelled queued BUY order {order['id']} — insufficient funds "
                            f"at market open price {price}"
                        )
                else:
                    await self.stock_data.paper_trading.execute_pending_sell(
                        order['guild_id'], order['user_id'], order['ticker'],
                        order['shares'], price,
                    )
                    await self.stock_data.paper_trading.mark_order_executed(order['id'], price)
                    logger.debug(
                        f"Executed queued SELL {order['shares']}x{order['ticker']} "
                        f"@ {price} (order {order['id']})"
                    )
            except Exception:
                logger.error(f"Failed to execute pending order {order['id']}", exc_info=True)

    async def _notify_order_cancelled(self, order: dict, market_price: float) -> None:
        """Post a cancellation notice to the trade channel when an order is cancelled at market open."""
        channel_id = await self.stock_data.channel_config.get_channel_id(order['guild_id'], TRADE)
        if not channel_id:
            return
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return
        needed = order['shares'] * market_price
        try:
            await channel.send(
                f"<@{order['user_id']}> Your queued **BUY** order for "
                f"**{order['shares']:,}x {order['ticker']}** was cancelled at market open — "
                f"insufficient funds. "
                f"Market open price: **${market_price:,.2f}** "
                f"(total needed: **${needed:,.2f}**)."
            )
        except Exception:
            logger.warning(
                f"Failed to post cancellation notice for order {order['id']}",
                exc_info=True,
            )

    @tasks.loop(time=datetime.time(hour=_SNAPSHOT_HOUR_UTC, minute=_SNAPSHOT_MINUTE_UTC))
    async def daily_snapshot(self):
        """Take daily portfolio snapshots at market close (21:05 UTC)."""
        await self._run_task("daily_snapshot", self._daily_snapshot_impl())

    async def _daily_snapshot_impl(self):
        if not self.mutils.market_open_today():
            return

        today = datetime.date.today()
        guild_ids = await self.stock_data.paper_trading.get_distinct_guild_ids()
        logger.info(f"Taking daily snapshots for {len(guild_ids)} guilds")

        for guild_id in guild_ids:
            portfolios = await self.stock_data.paper_trading.get_all_portfolios(guild_id)
            for portfolio in portfolios:
                try:
                    user_id = portfolio['user_id']
                    positions = await self.stock_data.paper_trading.get_positions(guild_id, user_id)
                    positions_value = 0.0
                    for pos in positions:
                        quote = await self.stock_data.schwab.get_quote(pos['ticker'])
                        price = self.mutils.get_current_price(quote) if quote else pos['avg_cost_basis']
                        positions_value += calculate_position_value(pos['shares'], price)
                    total_value = calculate_portfolio_total(portfolio['cash'], positions_value)
                    await self.stock_data.paper_trading.insert_snapshot(
                        guild_id=guild_id,
                        user_id=user_id,
                        snapshot_date=today,
                        portfolio_value=total_value,
                        cash=portfolio['cash'],
                        positions_value=positions_value,
                    )
                except Exception:
                    logger.error(
                        f"Failed snapshot for guild={guild_id} user={portfolio.get('user_id')}",
                        exc_info=True,
                    )

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    async def _ensure_portfolio(self, guild_id: int, user_id: int) -> dict:
        """Get or auto-create a portfolio for the user."""
        portfolio = await self.stock_data.paper_trading.get_portfolio(guild_id, user_id)
        if portfolio is None:
            await self.stock_data.paper_trading.create_portfolio(guild_id, user_id)
            portfolio = await self.stock_data.paper_trading.get_portfolio(guild_id, user_id)
        return portfolio

    async def _get_price(self, ticker: str) -> float:
        """Fetch the current price for a ticker via Schwab quote."""
        self.mutils._refresh_schedule_if_needed()
        quote = await self.stock_data.schwab.get_quote(ticker)
        return self.mutils.get_current_price(quote) if quote else 0.0

    async def _build_portfolio_view_data(
        self, guild_id: int, user_id: int, user_name: str
    ) -> PortfolioViewData:
        """Build PortfolioViewData by fetching live prices for all positions."""
        portfolio = await self._ensure_portfolio(guild_id, user_id)
        positions_raw = await self.stock_data.paper_trading.get_positions(guild_id, user_id)
        pending_orders = await self.stock_data.paper_trading.get_pending_orders(guild_id, user_id)

        positions = []
        positions_value = 0.0
        for pos in positions_raw:
            price = await self._get_price(pos['ticker'])
            if price <= 0:
                price = pos['avg_cost_basis']
            market_value = calculate_position_value(pos['shares'], price)
            gain_loss, gain_loss_pct = calculate_gain_loss(pos['shares'], pos['avg_cost_basis'], price)
            positions_value += market_value
            positions.append(PortfolioPosition(
                ticker=pos['ticker'],
                shares=pos['shares'],
                avg_cost_basis=pos['avg_cost_basis'],
                current_price=price,
                market_value=market_value,
                gain_loss=gain_loss,
                gain_loss_pct=gain_loss_pct,
            ))

        total_value = calculate_portfolio_total(portfolio['cash'], positions_value)
        total_gain_loss, total_gain_loss_pct = calculate_total_gain_loss(total_value)

        return PortfolioViewData(
            user_name=user_name,
            cash=portfolio['cash'],
            positions=positions,
            pending_orders=pending_orders,
            total_value=total_value,
            total_gain_loss=total_gain_loss,
            total_gain_loss_pct=total_gain_loss_pct,
        )

    # -------------------------------------------------------------------------
    # Trade announcement helper
    # -------------------------------------------------------------------------

    async def _post_trade_announcement(
        self, guild_id: int, announce_data: TradeAnnouncementData
    ) -> None:
        """Post a compact trade embed to the configured TRADE channel (best-effort)."""
        channel_id = await self.stock_data.channel_config.get_channel_id(guild_id, TRADE)
        if not channel_id:
            return
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return
        embed = spec_to_embed(TradeAnnouncement(announce_data).build())
        try:
            await channel.send(embed=embed)
        except Exception:
            logger.warning(
                f"Failed to post trade announcement to channel {channel_id} guild={guild_id}",
                exc_info=True,
            )

    # -------------------------------------------------------------------------
    # Ticker autocomplete
    # -------------------------------------------------------------------------

    async def ticker_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        """Autocomplete for a single ticker symbol."""
        partial = current.upper()
        all_tickers = await self.stock_data.tickers.get_all_tickers()
        return [
            app_commands.Choice(name=t, value=t)
            for t in all_tickers if t.startswith(partial)
        ][:25]

    # -------------------------------------------------------------------------
    # Command group
    # -------------------------------------------------------------------------

    trade_group = app_commands.Group(name="trade", description="Paper trading commands")

    # -------------------------------------------------------------------------
    # /trade buy
    # -------------------------------------------------------------------------

    @trade_group.command(name="buy", description="Buy shares in your paper trading portfolio")
    @app_commands.describe(ticker="Stock ticker symbol", shares="Number of shares to buy")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def trade_buy(self, interaction: discord.Interaction, ticker: str, shares: int):
        await interaction.response.defer(ephemeral=True)
        ticker = ticker.upper()

        if shares <= 0:
            await interaction.followup.send("Shares must be greater than 0.", ephemeral=True)
            return
        if shares > _MAX_SHARES_PER_ORDER:
            await interaction.followup.send(
                f"Maximum {_MAX_SHARES_PER_ORDER:,} shares per order.", ephemeral=True
            )
            return

        valid = await self.stock_data.tickers.validate_ticker(ticker)
        if not valid:
            await interaction.followup.send(f"Unknown ticker: **{ticker}**", ephemeral=True)
            return

        portfolio = await self._ensure_portfolio(interaction.guild_id, interaction.user.id)
        self.mutils._refresh_schedule_if_needed()
        price = await self._get_price(ticker)
        if price <= 0:
            await interaction.followup.send(
                f"Could not get a current price for **{ticker}**. Try again.", ephemeral=True
            )
            return

        total = shares * price
        if portfolio['cash'] < total:
            await interaction.followup.send(
                f"Insufficient cash. You need **${total:,.2f}** but have **${portfolio['cash']:,.2f}**.",
                ephemeral=True,
            )
            return

        ticker_info = await self.stock_data.tickers.get_ticker_info(ticker)
        ticker_name = ticker_info.get('name', ticker) if ticker_info else ticker
        cash_after = portfolio['cash'] - total

        quote_data = TradeQuoteData(
            ticker=ticker,
            ticker_name=ticker_name,
            side="BUY",
            shares=shares,
            price=price,
            total=total,
            cash_after=cash_after,
        )
        view = TradeConfirmView(side="BUY")
        embed = spec_to_embed(TradeQuote(quote_data).build())
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        await view.wait()

        if view.confirmed is None:
            await interaction.edit_original_response(
                content="Order expired — no action taken.", embed=None, view=None
            )
            return
        if not view.confirmed:
            await interaction.edit_original_response(
                content="Order cancelled.", embed=None, view=None
            )
            return

        # Execute or queue
        was_queued = not self.mutils.in_intraday()
        if was_queued:
            await self.stock_data.paper_trading.queue_buy_order(
                interaction.guild_id, interaction.user.id, ticker, shares, price
            )
            # Refresh cash after queue deduction
            portfolio = await self.stock_data.paper_trading.get_portfolio(
                interaction.guild_id, interaction.user.id
            )
        else:
            await self.stock_data.paper_trading.execute_buy(
                interaction.guild_id, interaction.user.id, ticker, shares, price
            )
            portfolio = await self.stock_data.paper_trading.get_portfolio(
                interaction.guild_id, interaction.user.id
            )

        confirm_data = TradeConfirmationData(
            ticker=ticker,
            ticker_name=ticker_name,
            side="BUY",
            shares=shares,
            price=price,
            total=total,
            cash_remaining=portfolio['cash'],
            was_queued=was_queued,
        )
        confirm_embed = spec_to_embed(TradeConfirmation(confirm_data).build())
        await interaction.edit_original_response(embed=confirm_embed, view=None)

        await self._post_trade_announcement(
            interaction.guild_id,
            TradeAnnouncementData(
                user_name=interaction.user.display_name,
                ticker=ticker,
                ticker_name=ticker_name,
                side="BUY",
                shares=shares,
                price=price,
                total=total,
                was_queued=was_queued,
            ),
        )

    # -------------------------------------------------------------------------
    # /trade sell
    # -------------------------------------------------------------------------

    @trade_group.command(name="sell", description="Sell shares from your paper trading portfolio")
    @app_commands.describe(ticker="Stock ticker symbol", shares="Number of shares to sell")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def trade_sell(self, interaction: discord.Interaction, ticker: str, shares: int):
        await interaction.response.defer(ephemeral=True)
        ticker = ticker.upper()

        if shares <= 0:
            await interaction.followup.send("Shares must be greater than 0.", ephemeral=True)
            return

        position = await self.stock_data.paper_trading.get_position(
            interaction.guild_id, interaction.user.id, ticker
        )
        if not position:
            await interaction.followup.send(
                f"You don't own any shares of **{ticker}**.", ephemeral=True
            )
            return
        if shares > position['shares']:
            await interaction.followup.send(
                f"You only own {position['shares']:,} shares of **{ticker}**.", ephemeral=True
            )
            return

        self.mutils._refresh_schedule_if_needed()
        price = await self._get_price(ticker)
        if price <= 0:
            await interaction.followup.send(
                f"Could not get a current price for **{ticker}**. Try again.", ephemeral=True
            )
            return

        total = shares * price
        portfolio = await self._ensure_portfolio(interaction.guild_id, interaction.user.id)
        cash_after = portfolio['cash'] + total

        ticker_info = await self.stock_data.tickers.get_ticker_info(ticker)
        ticker_name = ticker_info.get('name', ticker) if ticker_info else ticker

        quote_data = TradeQuoteData(
            ticker=ticker,
            ticker_name=ticker_name,
            side="SELL",
            shares=shares,
            price=price,
            total=total,
            cash_after=cash_after,
        )
        view = TradeConfirmView(side="SELL")
        embed = spec_to_embed(TradeQuote(quote_data).build())
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        await view.wait()

        if view.confirmed is None:
            await interaction.edit_original_response(
                content="Order expired — no action taken.", embed=None, view=None
            )
            return
        if not view.confirmed:
            await interaction.edit_original_response(
                content="Order cancelled.", embed=None, view=None
            )
            return

        was_queued = not self.mutils.in_intraday()
        if was_queued:
            await self.stock_data.paper_trading.queue_sell_order(
                interaction.guild_id, interaction.user.id, ticker, shares, price
            )
            portfolio = await self.stock_data.paper_trading.get_portfolio(
                interaction.guild_id, interaction.user.id
            )
        else:
            await self.stock_data.paper_trading.execute_sell(
                interaction.guild_id, interaction.user.id, ticker, shares, price
            )
            portfolio = await self.stock_data.paper_trading.get_portfolio(
                interaction.guild_id, interaction.user.id
            )

        confirm_data = TradeConfirmationData(
            ticker=ticker,
            ticker_name=ticker_name,
            side="SELL",
            shares=shares,
            price=price,
            total=total,
            cash_remaining=portfolio['cash'],
            was_queued=was_queued,
        )
        confirm_embed = spec_to_embed(TradeConfirmation(confirm_data).build())
        await interaction.edit_original_response(embed=confirm_embed, view=None)

        await self._post_trade_announcement(
            interaction.guild_id,
            TradeAnnouncementData(
                user_name=interaction.user.display_name,
                ticker=ticker,
                ticker_name=ticker_name,
                side="SELL",
                shares=shares,
                price=price,
                total=total,
                was_queued=was_queued,
            ),
        )

    # -------------------------------------------------------------------------
    # /trade portfolio
    # -------------------------------------------------------------------------

    @trade_group.command(name="portfolio", description="View your paper trading portfolio")
    @app_commands.describe(user="User to view (defaults to yourself)")
    async def trade_portfolio(
        self, interaction: discord.Interaction, user: discord.Member | None = None
    ):
        await interaction.response.defer(ephemeral=True)
        target = user or interaction.user
        view_data = await self._build_portfolio_view_data(
            interaction.guild_id, target.id, target.display_name
        )
        embed = spec_to_embed(PortfolioView(view_data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    # -------------------------------------------------------------------------
    # /trade history
    # -------------------------------------------------------------------------

    @trade_group.command(name="history", description="View your recent trades")
    @app_commands.describe(user="User to view (defaults to yourself)")
    async def trade_history(
        self, interaction: discord.Interaction, user: discord.Member | None = None
    ):
        await interaction.response.defer(ephemeral=True)
        target = user or interaction.user
        transactions = await self.stock_data.paper_trading.get_transactions(
            interaction.guild_id, target.id
        )
        history_data = TradeHistoryData(
            user_name=target.display_name,
            transactions=transactions,
        )
        embed = spec_to_embed(TradeHistory(history_data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    # -------------------------------------------------------------------------
    # /trade cancel
    # -------------------------------------------------------------------------

    @trade_group.command(name="cancel", description="Cancel a pending queued order")
    @app_commands.describe(order_id="Order ID to cancel (leave blank to see pending orders)")
    async def trade_cancel(
        self, interaction: discord.Interaction, order_id: int | None = None
    ):
        await interaction.response.defer(ephemeral=True)

        if order_id is None:
            # Show pending orders
            pending = await self.stock_data.paper_trading.get_pending_orders(
                interaction.guild_id, interaction.user.id
            )
            if not pending:
                await interaction.followup.send("You have no pending orders.", ephemeral=True)
                return
            lines = [
                f"**#{o['id']}** — {o['side']} {o['shares']:,}x **{o['ticker']}** "
                f"@ ${o['quoted_price']:,.2f}"
                for o in pending
            ]
            await interaction.followup.send(
                "**Pending Orders:**\n" + "\n".join(lines)
                + "\n\nRun `/trade cancel <order_id>` to cancel a specific order.",
                ephemeral=True,
            )
            return

        # Determine order side before cancelling
        pending = await self.stock_data.paper_trading.get_pending_orders(
            interaction.guild_id, interaction.user.id
        )
        order = next((o for o in pending if o['id'] == order_id), None)
        if not order:
            await interaction.followup.send(
                f"Order #{order_id} not found or already executed/cancelled.", ephemeral=True
            )
            return

        if order['side'] == 'BUY':
            cancelled = await self.stock_data.paper_trading.cancel_buy_order(
                order_id, interaction.guild_id, interaction.user.id
            )
        else:
            cancelled = await self.stock_data.paper_trading.cancel_sell_order(
                order_id, interaction.guild_id, interaction.user.id
            )

        if cancelled:
            await interaction.followup.send(
                f"Cancelled order #{order_id}: {order['side']} {order['shares']:,}x "
                f"**{order['ticker']}** @ ${order['quoted_price']:,.2f}.",
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                f"Could not cancel order #{order_id}.", ephemeral=True
            )

    # -------------------------------------------------------------------------
    # /trade reset
    # -------------------------------------------------------------------------

    @trade_group.command(name="reset", description="Reset your portfolio to $10,000 starting cash")
    async def trade_reset(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        view = ConfirmResetView()
        await interaction.followup.send(
            "**Reset portfolio?** This will clear all positions, transactions, and pending orders. "
            "Your cash will be restored to **$10,000**.",
            view=view,
            ephemeral=True,
        )
        await view.wait()

        if view.confirmed is None:
            await interaction.edit_original_response(
                content="Reset timed out — no action taken.", view=None
            )
            return
        if not view.confirmed:
            await interaction.edit_original_response(
                content="Reset cancelled.", view=None
            )
            return

        await self.stock_data.paper_trading.reset_portfolio(
            interaction.guild_id, interaction.user.id
        )
        await interaction.edit_original_response(
            content="Portfolio reset! You now have **$10,000** to invest.", view=None
        )


    # -------------------------------------------------------------------------
    # /trade leaderboard
    # -------------------------------------------------------------------------

    @trade_group.command(name="leaderboard", description="View the paper trading leaderboard for this server")
    async def trade_leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        portfolios = await self.stock_data.paper_trading.get_all_portfolios(interaction.guild_id)
        if not portfolios:
            await interaction.followup.send("No portfolios yet. Use `/trade buy` to get started!", ephemeral=True)
            return

        entries = []
        for portfolio in portfolios:
            user_id = portfolio['user_id']
            positions = await self.stock_data.paper_trading.get_positions(interaction.guild_id, user_id)

            positions_value = 0.0
            for pos in positions:
                price = await self._get_price(pos['ticker'])
                if price <= 0:
                    price = pos['avg_cost_basis']
                positions_value += calculate_position_value(pos['shares'], price)

            total_value = calculate_portfolio_total(portfolio['cash'], positions_value)
            total_gain_loss, total_gain_loss_pct = calculate_total_gain_loss(total_value)

            member = interaction.guild.get_member(user_id)
            user_name = member.display_name if member else f"User #{user_id}"

            entries.append(LeaderboardEntry(
                user_id=user_id,
                user_name=user_name,
                total_value=total_value,
                total_gain_loss=total_gain_loss,
                total_gain_loss_pct=total_gain_loss_pct,
                position_count=len(positions),
            ))

        entries.sort(key=lambda e: e.total_value, reverse=True)
        guild_name = interaction.guild.name if interaction.guild else "This Server"
        view_data = LeaderboardViewData(guild_name=guild_name, entries=entries)
        embed = spec_to_embed(Leaderboard(view_data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    # -------------------------------------------------------------------------
    # /trade performance
    # -------------------------------------------------------------------------

    @trade_group.command(name="performance", description="View your portfolio performance over time")
    @app_commands.describe(
        days="Number of days to look back (1–30, default 7)",
        user="User to view (defaults to yourself)",
    )
    async def trade_performance(
        self,
        interaction: discord.Interaction,
        days: int = 7,
        user: discord.Member | None = None,
    ):
        await interaction.response.defer(ephemeral=True)

        days = max(1, min(days, _MAX_PERFORMANCE_DAYS))
        target = user or interaction.user

        portfolio = await self._ensure_portfolio(interaction.guild_id, target.id)
        positions = await self.stock_data.paper_trading.get_positions(interaction.guild_id, target.id)

        positions_value = 0.0
        for pos in positions:
            price = await self._get_price(pos['ticker'])
            if price <= 0:
                price = pos['avg_cost_basis']
            positions_value += calculate_position_value(pos['shares'], price)
        current_value = calculate_portfolio_total(portfolio['cash'], positions_value)
        total_gain_loss, total_gain_loss_pct = calculate_total_gain_loss(current_value)

        import datetime as _dt
        end_date = _dt.date.today()
        start_date = end_date - _dt.timedelta(days=days - 1)
        snapshots = await self.stock_data.paper_trading.get_snapshots(
            interaction.guild_id, target.id, start_date, end_date
        )

        perf_data = PerformanceViewData(
            user_name=target.display_name,
            snapshots=snapshots,
            days=days,
            current_value=current_value,
            total_gain_loss=total_gain_loss,
            total_gain_loss_pct=total_gain_loss_pct,
        )
        embed = spec_to_embed(PerformanceView(perf_data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)


    @tasks.loop(time=datetime.time(hour=_ROUNDUP_HOUR_UTC, minute=0))
    async def weekly_roundup(self):
        """Post the weekly paper trading roundup every Sunday at 18:00 UTC."""
        await self._run_task("weekly_roundup", self._weekly_roundup_impl())

    async def _weekly_roundup_impl(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        if now.weekday() != _ROUNDUP_WEEKDAY:
            return

        channels = await self.bot.iter_channels(TRADE)
        if not channels:
            return

        week_end = now.date()
        week_start = week_end - datetime.timedelta(days=6)
        week_label = (
            f"{week_start.strftime('%b %-d')}–{week_end.strftime('%-d, %Y')}"
        )

        for guild_id, channel in channels:
            try:
                await self._post_guild_roundup(guild_id, channel, week_start, week_end, week_label)
            except Exception:
                logger.error(f"Failed weekly roundup for guild={guild_id}", exc_info=True)

    async def _post_guild_roundup(
        self,
        guild_id: int,
        channel,
        week_start: datetime.date,
        week_end: datetime.date,
        week_label: str,
    ) -> None:
        portfolios = await self.stock_data.paper_trading.get_all_portfolios(guild_id)
        if not portfolios:
            return

        guild = self.bot.get_guild(guild_id)
        guild_name = guild.name if guild else f"Guild #{guild_id}"

        # Collect user display names
        user_names: dict = {}
        for portfolio in portfolios:
            uid = portfolio['user_id']
            member = guild.get_member(uid) if guild else None
            user_names[uid] = member.display_name if member else f"User #{uid}"

        # Fetch current prices and build positions_by_user / portfolio_values_by_user
        positions_by_user: dict = {}
        portfolio_values_by_user: dict = {}
        leaderboard_entries = []

        for portfolio in portfolios:
            uid = portfolio['user_id']
            raw_positions = await self.stock_data.paper_trading.get_positions(guild_id, uid)
            enriched = []
            positions_value = 0.0
            for pos in raw_positions:
                price = await self._get_price(pos['ticker'])
                if price <= 0:
                    price = pos['avg_cost_basis']
                market_value = calculate_position_value(pos['shares'], price)
                positions_value += market_value
                enriched.append({**pos, 'market_value': market_value, 'current_price': price})
            positions_by_user[uid] = enriched
            total_value = calculate_portfolio_total(portfolio['cash'], positions_value)
            portfolio_values_by_user[uid] = total_value
            gain_loss, gain_loss_pct = calculate_total_gain_loss(total_value)
            leaderboard_entries.append(LeaderboardEntry(
                user_id=uid,
                user_name=user_names[uid],
                total_value=total_value,
                total_gain_loss=gain_loss,
                total_gain_loss_pct=gain_loss_pct,
                position_count=len(enriched),
            ))

        leaderboard_entries.sort(key=lambda e: e.total_gain_loss_pct, reverse=True)

        # Fetch weekly snapshots
        snapshots_by_user: dict = {}
        for portfolio in portfolios:
            uid = portfolio['user_id']
            snaps = await self.stock_data.paper_trading.get_snapshots(
                guild_id, uid, week_start, week_end
            )
            if snaps:
                snapshots_by_user[uid] = snaps

        # Fetch weekly transactions
        week_start_dt = datetime.datetime.combine(
            week_start, datetime.time.min, tzinfo=datetime.timezone.utc
        )
        transactions = await self.stock_data.paper_trading.get_guild_transactions(
            guild_id, week_start_dt
        )

        # Fetch daily price history for held tickers
        held_tickers = {
            pos['ticker']
            for positions in positions_by_user.values()
            for pos in positions
        }
        price_history: dict = {}
        for ticker in held_tickers:
            df = await self.stock_data.price_history.fetch_daily_price_history(
                ticker, start_date=week_start, end_date=week_end
            )
            if df is not None and not df.empty:
                price_history[ticker] = [
                    {
                        'date': row['date'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                    }
                    for _, row in df.iterrows()
                ]

        # Fetch ticker sectors
        ticker_sectors: dict = {}
        for ticker in held_tickers:
            info = await self.stock_data.tickers.get_ticker_info(ticker)
            if info and info.get('sector'):
                ticker_sectors[ticker] = info['sector']

        # Evaluate awards
        all_user_ids = [p['user_id'] for p in portfolios]
        awards = evaluate_weekly_awards(
            snapshots_by_user=snapshots_by_user,
            transactions=transactions,
            positions_by_user=positions_by_user,
            portfolio_values_by_user=portfolio_values_by_user,
            price_history=price_history,
            ticker_sectors=ticker_sectors,
            all_user_ids=all_user_ids,
            user_names=user_names,
        )

        # Server stats
        active_traders = len({tx['user_id'] for tx in transactions})
        total_volume = sum(tx['total'] for tx in transactions)
        ticker_trade_counts: dict = {}
        for tx in transactions:
            ticker_trade_counts[tx['ticker']] = ticker_trade_counts.get(tx['ticker'], 0) + 1
        most_traded = max(ticker_trade_counts, key=ticker_trade_counts.get) if ticker_trade_counts else None

        roundup_data = WeeklyRoundupData(
            guild_name=guild_name,
            week_label=week_label,
            leaderboard=leaderboard_entries,
            awards=awards,
            server_stats={
                'total_trades': len(transactions),
                'active_traders': active_traders,
                'most_traded_ticker': most_traded,
                'total_volume': total_volume,
            },
        )

        content = WeeklyRoundup(roundup_data)
        primary_embed = spec_to_embed(content.build())
        await channel.send(embed=primary_embed)
        if content.needs_split():
            awards_embed = spec_to_embed(content.build_awards_embed())
            await channel.send(embed=awards_embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(PaperTrading(bot=bot, stock_data=bot.stock_data))

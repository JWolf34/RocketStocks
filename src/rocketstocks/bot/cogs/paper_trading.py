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
)
from rocketstocks.core.content.models import (
    PortfolioPosition,
    PortfolioViewData,
    TradeConfirmationData,
    TradeHistoryData,
    TradeQuoteData,
)
from rocketstocks.core.content.reports.trade_quote import TradeQuote
from rocketstocks.core.content.reports.trade_confirmation import TradeConfirmation
from rocketstocks.core.content.reports.portfolio_view import PortfolioView
from rocketstocks.core.content.reports.trade_history import TradeHistory
from rocketstocks.bot.senders.embed_utils import spec_to_embed
from rocketstocks.bot.views.paper_trading_views import ConfirmResetView, TradeConfirmView

logger = logging.getLogger(__name__)

_STARTING_CASH = 10000.0
_MAX_SHARES_PER_ORDER = 10_000
_SNAPSHOT_HOUR_UTC = 21
_SNAPSHOT_MINUTE_UTC = 5


class PaperTrading(commands.Cog):
    """Virtual portfolio trading for Discord users."""

    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.mutils = MarketUtils()

        self.execute_pending_orders.start()
        self.daily_snapshot.start()

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

        logger.info(f"Executing {len(pending)} pending paper trading orders")
        for order in pending:
            try:
                quote = await self.stock_data.schwab.get_quote(order['ticker'])
                price = self.mutils.get_current_price(quote) if quote else 0.0
                if price <= 0:
                    logger.warning(f"Skipping order {order['id']}: could not get price for {order['ticker']}")
                    continue
                if order['side'] == 'BUY':
                    await self.stock_data.paper_trading.execute_buy(
                        order['guild_id'], order['user_id'], order['ticker'], order['shares'], price
                    )
                else:
                    await self.stock_data.paper_trading.execute_sell(
                        order['guild_id'], order['user_id'], order['ticker'], order['shares'], price
                    )
                await self.stock_data.paper_trading.mark_order_executed(order['id'], price)
                logger.debug(
                    f"Executed queued {order['side']} {order['shares']}x{order['ticker']} "
                    f"@ {price} (order {order['id']})"
                )
            except Exception:
                logger.error(f"Failed to execute pending order {order['id']}", exc_info=True)

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


async def setup(bot: commands.Bot):
    await bot.add_cog(PaperTrading(bot=bot, stock_data=bot.stock_data))

import datetime
import logging
import random
import asyncio
import time
import traceback as tb
import pandas as pd
import pandas_market_calendars as mcal
import discord
from discord import app_commands
from discord.ext import commands, tasks

from src.rocketstocks.data.stockdata import StockData
from rocketstocks.data.channel_config import REPORTS, SCREENERS, ALERTS
from rocketstocks.data.discord_state import DiscordState
from rocketstocks.data.clients.schwab import SchwabTokenError, SchwabRateLimitError
from rocketstocks.data.clients.news import News
from rocketstocks.core.utils.market import MarketUtils
from rocketstocks.core.utils.dates import round_down_nearest_minute, seconds_until_minute_interval, timezone
from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.notifications.event import NotificationEvent

from rocketstocks.core.content.models import (
    AlertSummaryData,
    StockReportData,
    NewsReportData,
    PopularityReportData,
    PopularityScreenerData,
    GainerScreenerData,
    VolumeScreenerData,
    EarningsSpotlightData,
    EarningsResultData,
    WeeklyEarningsData,
    PoliticianReportData,
)
from rocketstocks.core.content.reports.alert_summary import AlertSummary
from rocketstocks.core.content.reports.stock_report import StockReport
from rocketstocks.core.content.reports.news_report import NewsReport
from rocketstocks.core.content.reports.popularity_report import PopularityReport
from rocketstocks.core.content.reports.earnings_report import EarningsSpotlightReport
from rocketstocks.core.content.reports.earnings_result_report import EarningsResultReport
from rocketstocks.core.content.reports.politician_report import PoliticianReport
from rocketstocks.core.content.screeners.popularity_screener import PopularityScreener
from rocketstocks.core.content.screeners.gainer_screener import GainerScreener
from rocketstocks.core.content.screeners.volume_screener import VolumeScreener
from rocketstocks.core.content.screeners.earnings_screener import WeeklyEarningsScreener

from rocketstocks.bot.views.report_views import (
    StockReportButtons, GainerScreenerButtons, VolumeScreenerButtons,
    PopularityScreenerButtons, PopularityReportButtons, PoliticianReportButtons,
)
from rocketstocks.bot.views.subscription_views import AlertSubscriptionSelect, AlertSubscriptionView
from rocketstocks.bot.senders.report_sender import send_report, send_screener
from rocketstocks.bot.senders.embed_utils import spec_to_embed

logger = logging.getLogger(__name__)


def _resolve_since_dt(value: str) -> tuple[datetime.datetime, str]:
    """Map choice value → (since_datetime, human_label)."""
    calendar = mcal.get_calendar('NYSE')
    today = datetime.date.today()

    if value == 'market_open_today':
        # 9:30 AM ET = 14:30 UTC
        market_open = datetime.datetime.combine(today, datetime.time(14, 30))
        return market_open, 'since market open today'

    if value == 'last_3_days':
        return datetime.datetime.combine(today - datetime.timedelta(days=3), datetime.time.min), 'last 3 days'

    if value == 'last_7_days':
        return datetime.datetime.combine(today - datetime.timedelta(days=7), datetime.time.min), 'last 7 days'

    # Default: 'last_close' → previous trading day at 4 PM ET (21:00 UTC)
    valid = calendar.valid_days(start_date=today - datetime.timedelta(days=10), end_date=today)
    prev_day = [d.date() for d in valid if d.date() < today][-1]
    prev_close = datetime.datetime.combine(prev_day, datetime.time(21, 0))  # 4 PM ET in UTC
    return prev_close, f'since last close ({prev_day.strftime("%b %d")})'


class Reports(commands.Cog):
    """Cog for managing Reports and Screeners to be posted to Discord"""

    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.mutils = MarketUtils()
        self.dstate = DiscordState(db=self.stock_data.db)

        self.post_popularity_screener.start()
        self.post_volume_screener.start()
        self.post_volume_at_time_screener.start()
        self.post_gainer_screener.start()
        self.update_earnings_calendar.start()
        self.post_earnings_spotlight.start()
        self.post_weekly_earnings.start()
        self.post_earnings_results.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"{__name__} loaded")

    async def get_watchlist_options(self, interaction: discord.Interaction, current: str):
        return await self.bot.get_cog('Watchlists').watchlist_options(interaction=interaction, current=current)

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

    #########
    # Tasks #
    #########

    @tasks.loop(minutes=30)
    async def post_popularity_screener(self):
        """Retrieve latest popularity, insert into database, and post screener"""
        await self._run_task("post_popularity_screener", self._post_popularity_screener_impl())

    async def _post_popularity_screener_impl(self):
        popular_stocks = await asyncio.to_thread(self.stock_data.popularity.get_popular_stocks)

        if not popular_stocks.empty:
            popular_stocks.insert(loc=0,
                                  column='datetime',
                                  value=pd.Series([round_down_nearest_minute(30)] * popular_stocks.shape[0]).values)

            await self.stock_data.popularity.insert_popularity(popular_stocks=popular_stocks)

            content = self.build_popularity_screener(popular_stocks=popular_stocks)
            await self._update_screener_watchlist(content)
            await self.stock_data.update_alert_tickers(tickers=content.get_tickers()[:250], source='popularity')

            logger.info("Posting popularity screener")
            view = PopularityScreenerButtons()
            for _, channel in await self.bot.iter_channels(SCREENERS):
                await send_screener(content, channel, self.dstate, view=view)
        else:
            logger.error("No popular stocks found when attempting to update screener")

    @tasks.loop(minutes=5)
    async def post_volume_screener(self):
        """Retrieve latest unusual volume data and post screener"""
        await self._run_task("post_volume_screener", self._post_volume_screener_impl())

    async def _post_volume_screener_impl(self):
        market_period = self.mutils.get_market_period()
        if self.mutils.market_open_today() and market_period != 'EOD':
            unusual_volume = await asyncio.to_thread(self.stock_data.trading_view.get_unusual_volume_movers)
            content = self.build_volume_screener(unusual_volume=unusual_volume)
            await self._update_screener_watchlist(content)
            await self.stock_data.update_alert_tickers(tickers=content.get_tickers(), source='unusual-volume')

            logger.info("Posting unusual volume screener")
            view = VolumeScreenerButtons()
            for _, channel in await self.bot.iter_channels(SCREENERS):
                await send_screener(content, channel, self.dstate, view=view)

    @tasks.loop(minutes=5)
    async def post_volume_at_time_screener(self):
        """Retrieve latest volume spike data — used only to update alert_tickers"""
        await self._run_task("post_volume_at_time_screener", self._post_volume_at_time_screener_impl())

    async def _post_volume_at_time_screener_impl(self):
        market_period = self.mutils.get_market_period()
        if self.mutils.market_open_today() and market_period != 'EOD':
            volume_spike = await asyncio.to_thread(self.stock_data.trading_view.get_unusual_volume_at_time_movers)
            await self.stock_data.update_alert_tickers(tickers=volume_spike['name'].to_list(), source='volume-spike')

    @tasks.loop(minutes=5)
    async def post_gainer_screener(self):
        """Retrieve latest gainers data based on market period and post screener"""
        await self._run_task("post_gainer_screener", self._post_gainer_screener_impl())

    async def _post_gainer_screener_impl(self):
        market_period = self.mutils.get_market_period()
        if self.mutils.market_open_today() and market_period != 'EOD':
            if market_period == 'premarket':
                gainers = await asyncio.to_thread(self.stock_data.trading_view.get_premarket_gainers)
            elif market_period == 'intraday':
                gainers = await asyncio.to_thread(self.stock_data.trading_view.get_intraday_gainers)
            elif market_period == 'aftermarket':
                gainers = await asyncio.to_thread(self.stock_data.trading_view.get_postmarket_gainers)
            else:
                gainers = pd.DataFrame()
            content = self.build_gainer_screener(market_period=market_period, gainers=gainers)
            await self._update_screener_watchlist(content)
            await self.stock_data.update_alert_tickers(tickers=content.get_tickers(), source='gainers')

            logger.info(f"Sending {content.market_period} gainers screener")
            view = GainerScreenerButtons(market_period=market_period)
            for _, channel in await self.bot.iter_channels(SCREENERS):
                await send_screener(content, channel, self.dstate, view=view)

    @post_gainer_screener.before_loop
    @post_volume_screener.before_loop
    @post_volume_at_time_screener.before_loop
    async def sleep_until_5m(self):
        sleep_time = seconds_until_minute_interval(minute=5)
        logger.info(f"5m reports will begin posting in {sleep_time} seconds")
        await asyncio.sleep(sleep_time)

    @post_popularity_screener.before_loop
    async def sleep_until_30m(self):
        sleep_time = seconds_until_minute_interval(minute=30)
        logger.info(f"30m reports will begin posting in {sleep_time} seconds")
        await asyncio.sleep(sleep_time)

    @tasks.loop(time=datetime.time(hour=12, minute=30, second=0))
    async def post_earnings_spotlight(self):
        """Find random ticker reporting earnings today and post spotlight report"""
        await self._run_task("post_earnings_spotlight", self._post_earnings_spotlight_impl())

    async def _post_earnings_spotlight_impl(self):
        if not self.mutils.market_open_today():
            return

        earnings_today = await self.stock_data.earnings.get_earnings_on_date(date=datetime.date.today())
        if earnings_today.empty:
            logger.warning("No earnings today — skipping spotlight")
            return

        remaining = list(earnings_today['ticker'])
        random.shuffle(remaining)
        valid_ticker = None
        for t in remaining:
            if await self.stock_data.tickers.validate_ticker(t):
                valid_ticker = t
                break
        if valid_ticker is None:
            logger.warning("No valid earnings tickers today — skipping spotlight")
            return

        content = await self.build_earnings_spotlight_report(ticker=valid_ticker)
        logger.info(f"Posting today's earnings spotlight: '{content.ticker}'")
        view = StockReportButtons(ticker=content.ticker)
        for _, channel in await self.bot.iter_channels(REPORTS):
            await send_report(content, channel, view=view)

    @tasks.loop(time=datetime.time(hour=12, minute=0, second=0))
    async def post_weekly_earnings(self):
        """Retrieve upcoming earnings data and post screener for earnings reporting this week"""
        await self._run_task("post_weekly_earnings", self._post_weekly_earnings_impl())

    async def _post_weekly_earnings_impl(self):
        today = datetime.datetime.now(tz=timezone()).date()
        if today.weekday() == 0:
            content = await self.build_weekly_earnings_screener()
            logger.info("Posting weekly earnings screener...")
            try:
                files = [discord.File(content.filepath)]
            except OSError:
                logger.error(f"Weekly earnings file not found: {content.filepath}", exc_info=True)
                files = []
            for _, channel in await self.bot.iter_channels(SCREENERS):
                await send_screener(content, channel, self.dstate, files=files)

    @tasks.loop(minutes=10)
    async def post_earnings_results(self):
        """Poll for newly available earnings results and post reports to the reports channel."""
        await self._run_task("post_earnings_results", self._post_earnings_results_impl())

    @post_earnings_results.before_loop
    async def sleep_until_10m(self):
        sleep_time = seconds_until_minute_interval(minute=10)
        logger.info(f"Earnings results polling will begin in {sleep_time} seconds")
        await asyncio.sleep(sleep_time)

    async def _post_earnings_results_impl(self):
        if not self.mutils.market_open_today():
            return

        # Cover BMO (7 AM ET = 12:00 UTC) through AMC (8 PM ET = 01:00 UTC next day)
        now_utc = datetime.datetime.utcnow()
        hour_utc = now_utc.hour
        if not (12 <= hour_utc <= 23 or hour_utc == 0):
            return

        today = datetime.date.today()
        earnings_today = await self.stock_data.earnings.get_earnings_on_date(date=today)
        if earnings_today.empty:
            return

        watchlist_tickers = set(await self.stock_data.watchlists.get_all_watchlist_tickers(
            watchlist_types=['named']
        ))
        watchlist_earnings = [
            t for t in earnings_today['ticker'].tolist() if t in watchlist_tickers
        ]
        if not watchlist_earnings:
            return

        already_posted = await self.stock_data.earnings_results.get_posted_tickers_today(today)
        pending = [t for t in watchlist_earnings if t not in already_posted]
        if not pending:
            return

        logger.info(f"Checking earnings results for {len(pending)} watchlist tickers: {pending}")
        for ticker in pending:
            try:
                result = await asyncio.to_thread(self.stock_data.yfinance.get_earnings_result, ticker)
                await asyncio.sleep(1)
                if result is None:
                    logger.debug(f"[post_earnings_results] No result yet for '{ticker}'")
                    continue

                content = await self.build_earnings_result_report(
                    ticker=ticker,
                    eps_actual=result['eps_actual'],
                    eps_estimate=result['eps_estimate'],
                    surprise_pct=result['surprise_pct'],
                )
                view = StockReportButtons(ticker=ticker)
                for _, channel in await self.bot.iter_channels(REPORTS):
                    await send_report(content, channel, view=view)

                await self.stock_data.earnings_results.insert_result(
                    date=today,
                    ticker=ticker,
                    eps_actual=result['eps_actual'],
                    eps_estimate=result['eps_estimate'],
                    surprise_pct=result['surprise_pct'],
                )
                logger.info(f"[post_earnings_results] Posted earnings result for '{ticker}'")
            except Exception:
                logger.error(f"[post_earnings_results] Failed for '{ticker}'", exc_info=True)

    @tasks.loop(time=datetime.time(hour=6, minute=0, second=0))
    async def update_earnings_calendar(self):
        """Update guild Discord calendar with upcoming earnings report dates for tickers on watchlists"""
        await self._run_task("update_earnings_calendar", self._update_earnings_calendar_impl())

    async def _update_earnings_calendar_impl(self):
        logger.info("Creating calendar events for upcoming earnings dates")
        tickers = await self.stock_data.watchlists.get_all_watchlist_tickers(watchlist_types=['named', 'personal'])
        logger.debug(f"Identified {len(tickers)} watchlist tickers to create earnings events for")

        for gld in self.bot.guilds:
            curr_events = await gld.fetch_scheduled_events()
            logger.debug(f"Guild '{gld.name}': {len(curr_events)} events already in the calendar")

            for ticker in tickers:
                try:
                    earnings_info = await self.stock_data.earnings.get_next_earnings_info(ticker)
                    if earnings_info:
                        event_exists = False
                        name = f"{ticker} Earnings"

                        if curr_events:
                            for event in curr_events:
                                if event.name == name:
                                    event_exists = True
                                    break

                        if not event_exists:
                            release_time = "unspecified"
                            start_time = datetime.datetime.combine(earnings_info['date'], datetime.datetime.strptime('1230', '%H%M').time()).astimezone()
                            time_list = earnings_info.get('time') or []
                            time_str = time_list[0] if time_list else None
                            if time_str and "pre-market" in time_str:
                                start_time = start_time.replace(hour=8, minute=30)
                                release_time = "pre-market"
                            elif time_str and "after-hours" in time_str:
                                release_time = "after hours"
                                start_time = start_time.replace(hour=15, minute=0)

                            now = datetime.datetime.now().astimezone()
                            if start_time > now:
                                description = f"**Quarter:** {earnings_info['fiscal_quarter_ending']}\n"
                                description += f"**Release Time:** {release_time}\n"
                                description += f"**EPS Forecast:** {earnings_info['eps_forecast']}\n"
                                description += f"**Last Year's EPS:** {earnings_info['last_year_eps']}\n"
                                description += f"**Last Year's Report Date:** {earnings_info['last_year_rpt_dt']}\n"

                                await gld.create_scheduled_event(
                                    name=name,
                                    description=description,
                                    start_time=start_time,
                                    end_time=start_time + datetime.timedelta(minutes=30),
                                    entity_type=discord.EntityType.external,
                                    privacy_level=discord.PrivacyLevel.guild_only,
                                    location="Wall Street",
                                )
                                logger.info(f"Earnings report '{name}' created at {start_time} for guild '{gld.name}'")
                            else:
                                logger.info(f"Start time {start_time} for event '{name}' is in the past - skipping...")
                        else:
                            logger.info(f"Event '{name}' already exists in the calendar. Skipping...")
                except Exception:
                    logger.error(f"Failed to create calendar event for '{ticker}' in guild '{gld.name}'", exc_info=True)
        logger.info("Completed updating earnings calendar")

    #####################
    # Slash commands    #
    #####################

    report_group = app_commands.Group(name="report", description="Generate in-depth stock reports")
    alert_group = app_commands.Group(name="alert", description="View alert history and manage subscriptions")

    @report_group.command(name="watchlist", description="Generate stock reports for every ticker on a watchlist")
    @app_commands.describe(watchlist="Which watchlist to fetch reports for")
    @app_commands.autocomplete(watchlist=get_watchlist_options)
    @app_commands.describe(visibility="'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility=[
        app_commands.Choice(name="private", value='private'),
        app_commands.Choice(name="public", value='public'),
    ])
    async def report_watchlist(self, interaction: discord.Interaction, watchlist: str, visibility: app_commands.Choice[str]):
        """Generate and send Stock Reports for all tickers on the input watchlist"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/report watchlist function called by user '{interaction.user.name}'")

        watchlist_id = self.stock_data.watchlists.resolve_personal_id(interaction.user.id) if watchlist == 'personal' else watchlist

        if not await self.stock_data.watchlists.validate_watchlist(watchlist_id):
            await interaction.followup.send(f"Watchlist '{watchlist}' does not exist", ephemeral=True)
            return

        tickers = await self.stock_data.watchlists.get_watchlist_tickers(watchlist_id)
        logger.info(f"Reports requested for watchlist '{watchlist}' with tickers {tickers}")

        if not tickers:
            await interaction.followup.send("No tickers on the watchlist. Use /watchlist add to build a watchlist.", ephemeral=True)
        else:
            channel = await self.bot.get_channel_for_guild(interaction.guild_id, REPORTS)
            if channel is None:
                await interaction.followup.send("Use `/server setup` to configure the reports channel.", ephemeral=True)
                return
            message = None
            try:
                for ticker in tickers:
                    content = await self.build_stock_report(ticker=ticker, guild_id=interaction.guild_id)
                    view = StockReportButtons(ticker=content.ticker)
                    message = await send_report(content, channel, interaction=interaction,
                                                visibility=visibility.value, view=view)
            except SchwabRateLimitError:
                await interaction.followup.send(
                    "Schwab API rate limit exceeded — please wait a moment and try again.", ephemeral=True
                )
                return

            logger.info("Reports have been posted")
            if message is not None:
                follow_up = f"Posted reports for tickers [{', '.join(tickers)}]({message.jump_url})!"
            else:
                follow_up = f"Posted reports for tickers {', '.join(tickers)}."
            await interaction.followup.send(follow_up, ephemeral=True)

    async def ticker_autocomplete(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        """Autocomplete last token in space-separated tickers string from DB."""
        tokens = current.upper().split()
        if not current.endswith(" ") and tokens:
            prefix_tokens = tokens[:-1]
            partial = tokens[-1]
        else:
            prefix_tokens = tokens
            partial = ""
        all_tickers = await self.stock_data.tickers.get_all_tickers()
        prefix_str = (" ".join(prefix_tokens) + " ") if prefix_tokens else ""
        return [
            app_commands.Choice(name=f"{prefix_str}{ticker}", value=f"{prefix_str}{ticker}")
            for ticker in all_tickers if ticker.startswith(partial)
        ][:25]

    @report_group.command(name="ticker", description="Generate a detailed stock report for one or more tickers")
    @app_commands.describe(tickers="Tickers to post reports for (separated by spaces)")
    @app_commands.describe(visibility="'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility=[
        app_commands.Choice(name="private", value='private'),
        app_commands.Choice(name="public", value='public'),
    ])
    @app_commands.autocomplete(tickers=ticker_autocomplete)
    async def report_ticker(self, interaction: discord.interactions, tickers: str, visibility: app_commands.Choice[str]):
        """Generate and send Stock Reports for all valid tickers input by the user"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/report ticker function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers.upper())
        logger.info(f"Reports requested for tickers {tickers}. Invalid tickers: {invalid_tickers}")
        channel = await self.bot.get_channel_for_guild(interaction.guild_id, REPORTS)
        if channel is None and visibility.value == 'public':
            await interaction.followup.send("Use `/server setup` to configure the reports channel.", ephemeral=True)
            return
        message = None
        try:
            for ticker in tickers:
                content = await self.build_stock_report(ticker=ticker, guild_id=interaction.guild_id)
                view = StockReportButtons(ticker=content.ticker)
                message = await send_report(content, channel, interaction=interaction,
                                            visibility=visibility.value, view=view)
        except SchwabRateLimitError:
            await interaction.followup.send(
                "Schwab API rate limit exceeded — please wait a moment and try again.", ephemeral=True
            )
            return

        follow_up = ""
        if message is not None:
            logger.info(f"Reports posted for tickers {tickers}")
            follow_up = f"Posted reports for tickers [{', '.join(tickers)}]({message.jump_url})!"
            if invalid_tickers:
                follow_up += f" Invalid tickers: {', '.join(invalid_tickers)}"
        if not tickers:
            follow_up = f" No valid tickers input: {', '.join(invalid_tickers)}"
        await interaction.followup.send(follow_up, ephemeral=True)

    async def autocomplete_searchin(self, interaction: discord.Interaction, current: str):
        return [
            app_commands.Choice(name=name, value=value)
            for name, value in News().search_in.items() if current.lower() in name.lower()
        ]

    async def autocomplete_sortby(self, interaction: discord.Interaction, current: str):
        return [
            app_commands.Choice(name=name, value=value)
            for name, value in News().sort_by.items() if current.lower() in name.lower()
        ]

    @app_commands.command(name="news", description="Search for recent news articles on any topic")
    @app_commands.describe(query="The search terms or terms to query for")
    @app_commands.describe(sort_by="Field by which to sort returned articles")
    @app_commands.autocomplete(sort_by=autocomplete_sortby,)
    async def news(self, interaction: discord.Interaction, query: str, sort_by: str = 'publishedAt'):
        await interaction.response.defer()
        logger.info(f"/news function called by user {interaction.user.name}")
        try:
            news_data = await asyncio.to_thread(News().get_news, query=query, sort_by=sort_by)
        except Exception:
            logger.exception(f"Failed to fetch news for query '{query}'")
            await interaction.followup.send("Failed to fetch news — please try again.", ephemeral=True)
            return
        content = NewsReport(data=NewsReportData(query=query, news=news_data))
        embed = spec_to_embed(content.build())
        await interaction.followup.send(embed=embed)
        logger.info(f"Posted news for query '{query}'")

    async def autocomplete_filter(self, interaction: discord.Interaction, current: str):
        return [
            app_commands.Choice(name=name, value=name)
            for name in self.stock_data.popularity.filters_map.keys() if current.lower() in name.lower()
        ]

    @report_group.command(name="popularity", description="See the most-talked-about stocks from a chosen source")
    @app_commands.describe(source="The source to pull popular stocks from")
    @app_commands.autocomplete(source=autocomplete_filter,)
    @app_commands.describe(visibility="'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility=[
        app_commands.Choice(name="private", value='private'),
        app_commands.Choice(name="public", value='public'),
    ])
    async def report_popularity(self, interaction: discord.Interaction, source: str, visibility: app_commands.Choice[str]):
        """Generate and send Popularity Report for tickers from the input source"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/report popularity function called by user {interaction.user.name}")

        popular_stocks = await asyncio.to_thread(self.stock_data.popularity.get_popular_stocks, filter_name=source)
        filter_val = self.stock_data.popularity.get_filter(source)

        if not popular_stocks.empty:
            channel = await self.bot.get_channel_for_guild(interaction.guild_id, REPORTS)
            if channel is None and visibility.value == 'public':
                await interaction.followup.send("Use `/server setup` to configure the reports channel.", ephemeral=True)
                return
            content = PopularityReport(data=PopularityReportData(popular_stocks=popular_stocks, filter=filter_val))
            view = PopularityReportButtons()
            try:
                files = [discord.File(content.filepath)]
            except OSError:
                logger.error(f"Popularity report file not found: {content.filepath}", exc_info=True)
                files = []
            message = await send_report(content, channel, interaction=interaction,
                                        visibility=visibility.value, view=view, files=files)
            follow_up = f"[Posted popularity reports!]({message.jump_url})"
            await interaction.followup.send(follow_up, ephemeral=True)
            logger.info(f"Popularity posted from source {source}")
        else:
            logger.info(f"No popular stocks found with filter '{source}'")
            await interaction.followup.send(f"No popular stocks found with filter '{source}'", ephemeral=True)

    async def politician_options(self, interaction: discord.Interaction, current: str):
        politicians = await self.stock_data.capitol_trades.all_politicians()
        names = [politician['name'] for politician in politicians]
        return [
            app_commands.Choice(name=p_name, value=p_name)
            for p_name in names if current.lower() in p_name.lower()
        ][:25]

    @report_group.command(name="politician", description="See recent stock trades made by a U.S. politician")
    @app_commands.describe(politician_name="Politician to return trades for")
    @app_commands.autocomplete(politician_name=politician_options,)
    @app_commands.describe(visibility="'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility=[
        app_commands.Choice(name="private", value='private'),
        app_commands.Choice(name="public", value='public'),
    ])
    async def report_politician(self, interaction: discord.Interaction, politician_name: str, visibility: app_commands.Choice[str]):
        """Generate and send Politician Report for input politician"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/report politician function called by user {interaction.user.name}")

        politician = await self.stock_data.capitol_trades.politician(name=politician_name)

        if politician:
            channel = await self.bot.get_channel_for_guild(interaction.guild_id, REPORTS)
            if channel is None and visibility.value == 'public':
                await interaction.followup.send("Use `/server setup` to configure the reports channel.", ephemeral=True)
                return
            content = await self.build_politician_report(politician=politician)
            view = PoliticianReportButtons(pid=politician['politician_id'])
            try:
                files = [discord.File(content.filepath)]
            except OSError:
                logger.error(f"Politician report file not found: {content.filepath}", exc_info=True)
                files = []
            message = await send_report(content, channel, interaction=interaction,
                                        visibility=visibility.value, view=view, files=files)
            follow_up = f"Posted report on [{politician['name']}]({message.jump_url})"
            await interaction.followup.send(follow_up, ephemeral=True)
            logger.info(f"Posted report on {politician['name']}")
        else:
            logger.info(f"No politician found with name {politician_name}")
            await interaction.followup.send(f"No politician found with name {politician_name}", ephemeral=True)

    ###########
    # Builders #
    ###########

    async def _update_screener_watchlist(self, screener) -> None:
        """Update the system-generated watchlist for the given screener (moved from Screener.__init__)."""
        watchlist_id = screener.screener_type
        watchlist_tickers = screener.get_tickers()[:20]

        if not await self.stock_data.watchlists.validate_watchlist(watchlist_id):
            await self.stock_data.watchlists.create_watchlist(
                watchlist_id=watchlist_id, tickers=watchlist_tickers, systemGenerated=True
            )
            logger.info(f"Created new watchlist from '{screener.screener_type}' screener")
            logger.debug(f"Watchlist created with {len(watchlist_tickers)} tickers: {watchlist_tickers}")
        else:
            await self.stock_data.watchlists.update_watchlist(
                watchlist_id=watchlist_id, tickers=watchlist_tickers
            )
            logger.info(f"Updated watchlist '{screener.screener_type}'")
            logger.debug(f"Watchlist updated with {len(watchlist_tickers)} tickers: {watchlist_tickers}")

    def build_popularity_screener(self, **kwargs) -> PopularityScreener:
        popular_stocks = kwargs.pop('popular_stocks', None)
        if popular_stocks is None:
            popular_stocks = self.stock_data.popularity.get_popular_stocks()
        return PopularityScreener(data=PopularityScreenerData(popular_stocks=popular_stocks))

    def build_volume_screener(self, **kwargs) -> VolumeScreener:
        unusual_volume = kwargs.pop('unusual_volume', None)
        if unusual_volume is None:
            unusual_volume = self.stock_data.trading_view.get_unusual_volume_movers()
        return VolumeScreener(data=VolumeScreenerData(unusual_volume=unusual_volume))

    def build_gainer_screener(self, market_period: str, **kwargs) -> GainerScreener:
        if 'gainers' in kwargs:
            gainers = kwargs['gainers']
        elif market_period == 'premarket':
            gainers = self.stock_data.trading_view.get_premarket_gainers()
        elif market_period == 'intraday':
            gainers = self.stock_data.trading_view.get_intraday_gainers()
        elif market_period == 'aftermarket':
            gainers = self.stock_data.trading_view.get_postmarket_gainers()
        else:
            gainers = pd.DataFrame()
        return GainerScreener(data=GainerScreenerData(market_period=market_period, gainers=gainers))

    async def build_stock_report(self, ticker: str, guild_id: int | None = None, **kwargs) -> StockReport:
        ticker_info = kwargs.pop('ticker_info', await self.stock_data.tickers.get_ticker_info(ticker=ticker))
        daily_price_history = kwargs.pop('daily_price_history', await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker))
        popularity = kwargs.pop('popularity', await self.stock_data.popularity.fetch_popularity(ticker=ticker))
        recent_sec_filings = kwargs.pop('recent_sec_filings', await self.stock_data.sec.get_recent_filings(ticker=ticker))
        historical_earnings = kwargs.pop('historical_earnings', await self.stock_data.earnings.get_historical_earnings(ticker=ticker))
        next_earnings_info = kwargs.pop('next_earnings_info', await self.stock_data.earnings.get_next_earnings_info(ticker=ticker))
        quote = kwargs.pop('quote', None)
        if quote is None:
            try:
                quote = await self.stock_data.schwab.get_quote(ticker=ticker)
            except SchwabTokenError:
                logger.warning(f"[build_stock_report] Schwab unavailable — quote missing for '{ticker}'")
                quote = {}
        fundamentals = kwargs.pop('fundamentals', None)
        if fundamentals is None:
            try:
                fundamentals = await self.stock_data.schwab.get_fundamentals(tickers=[ticker])
            except SchwabTokenError:
                logger.warning(f"[build_stock_report] Schwab unavailable — fundamentals missing for '{ticker}'")
                fundamentals = {}

        raw_alerts = await self.dstate.get_recent_alerts_for_ticker(ticker)
        recent_alerts = []
        if raw_alerts and guild_id is not None:
            alerts_channel_id = await self.stock_data.channel_config.get_channel_id(guild_id, ALERTS)
            for date, alert_type, messageid in raw_alerts:
                url = None
                if alerts_channel_id and messageid:
                    url = f"https://discord.com/channels/{guild_id}/{alerts_channel_id}/{messageid}"
                recent_alerts.append({'date': date, 'alert_type': alert_type, 'url': url})

        return StockReport(data=StockReportData(
            ticker=ticker,
            ticker_info=ticker_info,
            daily_price_history=daily_price_history,
            popularity=popularity,
            recent_sec_filings=recent_sec_filings,
            historical_earnings=historical_earnings,
            next_earnings_info=next_earnings_info,
            quote=quote,
            fundamentals=fundamentals,
            recent_alerts=recent_alerts,
        ))

    async def build_earnings_spotlight_report(self, ticker: str, **kwargs) -> EarningsSpotlightReport:
        ticker_info = await self.stock_data.tickers.get_ticker_info(ticker=ticker)
        daily_price_history = await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
        next_earnings_info = await self.stock_data.earnings.get_next_earnings_info(ticker=ticker)
        historical_earnings = await self.stock_data.earnings.get_historical_earnings(ticker=ticker)
        try:
            quote = await self.stock_data.schwab.get_quote(ticker=ticker)
        except SchwabTokenError:
            logger.warning(f"[build_earnings_spotlight_report] Schwab unavailable — quote missing for '{ticker}'")
            quote = {}
        try:
            fundamentals = await self.stock_data.schwab.get_fundamentals(tickers=[ticker])
        except SchwabTokenError:
            logger.warning(f"[build_earnings_spotlight_report] Schwab unavailable — fundamentals missing for '{ticker}'")
            fundamentals = {}
        return EarningsSpotlightReport(data=EarningsSpotlightData(
            ticker=ticker,
            ticker_info=ticker_info,
            daily_price_history=daily_price_history,
            next_earnings_info=next_earnings_info,
            historical_earnings=historical_earnings,
            quote=quote,
            fundamentals=fundamentals,
        ))

    async def build_earnings_result_report(
        self,
        ticker: str,
        eps_actual: float,
        eps_estimate: float | None,
        surprise_pct: float | None,
        **kwargs,
    ) -> EarningsResultReport:
        ticker_info = kwargs.pop('ticker_info', await self.stock_data.tickers.get_ticker_info(ticker=ticker))
        quote = kwargs.pop('quote', None)
        if quote is None:
            try:
                quote = await self.stock_data.schwab.get_quote(ticker=ticker)
            except SchwabTokenError:
                logger.warning(f"[build_earnings_result_report] Schwab unavailable — quote missing for '{ticker}'")
                quote = {}
        daily_price_history = kwargs.pop('daily_price_history', await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker))
        historical_earnings = kwargs.pop('historical_earnings', await self.stock_data.earnings.get_historical_earnings(ticker=ticker))
        next_earnings_info = kwargs.pop('next_earnings_info', await self.stock_data.earnings.get_next_earnings_info(ticker=ticker))
        return EarningsResultReport(data=EarningsResultData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            eps_actual=eps_actual,
            eps_estimate=eps_estimate,
            surprise_pct=surprise_pct,
            historical_earnings=historical_earnings,
            next_earnings_info=next_earnings_info,
            daily_price_history=daily_price_history,
        ))

    async def build_weekly_earnings_screener(self, **kwargs) -> WeeklyEarningsScreener:
        upcoming_earnings = kwargs.pop('upcoming_earnings', await self.stock_data.earnings.fetch_upcoming_earnings())
        watchlist_tickers = kwargs.pop('watchlist_tickers',
                                       await self.stock_data.watchlists.get_all_watchlist_tickers(watchlist_types=['named']))
        return WeeklyEarningsScreener(data=WeeklyEarningsData(
            upcoming_earnings=upcoming_earnings,
            watchlist_tickers=watchlist_tickers,
        ))

    async def build_politician_report(self, politician_name: str = None, **kwargs) -> PoliticianReport:
        politician = kwargs.pop('politician', None)
        trades = kwargs.pop('trades', await asyncio.to_thread(self.stock_data.capitol_trades.trades, pid=politician['politician_id']))
        politician_facts = kwargs.pop('politician_facts', await asyncio.to_thread(self.stock_data.capitol_trades.politician_facts, pid=politician['politician_id']))
        return PoliticianReport(data=PoliticianReportData(
            politician=politician,
            trades=trades,
            politician_facts=politician_facts,
        ))

    async def build_alert_summary(self, since_dt: datetime.datetime, label: str) -> AlertSummary:
        alerts = await self.dstate.get_alerts_since(since_dt)
        return AlertSummary(data=AlertSummaryData(since_dt=since_dt, label=label, alerts=alerts))

    @alert_group.command(name="summary", description="View a summary of recent alerts grouped by type")
    @app_commands.describe(
        since_when="Time period to summarize (defaults to since last close)",
        visibility="public posts to the alerts channel; private sends only to you",
    )
    @app_commands.choices(
        since_when=[
            app_commands.Choice(name="Since last close (default)", value="last_close"),
            app_commands.Choice(name="Since market open today",    value="market_open_today"),
            app_commands.Choice(name="Last 3 days",                value="last_3_days"),
            app_commands.Choice(name="Last 7 days",                value="last_7_days"),
        ],
        visibility=[
            app_commands.Choice(name="public",  value="public"),
            app_commands.Choice(name="private", value="private"),
        ],
    )
    async def alert_summary(
        self,
        interaction: discord.Interaction,
        since_when: app_commands.Choice[str] = None,
        visibility: app_commands.Choice[str] = None,
    ):
        """Summarize recent alerts grouped by type."""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/alert summary called by user '{interaction.user.name}'")
        since_dt, label = _resolve_since_dt(since_when.value if since_when else 'last_close')
        content = await self.build_alert_summary(since_dt, label)
        channel = await self.bot.get_channel_for_guild(interaction.guild_id, ALERTS)
        vis = visibility.value if visibility else "private"
        message = await send_report(content, channel, interaction=interaction, visibility=vis)
        if message is not None:
            await interaction.followup.send(f"[Alert summary posted]({message.jump_url})", ephemeral=True)
        else:
            await interaction.followup.send("Alert summary posted.", ephemeral=True)

    async def _send_subscription_select(self, interaction: discord.Interaction) -> None:
        """Send an ephemeral subscription dropdown to the interacting user."""
        guild_roles = await self.bot.stock_data.alert_roles.get_all_for_guild(interaction.guild_id)
        member_role_ids = {r.id for r in interaction.user.roles}
        select = AlertSubscriptionSelect(guild_roles, member_role_ids)
        view = AlertSubscriptionView(select)
        await interaction.response.send_message(
            "Select the alerts you want to be notified about:",
            view=view,
            ephemeral=True,
        )

    @alert_group.command(name="subscribe", description="Choose which alert types you want to be pinged for")
    async def alert_subscribe(self, interaction: discord.Interaction):
        """Open the subscription selector for the interacting user."""
        await self._send_subscription_select(interaction)


async def setup(bot):
    await bot.add_cog(Reports(bot=bot, stock_data=bot.stock_data))

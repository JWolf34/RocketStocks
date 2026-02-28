import datetime
import logging
import random
import asyncio
import time
import traceback as tb
import pandas as pd
import discord
from discord import app_commands
from discord.ext import commands, tasks

from src.rocketstocks.data.stockdata import StockData
from rocketstocks.data.channel_config import REPORTS, SCREENERS
from rocketstocks.data.discord_state import DiscordState
from rocketstocks.data.clients.news import News
from rocketstocks.core.utils.market import market_utils
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.notifications.event import NotificationEvent

from rocketstocks.core.content.models import (
    StockReportData,
    NewsReportData,
    PopularityReportData,
    PopularityScreenerData,
    GainerScreenerData,
    VolumeScreenerData,
    EarningsSpotlightData,
    WeeklyEarningsData,
    PoliticianReportData,
)
from rocketstocks.core.content.reports.stock_report import StockReport
from rocketstocks.core.content.reports.news_report import NewsReport
from rocketstocks.core.content.reports.popularity_report import PopularityReport
from rocketstocks.core.content.reports.earnings_report import EarningsSpotlightReport
from rocketstocks.core.content.reports.politician_report import PoliticianReport
from rocketstocks.core.content.screeners.popularity_screener import PopularityScreener
from rocketstocks.core.content.screeners.gainer_screener import GainerScreener
from rocketstocks.core.content.screeners.volume_screener import VolumeScreener
from rocketstocks.core.content.screeners.earnings_screener import WeeklyEarningsScreener

from rocketstocks.bot.views.report_views import (
    StockReportButtons, GainerScreenerButtons, VolumeScreenerButtons,
    PopularityScreenerButtons, PopularityReportButtons, PoliticianReportButtons,
)
from rocketstocks.bot.senders.report_sender import send_report, send_screener
from rocketstocks.bot.senders.embed_utils import spec_to_embed

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dummy data helpers for /test-report
# ---------------------------------------------------------------------------

def _dummy_quote(ticker: str = "AAPL", pct: float = 2.5, price: float = 188.9) -> dict:
    return {
        'symbol': ticker,
        'quote': {
            'openPrice': price - 2,
            'highPrice': price + 2,
            'lowPrice': price - 3,
            'totalVolume': 52_000_000,
            'netPercentChange': pct,
        },
        'regular': {'regularMarketLastPrice': price},
        'reference': {
            'exchangeName': 'NASDAQ',
            'isShortable': True,
            'isHardToBorrow': False,
        },
        'assetSubType': 'CS',
    }


def _dummy_ticker_info() -> dict:
    return {
        'name': 'Apple Inc',
        'sector': 'Technology',
        'industry': 'Consumer Electronics',
        'country': 'US',
    }


def _dummy_fundamentals() -> dict:
    return {
        'instruments': [{'fundamental': {
            'marketCap': 2_900_000_000_000,
            'eps': 6.42,
            'epsTTM': 6.42,
            'peRatio': 29.42,
            'beta': 1.24,
            'dividendAmount': 0.96,
        }}]
    }


def _dummy_price_history(rows: int = 250) -> pd.DataFrame:
    import datetime as _dt
    dates = [_dt.date.today() - _dt.timedelta(days=i) for i in range(rows)]
    dates.reverse()
    return pd.DataFrame({
        'date': dates,
        'open': [180.0] * rows,
        'high': [195.0] * rows,
        'low': [175.0] * rows,
        'close': [188.9] * rows,
        'volume': [50_000_000] * rows,
    })


def _dummy_earnings_df() -> pd.DataFrame:
    import datetime as _dt
    return pd.DataFrame({
        'date': [_dt.date(2025, 10, 31), _dt.date(2025, 7, 31)],
        'eps': [1.58, 1.40],
        'surprise': [3.1, 1.8],
        'epsforecast': [1.53, 1.38],
        'fiscalquarterending': ['Sep 2025', 'Jun 2025'],
    })


def _build_dummy_report(report_type: str):
    """Build a report object with hardcoded dummy data for preview purposes."""
    import datetime as _dt
    if report_type == "stock":
        return StockReport(data=StockReportData(
            ticker="AAPL",
            ticker_info=_dummy_ticker_info(),
            quote=_dummy_quote(),
            fundamentals=_dummy_fundamentals(),
            daily_price_history=_dummy_price_history(),
            popularity=pd.DataFrame(),
            historical_earnings=_dummy_earnings_df(),
            next_earnings_info={},
            recent_sec_filings=pd.DataFrame(),
        ))
    elif report_type == "earnings":
        return EarningsSpotlightReport(data=EarningsSpotlightData(
            ticker="NVDA",
            ticker_info=_dummy_ticker_info(),
            quote=_dummy_quote("NVDA", pct=1.5),
            fundamentals=_dummy_fundamentals(),
            daily_price_history=_dummy_price_history(),
            historical_earnings=_dummy_earnings_df(),
            next_earnings_info={
                'date': _dt.date(2026, 3, 15),
                'time': 'after-hours',
                'fiscal_quarter_ending': 'Jan 2026',
                'eps_forecast': '0.89',
                'no_of_ests': '42',
                'last_year_rpt_dt': '2025-02-26',
                'last_year_eps': '0.76',
            },
        ))
    elif report_type == "news":
        news = {'articles': [
            {
                'title': f'Sample Article {i + 1}',
                'url': f'https://example.com/article-{i + 1}',
                'source': {'name': 'Reuters'},
                'publishedAt': '2026-02-27T10:00:00Z',
            }
            for i in range(5)
        ]}
        return NewsReport(data=NewsReportData(query="AAPL", news=news))
    elif report_type == "popularity":
        df = pd.DataFrame({
            'rank': range(1, 21),
            'ticker': [f'TK{i}' for i in range(1, 21)],
            'mentions': [100 - i for i in range(20)],
            'rank_24h_ago': range(2, 22),
            'mentions_24h_ago': [90 - i for i in range(20)],
        })
        return PopularityReport(data=PopularityReportData(popular_stocks=df, filter="all"))
    elif report_type == "politician":
        politician = {
            'name': 'Nancy Pelosi',
            'party': 'Democrat',
            'state': 'California',
            'politician_id': 'demo-pelosi',
        }
        trades_df = pd.DataFrame({
            'Ticker': ['NVDA', 'AAPL', 'MSFT'],
            'Published Date': ['2026-01-10', '2026-01-05', '2025-12-20'],
            'Order Type': ['Purchase', 'Sale', 'Purchase'],
            'Order Size': ['$500K - $1M', '$250K - $500K', '$1M - $5M'],
            'Filed After': ['5 days', '12 days', '30 days'],
        })
        return PoliticianReport(data=PoliticianReportData(
            politician=politician,
            trades=trades_df,
            politician_facts={'Net Worth': '$120M', 'In Office Since': '1987'},
        ))
    raise ValueError(f"Unknown report type: {report_type}")


class Reports(commands.Cog):
    """Cog for managing Reports and Screeners to be posted to Discord"""

    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.mutils = market_utils()
        self.dstate = DiscordState()

        self.post_popularity_screener.start()
        self.post_volume_screener.start()
        self.post_volume_at_time_screener.start()
        self.post_gainer_screener.start()
        self.update_earnings_calendar.start()
        self.post_earnings_spotlight.start()
        self.post_weekly_earnings.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"{__name__} loaded")

    async def get_watchlist_options(self, interaction: discord.Interaction, current: str):
        return await self.bot.get_cog('Watchlists').watchlist_options(interaction=interaction, current=current)

    #########
    # Tasks #
    #########

    @tasks.loop(minutes=30)
    async def post_popularity_screener(self):
        """Retrieve latest popularity, insert into database, and post screener"""
        _start = time.monotonic()
        try:
            popular_stocks = self.stock_data.popularity.get_popular_stocks()

            if not popular_stocks.empty:
                popular_stocks.insert(loc=0,
                                      column='datetime',
                                      value=pd.Series([date_utils.round_down_nearest_minute(30)] * popular_stocks.shape[0]).values)

                self.stock_data.popularity.insert_popularity(popular_stocks=popular_stocks)

                content = self.build_popularity_screener(popular_stocks=popular_stocks)
                self._update_screener_watchlist(content)
                await self.stock_data.update_alert_tickers(tickers=content.get_tickers()[:250], source='popularity')

                logger.info("Posting popularity screener")
                view = PopularityScreenerButtons()
                for _, channel in self.bot.iter_channels(SCREENERS):
                    await send_screener(content, channel, self.dstate, view=view)
            else:
                logger.error("No popular stocks found when attempting to update screener")

            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS,
                source=__name__,
                job_name="post_popularity_screener",
                message="Task completed successfully",
                elapsed_seconds=time.monotonic() - _start,
            ))
        except Exception as exc:
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source=__name__,
                job_name="post_popularity_screener",
                message=str(exc),
                traceback=tb.format_exc(),
                elapsed_seconds=time.monotonic() - _start,
            ))
            raise

    @tasks.loop(minutes=5)
    async def post_volume_screener(self):
        """Retrieve latest unusual volume data and post screener"""
        _start = time.monotonic()
        try:
            market_period = self.mutils.get_market_period()
            if self.mutils.market_open_today() and market_period != 'EOD':
                unusual_volume = self.stock_data.trading_view.get_unusual_volume_movers()
                content = self.build_volume_screener(unusual_volume=unusual_volume)
                self._update_screener_watchlist(content)
                await self.stock_data.update_alert_tickers(tickers=content.get_tickers(), source='unusual-volume')

                logger.info("Posting unusual volume screener")
                view = VolumeScreenerButtons()
                for _, channel in self.bot.iter_channels(SCREENERS):
                    await send_screener(content, channel, self.dstate, view=view)

            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS,
                source=__name__,
                job_name="post_volume_screener",
                message="Task completed successfully",
                elapsed_seconds=time.monotonic() - _start,
            ))
        except Exception as exc:
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source=__name__,
                job_name="post_volume_screener",
                message=str(exc),
                traceback=tb.format_exc(),
                elapsed_seconds=time.monotonic() - _start,
            ))
            raise

    @tasks.loop(minutes=5)
    async def post_volume_at_time_screener(self):
        """Retrieve latest volume spike data — used only to update alert_tickers"""
        _start = time.monotonic()
        try:
            market_period = self.mutils.get_market_period()
            if self.mutils.market_open_today() and market_period != 'EOD':
                volume_spike = self.stock_data.trading_view.get_unusual_volume_at_time_movers()
                await self.stock_data.update_alert_tickers(tickers=volume_spike['name'].to_list(), source='volume-spike')

            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS,
                source=__name__,
                job_name="post_volume_at_time_screener",
                message="Task completed successfully",
                elapsed_seconds=time.monotonic() - _start,
            ))
        except Exception as exc:
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source=__name__,
                job_name="post_volume_at_time_screener",
                message=str(exc),
                traceback=tb.format_exc(),
                elapsed_seconds=time.monotonic() - _start,
            ))
            raise

    @tasks.loop(minutes=5)
    async def post_gainer_screener(self):
        """Retrieve latest gainers data based on market period and post screener"""
        _start = time.monotonic()
        try:
            market_period = self.mutils.get_market_period()
            if self.mutils.market_open_today() and market_period != 'EOD':
                content = self.build_gainer_screener(market_period=market_period)
                self._update_screener_watchlist(content)
                await self.stock_data.update_alert_tickers(tickers=content.get_tickers(), source='gainers')

                logger.info(f"Sending {content.market_period} gainers screener")
                view = GainerScreenerButtons(market_period=market_period)
                for _, channel in self.bot.iter_channels(SCREENERS):
                    await send_screener(content, channel, self.dstate, view=view)

            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS,
                source=__name__,
                job_name="post_gainer_screener",
                message="Task completed successfully",
                elapsed_seconds=time.monotonic() - _start,
            ))
        except Exception as exc:
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source=__name__,
                job_name="post_gainer_screener",
                message=str(exc),
                traceback=tb.format_exc(),
                elapsed_seconds=time.monotonic() - _start,
            ))
            raise

    @post_gainer_screener.before_loop
    @post_volume_screener.before_loop
    @post_volume_at_time_screener.before_loop
    async def sleep_until_5m(self):
        sleep_time = date_utils.seconds_until_minute_interval(minute=5)
        logger.info(f"5m reports will begin posting in {sleep_time} seconds")
        await asyncio.sleep(sleep_time)

    @post_popularity_screener.before_loop
    async def sleep_until_30m(self):
        sleep_time = date_utils.seconds_until_minute_interval(minute=30)
        logger.info(f"30m reports will begin posting in {sleep_time} seconds")
        await asyncio.sleep(sleep_time)

    @tasks.loop(time=datetime.time(hour=12, minute=30, second=0))
    async def post_earnings_spotlight(self):
        """Find random ticker reporting earnings today and post spotlight report"""
        _start = time.monotonic()
        try:
            if self.mutils.market_open_today():
                earnings_today = self.stock_data.earnings.get_earnings_on_date(date=datetime.date.today())
                spotlight_ticker = earnings_today['ticker'].iloc[random.randint(0, earnings_today['ticker'].size - 1)]
                while not await self.stock_data.tickers.validate_ticker(ticker=spotlight_ticker):
                    spotlight_ticker = earnings_today['ticker'].iloc[random.randint(0, earnings_today['ticker'].size - 1)]

                content = await self.build_earnings_spotlight_report(ticker=spotlight_ticker)
                logger.info(f"Posting today's earnings spotlight: '{content.ticker}'")
                view = StockReportButtons(ticker=content.ticker)
                for _, channel in self.bot.iter_channels(REPORTS):
                    await send_report(content, channel, view=view)

            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS,
                source=__name__,
                job_name="post_earnings_spotlight",
                message="Task completed successfully",
                elapsed_seconds=time.monotonic() - _start,
            ))
        except Exception as exc:
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source=__name__,
                job_name="post_earnings_spotlight",
                message=str(exc),
                traceback=tb.format_exc(),
                elapsed_seconds=time.monotonic() - _start,
            ))
            raise

    @tasks.loop(time=datetime.time(hour=12, minute=0, second=0))
    async def post_weekly_earnings(self):
        """Retrieve upcoming earnings data and post screener for earnings reporting this week"""
        _start = time.monotonic()
        try:
            today = datetime.datetime.now(tz=date_utils.timezone()).date()
            if today.weekday() == 0:
                content = self.build_weekly_earnings_screener()
                logger.info("Posting weekly earnings screener...")
                files = [discord.File(content.filepath)]
                for _, channel in self.bot.iter_channels(SCREENERS):
                    await send_screener(content, channel, self.dstate, files=files)

            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS,
                source=__name__,
                job_name="post_weekly_earnings",
                message="Task completed successfully",
                elapsed_seconds=time.monotonic() - _start,
            ))
        except Exception as exc:
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source=__name__,
                job_name="post_weekly_earnings",
                message=str(exc),
                traceback=tb.format_exc(),
                elapsed_seconds=time.monotonic() - _start,
            ))
            raise

    @tasks.loop(time=datetime.time(hour=6, minute=0, second=0))
    async def update_earnings_calendar(self):
        """Update guild Discord calendar with upcoming earnings report dates for tickers on watchlists"""
        _start = time.monotonic()
        try:
            logger.info("Creating calendar events for upcoming earnings dates")
            tickers = self.stock_data.watchlists.get_all_watchlist_tickers(no_personal=False, no_systemGenerated=True)
            logger.debug(f"Identified {len(tickers)} watchlist tickers to create earnings events for")

            for gld in self.bot.guilds:
                curr_events = await gld.fetch_scheduled_events()
                logger.debug(f"Guild '{gld.name}': {len(curr_events)} events already in the calendar")

                for ticker in tickers:
                    earnings_info = self.stock_data.earnings.get_next_earnings_info(ticker)
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
                            if "pre-market" in earnings_info['time'][0]:
                                start_time = start_time.replace(hour=8, minute=30)
                                release_time = "pre-market"
                            elif "after-hours" in earnings_info['time'][0]:
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
            logger.info("Completed updating earnings calendar")

            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS,
                source=__name__,
                job_name="update_earnings_calendar",
                message="Task completed successfully",
                elapsed_seconds=time.monotonic() - _start,
            ))
        except Exception as exc:
            self.bot.emitter.emit(NotificationEvent(
                level=NotificationLevel.FAILURE,
                source=__name__,
                job_name="update_earnings_calendar",
                message=str(exc),
                traceback=tb.format_exc(),
                elapsed_seconds=time.monotonic() - _start,
            ))
            raise

    #####################
    # Slash commands    #
    #####################

    @app_commands.command(name="report-watchlist", description="Post stock reports on all tickers on a watchlist",)
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
        logger.info(f"/report-watchlist function called by user '{interaction.user.name}'")

        watchlist_id = watchlist
        if watchlist == 'personal':
            watchlist_id = str(interaction.user.id)

        if watchlist_id not in self.stock_data.watchlists.get_watchlists():
            await interaction.followup.send(f"Watchlist '{watchlist_id}' does not exist")
            return

        tickers = self.stock_data.watchlists.get_watchlist_tickers(watchlist_id)
        logger.info(f"Reports requested for watchlist '{watchlist}' with tickers {tickers}")

        if not tickers:
            await interaction.followup.send("No tickers on the watchlist. Use /addticker to build a watchlist.", ephemeral=True)
        else:
            channel = self.bot.get_channel_for_guild(interaction.guild_id, REPORTS)
            if channel is None:
                await interaction.followup.send("Use `/setup` to configure the reports channel.", ephemeral=True)
                return
            message = None
            for ticker in tickers:
                content = await self.build_stock_report(ticker=ticker)
                view = StockReportButtons(ticker=content.ticker)
                message = await send_report(content, channel, interaction=interaction,
                                            visibility=visibility.value, view=view)

            logger.info("Reports have been posted")
            follow_up = f"Posted reports for tickers [{', '.join(tickers)}]({message.jump_url})!"
            await interaction.followup.send(follow_up, ephemeral=True)

    @app_commands.command(name="report", description="Fetch stock reports of the specified tickers",)
    @app_commands.describe(tickers="Tickers to post reports for (separated by spaces)")
    @app_commands.describe(visibility="'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility=[
        app_commands.Choice(name="private", value='private'),
        app_commands.Choice(name="public", value='public'),
    ])
    async def report(self, interaction: discord.interactions, tickers: str, visibility: app_commands.Choice[str]):
        """Generate and send Stock Reports for all valid tickers input by the user"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/report function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers.upper())
        logger.info(f"Reports requested for tickers {tickers}. Invalid tickers: {invalid_tickers}")
        channel = self.bot.get_channel_for_guild(interaction.guild_id, REPORTS)
        if channel is None and visibility.value == 'public':
            await interaction.followup.send("Use `/setup` to configure the reports channel.", ephemeral=True)
            return
        message = None
        for ticker in tickers:
            content = await self.build_stock_report(ticker=ticker)
            view = StockReportButtons(ticker=content.ticker)
            message = await send_report(content, channel, interaction=interaction,
                                        visibility=visibility.value, view=view)

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

    @app_commands.command(name="news", description="Fetch news on the query provided")
    @app_commands.describe(query="The search terms or terms to query for")
    @app_commands.describe(sort_by="Field by which to sort returned articles")
    @app_commands.autocomplete(sort_by=autocomplete_sortby,)
    async def news(self, interaction: discord.Interaction, query: str, sort_by: str = 'publishedAt'):
        """Generate and send News Report for the input query"""
        logger.info(f"/news function called by user {interaction.user.name}")
        news_data = News().get_news(query=query, sort_by=sort_by)
        content = NewsReport(data=NewsReportData(query=query, news=news_data))
        message_text = content.build_report()
        await interaction.response.send_message(message_text)
        logger.info(f"Posted news for query '{query}'")

    async def autocomplete_filter(self, interaction: discord.Interaction, current: str):
        return [
            app_commands.Choice(name=name, value=name)
            for name in self.stock_data.popularity.filters_map.keys() if current.lower() in name.lower()
        ]

    @app_commands.command(name="popular-stocks", description="Fetch a report on the most popular stocks from the source provided")
    @app_commands.describe(source="The source to pull popular stocks from")
    @app_commands.autocomplete(source=autocomplete_filter,)
    @app_commands.describe(visibility="'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility=[
        app_commands.Choice(name="private", value='private'),
        app_commands.Choice(name="public", value='public'),
    ])
    async def popular_stocks(self, interaction: discord.Interaction, source: str, visibility: app_commands.Choice[str]):
        """Generate and send Popularity Report for tickers from the input source"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/popular-stocks function called by user {interaction.user.name}")

        popular_stocks = self.stock_data.popularity.get_popular_stocks(filter_name=source)
        filter_val = self.stock_data.popularity.get_filter(source)

        if not popular_stocks.empty:
            channel = self.bot.get_channel_for_guild(interaction.guild_id, REPORTS)
            if channel is None and visibility.value == 'public':
                await interaction.followup.send("Use `/setup` to configure the reports channel.", ephemeral=True)
                return
            content = PopularityReport(data=PopularityReportData(popular_stocks=popular_stocks, filter=filter_val))
            view = PopularityReportButtons()
            files = [discord.File(content.filepath)]
            message = await send_report(content, channel, interaction=interaction,
                                        visibility=visibility.value, view=view, files=files)
            follow_up = f"[Posted popularity reports!]({message.jump_url})"
            await interaction.followup.send(follow_up, ephemeral=True)
            logger.info(f"Popularity posted from source {source}")
        else:
            logger.info(f"No popular stocks found with filter '{source}'")
            await interaction.followup.send(f"No popular stocks found with filter '{source}'", ephemeral=True)

    async def politician_options(self, interaction: discord.Interaction, current: str):
        politicians = self.stock_data.capitol_trades.all_politicians()
        names = [politician['name'] for politician in politicians]
        return [
            app_commands.Choice(name=p_name, value=p_name)
            for p_name in names if current.lower() in p_name.lower()
        ][:25]

    @app_commands.command(name="politician", description="Fetch a report on the latest stocks traded by a politician")
    @app_commands.describe(politician_name="Politician to return trades for")
    @app_commands.autocomplete(politician_name=politician_options,)
    @app_commands.describe(visibility="'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility=[
        app_commands.Choice(name="private", value='private'),
        app_commands.Choice(name="public", value='public'),
    ])
    async def politician(self, interaction: discord.Interaction, politician_name: str, visibility: app_commands.Choice[str]):
        """Generate and send Politician Report for input politician"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/politician function called by user {interaction.user.name}")

        politician = self.stock_data.capitol_trades.politician(name=politician_name)

        if politician:
            channel = self.bot.get_channel_for_guild(interaction.guild_id, REPORTS)
            if channel is None and visibility.value == 'public':
                await interaction.followup.send("Use `/setup` to configure the reports channel.", ephemeral=True)
                return
            content = self.build_politician_report(politician=politician)
            view = PoliticianReportButtons(pid=politician['politician_id'])
            files = [discord.File(content.filepath)]
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

    def _update_screener_watchlist(self, screener) -> None:
        """Update the system-generated watchlist for the given screener (moved from Screener.__init__)."""
        watchlist_id = screener.screener_type
        watchlist_tickers = screener.get_tickers()[:20]

        if not self.stock_data.watchlists.validate_watchlist(watchlist_id):
            self.stock_data.watchlists.create_watchlist(
                watchlist_id=watchlist_id, tickers=watchlist_tickers, systemGenerated=True
            )
            logger.info(f"Created new watchlist from '{screener.screener_type}' screener")
            logger.debug(f"Watchlist created with {len(watchlist_tickers)} tickers: {watchlist_tickers}")
        else:
            self.stock_data.watchlists.update_watchlist(
                watchlist_id=watchlist_id, tickers=watchlist_tickers
            )
            logger.info(f"Updated watchlist '{screener.screener_type}'")
            logger.debug(f"Watchlist updated with {len(watchlist_tickers)} tickers: {watchlist_tickers}")

    def build_popularity_screener(self, **kwargs) -> PopularityScreener:
        popular_stocks = kwargs.pop('popular_stocks', self.stock_data.popularity.get_popular_stocks())
        return PopularityScreener(data=PopularityScreenerData(popular_stocks=popular_stocks))

    def build_volume_screener(self, **kwargs) -> VolumeScreener:
        unusual_volume = kwargs.pop('unusual_volume', self.stock_data.trading_view.get_unusual_volume_movers())
        return VolumeScreener(data=VolumeScreenerData(unusual_volume=unusual_volume))

    def build_gainer_screener(self, market_period: str) -> GainerScreener:
        if market_period == 'premarket':
            gainers = self.stock_data.trading_view.get_premarket_gainers()
        elif market_period == 'intraday':
            gainers = self.stock_data.trading_view.get_intraday_gainers()
        elif market_period == 'aftermarket':
            gainers = self.stock_data.trading_view.get_postmarket_gainers()
        else:
            gainers = pd.DataFrame()
        return GainerScreener(data=GainerScreenerData(market_period=market_period, gainers=gainers))

    async def build_stock_report(self, ticker: str, **kwargs) -> StockReport:
        ticker_info = kwargs.pop('ticker_info', self.stock_data.tickers.get_ticker_info(ticker=ticker))
        daily_price_history = kwargs.pop('daily_price_history', self.stock_data.price_history.fetch_daily_price_history(ticker=ticker))
        popularity = kwargs.pop('popularity', self.stock_data.popularity.fetch_popularity(ticker=ticker))
        recent_sec_filings = kwargs.pop('recent_sec_filings', self.stock_data.sec.get_recent_filings(ticker=ticker))
        historical_earnings = kwargs.pop('historical_earnings', self.stock_data.earnings.get_historical_earnings(ticker=ticker))
        next_earnings_info = kwargs.pop('next_earnings_info', self.stock_data.earnings.get_next_earnings_info(ticker=ticker))
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        fundamentals = kwargs.pop('fundamentals', await self.stock_data.schwab.get_fundamentals(tickers=[ticker]))
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
        ))

    async def build_earnings_spotlight_report(self, ticker: str, **kwargs) -> EarningsSpotlightReport:
        ticker_info = self.stock_data.tickers.get_ticker_info(ticker=ticker)
        daily_price_history = self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
        next_earnings_info = self.stock_data.earnings.get_next_earnings_info(ticker=ticker)
        historical_earnings = self.stock_data.earnings.get_historical_earnings(ticker=ticker)
        quote = await self.stock_data.schwab.get_quote(ticker=ticker)
        fundamentals = await self.stock_data.schwab.get_fundamentals(tickers=[ticker])
        return EarningsSpotlightReport(data=EarningsSpotlightData(
            ticker=ticker,
            ticker_info=ticker_info,
            daily_price_history=daily_price_history,
            next_earnings_info=next_earnings_info,
            historical_earnings=historical_earnings,
            quote=quote,
            fundamentals=fundamentals,
        ))

    def build_weekly_earnings_screener(self, **kwargs) -> WeeklyEarningsScreener:
        upcoming_earnings = kwargs.pop('upcoming_earnings', self.stock_data.earnings.fetch_upcoming_earnings())
        watchlist_tickers = kwargs.pop('watchlist_tickers',
                                       self.stock_data.watchlists.get_all_watchlist_tickers(no_personal=True, no_systemGenerated=True))
        return WeeklyEarningsScreener(data=WeeklyEarningsData(
            upcoming_earnings=upcoming_earnings,
            watchlist_tickers=watchlist_tickers,
        ))

    def build_politician_report(self, politician_name: str = None, **kwargs) -> PoliticianReport:
        politician = kwargs.pop('politician', self.stock_data.capitol_trades.politician(name=politician_name))
        trades = kwargs.pop('trades', self.stock_data.capitol_trades.trades(pid=politician['politician_id']))
        politician_facts = kwargs.pop('politician_facts', self.stock_data.capitol_trades.politician_facts(pid=politician['politician_id']))
        return PoliticianReport(data=PoliticianReportData(
            politician=politician,
            trades=trades,
            politician_facts=politician_facts,
        ))

    @app_commands.command(name="test-screener", description="Preview a screener in embed format (ephemeral)")
    @app_commands.choices(screener=[
        app_commands.Choice(name="gainers", value="gainers"),
        app_commands.Choice(name="unusual-volume", value="volume"),
        app_commands.Choice(name="popularity", value="popularity"),
        app_commands.Choice(name="weekly-earnings", value="earnings"),
    ])
    async def test_screener(self, interaction: discord.Interaction, screener: app_commands.Choice[str]):
        """Preview a screener embed with live data."""
        await interaction.response.defer(ephemeral=True)
        if screener.value == "gainers":
            market_period = self.mutils.get_market_period()
            if market_period == "EOD":
                market_period = "intraday"
            content = self.build_gainer_screener(market_period=market_period)
        elif screener.value == "volume":
            content = self.build_volume_screener()
        elif screener.value == "popularity":
            content = self.build_popularity_screener()
        else:
            content = self.build_weekly_earnings_screener()
        embed = spec_to_embed(content.build_embed_spec())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="test-report", description="Preview a report type with dummy data (ephemeral)")
    @app_commands.choices(report=[
        app_commands.Choice(name="stock", value="stock"),
        app_commands.Choice(name="earnings-spotlight", value="earnings"),
        app_commands.Choice(name="news", value="news"),
        app_commands.Choice(name="popularity", value="popularity"),
        app_commands.Choice(name="politician", value="politician"),
    ])
    async def test_report(self, interaction: discord.Interaction, report: app_commands.Choice[str]):
        """Preview a report embed with dummy data."""
        await interaction.response.defer(ephemeral=True)
        content = _build_dummy_report(report.value)
        embed = spec_to_embed(content.build_embed_spec())
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(Reports(bot=bot, stock_data=bot.stock_data))

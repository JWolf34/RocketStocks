import datetime
import logging
import random
import asyncio
import pandas as pd
import discord
from discord import app_commands
from discord.ext import commands, tasks

from rocketstocks.data.stock_data import StockData
from rocketstocks.data.discord_state import DiscordState
from rocketstocks.data.clients.news import News
from rocketstocks.core.utils.market import market_utils
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.core.config.settings import reports_channel_id, screeners_channel_id, guild_id

from rocketstocks.core.reports.stock_report import StockReport
from rocketstocks.core.reports.news_report import NewsReport
from rocketstocks.core.reports.popularity_report import PopularityReport
from rocketstocks.core.reports.popularity_screener import PopularityScreener
from rocketstocks.core.reports.gainer_screener import GainerScreener
from rocketstocks.core.reports.volume_screener import VolumeScreener
from rocketstocks.core.reports.earnings_report import EarningsSpotlightReport
from rocketstocks.core.reports.earnings_screener import WeeklyEarningsScreener
from rocketstocks.core.reports.politician_report import PoliticianReport

from rocketstocks.bot.views.report_views import (
    StockReportButtons, GainerScreenerButtons, VolumeScreenerButtons,
    PopularityScreenerButtons, PopularityReportButtons, PoliticianReportButtons,
)
from rocketstocks.bot.senders.report_sender import send_report, send_screener

logger = logging.getLogger(__name__)


class Reports(commands.Cog):
    """Cog for managing Reports and Screeners to be posted to Discord"""

    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.mutils = market_utils()
        self.dstate = DiscordState()

        self.reports_channel = self.bot.get_channel(reports_channel_id)
        self.screeners_channel = self.bot.get_channel(screeners_channel_id)

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
        popular_stocks = self.stock_data.popularity.get_popular_stocks()

        if not popular_stocks.empty:
            popular_stocks.insert(loc=0,
                                  column='datetime',
                                  value=pd.Series([date_utils.round_down_nearest_minute(30)] * popular_stocks.shape[0]).values)

            self.stock_data.insert_popularity(popular_stocks=popular_stocks)

            content = self.build_popularity_screener(popular_stocks=popular_stocks)
            await self.stock_data.update_alert_tickers(tickers=content.get_tickers()[:250], source='popularity')

            logger.info("Posting popularity screener")
            view = PopularityScreenerButtons()
            await send_screener(content, self.screeners_channel, self.dstate, view=view)
        else:
            logger.error("No popular stocks found when attempting to update screener")

    @tasks.loop(minutes=5)
    async def post_volume_screener(self):
        """Retrieve latest unusual volume data and post screener"""
        market_period = self.mutils.get_market_period()
        if self.mutils.market_open_today() and market_period != 'EOD':
            unusual_volume = self.stock_data.trading_view.get_unusual_volume_movers()
            content = self.build_volume_screener(unusual_volume=unusual_volume)
            await self.stock_data.update_alert_tickers(tickers=content.get_tickers(), source='unusual-volume')

            logger.info("Posting unusual volume screener")
            view = VolumeScreenerButtons()
            await send_screener(content, self.screeners_channel, self.dstate, view=view)

    @tasks.loop(minutes=5)
    async def post_volume_at_time_screener(self):
        """Retrieve latest volume spike data — used only to update alert_tickers"""
        market_period = self.mutils.get_market_period()
        if self.mutils.market_open_today() and market_period != 'EOD':
            volume_spike = self.stock_data.trading_view.get_unusual_volume_at_time_movers()
            await self.stock_data.update_alert_tickers(tickers=volume_spike['name'].to_list(), source='volume-spike')

    @tasks.loop(minutes=5)
    async def post_gainer_screener(self):
        """Retrieve latest gainers data based on market period and post screener"""
        market_period = self.mutils.get_market_period()
        if self.mutils.market_open_today() and market_period != 'EOD':
            content = self.build_gainer_screener(market_period=market_period)
            await self.stock_data.update_alert_tickers(tickers=content.get_tickers(), source='gainers')

            logger.info(f"Sending {content.market_period} gainers screener")
            view = GainerScreenerButtons(market_period=market_period)
            await send_screener(content, self.screeners_channel, self.dstate, view=view)

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
        if self.mutils.market_open_today():
            earnings_today = self.stock_data.earnings.get_earnings_on_date(date=datetime.date.today())
            spotlight_ticker = earnings_today['ticker'].iloc[random.randint(0, earnings_today['ticker'].size - 1)]
            while not await self.stock_data.validate_ticker(ticker=spotlight_ticker):
                spotlight_ticker = earnings_today['ticker'].iloc[random.randint(0, earnings_today['ticker'].size - 1)]

            content = await self.build_earnings_spotlight_report(ticker=spotlight_ticker)
            logger.info(f"Posting today's earnings spotlight: '{content.ticker}'")
            view = StockReportButtons(ticker=content.ticker)
            await send_report(content, self.reports_channel, view=view)

    @tasks.loop(time=datetime.time(hour=12, minute=0, second=0))
    async def post_weekly_earnings(self):
        """Retrieve upcoming earnings data and post screener for earnings reporting this week"""
        today = datetime.datetime.now(tz=date_utils.timezone()).date()
        if today.weekday() == 0:
            content = self.build_weekly_earnings_screener()
            logger.info("Posting weekly earnings screener...")
            files = [discord.File(content.filepath)]
            await send_screener(content, self.screeners_channel, self.dstate, files=files)

    @tasks.loop(time=datetime.time(hour=6, minute=0, second=0))
    async def update_earnings_calendar(self):
        """Update guild Discord calendar with upcoming earnings report dates for tickers on watchlists"""
        logger.info("Creating calendar events for upcoming earnings dates")
        gld = self.bot.get_guild(guild_id)
        tickers = self.stock_data.watchlists.get_all_watchlist_tickers(no_personal=False, no_systemGenerated=True)
        logger.debug(f"Identified {len(tickers)} watchlist tickers to create earnings events for")

        curr_events = await gld.fetch_scheduled_events()
        logger.debug(f"{len(curr_events)} events already in the calendar")

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
                        logger.info(f"Earnings report '{name}' created at {start_time}")
                    else:
                        logger.info(f"Start time {start_time} for event '{name}' is in the past - skipping...")
                else:
                    logger.info(f"Event '{name}' already exists in the calendar. Skipping...")
        logger.info("Completed updating earnings calendar")

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
            message = None
            for ticker in tickers:
                content = await self.build_stock_report(ticker=ticker)
                view = StockReportButtons(ticker=content.ticker)
                message = await send_report(content, self.reports_channel, interaction=interaction,
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

        tickers, invalid_tickers = await self.stock_data.parse_valid_tickers(tickers.upper())
        logger.info(f"Reports requested for tickers {tickers}. Invalid tickers: {invalid_tickers}")
        message = None
        for ticker in tickers:
            content = await self.build_stock_report(ticker=ticker)
            view = StockReportButtons(ticker=content.ticker)
            message = await send_report(content, self.reports_channel, interaction=interaction,
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
        logger.info("/news function called by user {}".format(interaction.user.name))
        news_data = News().get_news(query=query, sort_by=sort_by)
        content = NewsReport(query=query, news=news_data)
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
        logger.info("/popular-stocks function called by user {}".format(interaction.user.name))

        popular_stocks = self.stock_data.popularity.get_popular_stocks(filter_name=source)
        filter_val = self.stock_data.popularity.get_filter(source)

        if not popular_stocks.empty:
            content = PopularityReport(popular_stocks=popular_stocks, filter=filter_val)
            view = PopularityReportButtons()
            files = [discord.File(content.filepath)]
            message = await send_report(content, self.reports_channel, interaction=interaction,
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
            content = self.build_politician_report(politician=politician)
            view = PoliticianReportButtons(pid=politician['politician_id'])
            files = [discord.File(content.filepath)]
            message = await send_report(content, self.reports_channel, interaction=interaction,
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

    def build_popularity_screener(self, **kwargs) -> PopularityScreener:
        popular_stocks = kwargs.pop('popular_stocks', self.stock_data.popularity.get_popular_stocks())
        return PopularityScreener(popular_stocks=popular_stocks)

    def build_volume_screener(self, **kwargs) -> VolumeScreener:
        unusual_volume = kwargs.pop('unusual_volume', self.stock_data.trading_view.get_unusual_volume_movers())
        return VolumeScreener(unusual_volume=unusual_volume)

    def build_gainer_screener(self, market_period: str) -> GainerScreener:
        if market_period == 'premarket':
            gainers = self.stock_data.trading_view.get_premarket_gainers()
        elif market_period == 'intraday':
            gainers = self.stock_data.trading_view.get_intraday_gainers()
        elif market_period == 'aftermarket':
            gainers = self.stock_data.trading_view.get_postmarket_gainers()
        else:
            gainers = pd.DataFrame()
        return GainerScreener(market_period=market_period, gainers=gainers)

    async def build_stock_report(self, ticker: str, **kwargs) -> StockReport:
        ticker_info = kwargs.pop('ticker_info', self.stock_data.get_ticker_info(ticker=ticker))
        daily_price_history = kwargs.pop('daily_price_history', self.stock_data.fetch_daily_price_history(ticker=ticker))
        popularity = kwargs.pop('popularity', self.stock_data.fetch_popularity(ticker=ticker))
        recent_sec_filings = kwargs.pop('recent_sec_filings', self.stock_data.sec.get_recent_filings(ticker=ticker))
        historical_earnings = kwargs.pop('historical_earnings', self.stock_data.earnings.get_historical_earnings(ticker=ticker))
        next_earnings_info = kwargs.pop('next_earnings_info', self.stock_data.earnings.get_next_earnings_info(ticker=ticker))
        quote = kwargs.pop('quote', await self.stock_data.schwab.get_quote(ticker=ticker))
        fundamentals = kwargs.pop('fundamentals', await self.stock_data.schwab.get_fundamentals(tickers=[ticker]))
        return StockReport(
            ticker_info=ticker_info,
            daily_price_history=daily_price_history,
            popularity=popularity,
            recent_sec_filings=recent_sec_filings,
            historical_earnings=historical_earnings,
            next_earnings_info=next_earnings_info,
            quote=quote,
            fundamentals=fundamentals,
        )

    async def build_earnings_spotlight_report(self, ticker: str, **kwargs) -> EarningsSpotlightReport:
        ticker_info = self.stock_data.get_ticker_info(ticker=ticker)
        daily_price_history = self.stock_data.fetch_daily_price_history(ticker=ticker)
        next_earnings_info = self.stock_data.earnings.get_next_earnings_info(ticker=ticker)
        historical_earnings = self.stock_data.earnings.get_historical_earnings(ticker=ticker)
        quote = await self.stock_data.schwab.get_quote(ticker=ticker)
        fundamentals = await self.stock_data.schwab.get_fundamentals(tickers=[ticker])
        return EarningsSpotlightReport(
            ticker_info=ticker_info,
            daily_price_history=daily_price_history,
            next_earnings_info=next_earnings_info,
            historical_earnings=historical_earnings,
            quote=quote,
            fundamentals=fundamentals,
        )

    def build_weekly_earnings_screener(self, **kwargs) -> WeeklyEarningsScreener:
        upcoming_earnings = kwargs.pop('upcoming_earnings', self.stock_data.earnings.fetch_upcoming_earnings())
        watchlist_tickers = kwargs.pop('watchlist_tickers',
                                       self.stock_data.watchlists.get_all_watchlist_tickers(no_personal=True, no_systemGenerated=True))
        return WeeklyEarningsScreener(upcoming_earnings=upcoming_earnings, watchlist_tickers=watchlist_tickers)

    def build_politician_report(self, politician_name: str = None, **kwargs) -> PoliticianReport:
        politician = kwargs.pop('politician', self.stock_data.capitol_trades.politician(name=politician_name))
        trades = kwargs.pop('trades', self.stock_data.capitol_trades.trades(pid=politician['politician_id']))
        politician_facts = kwargs.pop('politician_facts', self.stock_data.capitol_trades.politician_facts(pid=politician['politician_id']))
        return PoliticianReport(politician=politician, trades=trades, politician_facts=politician_facts)


async def setup(bot):
    await bot.add_cog(Reports(bot=bot, stock_data=bot.stock_data))

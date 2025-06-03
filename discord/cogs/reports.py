import sys
sys.path.append('../RocketStocks/discord/cogs')
import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
from discord.cogs.watchlists import Watchlists as cog_Watchlists                # cog
from RocketStocks.stockdata.watchlists import Watchlists as data_Watchlists     # stockdata module      
import datetime
from stockdata import StockData
import pandas as pd
import datetime as dt
import utils
from utils import market_utils, date_utils, discord_utils
import asyncio
from table2ascii import table2ascii, PresetStyle
import logging
import random

# Logging configuration
logger = logging.getLogger(__name__)  

class Reports(commands.Cog):
    def __init__(self, bot, stock_data:StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.mutils = market_utils()

        # Init channels
        self.reports_channel = self.bot.get_channel(discord_utils.reports_channel_id)
        self.screeners_channel = self.bot.get_channel(discord_utils.screeners_channel_id)

        # Start reports
        self.post_popularity_screener.start()
        self.post_volume_screener.start()
        self.post_gainer_screener.start()
        
        self.update_earnings_calendar.start()
        
        self.post_earnings_spotlight.start()
        self.post_weekly_earnings.start()
        
        

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"{__name__} loaded")

    
    #########
    # Tasks #
    #########

    # Screener on most popular stocks across reddit daily
    #@tasks.loop(time=datetime.time(hour=22 , minute=0, second=0)) # time in UTC
    @tasks.loop(minutes=30)
    async def post_popularity_screener(self):
        # Fetch most popular stocks
        popular_stocks = self.stock_data.popularity.get_popular_stocks()

        # Append datetime column to df, rounded to pervious 30m interval
        popular_stocks.insert(loc=0,
                              column='datetime',
                              value=pd.Series([date_utils.round_down_nearest_minute(30)] * popular_stocks.shape[0]).values)

        # Generate screener
        report = PopularityScreener(channel=self.screeners_channel,
                                    market_period='None',
                                    popular_stocks=popular_stocks)

        # Post screener
        logger.info("Posting populairty screener")
        await report.send_report()

    # Generate and send premarket gainer reports to the reports channel
    @tasks.loop(minutes=5)
    async def post_volume_screener(self):
        market_period = self.mutils.get_market_period()
        if  (self.mutils.market_open_today() and market_period != 'EOD'):

            # Get unusual volume data
            unusual_volume = self.stock_data.trading_view.get_unusual_volume_movers()

            # Generate screener
            report = VolumeScreener(channel=self.screeners_channel,
                                    market_period=market_period,
                                    unusual_volume=unusual_volume)

            # Update alert tickers with unusual volume movers
            self.stock_data.update_alert_tickers(tickers=report.get_tickers(), source='unusual-volume')

            # Send report
            logger.info(f"Posting {report.market_period} volume screener")
            await report.send_report()
        else:
            # Not a weekday - do not post volume screener
            pass
       

    # Generate and send premarket gainer reports to the reports channel
    @tasks.loop(minutes=5)
    async def post_gainer_screener(self):
        market_period = self.mutils.get_market_period()
        if  (self.mutils.market_open_today() and market_period != 'EOD'):

            # Get gainers based on market period
            if market_period == 'premarket':
                gainers = self.stock_data.trading_view.get_premarket_gainers()
            elif market_period == 'intraday':
                gainers = self.stock_data.trading_view.get_intraday_gainers()
            elif market_period == 'aftermarket':
                gainers = self.stock_data.trading_view.get_postmarket_gainers()

            # Generate screener
            report = GainerScreener(channel=self.screeners_channel,
                                    market_period=market_period,
                                    gainers=gainers)

            # Update alert tickers with gainers
            self.stock_data.update_alert_tickers(tickers=report.get_tickers(), source='gainers')

            # Send report
            logger.info(f"Sending {report.market_period} gainers report")
            await report.send_report()
        else:
            # Not a weekday - do not post gainer reports
            pass

    

    # Start posting report at next 0 or 5 minute interval
    @post_gainer_screener.before_loop
    @post_volume_screener.before_loop
    async def sleep_until_5m(self):
        sleep_time = date_utils.seconds_until_minute_interval(minute=5)
        logger.info(f"5m reports will begin posting in {sleep_time} seconds")
        await asyncio.sleep(sleep_time)

    # Start posting reports at next 0 or 30 minute interval
    @post_popularity_screener.before_loop
    async def sleep_until_30m(self):
        sleep_time = date_utils.seconds_until_minute_interval(minute=30)
        logger.info(f"30m reports will begin posting in {sleep_time} seconds")
        await asyncio.sleep(sleep_time)


    #@tasks.loop(time=datetime.time(hour=12, minute=30, second=0)) # time in UTC
    @tasks.loop(minutes=5)
    async def post_earnings_spotlight(self):
        if self.mutils.market_open_today():

            # Select random ticker from earnings today
            earnings_today = self.stock_data.earnings.get_earnings_on_date(date=datetime.date.today())
            spotlight_ticker = earnings_today['ticker'].iloc[random.randint(0, earnings_today['ticker'].size)]

            # Get ticker info, earnings info, quote for spotlight report
            ticker_info = self.stock_data.get_ticker_info(ticker=spotlight_ticker)
            next_earnings_info = self.stock_data.earnings.get_next_earnings_info(ticker=spotlight_ticker)
            historical_earnings = self.stock_data.earnings.get_historical_earnings(ticker=spotlight_ticker)
            quote = await self.stock_data.schwab.get_quote(ticker=spotlight_ticker)

            report = EarningsSpotlightReport(channel = self.reports_channel,
                                             ticker_info=ticker_info,
                                             next_earnings_info=next_earnings_info,
                                             historical_earnings=historical_earnings,
                                             quote=quote)
            logger.info(f"Posting today's earnings spotlight: '{report.ticker}'")
            await report.send_report()


    @tasks.loop(time=datetime.time(hour=12, minute=0, second=0)) # time in UTC
    #@tasks.loop(minutes=5)
    async def post_weekly_earnings(self):
        today = datetime.datetime.today()
        if datetime.datetime.today().weekday() == 0:
            report = WeeklyEarningsReport(self.reports_channel)
            logger.info(f"Posting weekly earnings report. Earnings reporting this week: {report.upcoming_earnings}")
            await report.send_report()


    # Create earnings events on calendar for all stocks on watchlists
    @tasks.loop(time=datetime.time(hour=6, minute=0, second=0)) # time in UTC
    #@tasks.loop(minutes=5)
    async def update_earnings_calendar(self):
        logger.info("Creating events for upcoming earnings dates")
        guild = self.bot.get_guild(utils.discord_utils.guild_id)
        tickers = self.bot.stock_data.watchlists.get_tickers_from_all_watchlists(no_personal=False) # Get all tickers except from system-generated watchlists
        for ticker in tickers:
            earnings_info = sd.StockData.Earnings.get_next_earnings_info(ticker)
            if len(earnings_info) > 0:
                event_exists = False
                name = f"{ticker} Earnings"
                curr_events = await guild.fetch_scheduled_events()
                if curr_events:
                    for event in curr_events:
                        if event.name == name:
                            event_exists = True
                            break
                
                # Event does not exist, create one
                if not event_exists:
                    release_time = "unspecified"
                    start_time = datetime.datetime.combine(earnings_info['date'][0], datetime.datetime.strptime('1230','%H%M').time()).astimezone()
                    #start_time= start_time.replace(hour=12, minute=0, second=0)
                    if "pre-market" in earnings_info['time'][0]:
                        start_time = start_time.replace(hour = 8, minute=30)
                        release_time = "pre-market"
                    elif "after-hours" in earnings_info['time'][0]:
                        release_time = "after hours"
                        start_time = start_time.replace(hour = 15, minute=0)

                    now = datetime.datetime.now().astimezone()
                    if start_time > now:
                        description = f"""**Quarter:** {earnings_info['fiscalquarterending'][0]}
    **Release Time:** {release_time}
    **EPS Forecast:** {earnings_info['epsforecast'][0]}
    **Last Year's EPS:** {earnings_info['lastyeareps'][0]}
    **Last Year's Report Date:** {earnings_info['lastyearrptdt'][0]}
                        """
                        channel = self.bot.get_channel(utils.discord_utils.alerts_channel_id)
                        event = await guild.create_scheduled_event(name=name, 
                                                                description=description,
                                                                start_time=start_time,
                                                                end_time= start_time + datetime.timedelta(minutes=30),
                                                                entity_type = discord.EntityType.external,
                                                                privacy_level=discord.PrivacyLevel.guild_only,
                                                                location="Wall Street")
                        logger.debug(f"Event '{event.name} created at {event.start_time}")
                    else:
                        # Event start time is in the past
                        logger.debug(f"Event start time {start_time} is in the past. Skipping...")
                        pass
                else:
                    # Event already exists
                    logger.debug(f"Event '{event.name}' already exists. Skipping...")
                    pass
        logger.info("Completed updating earnings calendar")

    @app_commands.command(name = "report-watchlist", description= "Post analysis of a given watchlist (use /fetch-reports for individual or non-watchlist stocks)",)
    @app_commands.describe(watchlist = "Which watchlist to fetch reports for")
    @app_commands.autocomplete(watchlist=cog_Watchlists.watchlist_options,)
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])
    async def report_watchlist(self, interaction: discord.Interaction, watchlist: str, visibility: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/report-watchlist function called by user {}".format(interaction.user.name))
        
        
        message = ""
        watchlist_id = watchlist

        # Populate tickers based on value of watchlist
        if watchlist == 'personal':
            watchlist_id = str(interaction.user.id)
        
        if watchlist_id not in self.bot.stock_data.watchlists.get_watchlists():
            await interaction.followup.send(f"Watchlist '{watchlist_id}' does not exist")
            logger.debug(f"Watchlist '{watchlist}' does not exist. No reports generated. ")
            return

        tickers = self.bot.stock_data.watchlists.get_tickers_from_watchlist(watchlist_id)
        logger.info(f"Reports requested for watchlist '{watchlist}' with tickers {tickers}")

        if len(tickers) == 0:
            # Empty watchlist
            logger.debug("Selected watchlist '{}' is empty".format(watchlist))
            message = "No tickers on the watchlist. Use /addticker to build a watchlist."
            await interaction.followup.send(message, ephemeral=True)
        else:
            # Send reports
            logger.info(f"Generating reports for tickers {tickers}")
            message = None
            for ticker in tickers:
                report = StockReport(ticker, self.reports_channel)
                message = await report.send_report(interaction, visibility.value)
            logger.info("Reports have been posted")

            # Follow-up message
            follow_up = f"Posted reports for tickers [{", ".join(tickers)}]({message.jump_url})!"
            await interaction.followup.send(follow_up, ephemeral=True)


    @app_commands.command(name = "report", description= "Fetch analysis reports of the specified tickers (use /run-reports to analyze a watchlist)",)
    @app_commands.describe(tickers = "Tickers to post reports for (separated by spaces)")
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])        
    async def report(self, interaction: discord.interactions, tickers: str, visibility: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/report function called by user {}".format(interaction.user.name))
        logger.info(f"Reports requested for tickers '{tickers}'")
    
        # Validate each ticker in the list is valid
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)
        message = None
        for ticker in tickers:
            report = StockReport(ticker, self.reports_channel)
            message = await report.send_report(interaction, visibility.value)

        # Follow-up message
        follow_up = ""
        if message is not None: # Message was generated
            logger.info(f"Reports posted for tickers {tickers}")
            follow_up = f"Posted reports for tickers [{", ".join(tickers)}]({message.jump_url})!"
            if len(invalid_tickers) > 0: # More than one invalid ticke input
                follow_up += f" Invalid tickers: {", ".join(invalid_tickers)}"
        if len(tickers) == 0: # No valid tickers input
            logger.info(f"No valid tickers input. No reports generated")
            follow_up = f" No valid tickers input: {", ".join(invalid_tickers)}"
        await interaction.followup.send(follow_up, ephemeral=True)

    # Autocomplete functions

    async def autocomplete_searchin(self, interaction:discord.Interaction, current:str):
        return [
            app_commands.Choice(name = search_in_name, value= search_in_value)
            for search_in_name, search_in_value in sd.News().search_in.items() if current.lower() in search_in_name.lower()
        ]

    async def autocomplete_sortby(self, interaction:discord.Interaction, current:str):
        return [
            app_commands.Choice(name = sort_by_name, value= sort_by_value)
            for sort_by_name, sort_by_value in sd.News().sort_by.items() if current.lower() in sort_by_name.lower()
        ]

    @app_commands.command(name="news", description="Fetch news on the query provided")
    @app_commands.describe(query= "The search terms or terms to query for")  
    @app_commands.describe(sort_by = "Field by which to sort returned articles")
    @app_commands.autocomplete(sort_by=autocomplete_sortby,)
    async def news(self, interaction:discord.Interaction, query:str, sort_by:str = 'publishedAt'):
        logger.info("/news function called by user {}".format(interaction.user.name))
        kwargs = {'sort_by': sort_by}
        report = NewsReport(query=query, **kwargs)
        message = await report.send_report(interaction=interaction)
        logger.info(f"Posted news for query '{query}'")

    async def autocomplete_filter(self, interaction:discord.Interaction, current:str):
        return [
            app_commands.Choice(name = filter_name, value= filter_name)
            for filter_name in sd.ApeWisdom().filters_map.keys() if current.lower() in filter_name.lower()
        ]

    @app_commands.command(name="popular-stocks", description="Fetch a report on the most popular stocks from the source provided")
    @app_commands.describe(source = "The source to pull popular stocks from")
    @app_commands.autocomplete(source=autocomplete_filter,)
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])   
    async def popular_stocks(self, interaction:discord.Interaction, source:str,  visibility: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/popular-stocks function called by user {}".format(interaction.user.name))
        logger.info(f"Latest popularity request from source '{source}'")
        report = PopularityReport(self.reports_channel, filter_name=source)
        message = await report.send_report(interaction=interaction, visibility=visibility.value)

        # Follow-up message
        follow_up = f"[Posted popularity reports!]({message.jump_url})"
        await interaction.followup.send(follow_up, ephemeral=True)
        logger.info(f"Popularity posted from source {source}")

##################
# Report Classes #
##################

class Report(object):
    def __init__(self, channel:discord.channel, **kwargs):
       
        self.channel = channel

        # Parse data from keywords args
        self.ticker_info = kwargs.pop('ticker_info', None)
        self.ticker = self.ticker_info['ticker'] if self.ticker_info else None
        self.quote = kwargs.pop('quote', None)
        self.daily_price_history = kwargs.pop('daily_price_history', None)
        self.next_earnings_info = kwargs.pop('next_earnings_info', None)
        self.historical_earnings = kwargs.pop('historical_earnings', None)

        # ASCII table styles
        self.table_styles = {'ascii':PresetStyle.ascii,
                        'asci_borderless':PresetStyle.ascii_borderless,
                        'ascii_box':PresetStyle.ascii_box,
                        'ascii_compact':PresetStyle.ascii_compact,
                        'ascii_double':PresetStyle.ascii_double,
                        'ascii_minimalist':PresetStyle.ascii_minimalist,
                        'ascii_rounded':PresetStyle.ascii_rounded,
                        'ascii_rounded_box':PresetStyle.ascii_rounded_box,
                        'ascii_simple':PresetStyle.ascii_simple,
                        'borderless':PresetStyle.borderless,
                        'double':PresetStyle.double_box,
                        'double_box':PresetStyle.double_box,
                        'double_compact':PresetStyle.double_compact,
                        'double_thin_box':PresetStyle.double_thin_box,
                        'double_thin_compact':PresetStyle.double_thin_compact,
                        'markdown':PresetStyle.markdown,
                        'minimalist':PresetStyle.minimalist,
                        'plain':PresetStyle.plain,
                        'simple':PresetStyle.simple,
                        'thick':PresetStyle.thick,
                        'thick_box':PresetStyle.thick_box,
                        'thick_compact':PresetStyle.thick_compact,
                        'thin':PresetStyle.thin,
                        'thin_box':PresetStyle.thin_box,
                        'thin_compact':PresetStyle.thin_compact,
                        'thin_compact_rounded':PresetStyle.thin_compact_rounded,
                        'thin_double':PresetStyle.thin_double,
                        'thin_double_rounded':PresetStyle.thin_double_rounded,
                        'thin_rounded':PresetStyle.thin_rounded,
                        'thin_thick':PresetStyle.thin_thick,
                        'thin_thick_rounded':PresetStyle.thin_thick_rounded}


    ############################
    # Report Builder Functions #
    ############################

    # Report Header
    def build_report_header(self):
        logger.debug("Building report header...")
        # Append ticker name, today's date, and external links to message
        header = "# " + self.ticker + " Report " + dt.date.today().strftime("%m/%d/%Y") + "\n"
        return header + "\n"

    # Ticker Info
    def build_ticker_info(self):
        logger.debug("Building ticker info...")
        message = "## Ticker Info\n"
        
        message += f"**Name:** {self.ticker_info['name']}\n"
        message += f"**Sector:** {self.ticker_info['sector'] if self.ticker_info['sector'] else "N/A"}\n"
        message += f"**Industry:** {self.ticker_info['industry'] if self.ticker_info['industry'] else "N/A"}\n" 
        message += f"**Country:** {self.ticker_info['country'] if self.ticker_info['country'] else "N/A"}\n"
        message += f"**Exchange:** {self.quote['reference']['exchangeName']}\n"
        return message 
        
        
    def build_recent_SEC_filings(self):
        logger.debug("Building latest SEC filings...")
        message = "## Recent SEC Filings\n\n"
        filings = sd.SEC().get_recent_filings(ticker=self.ticker)
        for index, filing in filings[:5].iterrows():
            message += f"[Form {filing['form']} - {filing['filingDate']}]({sd.SEC().get_link_to_filing(ticker=self.ticker, filing=filing)})\n"
        return message

    def build_todays_sec_filings(self):
        logger.debug("Building today's SEC filings...")
        message = "## Today's SEC Filings\n\n"
        filings = sd.SEC().get_filings_from_today(ticker=self.ticker)
        for index, filing in filings.iterrows():
            message += f"[Form {filing['form']} - {filing['filingDate']}]({sd.SEC().get_link_to_filing(ticker=self.ticker, filing=filing)})\n"
        return message

    def build_table(self, df:pd.DataFrame, style='thick_compact'):
        logger.debug(f"Building table of shape {df.shape} with headers {df.columns.to_list()} and of style '{style}'")
        table_style = self.table_styles.get(style, PresetStyle.double_thin_compact)
        table = table2ascii(
            header = df.columns.tolist(),
            body = df.values.tolist(),
            style=table_style 
        )
        return "```\n" + table + "\n```"

    def build_earnings_date(self):
        logger.debug("Building earnings date...")

        # Earnings date
        message = f"{self.ticker} reports earnings on "
        message += f"{date_utils.format_date_mdy(self.next_earnings_info['date'])}, "

        # Earnings time
        earnings_time = self.next_earnings_info['time']
        if "pre-market" in earnings_time:
            message += "before market open"
        elif "after-hours" in earnings_time:
            message += "after market close"
        else:
            message += "time not specified"

        return message + "\n"

    def build_upcoming_earnings_summary(self):
        logger.debug("Building upcoming earnings summary...")

        message = "## Next Earnings Summary\n\n"
        message += f"**Date:** {self.next_earnings_info['date']}\n"
        message += "**Time:** {}\n".format("Premarket" if "pre-market" in self.next_earnings_info['time']
                                else "After hours" if "after-hours" in self.next_earnings_info['time']
                                else "Not supplied")
        message += f"**Fiscal Quarter:** {self.next_earnings_info['fiscal_quarter_ending']}\n"
        message += f"**EPS Forecast: ** {self.next_earnings_info['eps_forecast'] if len(self.next_earnings_info['eps_forecast']) > 0 else "N/A"}\n"
        message += f"**No. of Estimates:** {self.next_earnings_info['no_of_ests']}\n"
        message += f"**Last Year Report Date:** {self.next_earnings_info['last_year_rpt_dt']}\n"
        message += f"**Last Year EPS:** {self.next_earnings_info['last_year_eps']}\n"
        return message + "\n\n"

    def build_recent_earnings(self):
        logger.debug("Building recent earnings...")
        message = "## Recent Earnings Overview\n"
        #message += f"**Next earnings date:** {self.next_earnings_info['date']}\n"
        column_map = {'date':'Date Reported', 
                      'eps':'EPS',
                      'surprise':'Surprise',
                      'epsforecast':'Estimate',
                      'fiscalquarterending':'Quarter'}
        
        recent_earnings = self.historical_earnings.tail(4)
        recent_earnings = recent_earnings.filter(list(column_map.keys()))
        recent_earnings = recent_earnings.rename(columns=column_map)
        recent_earnings['Date Reported'] = recent_earnings['Date Reported'].apply(lambda x: date_utils.format_date_mdy(x))
        recent_earnings['Surprise'] =  recent_earnings['Surprise'].apply(lambda x: f"{x}%")
        message += self.build_table(df=recent_earnings, style='plain')
        return message + "\n"

    def build_performance(self):
        logger.debug("Building performance...")
        today =  datetime.datetime.today().date()
        message = "## Performance \n\n"
        performance_frequency = {'1D': 1,
                                 '5D': 5,
                                 '1M': 30,
                                 '3M': 90,
                                 '6M': 180}
        curr_close = self.quote['regular']['regularMarketLastPrice']
        for frequency, days in performance_frequency.items():
            try:
                #close_df = self.data.loc[self.data['date'] == (today - datetime.timedelta(days=days)), 'close'].iloc[0]
                # Get old_close based on date offset from today's date
                old_date = today - datetime.timedelta(days=days)
                while old_date.weekday() > 4:
                    old_date = old_date - datetime.timedelta(days=1)
                old_close = float(self.data.loc[self.data['date'] == (old_date), 'close'].iloc[0])
                pct_change = ((curr_close - old_close) / old_close) * 100.0
                symbol = ":green_circle:" if pct_change > 0.0 else ":small_red_triangle_down:"
                message += f"**{frequency}:** {symbol} {"{:.2f}%".format(pct_change)}\n"
            except IndexError as e:
                # Stock has not been on the market long enough to generate full performance summary
                pass
        return message

    def build_daily_summary(self):
        logger.debug("Building daily summary...")
        message = "## Today's Summary\n\n"
        OHLCV = {'Open': "{:.2f}".format(self.quote['quote']['openPrice']),
                 'High': "{:.2f}".format(self.quote['quote']['highPrice']),
                 'Low': "{:.2f}".format(self.quote['quote']['lowPrice']),
                 'Close': "{:.2f}".format(self.quote['regular']['regularMarketLastPrice']),
                 'Volume': self.format_large_num(self.quote['quote']['totalVolume'])
                }
        message += " | ".join(f"**{column}:** {value}" for column, value in OHLCV.items())
        return message + "\n"  

    def build_stats(self):
        logger.debug("Building ticker stats...")
        message = "## Stats\n"
        message += f"**Market Cap:** {self.format_large_num(self.ticker_info['marketcap'])}\n"
        message += f"**52 Week High:** {self.quote['quote']['52WeekHigh']}\n"
        message += f"**52 Week Low:** {self.quote['quote']['52WeekLow']}\n"
        message += f"**Average Volume 10D:** {self.format_large_num(self.quote['fundamental']['avg10DaysVolume'])}\n"
        message += f"**Average Volume 1Y:** {self.format_large_num(self.quote['fundamental']['avg1YearVolume'])}\n"
        message += f"**P/E Ratio:** {"{:.2f}".format(self.quote['fundamental']['peRatio'])}\n"
        return message

    def build_popularity(self):
        logger.debug("Building popularity...")
        message = "## Popularity\n"

        # If no historical popularity, return
        if self.popularity.empty:
            message += "No popularity records available"
            return message + "\n"
        
        # Find today's popularity rank for ticker
        if self.ticker in self.top_stocks['ticker'].values:
            todays_rank = self.top_stocks['rank'].where(self.top_stocks['ticker'] == self.ticker)
        else:
            todays_rank = 501  
        #popularity = sd.StockData.get_historical_popularity(self.ticker)
        
        
        message += f"**Today:** {todays_rank}\n"
        for i in range(1, 6):
            date = datetime.date.today() - datetime.timedelta(i)
            try:
                old_rank = popularity[popularity['date'] == date]['rank'].iloc[0]
                symbol = ":green_circle:" if (old_rank-todays_rank) > 0 else ":small_red_triangle_down:"
                change = f"{old_rank - todays_rank} spots"
            except IndexError as e:
                old_rank = 'N/A'
                symbol = ''
                change = ''
            message +=f"**{i} Day(s) Ago:** {old_rank}, {symbol} {change}\n"
        return message

    def build_report(self):
        report = ''
        report += self.build_report_header()
        return report   

    async def send_report(self, interaction:discord.Interaction = None, visibility:str = "public", files=None, view=None):
        self.message = self.build_report() + "\n\n"
        if visibility == 'private' and interaction is not None:
            message = await interaction.user.send(self.message, files=files, view=view)
            return message
        else:
            message = await self.channel.send(self.message, files=files, view=view)
            return message

    #####################
    # Utility functions #
    #####################

    # Tool to format large numbers
    def format_large_num(self, number):
        try:
            number = float('{:.3g}'.format(float(number)))
            magnitude = 0
            while abs(number) >= 1000:
                magnitude += 1
                number /= 1000.0
            return '{}{}'.format('{:f}'.format(number).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])
        except TypeError as e:
            return "N/A"
    
    class Buttons(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=None)

class Screener(Report):

    def __init__(self, channel, screener_type:str, market_period:str, data:pd.DataFrame, column_map:dict):
        super().__init__(channel=channel)
        self.dutils = discord_utils()
        self.screener_type = screener_type
        self.market_period = market_period
        self.column_map = column_map

        # Init data, update watchlist, and format for posting
        self.data = data
        self.format_columns()
        self.update_watchlist()
        

    def get_tickers(self):
        return self.data['Ticker'].to_list()

    def update_watchlist(self):
        from db import Postgres

        watchlists = data_Watchlists(db=Postgres())
        watchlist_id = self.screener_type
        watchlist_tickers = self.get_tickers()
        watchlist_tickers = watchlist_tickers[:15]

        if not watchlists.validate_watchlist(watchlist_id):
            watchlists.create_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers, systemGenerated=True)
        else:
            watchlists.update_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers)

    def format_columns(self):
        logger.debug("Formatting gainers dataframe for table viewing")

        # Drop all unwanted columns and map column names
        self.data = self.data.filter(list(self.column_map.keys()))
        self.data = self.data.rename(columns=self.column_map)

        

    # Override
    async def send_report(self):
        """Send gainer report to the screeners channel"""
        self.message = self.build_report() + "\n\n"

        logger.debug(f"Posting '{self.screener_type}' screener...")
        today = datetime.datetime.today()

        # Format screener type for db insertion
        self.screener_type = self.screener_type.upper().replace("-","_")
        
        # Check if a gainer message already exists and update if present
        message_id = self.dutils.get_screener_message_id(screener_type=self.screener_type)
        if message_id:
            curr_message = await self.channel.fetch_message(message_id)
            if curr_message.created_at.date() < today.date():
                message = await self.channel.send(self.message)
                self.dutils.update_screener_message_id(message_id=message_id, screener_type=self.screener_type)
                return message
            else:
                logger.debug(f"{self.market_period.upper()} Gainer Report already exists today. Updating... ")
                await curr_message.edit(content=self.message)
        # No existing report, send new one
        else:
            message = await self.channel.send(self.message)
            self.dutils.insert_screener_message_id(message_id=message.id, screener_type=self.screener_type)
            return message

class StockReport(Report):
    
    def __init__(self, ticker : str, channel):
        self.ticker = ticker
        self.data =  sd.StockData.fetch_daily_price_history(self.ticker)
        self.popularity = sd.StockData.get_historical_popularity(ticker=self.ticker)
        self.top_stocks = pd.concat([sd.ApeWisdom().get_top_stocks(page=i) for i in range(1, 6)])
        self.buttons = self.Buttons(self.ticker)
        super().__init__(channel)

    async def init(self):
        self.quote = await sd.Schwab().get_quote(self.ticker)
        
    # Override
    def build_report(self):
        logger.debug("Building Stock Report...")
        report = ''
        report += self.build_report_header()
        report += self.build_ticker_info()
        report += self.build_daily_summary()
        report += self.build_performance()
        report += self.build_stats()
        report += self.build_popularity()
        report += self.build_recent_earnings()
        report += self.build_recent_SEC_filings()
        
        return report

    # Override
    async def send_report(self, interaction: discord.Interaction, visibility:str):
        logger.debug("Sending Stock Report...")
        message = await super().send_report(interaction=interaction, visibility=visibility, view=self.buttons)
        return message

    # Override
    class Buttons(discord.ui.View):
            def __init__(self, ticker : str):
                super().__init__(timeout=None)
                self.ticker = ticker
                self.add_item(discord.ui.Button(label="Google it", style=discord.ButtonStyle.url, url = "https://www.google.com/search?q={}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="StockInvest", style=discord.ButtonStyle.url, url = "https://stockinvest.us/stock/{}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="FinViz", style=discord.ButtonStyle.url, url = "https://finviz.com/quote.ashx?t={}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="Yahoo! Finance", style=discord.ButtonStyle.url, url = "https://finance.yahoo.com/quote/{}".format(self.ticker)))

                
            @discord.ui.button(label="Generate chart", style=discord.ButtonStyle.primary)
            async def generate_chart(self, interaction:discord.Interaction, button:discord.ui.Button,):
                await interaction.response.send_message("Generate chart!")

            @discord.ui.button(label="Get news", style=discord.ButtonStyle.primary)
            async def get_news(self, interaction:discord.Interaction, button:discord.ui.Button):
                news_report = NewsReport(self.ticker)
                await news_report.send_report(interaction)
                await interaction.response.send_message(f"Fetched news for {self.ticker}!", ephemeral=True)

class GainerScreener(Screener):
    def __init__(self, channel:discord.channel, market_period:str, gainers:pd.DataFrame):
        
        # Set column map
        if market_period == 'premarket':
            column_map = {'name':'Ticker',
                          'premarket_change':'Change (%)',
                          'premarket_close':'Price',
                          'close':'Prev Close',
                          'premarket_volume':'Pre Market Volume',
                          'market_cap_basic':'Market Cap'}
        elif market_period == 'intraday':
            column_map = {'name':'Ticker',
                          'change':'Change (%)',
                          'close':'Price',
                          'volume':'Volume',
                          'market_cap_basic':'Market Cap'}
        elif market_period == 'aftermarket':
            column_map = {'name':'Ticker',
                          'postmarket_change':'Change (%)',
                          'postmarket_close':'Price',
                          'close':'Price at Close',
                          'postmarket_volume':'After Hours Volume',
                          'market_cap_basic':'Market Cap'}
            
        super().__init__(channel=channel, 
                         screener_type=f"{market_period}-gainers", 
                         market_period=market_period,
                         data = gainers,
                         column_map=column_map)
        

    # Extends
    def format_columns(self):
        super().format_columns()

        # Format all volume columns in df
        volume_cols = self.data.filter(like='Volume').columns.to_list()
        for volume_col in volume_cols:
            self.data[volume_col] = self.data[volume_col].apply(lambda x: self.format_large_num(x))

        # Format Market Cap
        self.data['Market Cap'] = self.data['Market Cap'].apply(lambda x: self.format_large_num(x))

        # Format % change columns
        self.data['Change (%)'] = self.data['Change (%)'].apply(lambda x: "{:.2f}%".format(float(x)) if x is not None else 0.00)

    

    # Override
    def build_report_header(self):
        logger.debug("Building report header...")
        now = datetime.datetime.now(tz=date_utils.timezone())
        header = "### :rotating_light: {} Gainers {} (Updated {})\n\n".format(
                    "Pre-market" if self.market_period == 'premarket'
                    else "Intraday" if self.market_period == 'intraday'
                    else "After Hours" if self.market_period == 'aftermarket'
                    else "",
                    now.date().strftime("%m/%d/%Y"),
                    now.strftime("%I:%M %p"))
        return header

    # Override
    def build_report(self):
        logger.debug("Building Gainer Report...")
        report = ""
        report +=  self.build_report_header()
        report +=  self.build_table(self.data[:15])
        return report
    

class VolumeScreener(Screener):
    def __init__(self, channel:discord.channel, market_period:str, unusual_volume:pd.DataFrame):

        # Set column map
        column_map = {'name':'Ticker',
                      'close':'Price',
                      'change':'Change (%)',
                      'relative_volume_10d_calc':'Relative Volume (10 Day)',
                      'volume':'Volume',
                      'average_volume_10d_calc':'Avg Volume (10 Day)',
                      'market_cap_basic':'Market Cap'}


        super().__init__(channel=channel, 
                         screener_type=f"unusual-volume", 
                         market_period=market_period,
                         data = unusual_volume,
                         column_map=column_map)
    
    def format_columns(self):
        super().format_columns()

        # Format all volume columns in df
        volume_cols = self.data.filter(like='Volume').columns.to_list()
        for volume_col in volume_cols:
            self.data[volume_col] = self.data[volume_col].apply(lambda x: self.format_large_num(x))

        # Format Market Cap
        self.data['Market Cap'] = self.data['Market Cap'].apply(lambda x: self.format_large_num(x))

        # Format % change columns
        self.data['Change (%)'] = self.data['Change (%)'].apply(lambda x: "{:.2f}%".format(float(x)) if x is not None else 0.00)

        # Append 'x' to all values in 'Relative Volume (10 Day)' column
        self.data['Relative Volume (10 Day)'] = self.data['Relative Volume (10 Day)'].apply(lambda x: f"{x}x".format(x))
        

    # Override
    def build_report_header(self):
        logger.debug("Building report header...")
        now = datetime.datetime.now(tz=date_utils.timezone())
        header = "### :rotating_light: Unusual Volume {} (Updated {})\n\n".format(
                    now.date().strftime("%m/%d/%Y"),
                    now.strftime("%I:%M %p"))
        return header

    # Override
    def build_report(self):
        logger.debug("Building Volume Mover Report...")
        report = ""
        report += self.build_report_header()
        report += self.build_table(self.data[:12])
        return report

class PopularityScreener(Screener):
    def __init__(self, channel:discord.channel, market_period:str, popular_stocks:pd.DataFrame):
        column_map = {'rank':'Rank',
                      'ticker':'Ticker',
                      'mentions': 'Mentions',
                      'rank_24h_ago':"Rank 24H Ago",
                      'mentions_24h_ago':'Mentions 24H Ago'}


        super().__init__(channel=channel, 
                         screener_type="popular-stocks", 
                         market_period=market_period,
                         data=popular_stocks,
                         column_map=column_map)
        

    def format_columns(self):
        super().format_columns()

    

    # Override
    def build_report_header(self):
        logger.debug("Building report header...")
        now = datetime.datetime.now(tz=date_utils.timezone())
        header = "### :rotating_light: Popular Stocks {} (Updated {})\n\n".format(
                    now.date().strftime("%m/%d/%Y"),
                    date_utils.round_down_nearest_minute(30).astimezone(date_utils.timezone()).strftime("%I:%M %p"))
        return header

    # Override
    def build_report(self):
        logger.debug("Building Gainer Report...")
        report = ""
        report +=  self.build_report_header()
        report +=  self.build_table(df=self.data[:25])
        return report
# TODO
class NewsReport(Report):
    def __init__(self, query, breaking=False, **kwargs):
        logger.debug(f"News report created with query '{query}'")
        self.query = query
        self.breaking=breaking
        self.news_kwargs=kwargs
        super().__init__(None) # no channel for this report

    # Override
    def build_report_header(self):
        logger.debug("Building report header...")
        # Append ticker name, today's date, and external links to message
        header = f"## News articles for '{self.query}'\n"
        return header + "\n"

    def build_news(self):
        logger.debug("Building news...")
        report = ''
        if self.breaking:
            news = sd.News().get_breaking_news(query=self.query, **self.news_kwargs)
        else:
            news = sd.News().get_news(query=self.query, **self.news_kwargs)
        for article in news['articles'][:10]:
            article = f"[{article['title']} - {article['source']['name']} ({sd.News().format_article_date(article['publishedAt'])})](<{article['url']}>)\n"
            if len(report + article) <= 1900:
                report += article
            else:
                break
        return report

    def build_report(self):
        logger.debug("Building News Report...")
        report = ''
        report += self.build_report_header()
        report += self.build_news()
        return report + '\n'  

    # Override
    async def send_report(self, interaction):
        self.message = self.build_report() + "\n\n"
        logger.debug("Sending News Report...")
        await interaction.response.send_message(self.message)

# TODO
class PopularityReport(Report):
    def __init__(self, channel, filter_name='all stock subreddits'):
        self.filter_name = filter_name
        self.filter = sd.ApeWisdom().get_filter(filter_name=self.filter_name)
        self.top_stocks = sd.ApeWisdom().get_top_stocks(filter_name=self.filter_name)
        for i in range(2, 6):
            self.top_stocks = pd.concat([self.top_stocks, sd.ApeWisdom().get_top_stocks(page=i)])
        utils.validate_path(utils.datapaths.attachments_path)
        self.filepath = f"{utils.datapaths.attachments_path}/top-stocks-{datetime.datetime.today().strftime("%m-%d-%Y")}.csv"
        self.top_stocks.to_csv(self.filepath, index=False)
        self.file = discord.File(self.filepath)
        self.buttons = self.Buttons()
        
        super().__init__(channel)

    # Override
    def build_report_header(self):
        logger.debug("Building report header...")
        return f"# Most Popular Stocks ({self.filter_name}) {datetime.datetime.today().strftime("%m/%d/%Y")}\n\n"

    def build_report(self):
        logger.debug("Building Popularity Report...")
        report = ''
        report += self.build_report_header()
        report += self.build_table(self.top_stocks.drop(columns='name')[:15])
        return report

    async def send_report(self, interaction:discord.Interaction = None, visibility:str ="public"):
        logger.debug("Sending Popularity Report...")
        self.message = self.build_report()
        if interaction is not None:
            if visibility == "private":
                message = await interaction.user.send(self.message, files=[self.file], view=self.buttons)
                return message
            else:
                message = await self.channel.send(self.message, files=[self.file], view=self.buttons)
                return message
        else:
            message = await self.channel.send(self.message, files=[self.file], view=self.buttons)
            return message

    # Override
    class Buttons(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=None)
            self.add_item(discord.ui.Button(label="ApeWisdom", style=discord.ButtonStyle.url, url = "https://apewisdom.io/"))

class EarningsSpotlightReport(Report):
    def __init__(self, channel:discord.channel, ticker_info:pd.DataFrame, next_earnings_info:pd.DataFrame,
                 historical_earnings:pd.DataFrame, quote:dict):
        super().__init__(channel=channel,
                         ticker_info=ticker_info,
                         next_earnings_info=next_earnings_info,
                         historical_earnings=historical_earnings,
                         quote=quote)
        self.buttons = StockReport.Buttons(self.ticker)
        

    def build_report_header(self):
        logger.debug("Building report header...")
        return f"# :bulb: Earnings Spotight: {self.ticker}\n\n"

    def build_report(self):
        logger.debug("Building Earnings Spotlight Report...")
        report = ""
        report += self.build_report_header()
        report += self.build_earnings_date()
        report += self.build_ticker_info()
        report += self.build_stats()
        #report += self.build_performance()
        report += self.build_upcoming_earnings_summary()
        report += self.build_recent_earnings()
        return report
    
    async def send_report(self):
        logger.debug("Sending Earnings Spotlight Report...")
        message = await super().send_report(view=self.buttons)
        return message


    # Override
    class Buttons(discord.ui.View):
            def __init__(self, ticker : str):
                super().__init__(timeout=None)
                self.ticker = ticker
                self.add_item(discord.ui.Button(label="Google it", style=discord.ButtonStyle.url, url = "https://www.google.com/search?q={}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="StockInvest", style=discord.ButtonStyle.url, url = "https://stockinvest.us/stock/{}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="FinViz", style=discord.ButtonStyle.url, url = "https://finviz.com/quote.ashx?t={}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="Yahoo! Finance", style=discord.ButtonStyle.url, url = "https://finance.yahoo.com/quote/{}".format(self.ticker)))

                
            @discord.ui.button(label="Generate chart", style=discord.ButtonStyle.primary)
            async def generate_chart(self, interaction:discord.Interaction, button:discord.ui.Button,):
                await interaction.response.send_message("Generate chart!")

            @discord.ui.button(label="Get news", style=discord.ButtonStyle.primary)
            async def get_news(self, interaction:discord.Interaction, button:discord.ui.Button):
                news_report = NewsReport(self.ticker)
                await news_report.send_report(interaction)
                await interaction.response.send_message(f"Fetched news for {self.ticker}!", ephemeral=True)

class WeeklyEarningsReport(Report):
    def __init__(self, channel):
        self.upcoming_earnings = self.get_upcoming_earnings()
        filepath = f"{utils.datapaths.attachments_path}/upcoming_earnings.csv"
        self.upcoming_earnings.to_csv(filepath, index=False)
        self.file = discord.File(filepath)
        super().__init__(channel)
    
    def get_upcoming_earnings(self):
        logger.debug("Retrieving upcoming earnings for this week")
        today = datetime.datetime.today()
        weekly_earnings = pd.DataFrame()
        for i in range(0, 5):
            earnings_today = sd.StockData.Earnings.get_earnings_today(today + datetime.timedelta(days=i))
            if earnings_today.size > 0:
                weekly_earnings = pd.concat([weekly_earnings, earnings_today], ignore_index=True)
            else: 
                pass
        return weekly_earnings

    def build_report_header(self):
        logger.debug("Building report header...")
        return f"# Earnings Releasing the Week of {market_utils.format_date_mdy(datetime.datetime.today())}\n\n"

    def build_watchlist_earnings(self):
        logger.debug("Identifying upcoming earnings for tickers that exist on user watchlists")
        watchlist_tickers = self.bot.stock_data.watchlists.get_tickers_from_all_watchlists(no_personal=False)
        watchlist_earnings = self.upcoming_earnings[self.upcoming_earnings['ticker'].isin(watchlist_tickers)]
        if watchlist_earnings.size > 0:
            message = f" Tickers on your watchlists that report earnings this week:\n"
            message += self.build_table(watchlist_earnings['ticker', 'date', 'time'])
            logger.debug(f"Watchlist tickers reporting earnings this week: {watchlist_earnings['ticker'].to_list()}")
        else:
            message = "No tickers on your watchlists report earnings this week"
            logger.debug("No tickers on watchlists that report earnings this week")
        return message

    def build_report(self):
        logger.debug("Building Upcoming Earnings Report...")
        report = ""
        report += self.build_report_header()
        report += self.build_watchlist_earnings()
        return report

    async def send_report(self):
        logger.debug("Sending Upcoming Earnings Report...")
        message = await super().send_report(files=[self.file])
        return message


        
#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Reports(bot=bot, stock_data=bot.stock_data))
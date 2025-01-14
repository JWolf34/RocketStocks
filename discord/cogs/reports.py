import sys
sys.path.append('../RocketStocks/discord/cogs')
import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
from watchlists import Watchlists
from utils import Utils
import alerts
import datetime
import stockdata as sd
import numpy as np
import pandas as pd
import datetime as dt
import json
import config
from config import market_utils, date_utils
import psycopg2
import asyncio
from table2ascii import table2ascii, PresetStyle
import logging
import rocketstocks
import random

# Logging configuration
logger = logging.getLogger(__name__)  

class Reports(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.send_gainer_reports.start()
        self.send_volume_reports.start()
        self.update_earnings_calendar.start()
        self.send_popularity_reports.start()
        self.post_earnings_spotlight.start()
        self.post_weekly_earnings.start()
        self.reports_channel = self.bot.get_channel(config.discord_utils.reports_channel_id)
        self.screeners_channel = self.bot.get_channel(config.discord_utils.screeners_channel_id)
        

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"{__name__} loaded")

    
    #########
    # Tasks #
    #########

    # Report on most popular stocks across reddit daily
    @tasks.loop(time=datetime.time(hour=22 , minute=0, second=0)) # time in UTC
    #@tasks.loop(hours=24)
    async def send_popularity_reports(self):
        logger.info(f"Sending today's popularity report")
        report = PopularityReport(self.screeners_channel)
        await report.send_report()

        # Update popular-stocks watchlist
        watchlist_id = 'popular-stocks'
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(" ".join(report.top_stocks['ticker'].tolist()[:30]))
        if not sd.Watchlists().validate_watchlist(watchlist_id):
            sd.Watchlists().create_watchlist(watchlist_id=watchlist_id, tickers=tickers, systemGenerated=True)
        else:
            sd.Watchlists().update_watchlist(watchlist_id=watchlist_id, tickers=tickers)

        # Update DB table
        logger.debug(f"Updating popular-stocks table with latest tickers")
        fields = ['date', 'ticker', 'rank', 'mentions', 'upvotes']
        popular_stocks = report.top_stocks.drop(columns=['rank_24h_ago',
                                                       'mentions_24h_ago',
                                                       'name'])
        popular_stocks['date'] = datetime.datetime.today().strftime("%Y-%m-%d")
        popular_stocks = popular_stocks[fields]
        sd.Postgres().insert(table='popular_stocks', fields=fields, values=[tuple(row) for row in popular_stocks.values])


    # Generate and send premarket gainer reports to the reports channel
    @tasks.loop(minutes=5)
    async def send_gainer_reports(self):
        if (market_utils.market_open_today() and (market_utils.in_extended_hours() or market_utils.in_intraday())):
            report = GainerReport(self.screeners_channel)
            logger.info(f"Sending {report.market_period} gainers report")
            await report.send_report()
            await self.bot.get_cog("Alerts").update_alert_tickers(key='gainers',
                                                                  tickers = report.get_tickers())
        else:
            # Not a weekday - do not post gainer reports
            pass

    # Generate and send premarket gainer reports to the reports channel
    @tasks.loop(minutes=5)
    async def send_volume_reports(self):
        now = datetime.datetime.now()
        #if (market_utils.market_open_today() and market_utils.in_intraday()):
        if (market_utils.market_open_today() and (market_utils.in_extended_hours() or market_utils.in_intraday())):
            report = VolumeReport(self.screeners_channel)
            logger.info(f"Sending {report.market_period} unusual volume report")
            await report.send_report()
            await self.bot.get_cog("Alerts").update_alert_tickers(key='volume_movers',
                                                                  tickers = report.get_tickers())
        else:
            # Not a weekday - do not post gainer reports
            pass

    # Start posting report at next 0 or 5 minute interval
    @send_gainer_reports.before_loop
    @send_volume_reports.before_loop
    async def reports_before_loop(self):
        sleep_time = config.date_utils.seconds_until_5m_interval()
        logger.info(f"Reports will begin posting in {sleep_time} seconds")
        await asyncio.sleep(sleep_time)
            

    @tasks.loop(time=datetime.time(hour=12, minute=30, second=0)) # time in UTC
    #@tasks.loop(minutes=5)
    async def post_earnings_spotlight(self):
        if market_utils.market_open_today():
            report = EarningsSpotlightReport(self.reports_channel)
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
        guild = self.bot.get_guild(config.discord_utils.guild_id)
        tickers = sd.Watchlists().get_tickers_from_all_watchlists(no_personal=False) # Get all tickers except from system-generated watchlists
        for ticker in tickers:
            earnings_info = sd.StockData.Earnings.get_next_earnings_info(ticker).to_dict(orient='list')
            if len(earnings_info) > 0:
                event_exists = False
                name = f"{ticker} Earnings"
                curr_events = await guild.fetch_scheduled_events()
                if curr_events:
                    for event in curr_events:
                        if event.name == name:
                            event_exists = True
                            break
                
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
                        channel = self.bot.get_channel(config.discord_utils.alerts_channel_id)
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
    @app_commands.autocomplete(watchlist=Watchlists.watchlist_options,)
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
        
        if watchlist_id not in sd.Watchlists().get_watchlists():
            await interaction.followup.send(f"Watchlist '{watchlist_id}' does not exist")
            logger.debug(f"Watchlist '{watchlist}' does not exist. No reports generated. ")
            return

        tickers = sd.Watchlists().get_tickers_from_watchlist(watchlist_id)
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
    def __init__(self, channel):
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
        self.channel = channel

    async def init(self):
        pass

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
    async def build_ticker_info(self):
        logger.debug("Building ticker info...")
        message = "## Ticker Info\n"
        ticker_data = sd.StockData.get_ticker_info(self.ticker)
        if ticker_data is not None:
            message += f"**Name:** {ticker_data[1]}\n"
            message += f"**Sector:** {ticker_data[6] if ticker_data[6] else "N/A"}\n"
            message += f"**Industry:** {ticker_data[5] if ticker_data[5] else "N/A"}\n" 
            message += f"**Country:** {ticker_data[3] if ticker_data[3] else "N/A"}\n"
            message += f"**Exchange:** {self.quote['reference']['exchangeName']}\n"
            return message 
        else:
            logger.warning("While generating report summary, ticker_data was 'None'. Ticker likely does not exist in database")
            return message + f"Unable to get info for ticker {self.ticker}\n"
        
    async def build_recent_SEC_filings(self):
        logger.debug("Building latest SEC filings...")
        message = "## Recent SEC Filings\n\n"
        filings = sd.SEC().get_recent_filings(ticker=self.ticker)
        for index, filing in filings[:5].iterrows():
            message += f"[Form {filing['form']} - {filing['filingDate']}]({sd.SEC().get_link_to_filing(ticker=self.ticker, filing=filing)})\n"
        return message

    async def build_todays_sec_filings(self):
        logger.debug("Building today's SEC filings...")
        message = "## Today's SEC Filings\n\n"
        filings = sd.SEC().get_filings_from_today(ticker=self.ticker)
        for index, filing in filings.iterrows():
            message += f"[Form {filing['form']} - {filing['filingDate']}]({sd.SEC().get_link_to_filing(ticker=self.ticker, filing=filing)})\n"
        return message

    def build_table(self, df:pd.DataFrame, style='double_thin_compact'):
        logger.debug(f"Building table of shape {df.shape} with headers {df.columns.to_list()} and of style '{style}'")
        table_style = self.table_styles.get(style, PresetStyle.double_thin_compact)
        table = table2ascii(
            header = df.columns.tolist(),
            body = df.values.tolist(),
            style=table_style 
        )
        return "```\n" + table + "\n```"

    async def build_earnings_date(self):
        logger.debug(f"Building earnings date...")
        earnings_info = sd.StockData.Earnings.get_next_earnings_info(self.ticker)
        message = f"{self.ticker} reports earnings on "
        message += f"{earnings_info['date'].iloc[0].strftime("%m/%d/%Y")}, "
        earnings_time = earnings_info['time'].iloc[0]
        if "pre-market" in earnings_time:
            message += "before market open"
        elif "after-hours" in earnings_time:
            message += "after market close"
        else:
            message += "time not specified"

        return message + "\n"

    async def build_upcoming_earnings_summary(self):
        logger.debug("Building upcoming earnings summary...")
        earnings_info = sd.StockData.Earnings.get_next_earnings_info(self.ticker)
        message = "## Earnings Summary\n\n"
        message += f"**Date:** {earnings_info['date'].iloc[0]}\n"
        message += "**Time:** {}\n".format("Premarket" if "pre-market" in earnings_info['time'].iloc[0]
                                else "After hours" if "after-hours" in earnings_info['time'].iloc[0]
                                else "Not supplied")
        message += f"**Fiscal Quarter:** {earnings_info['fiscalquarterending'].iloc[0]}\n"
        message += f"**EPS Forecast: ** {earnings_info['epsforecast'].iloc[0] if len(earnings_info['epsforecast'].iloc[0]) > 0 else "N/A"}\n"
        message += f"**No. of Estimates:** {earnings_info['noofests'].iloc[0]}\n"
        message += f"**Last Year Report Date:** {earnings_info['lastyearrptdt'].iloc[0]}\n"
        message += f"**Last Year EPS:** {earnings_info['lastyeareps'].iloc[0]}\n"
        return message + "\n\n"

    async def build_recent_earnings(self):
        logger.debug("Building recent earnings...")
        message = "## Earnings\n"
        message += f"**Next earnings date:** {sd.StockData.Earnings.get_next_earnings_date(self.ticker)}\n"
        column_map = {'date':'Date Reported', 
                      'eps':'EPS',
                      'surprise':'Surprise',
                      'epsforecast':'Estimate',
                      'fiscalquarterending':'Quarter'}
        recent_earnings = sd.StockData.Earnings.get_historical_earnings(self.ticker).tail(4)
        recent_earnings = recent_earnings.drop(columns='ticker')
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
        message += f"**Market Cap:** {self.format_large_num(sd.StockData.get_market_cap(self.ticker))}\n"
        message += f"**52 Week High:** {self.quote['quote']['52WeekHigh']}\n"
        message += f"**52 Week Low:** {self.quote['quote']['52WeekLow']}\n"
        message += f"**Average Volume 10D:** {self.format_large_num(self.quote['fundamental']['avg10DaysVolume'])}\n"
        message += f"**Average Volume 1Y:** {self.format_large_num(self.quote['fundamental']['avg1YearVolume'])}\n"
        message += f"**P/E Ratio:** {"{:.2f}".format(self.quote['fundamental']['peRatio'])}\n"
        return message

    async def build_popularity(self):
        logger.debug("Building popularity...")
        message = "## Popularity\n"
        
        try:
            todays_top_stocks = sd.ApeWisdom().get_top_stocks()
            todays_rank = todays_top_stocks[todays_top_stocks['ticker'] == self.ticker]['rank'].iloc[0]
        except IndexError as e:
            message += "No popularity records available"
            return message + "\n"    
        popularity = sd.StockData.get_historical_popularity(self.ticker)
        if popularity is None:
            message += "No popularity records available"
            return message + "\n"
        else:
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

    async def build_report(self):
        report = ''
        report += self.build_report_header()
        return report   

    async def send_report(self, interaction:discord.Interaction = None, visibility:str = "public", files=None, view=None):
        await self.init()
        self.message =  await self.build_report() + "\n\n"
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

class StockReport(Report):
    
    def __init__(self, ticker : str, channel):
        self.ticker = ticker
        self.data =  sd.StockData.fetch_daily_price_history(self.ticker)
        self.buttons = self.Buttons(self.ticker)
        super().__init__(channel)

    async def init(self):
        self.quote = await sd.Schwab().get_quote(self.ticker)
        
    # Override
    async def build_report(self):
        logger.debug("Building Stock Report...")
        report = ''
        report += self.build_report_header()
        report += await self.build_ticker_info()
        report += self.build_daily_summary()
        report += self.build_performance()
        report += self.build_stats()
        report += await self.build_popularity()
        report += await self.build_recent_earnings()
        report += await self.build_recent_SEC_filings()
        
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

class GainerReport(Report):
    def __init__(self, channel):
        super().__init__(channel)

    async def init(self):
        self.gainers = None
        self.gainers_formatted = pd.DataFrame()
        self.market_period = market_utils.get_market_period()
        if self.market_period == 'premarket':
            self.gainers = sd.TradingView.get_premarket_gainers_by_market_cap(1000000)
        elif self.market_period == 'intraday':
            self.gainers = sd.TradingView.get_intraday_gainers_by_market_cap(1000000)
        elif self.market_period == 'afterhours':
            self.gainers = sd.TradingView.get_postmarket_gainers_by_market_cap(1000000)
        if self.gainers.size > 0:
            self.update_gainer_watchlist()
            self.gainers_formatted = self.format_df_for_table()
        else:
            self.gainers_formatted.columns = self.gainers.columns.to_list()

    def get_tickers(self):
        return self.gainers['Ticker'].to_list()

    def format_df_for_table(self):
        logger.debug("Formatting gainers dataframe for table viewing")
        gainers = self.gainers.copy()
        change_columns = ['Premarket Change', '% Change', 'After Hours Change']
        volume_columns = ['Premarket Volume', "After Hours Volume"]
        gainers['Volume'] = gainers['Volume'].apply(lambda x: self.format_large_num(x))
        gainers['Market Cap'] = gainers['Market Cap'].apply(lambda x: self.format_large_num(x))
        for column in change_columns:
            if column in gainers.columns:
                gainers[column] = gainers[column].apply(lambda x: "{:.2f}%".format(float(x)) if x is not None else 0.00)
        for column in volume_columns:
            if column in gainers.columns:
                gainers[column] = gainers[column].apply(lambda x: self.format_large_num(x))
        return gainers

    def update_gainer_watchlist(self):
        watchlist_id = f"{market_utils.get_market_period()}-gainers"
        watchlist_tickers = self.get_tickers()
        watchlist_tickers = watchlist_tickers[:15]

        if not sd.Watchlists().validate_watchlist(watchlist_id):
            sd.Watchlists().create_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers, systemGenerated=True)
        else:
            sd.Watchlists().update_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers)

    # Override
    async def build_report_header(self):
        logger.debug("Building report header...")
        today = datetime.datetime.today()
        header = "### :rotating_light: {} Gainers {} (Market Cap > $100M) (Updated {})\n\n".format(
                    "Pre-market" if self.market_period == 'premarket'
                    else "Intraday" if self.market_period == 'intraday'
                    else "After Hours" if self.market_period == 'afterhours'
                    else "",
                    today.strftime("%m/%d/%Y"),
                    today.strftime("%-I:%M %p"))
        return header

    # Override
    async def build_report(self):
        logger.debug("Building Gainer Report...")
        report = ""
        report += await self.build_report_header()
        report += await self.build_table(self.gainers_formatted[:15])
        return report

    # Override
    async def send_report(self):
        await self.init()
        self.message =  await self.build_report() + "\n\n"

        logger.debug("Sending Gainer Report...")
        today = datetime.datetime.today()
        message_id = config.discord_utils.get_gainer_message_id()
        try:
            curr_message = await self.channel.fetch_message(message_id)
            if curr_message.created_at.date() < today.date():
                message = await self.channel.send(self.message)
                config.discord_utils.update_gainer_message_id( message.id)
                return message
            else:
                logger.debug(f"{self.market_period.upper()} Gainer Report already exists today. Updating... ")
                await curr_message.edit(content=self.message)

        except discord.errors.NotFound as e:
            message = await self.channel.send(self.message)
            config.discord_utils.update_gainer_message_id(message.id)
            return message

class VolumeReport(Report):
    def __init__(self, channel):
        
        super().__init__(channel)

    async def init(self):
        self.volume_movers = sd.TradingView.get_unusual_volume_movers()
        self.volume_movers_formatted = self.format_df_for_table()
        self.market_period = market_utils.get_market_period()
        if self.volume_movers.size > 0:
            self.update_unusual_volume_watchlist()

    def get_tickers(self):
        return self.volume_movers['Ticker'].to_list()

    def format_df_for_table(self):
        logger.debug("Formatting volume movers dataframe for table viewing")
        movers = self.volume_movers.copy()
        movers['Volume'] = movers['Volume'].apply(lambda x: self.format_large_num(x))
        movers['Relative Volume'] = movers['Relative Volume'].apply(lambda x: "{:.2f}x".format(x))
        movers['Average Volume (10 Day)'] = movers['Average Volume (10 Day)'].apply(lambda x: self.format_large_num(x))
        movers['Market Cap'] = movers['Market Cap'].apply(lambda x: self.format_large_num(x))
        movers['% Change'] = movers['% Change'].apply(lambda x: "{:.2f}%".format(x))
        return movers

    # Override
    async def build_report_header(self):
        logger.debug("Building report header...")
        today = datetime.datetime.today()
        header = "### :rotating_light: Unusual Volume {} (Volume > 1M)(Updated {})\n\n".format(
                    today.strftime("%m/%d/%Y"),
                    today.strftime("%-I:%M %p"))
        return header

    # Override
    async def build_report(self):
        logger.debug("Building Volume Mover Report...")
        report = ""
        report += await self.build_report_header()
        report += await self.build_table(self.volume_movers_formatted[:10])
        return report

    # Override
    async def send_report(self):
        await self.init()
        self.message =  await self.build_report() + "\n\n"

        logger.debug("Sending Volume Mover Report...")
        today = datetime.datetime.today()
        market_period = market_utils.get_market_period()
        message_id = config.discord_utils.get_volume_message_id()
        try:
            curr_message = await self.channel.fetch_message(message_id)
            if curr_message.created_at.date() < today.date():
                message = await self.channel.send(self.message)
                config.discord_utils.update_volume_message_id( message.id)
                return message
            else:
                logger.debug(f"Volume Mover Report already exists today. Updating... ")
                await curr_message.edit(content=self.message)

        except discord.errors.NotFound as e:
            message = await self.channel.send(self.message)
            config.discord_utils.update_volume_message_id(message.id)
            return message

    async def get_volume_movers(self):
        headers = []
        rows = []
        unusual_volume = sd.TradingView.get_unusual_volume_movers()
        headers = ["Ticker", "Close", "% Change", "Volume", "Relative Volume", "Market Cap"]
        for index, row in unusual_volume.iterrows():
            ticker = row.iloc[1]
            rows.append([ticker, 
                        "{}".format(float('{:.2f}'.format(row.close))), 
                        "{:.2f}%".format(row.change),
                        self.format_large_num(row.volume), 
                        "{:.2f}%".format(row.relative_volume_10d_calc),
                        self.format_large_num(row.market_cap_basic)]) 
            

        return pd.DataFrame(rows, columns=headers)

    def update_unusual_volume_watchlist(self):
        watchlist_id = "unusual-volume"
        watchlist_tickers = self.get_tickers()

        watchlist_tickers, invalid_tickers = sd.StockData.get_valid_tickers(' '.join(watchlist_tickers))
        watchlist_tickers = watchlist_tickers[:15]

        if not sd.Watchlists().validate_watchlist(watchlist_id):
            sd.Watchlists().create_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers, systemGenerated=True)
        else:
            sd.Watchlists().update_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers)

class NewsReport(Report):
    def __init__(self, query, breaking=False, **kwargs):
        logger.debug(f"News report created with query '{query}'")
        self.query = query
        self.breaking=breaking
        self.news_kwargs=kwargs
        super().__init__(None) # no channel for this report

    # Override
    async def build_report_header(self):
        logger.debug("Building report header...")
        # Append ticker name, today's date, and external links to message
        header = f"## News articles for '{self.query}'\n"
        return header + "\n"

    async def build_news(self):
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

    async def build_report(self):
        logger.debug("Building News Report...")
        report = ''
        report += self.build_report_header()
        report += self.build_news()
        return report + '\n'  

    # Override
    async def send_report(self, interaction):
        self.message =  await self.build_report() + "\n\n"
        logger.debug("Sending News Report...")
        await interaction.response.send_message(self.message)

class PopularityReport(Report):
    def __init__(self, channel, filter_name='all stock subreddits'):
        self.filter_name = filter_name
        self.filter = sd.ApeWisdom().get_filter(filter_name=self.filter_name)
        self.top_stocks = sd.ApeWisdom().get_top_stocks(filter_name=self.filter_name)
        for i in range(2, 6):
            self.top_stocks = pd.concat([self.top_stocks, sd.ApeWisdom().get_top_stocks(page=i)])
        sd.validate_path(config.datapaths.attachments_path)
        self.filepath = f"{config.datapaths.attachments_path}/top-stocks-{datetime.datetime.today().strftime("%m-%d-%Y")}.csv"
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
    def __init__(self, channel):
        earnings_today = sd.StockData.Earnings.get_earnings_today(datetime.datetime.today())
        self.ticker = earnings_today['ticker'].iloc[random.randint(0, earnings_today['ticker'].size)]
        self.buttons = StockReport.Buttons(self.ticker)
        super().__init__(channel)

    def build_report_header(self):
        logger.debug("Building report header...")
        return f"# :bulb: Earnings Spotight: {self.ticker}\n\n"

    def build_report(self):
        logger.debug("Building Earnings Spotlight Report...")
        report = ""
        report += self.build_report_header()
        report += self.build_earnings_date()
        report += self.build_ticker_info()
        report += self.build_upcoming_earnings_summary()
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
        filepath = f"{config.datapaths.attachments_path}/upcoming_earnings.csv"
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
        watchlist_tickers = sd.Watchlists().get_tickers_from_all_watchlists(no_personal=False)
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
    await bot.add_cog(Reports(bot))
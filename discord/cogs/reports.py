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
from config import utils
import psycopg2
import asyncio
from table2ascii import table2ascii
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
        self.reports_channel = self.bot.get_channel(config.get_reports_channel_id())
        self.screeners_channel = self.bot.get_channel(config.get_screeners_channel_id())
        

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
        report = PopularityReport(self.screeners_channel)
        await report.send_report()

        # Update popular-stocks watchlist
        watchlist_id = 'popular-stocks'
        tickers, invalid_tickers = sd.StockData.get_list_from_tickers(" ".join(report.top_stocks['ticker'].tolist()[:30]))
        if not sd.Watchlists().validate_watchlist(watchlist_id):
            sd.Watchlists().create_watchlist(watchlist_id=watchlist_id, tickers=tickers, systemGenerated=True)
        else:
            sd.Watchlists().update_watchlist(watchlist_id=watchlist_id, tickers=tickers)

        # Update DB table
        fields = ['date', 'ticker', 'rank', 'mentions', 'upvotes']
        popular_stocks = report.top_stocks.drop(columns=['rank_24h_ago',
                                                       'mentions_24h_ago',
                                                       'name'])
        popular_stocks['date'] = datetime.datetime.today().strftime("%Y-%m-%d")
        popular_stocks = popular_stocks[fields]
        sd.Postgres().insert(table='popularstocks', fields=fields, values=[tuple(row) for row in popular_stocks.values])


    # Generate and send premarket gainer reports to the reports channel
    @tasks.loop(minutes=5)
    async def send_gainer_reports(self):
        
        if (utils.market_open_today() and (utils.in_extended_hours() or utils.in_intraday())):
            report = GainerReport(self.screeners_channel)
            await report.send_report()
            await self.bot.get_cog("Alerts").send_earnings_movers(report.gainers)
            #await self.bot.get_cog("Alerts").send_sec_filing_movers(report.gainers)
            await self.bot.get_cog("Alerts").send_watchlist_movers(report.gainers)
        else:
            # Not a weekday - do not post gainer reports
            pass


    @send_gainer_reports.before_loop
    async def before_send_gainer_reports(self):
        # Start posting report at next 0 or 5 minute interval
        now = datetime.datetime.now().astimezone()
        if now.minute % 5 == 0:
            return 0
        minutes_by_five = now.minute // 5
        # get the difference in times
        diff = (minutes_by_five + 1) * 5 - now.minute
        future = now + datetime.timedelta(minutes=diff)
        await asyncio.sleep((future-now).total_seconds())

    # Generate and send premarket gainer reports to the reports channel
    @tasks.loop(minutes=5)
    async def send_volume_reports(self):
        now = datetime.datetime.now()
        if (utils.market_open_today() and utils.in_intraday()):
            report = VolumeReport(self.screeners_channel)
            await report.send_report()
            await self.bot.get_cog("Alerts").send_unusual_volume_movers(report.volume_movers)
        else:
            # Not a weekday - do not post gainer reports
            pass
            
    @send_volume_reports.before_loop
    async def before_send_volume_reports(self):
        # Start posting report at next 0 or 5 minute interval
        now = datetime.datetime.now().astimezone()
        if now.minute % 5 == 0:
            return 0
        minutes_by_five = now.minute // 5
        # get the difference in times
        diff = (minutes_by_five + 1) * 5 - now.minute
        future = now + datetime.timedelta(minutes=diff)
        await asyncio.sleep((future-now).total_seconds())

    @tasks.loop(time=datetime.time(hour=12, minute=30, second=0)) # time in UTC
    #@tasks.loop(minutes=5)
    async def post_earnings_spotlight(self):
        if utils.market_open_today():
            report = EarningsSpotlightReport(self.reports_channel)
            await report.send_report()


    @tasks.loop(time=datetime.time(hour=12, minute=0, second=0)) # time in UTC
    #@tasks.loop(minutes=5)
    async def post_weekly_earnings(self):
        today = datetime.datetime.today()
        if datetime.datetime.today().weekday() == 0:
            report = WeeklyEarningsReport(self.reports_channel)
            await report.send_report()


    # Create earnings events on calendar for all stocks on watchlists
    @tasks.loop(time=datetime.time(hour=6, minute=0, second=0)) # time in UTC
    #@tasks.loop(minutes=5)
    async def update_earnings_calendar(self):
        logger.debug("Creating events for upcoming earnings dates")
        guild = self.bot.get_guild(config.get_discord_guild_id())
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
                        channel = self.bot.get_channel(config.get_alerts_channel_id())
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
                        pass
                else:
                    # Event already exists
                    pass
        logger.debug("Completed updating earnings calendar")

    @app_commands.command(name = "run-reports", description= "Post analysis of a given watchlist (use /fetch-reports for individual or non-watchlist stocks)",)
    @app_commands.describe(watchlist = "Which watchlist to fetch reports for")
    @app_commands.autocomplete(watchlist=Watchlists.watchlist_options,)
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])
    async def runreports(self, interaction: discord.Interaction, watchlist: str, visibility: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/run-reports function called by user {}".format(interaction.user.name))
        logger.debug("Selected watchlist is '{}'".format(watchlist))
        
        message = ""
        watchlist_id = watchlist

        # Populate tickers based on value of watchlist
        if watchlist == 'personal':
            watchlist_id = str(interaction.user.id)
        tickers = sd.Watchlists().get_tickers_from_watchlist(watchlist_id)

        if len(tickers) == 0:
            # Empty watchlist
            logger.warning("Selected watchlist '{}' is empty".format(watchlist))
            message = "No tickers on the watchlist. Use /addticker to build a watchlist."
            await interaction.followup.send(message, ephemeral=True)
        else:
            # Send reports
            logger.info("Running reports on tickers {}".format(tickers))
            message = None
            for ticker in tickers:
                logger.debug("Processing ticker {}".format(ticker))
                report = StockReport(ticker, self.reports_channel)
                message = await report.send_report(interaction, visibility.value)
                logger.info("Report posted for ticker {}".format(ticker))
            logger.info("Reports have been posted")

            # Follow-up message
            follow_up = f"Posted reports for tickers [{", ".join(tickers)}]({message.jump_url})!"
            await interaction.followup.send(follow_up, ephemeral=True)


    @app_commands.command(name = "fetch-reports", description= "Fetch analysis reports of the specified tickers (use /run-reports to analyze a watchlist)",)
    @app_commands.describe(tickers = "Tickers to post reports for (separated by spaces)")
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])        
    async def fetchreports(self, interaction: discord.interactions, tickers: str, visibility: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/fetch-reports function called by user {}".format(interaction.user.name))
    
        # Validate each ticker in the list is valid
        tickers, invalid_tickers = sd.StockData.get_list_from_tickers(tickers)
        logger.debug("Validated tickers {} | Invalid tickers: {}".format(tickers, invalid_tickers))
        message = None
        logger.info("Fetching reports for tickers {}".format(tickers))
        for ticker in tickers:
            logger.debug("Processing ticker {}".format(ticker))
            report = StockReport(ticker, self.reports_channel)
            message = await report.send_report(interaction, visibility.value)
            logger.info("Report posted for ticker {}".format(ticker))

        # Follow-up message
        follow_up = ""
        if message is not None: # Message was generated
            follow_up = f"Posted reports for tickers [{", ".join(tickers)}]({message.jump_url})!"
            if len(invalid_tickers) > 0: # More than one invalid ticke input
                follow_up += f" Invalid tickers: {", ".join(invalid_tickers)}"
        if len(tickers) == 0: # No valid tickers input
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
        report = PopularityReport(self.reports_channel, filter_name=source)
        message = await report.send_report(interaction=interaction, visibility=visibility.value)

        # Follow-up message
        follow_up = f"[Posted popularity reports!]({message.jump_url})"
        await interaction.followup.send(follow_up, ephemeral=True)

##################
# Report Classes #
##################

class Report(object):
    def __init__(self, channel):
        self.message = self.build_report() + "\n\n"
        self.channel = channel

    ############################
    # Report Builder Functions #
    ############################

    # Report Header
    def build_report_header(self):
        
        # Append ticker name, today's date, and external links to message
        header = "# " + self.ticker + " Report " + dt.date.today().strftime("%m/%d/%Y") + "\n"
        return header + "\n"

    # Ticker Info
    def build_ticker_info(self):

        message = "## Ticker Info\n"
        ticker_data = sd.StockData.get_ticker_info(self.ticker)
        if ticker_data is not None:
            message += f"**Name:** {ticker_data[1]}\n"
            message += f"**Sector:** {ticker_data[6] if ticker_data[6] else "N/A"}\n"
            message += f"**Industry:** {ticker_data[5] if ticker_data[5] else "N/A"}\n"
            message += f"**Market Cap:** ${self.format_large_num(ticker_data[2]) if ticker_data[2] else "N/A"}\n" 
            message += f"**Country:** {ticker_data[3] if ticker_data[3] else "N/A"}\n"
            message += f"**Next earnings date:** {sd.StockData.Earnings.get_next_earnings_date(self.ticker)}"
            return message + "\n"
        else:
            logger.warning("While generating report summary, ticker_data was 'None'. Ticker likely does not exist in database")
            return message + f"Unable to get info for ticker {self.ticker}\n"

    # Daily Summary
    def build_daily_summary(self):
        # Append day's summary to message
        summary = sd.get_days_summary(self.data)

        message = "## Summary \n| "
        for col in summary.keys():
            message += "**{}:** {}".format(col, f"{summary[col]:,.2f}")
            message += " | "

        return message + "\n"
        
    def build_recent_SEC_filings(self):
        message = "## Recent SEC Filings\n\n"
        filings = sd.SEC().get_recent_filings(ticker=self.ticker)
        for index, filing in filings[:5].iterrows():
            message += f"[Form {filing['form']} - {filing['filingDate']}]({sd.SEC().get_link_to_filing(ticker=self.ticker, filing=filing)})\n"
        return message

    def build_todays_sec_filings(self):
        message = "## Today's SEC Filings\n\n"
        filings = sd.SEC().get_filings_from_today(ticker=self.ticker)
        for index, filing in filings.iterrows():
            message += f"[Form {filing['form']} - {filing['filingDate']}]({sd.SEC().get_link_to_filing(ticker=self.ticker, filing=filing)})\n"
        return message

    def build_table(self, df:pd.DataFrame):
        table = table2ascii(
            header = df.columns.tolist(),
            body = df.values.tolist(), 
        )
        return "```\n" + table + "\n```"

    def build_earnings_date(self):
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

    def build_upcoming_earnings_summary(self):
        earnings_info = sd.StockData.Earnings.get_next_earnings_info(self.ticker)
        message = "### Earnings Summary\n\n"
        message += f"**Date:** {earnings_info['date'].iloc[0]}\n"
        message += "**Time:** {}\n".format("Premarket" if "pre-market" in earnings_info['time'].iloc[0]
                                else "After hours" if "after-hours" in earnings_info['time'].iloc[0]
                                else "Not supplied")
        message += f"**Fiscal Quarter:** {earnings_info['fiscalquarterending'].iloc[0]}\n"
        message += f"**EPS Forecast: ** {earnings_info['epsforecast'].iloc[0] if len(earnings_info['epsforecast'].iloc[0]) > 0 else "N/A"}\n"
        message += f"**\# of Estimates:** {earnings_info['noofests'].iloc[0]}\n"
        message += f"**Last Year Report Date:** {earnings_info['lastyearrptdt'].iloc[0]}\n"
        message += f"**Last Year EPS:** {earnings_info['lastyeareps'].iloc[0]}\n"
        return message + "\n\n"
                    
                

    def build_report(self):
        report = ''
        report += self.build_report_header()
        return report   

    async def send_report(self, interaction:discord.Interaction = None, visibility:str = "public", files=None, view=None):
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
        number = float('{:.3g}'.format(float(number)))
        magnitude = 0
        while abs(number) >= 1000:
            magnitude += 1
            number /= 1000.0
        return '{}{}'.format('{:f}'.format(number).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])
    
    # Tool to determine percentage change
    def percent_change(self, current, previous):
        return float('{:.3g}'.format(((current - previous) / previous) * 100.0))

    def percent_change_formatted(self, current, previous):
        change = float('{:.3g}'.format(((current - previous) / previous) * 100.0))
        return "{} {}%".format(":arrow_down_small:" if change > 0
                                else ":arrow_up_small:",
                                str(abs(change)))
    
    class Buttons(discord.ui.View):
            def __init__(self):
                super().__init__()

class StockReport(Report):
    
    def __init__(self, ticker : str, channel):
        self.ticker = ticker
        self.data =  sd.fetch_daily_data(self.ticker)
        self.buttons = self.Buttons(self.ticker)
        super().__init__(channel)
        
    # Override
    def build_report(self):
        report = ''
        report += self.build_report_header()
        report += self.build_ticker_info()
        #report += self.build_daily_summary()
        report += self.build_recent_SEC_filings()
        
        return report

    # Override
    async def send_report(self, interaction: discord.Interaction, visibility:str):
        message = await super().send_report(interaction=interaction, visibility=visibility, view=self.buttons)
        return message

    # Override
    class Buttons(discord.ui.View):
            def __init__(self, ticker : str):
                super().__init__()
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
        self.gainers = None
        self.market_period = utils.get_market_period()
        if utils.in_premarket():
            self.gainers = sd.TradingView.get_premarket_gainers_by_market_cap(1000000)
        elif utils.in_intraday():
            self.gainers = sd.TradingView.get_intraday_gainers_by_market_cap(1000000)
        elif utils.in_afterhours():
            self.gainers = sd.TradingView.get_postmarket_gainers_by_market_cap(1000000)
        if self.gainers.size > 0:
            self.update_gainer_watchlist()

        self.gainers_formatted = self.format_df_for_table()
        super().__init__(channel)

        

    def format_df_for_table(self):
        gainers = self.gainers.copy()
        change_columns = ['Premarket Change', '% Change', 'After Hours Change']
        volume_columns = ['Premarket Volume', "After Hours Volume"]
        gainers['Volume'] = gainers['Volume'].apply(lambda x: self.format_large_num(x))
        gainers['Market Cap'] = gainers['Market Cap'].apply(lambda x: self.format_large_num(x))
        for column in change_columns:
            if column in gainers.columns:
                gainers[column] = gainers[column].apply(lambda x: "{:.2f}%".format(float(x)))
        for column in volume_columns:
            if column in gainers.columns:
                gainers[column] = gainers[column].apply(lambda x: self.format_large_num(x))
        return gainers

        

        return pd.DataFrame(rows, columns=headers)
    def update_gainer_watchlist(self):
        watchlist_id = f"{utils.get_market_period()}-gainers"
        watchlist_tickers = self.gainers['Ticker'].to_list()
        watchlist_tickers = watchlist_tickers[:15]

        if not sd.Watchlists().validate_watchlist(watchlist_id):
            sd.Watchlists().create_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers, systemGenerated=True)
        else:
            sd.Watchlists().update_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers)

    # Override
    def build_report_header(self):
        today = datetime.datetime.today()
        header = "### :rotating_light: {} Gainers {} (Market Cap > $100M) (Updated {})\n\n".format(
                    "Pre-market" if utils.in_premarket()
                    else "Intraday" if utils.in_intraday()
                    else "After Hours" if utils.in_afterhours()
                    else "",
                    today.strftime("%m/%d/%Y"),
                    today.strftime("%-I:%M %p"))
        return header

    # Override
    def build_report(self):
        report = ""
        report += self.build_report_header()
        report += self.build_table(self.gainers_formatted[:15])
        return report

    # Override
    async def send_report(self):
        today = datetime.datetime.today()
        market_period = utils.get_market_period()
        message_id = config.get_gainer_message_id()
        try:
            curr_message = await self.channel.fetch_message(message_id)
            if curr_message.created_at.date() < today.date():
                message = await self.channel.send(self.message)
                config.update_gainer_message_id( message.id)
                return message
            else:
                await curr_message.edit(content=self.message)

        except discord.errors.NotFound as e:
            message = await self.channel.send(self.message)
            config.update_gainer_message_id(message.id)
            return message

class VolumeReport(Report):
    def __init__(self, channel):
        self.volume_movers = sd.TradingView.get_unusual_volume_movers()
        self.volume_movers_formatted = self.format_df_for_table()
        if self.volume_movers.size > 0:
            self.update_unusual_volume_watchlist()
        super().__init__(channel)

    def format_df_for_table(self):
        movers = self.volume_movers.copy()
        movers['Volume'] = movers['Volume'].apply(lambda x: self.format_large_num(x))
        movers['Relative Volume'] = movers['Relative Volume'].apply(lambda x: "{:.2f}x".format(x))
        movers['Average Volume (10 Day)'] = movers['Average Volume (10 Day)'].apply(lambda x: self.format_large_num(x))
        movers['Market Cap'] = movers['Market Cap'].apply(lambda x: self.format_large_num(x))
        movers['% Change'] = movers['% Change'].apply(lambda x: "{:.2f}%".format(x))
        return movers

    # Override
    def build_report_header(self):
        today = datetime.datetime.today()
        header = "### :rotating_light: Unusual Volume {} (Volume > 1M)(Updated {})\n\n".format(
                    today.strftime("%m/%d/%Y"),
                    today.strftime("%-I:%M %p"))
        return header

    # Override
    def build_report(self):
        report = ""
        report += self.build_report_header()
        report += self.build_table(self.volume_movers_formatted[:10])
        return report

    # Override
    async def send_report(self):
        if True: #utils.in_premarket() or utils.in_intraday() or utils.in_afterhours():
            today = datetime.datetime.today()
            market_period = utils.get_market_period()
            message_id = config.get_volume_message_id()
            try:
                curr_message = await self.channel.fetch_message(message_id)
                if curr_message.created_at.date() < today.date():
                    message = await self.channel.send(self.message)
                    config.update_volume_message_id( message.id)
                    return message
                else:
                    await curr_message.edit(content=self.message)

            except discord.errors.NotFound as e:
                message = await self.channel.send(self.message)
                config.update_volume_message_id(message.id)
                return message
        else: 
            # Outside market hours
            pass

    def get_volume_movers(self):
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
        watchlist_tickers = self.volume_movers['Ticker'].to_list()

        watchlist_tickers, invalid_tickers = sd.StockData.get_list_from_tickers(' '.join(watchlist_tickers))
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
    def build_report_header(self):
        # Append ticker name, today's date, and external links to message
        header = f"## News articles for '{self.query}'\n"
        return header + "\n"

    def build_news(self):
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
        report = ''
        report += self.build_report_header()
        report += self.build_news()
        return report + '\n'  

    # Override
    async def send_report(self, interaction):
        await interaction.response.send_message(self.message)

class PopularityReport(Report):
    def __init__(self, channel, filter_name='all stock subreddits'):
        self.filter_name = filter_name
        self.filter = sd.ApeWisdom().get_filter(filter_name=self.filter_name)
        self.top_stocks = sd.ApeWisdom().get_top_stocks(filter_name=self.filter_name)
        sd.validate_path(config.get_attachments_path())
        self.filepath = f"{config.get_attachments_path()}/top-stocks-{datetime.datetime.today().strftime("%m-%d-%Y")}.csv"
        self.top_stocks.to_csv(self.filepath, index=False)
        self.file = discord.File(self.filepath)
        self.buttons = self.Buttons()
        
        super().__init__(channel)

    # Override
    def build_report_header(self):
        return f"# Most Popular Stocks ({self.filter_name}) {datetime.datetime.today().strftime("%m/%d/%Y")}\n\n"

    def build_report(self):
        report = ''
        report += self.build_report_header()
        report += self.build_table(self.top_stocks.drop(columns='name')[:15])
        return report

    async def send_report(self, interaction:discord.Interaction = None, visibility:str ="public"):
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
            super().__init__()
            self.add_item(discord.ui.Button(label="ApeWisdom", style=discord.ButtonStyle.url, url = "https://apewisdom.io/"))

class EarningsSpotlightReport(Report):
    def __init__(self, channel):
        earnings_today = sd.StockData.Earnings.get_earnings_today(datetime.datetime.today())
        self.ticker = earnings_today['ticker'].iloc[random.randint(0, earnings_today['ticker'].size)]
        self.buttons = StockReport.Buttons(self.ticker)
        super().__init__(channel)

    def build_report_header(self):
        return f"# :bulb: Earnings Spotight: {self.ticker}\n\n"

    def build_report(self):
        report = ""
        report += self.build_report_header()
        report += self.build_earnings_date()
        report += self.build_ticker_info()
        report += self.build_upcoming_earnings_summary()
        return report
    
    async def send_report(self):
        message = await super().send_report(view=self.buttons)
        return message


    # Override
    class Buttons(discord.ui.View):
            def __init__(self, ticker : str):
                super().__init__()
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
        filepath = f"{config.get_attachments_path()}/upcoming_earnings.csv"
        self.upcoming_earnings.to_csv(filepath, index=False)
        self.file = discord.File(filepath)
        super().__init__(channel)
    
    def get_upcoming_earnings(self):
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
        return f"# Earnings Releasing the Week of {utils.format_date_mdy(datetime.datetime.today())}\n\n"

    def build_watchlist_earnings(self):
        watchlist_tickers = sd.Watchlists().get_tickers_from_all_watchlists(no_personal=False)
        watchlist_earnings = self.upcoming_earnings[self.upcoming_earnings['ticker'].isin(watchlist_tickers)]
        if watchlist_earnings.size > 0:
            message = f" Tickers on your watchlists that report earnings this week:\n"
            message += self.build_table(watchlist_earnings['ticker', 'date', 'time'])
        else:
            message = "No tickers on your watchlists report earnings this week"
        return message

    def build_report(self):
        report = ""
        report += self.build_report_header()
        report += self.build_watchlist_earnings()
        return report

    async def send_report(self):
        message = await super().send_report(files=[self.file])
        return message

    


        
#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Reports(bot))
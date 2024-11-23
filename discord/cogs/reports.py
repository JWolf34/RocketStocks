import sys
sys.path.append('../RocketStocks/discord/cogs')
import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
from watchlists import Watchlists
from utils import Utils
import datetime
import stockdata as sd
import numpy as np
import datetime as dt
import json
import config
import psycopg2
import asyncio
from table2ascii import table2ascii
import logging
import rocketstocks

# Logging configuration
logger = logging.getLogger(__name__)

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
    
        message += f"**Name:** {ticker_data[1]}\n"
        message += f"**Sector:** {ticker_data[6] if ticker_data[6] else "N/A"}\n"
        message += f"**Industry:** {ticker_data[5] if ticker_data[5] else "N/A"}\n"
        message += f"**Market Cap:** ${self.format_large_num(ticker_data[2]) if ticker_data[2] else "N/A"}\n" 
        message += f"**Country:** {ticker_data[3] if ticker_data[3] else "N/A"}\n"
        message += f"**Next earnings date:** {sd.StockData.Earnings.get_next_earnings_date(self.ticker)}"
        
        return message + "\n"

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

    def build_report(self):
        report = ''
        report += self.build_report_header()
        return report   
    
    async def send_report(self, interaction:discord.Interaction, visibility:str = "public"):
        if visibility == 'private':
            message = await interaction.user.send(self.message)
            return message
        else:
            message = await self.channel.send(self.message)
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
    async def send_report(self, interaction:discord.Interaction, visibility:str = "public"):
        if visibility == 'private':
            message = await interaction.user.send(self.message, view=self.buttons)
            return message
        else:
            message = await self.channel.send(self.message, view=self.buttons)
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
        self.today = dt.datetime.now().astimezone()
        self.PREMARKET_START = self.today.replace(hour=7, minute=0, second=0, microsecond=0)
        self.INTRADAY_START= self.today.replace(hour=8, minute=30, second=0, microsecond=0)
        self.AFTERHOURS_START = self.today.replace(hour=15, minute=0, second=0, microsecond=0)
        self.MARKET_END = self.today.replace(hour=18, minute=0, second=0, microsecond=0)
        super().__init__(channel)


    # Override
    def build_report_header(self):
        header = "### :rotating_light: {} Gainers {} (Market Cap > $100M) (Updated {})\n\n".format(
                    "Pre-market" if self.in_premarket()
                    else "Intraday" if self.in_intraday()
                    else "After Hours" if self.in_afterhours()
                    else "",
                    self.today.strftime("%m/%d/%Y"),
                    self.today.strftime("%-I:%M %p"))
        return header

    def build_gainer_table(self):
        watchlist_tickers = []
        watchlist_id = ''

        if self.in_premarket():
            watchlist_id = "premarket-gainers"
            gainers = sd.TradingView.get_premarket_gainers_by_market_cap(100000000)[:15]
            headers = ["Ticker", "Close", "Volume", "Market Cap", "Premarket Change", "Premarket Volume"]
            rows = []
            for index, row in gainers.iterrows():
                ticker = row.iloc[1]
                if sd.StockData.validate_ticker(ticker):
                    rows.append([row.iloc[1], 
                                "${}".format(float('{:.2f}'.format(row.close))), 
                                self.format_large_num(row.volume), 
                                self.format_large_num(row.market_cap_basic), 
                                "{:.2f}%".format(row.premarket_change), 
                                self.format_large_num(row.premarket_volume)])
                    watchlist_tickers.append(ticker)
                else:
                    pass
        elif self.in_intraday():
            watchlist_id = "intraday-gainers"
            gainers = sd.TradingView.get_intraday_gainers_by_market_cap(100000000)[:15]
            headers = ["Ticker", "Close", "Volume", "Market Cap", "% Change"]
            rows = []
            for index, row in gainers.iterrows():
                ticker = row.iloc[1]
                if sd.StockData.validate_ticker(ticker):
                    rows.append([ticker, 
                                "${}".format(float('{:.2f}'.format(row.close))), 
                                self.format_large_num(row.volume), 
                                self.format_large_num(row.market_cap_basic), 
                                "{:.2f}%".format(row.change)])
                    watchlist_tickers.append(ticker)
                else: 
                    pass
                
        elif self.in_afterhours():
            watchlist_id = "afterhours-gainers"
            gainers = sd.TradingView.get_postmarket_gainers_by_market_cap(100000000)[:15] 
            headers = ["Ticker", "Close", "Volume", "Market Cap", "After Hours Change", "After Hours Volume"]
            rows = []
            for index, row in gainers.iterrows():
                ticker = row.iloc[1]
                if sd.StockData.validate_ticker(ticker):
                    rows.append([ticker, 
                                "${}".format(float('{:.2f}'.format(row.close))), 
                                self.format_large_num(row.volume), 
                                self.format_large_num(row.market_cap_basic), 
                                "{:.2f}%".format(row.postmarket_change), 
                                self.format_large_num(row.postmarket_volume)])
                    watchlist_tickers.append(ticker)
                else: 
                    pass
            
        else:
            return ""

        if not sd.Watchlists().validate_watchlist(watchlist_id):
            sd.Watchlists().create_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers, systemGenerated=True)
        else:
            sd.Watchlists().update_watchlist(watchlist_id=watchlist_id, tickers=watchlist_tickers)
        
        table = table2ascii(
            header = headers,
            body = rows, 
        )
        return "```\n" + table + "\n```"

    # Override
    def build_report(self):
        report = ""
        report += self.build_report_header()
        report += self.build_gainer_table()
        return report

    # Override
    async def send_report(self):
        await self.channel.send("We made it to send reports")
        try:
            await self.channel.send("try statement")
            await self.channel.send(f"system time {datetime.datetime.now()}")
            if self.in_premarket() or self.in_intraday() or self.in_afterhours():
                await self.channel.send("in market hours")
                market_period = self.get_market_period()
                await self.channel.send("got market period")
                message_id = config.get_gainer_message_id(market_period)
                await self.channel.send("got message id")
                try:
                    curr_message = await self.channel.fetch_message(message_id)
                    if curr_message.created_at.date() < self.today.date():
                        message = await self.channel.send(self.message)
                        config.update_gainer_message_id(market_period, message.id)
                        return message
                    else:
                        await curr_message.edit(content=self.message)

                except discord.errors.NotFound as e:
                    await self.channel.send("message not found")
                    message = await self.channel.send(self.message)
                    await self.channel.send("sent message")
                    config.update_gainer_message_id(market_period, message.id)
                    await self.channel.send("update message id")
                    return message
            else: 
                await self.channel.send("not in market hours")
        except Exception as e:
            await self.channel.send(e)
            
    def update_message_id(self, message_id, report_type):
        data = get_config()
        if "gainers" not in data.keys():
            self.write_gainer_config()
        data = get_config()
        if self.in_premarket():
            data['reports']['gainers']['PREMARKET_MESSAGE_ID'] = message_id
        elif self.in_intraday():
            data['reports']['gainers']['INTRADAY_MESSAGE_ID'] = message_id
        elif self.in_afterhours():
            data['reports']['gainers']['AFTERHOURS_MESSAGE_ID'] = message_id
        write_config(data)

    def get_message_id(self):
        data = config.get_config()
        if self.in_premarket():
            return data['reports']['gainers']['PREMARKET_MESSAGE_ID']
        elif self.in_intraday():
            return data['reports']['gainers']['INTRADAY_MESSAGE_ID']
        elif self.in_afterhours():
            return data['reports']['gainers']['AFTERHOURS_MESSAGE_ID']



    def in_premarket(self):
        return self.today > self.PREMARKET_START and self.today < self.INTRADAY_START

    def in_intraday(self):
        return self.today > self.INTRADAY_START and self.today < self.AFTERHOURS_START
    
    def in_afterhours(self):
        return self.today > self.AFTERHOURS_START and self.today < self.MARKET_END

    def get_market_period(self):
        if self.in_premarket():
            return "premarket"
        elif self.in_intraday():
            return "intraday"
        if self.in_afterhours():
            return "afterhours"
        else:
            return "EOD"       
    
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


class Reports(commands.Cog):
    def __init__(self, bot):
        print("Reports cog initialized")
        self.bot = bot
        print("bot configured")
        self.send_gainer_reports.start()
        print("started gainer reports loop configured")
        self.update_earnings_calendar.start()
        print("started earnings calendar loop configured")
        self.reports_channel = self.bot.get_channel(config.get_reports_channel_id())
        print("reports channel configured")
        self.gainers_channel = self.bot.get_channel(config.get_gainers_channel_id())
        print("gainers channel configured")
        

    @commands.Cog.listener()
    async def on_ready(self):
        await self.bot.wait_until_ready()
        
        #await self.send_gainer_reports.start()
        #logger.info("started gainer reports loop configured")

    
    #########
    # Tasks #
    #########

    # Generate and send premarket gainer reports to the reports channel
    @tasks.loop(minutes=5)
    async def send_gainer_reports(self):
        try:
            await self.reports_channel.send("It's alive!")
            report = GainerReport(self.gainers_channel)
            await self.reports_channel.send("Created reports object")
            #if (report.today.weekday() < 5):
            await report.send_report()
            await self.reports_channel.send("Sent reports")
            #else:
            #    # Not a weekday - do not post gainer reports
            #    pass
        except Exception as e:
            print(e)

    #@send_gainer_reports.before_loop
    async def before_send_gainer_reports(self):
        try:
            await self.reports_channel.send("it's alive!")
            await asyncio.sleep(10)
            # Start posting report at next 0 or 5 minute interval
            now = datetime.datetime.now().astimezone()
            if now.minute % 5 == 0:
                return 0
            minutes_by_five = now.minute // 5
            # get the difference in times
            diff = (minutes_by_five + 1) * 5 - now.minute
            future = now + datetime.timedelta(minutes=diff)
            difference = (future-now).total_seconds()
            print(f"Posting gainer reports in {difference} seconds")
            await asyncio.sleep((future-now).total_seconds())
        except Exception as e:
            print(e)

    # Create earnings events on calendar for all stocks on watchlists
    @tasks.loop(time=datetime.time(hour=2, minute=0, second=0))
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
                    start_time = datetime.datetime.strptime(earnings_info['date'][0], "%Y-%m-%d").astimezone()
                    start_time= start_time.replace(hour=12, minute=0, second=0)
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

    @app_commands.command(name="start-tasks", description="Start report tasks")
    async def start_tasks(self, interaction:discord.Interaction):
        await self.send_gainer_reports.start()
        print("Started tasks")
        interaction.response.send_message("Started tasks")

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
                follow_up += f"Invalid tickers: {", ".join(invalid_tickers)}"
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


        
#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Reports(bot))
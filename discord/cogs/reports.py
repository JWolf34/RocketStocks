import sys
sys.path.append('../RocketStocks/discord/cogs')
import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
from watchlists import Watchlists
import stockdata as sd
import numpy as np
import datetime as dt
import json
import config
import psycopg2
from table2ascii import table2ascii
import logging

# Logging configuration
logger = logging.getLogger(__name__)

##################
# Report Classes #
##################

class Report(object):
    def __init__(self):
        self.message = self.build_report() + "\n\n"

    ############################
    # Report Builder Functions #
    ############################

    # Report Header
    def build_report_header(self):
        
        # Append ticker name, today's date, and external links to message
        header = "## " + self.ticker + " Report " + dt.date.today().strftime("%m/%d/%Y") + "\n"
        return header + "\n"

    # Ticker Info
    def build_ticker_info(self):

        message = "### Ticker Info\n"
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
        message = "### Summary \n| "
        for col in summary.keys():
            message += "**{}:** {}".format(col, f"{summary[col]:,.2f}")
            message += " | "

        return message + "\n"
    
    def build_recent_SEC_filings(self):
        message = "### Recent SEC Filings\n\n"
        filings = sd.SEC().get_recent_filings(ticker=self.ticker)
        for index, filing in filings[:5].iterrows():
            message += f"[{filing['form']} - {filing['filingDate']}]({sd.SEC().get_link_to_filing(ticker=self.ticker, filing=filing)})\n"
        return message

    def build_report(self):
        report = ''
        report += self.build_report_header()
        return report   
    
    async def send_report(self, interaction:discord.Interaction, visibility:str = "public"):
        if visibility == 'private':
            await interaction.user.send(self.message)
        else:
            await interaction.channel.send(self.message)

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
    
    def __init__(self, ticker : str):
        self.ticker = ticker
        self.data =  sd.fetch_daily_data(self.ticker)
        self.buttons = self.Buttons(self.ticker)
        super().__init__()
        
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
            await interaction.user.send(self.message, view=self.buttons)
        else:
            await interaction.channel.send(self.message, view=self.buttons)

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

class GainerReport(Report):
    def __init__(self):
        self.today = dt.datetime.now()
        self.PREMARKET_START = self.today.replace(hour=7, minute=0, second=0, microsecond=0)
        self.INTRADAY_START= self.today.replace(hour=8, minute=30, second=0, microsecond=0)
        self.AFTERHOURS_START = self.today.replace(hour=15, minute=0, second=0, microsecond=0)
        self.MARKET_END = self.today.replace(hour=18, minute=0, second=0, microsecond=0)
        super().__init__()


    # Override
    def build_report_header(self):
        header = "### :rotating_light: {} Gainers {} (Market Cap > $100M)\n\n".format(
                    "Pre-market" if self.in_premarket()
                    else "Intraday" if self.in_intraday()
                    else "After Hours" if self.in_afterhours()
                    else "",
                    self.today.strftime("%m/%d/%Y"))
        return header

    def build_gainer_table(self):

        if self.in_premarket():
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
                else:
                    pass
        elif self.in_intraday():
            # Placeholder - need to make query for intraday earners
            gainers = sd.TradingView.get_intraday_gainers_by_market_cap(100000000)[:15]
            headers = ["Ticker", "Close", "Volume", "Market Cap", "% Change"]
            rows = []
            for index, row in gainers.iterrows():
                ticker = row.iloc[1]
                if sd.StockData.validate_ticker(ticker):
                    rows.append([row.iloc[1], 
                                "${}".format(float('{:.2f}'.format(row.close))), 
                                self.format_large_num(row.volume), 
                                self.format_large_num(row.market_cap_basic), 
                                "{:.2f}%".format(row.change)])
                else: 
                    pass
        elif self.in_afterhours():
            gainers = sd.TradingView.get_postmarket_gainers_by_market_cap(100000000)[:15] 
            headers = ["Ticker", "Close", "Volume", "Market Cap", "After Hours Change", "After Hours Volume"]
            rows = []
            for index, row in gainers.iterrows():
                ticker = row.iloc[1]
                if sd.StockData.validate_ticker(ticker):
                    rows.append([row.iloc[1], 
                                "${}".format(float('{:.2f}'.format(row.close))), 
                                self.format_large_num(row.volume), 
                                self.format_large_num(row.market_cap_basic), 
                                "{:.2f}%".format(row.postmarket_change), 
                                self.format_large_num(row.postmarket_volume)])
                else: 
                    pass
        else:
            return ""
        
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
    async def send_report(self, channel):
        if self.in_premarket() or self.in_intraday() or self.in_afterhours():
            market_period = self.get_market_period()
            message_id = config.get_gainer_message_id(market_period)
            try:
                curr_message = await channel.fetch_message(message_id)
                if curr_message.created_at.date() < self.today.date():
                    message = await channel.send(self.message)
                    config.update_gainer_message_id(market_period, message.id)
                else:
                    await curr_message.edit(content=self.message)

            #if curr_message.created_at
            except discord.errors.NotFound as e:
                message = await channel.send(self.message)
                config.update_gainer_message_id(market_period, message.id)
        else: 
            pass
            
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
        super().__init__()

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
    

class Reports(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.send_gainer_reports.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")
    
    #########
    # Tasks #
    #########

    # Generate and send premarket gainer reports to the reports channel
    @tasks.loop(minutes=5)
    async def send_gainer_reports(self):
        report = GainerReport()
        if (report.today.weekday() < 5):
            channel_id = config.get_reports_channel_id()
            channel = self.bot.get_channel(channel_id)
            await report.send_report(self.bot.get_channel(config.get_reports_channel_id()))
        else:
            # Not a weekday - do not post gainer reports
            pass

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
        else:
            # Send reports
            logger.info("Running reports on tickers {}".format(tickers))
            for ticker in tickers:
                logger.debug("Processing ticker {}".format(ticker))
                report = StockReport(ticker)
                await report.send_report(interaction, visibility.value)
                logger.info("Report posted for ticker {}".format(ticker))
            message = "Reports have been posted!"
            logger.info("Reports have been posted")
        await interaction.followup.send(message, ephemeral=True)


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

        logger.info("Fetching reports for tickers {}".format(tickers))
        for ticker in tickers:
            logger.debug("Processing ticker {}".format(ticker))
            report = StockReport(ticker)
            await report.send_report(interaction, visibility.value)
            logger.info("Report posted for ticker {}".format(ticker))
        if len(invalid_tickers) > 0:
            await interaction.followup.send("Fetched reports for {}. Failed to fetch reports for {}.".format(", ".join(tickers), ", ".join(invalid_tickers)), ephemeral=True)
        else:
            logger.info("Reports have been posted")
            await interaction.followup.send("Fetched reports!", ephemeral=True)

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
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])  
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])    
    @app_commands.describe(sort_by = "Field by which to sort returned articles")
    @app_commands.autocomplete(sort_by=autocomplete_sortby,)
    async def news(self, interaction:discord.Interaction, query:str, visibility:app_commands.Choice[str], sort_by:str = 'publishedAt'):
        logger.info("/news function called by user {}".format(interaction.user.name))
        kwargs = {'sort_by': sort_by}
        report = NewsReport(query=query, **kwargs )
        await report.send_report(interaction=interaction)
        await interaction.response.send_message("News articles posted!", ephemeral=True)

    # Autocomplete functions

    async def autocomplete_categories(self, interaction:discord.Interaction, current:str):
        return [
            app_commands.Choice(name = category_name, value= category_value)
            for category_name, category_value in sd.News().categories.items() if current.lower() in category_name.lower()
        ]

    @app_commands.command(name="breaking-news", description="Fetch news on the query provided")
    @app_commands.describe(query= "The search terms or terms to query for")
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])  
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])    
    @app_commands.describe(category = "The element of the article to search for the query in")
    @app_commands.autocomplete(category=autocomplete_categories,)   
    async def breaking_news(self, interaction:discord.Interaction, query:str, visibility:app_commands.Choice[str], category:str = ""):
        logger.info("/breaking_news function called by user {}".format(interaction.user.name))
        kwargs = {}
        if category:
            kwargs['category'] = category
        report = NewsReport(query=query, breaking=True, **kwargs )
        await report.send_report(interaction=interaction)
        await interaction.response.send_message("News articles posted!", ephemeral=True)


        
#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Reports(bot))
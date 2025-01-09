import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
from reports import Report
from reports import StockReport
import stockdata as sd
import pandas as pd
import config
from config import market_utils
from config import date_utils
import datetime
import logging
import asyncio

# Logging configuration
logger = logging.getLogger(__name__)

class Alerts(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.alerts_channel=self.bot.get_channel(config.discord_utils.alerts_channel_id)
        self.reports_channel= self.bot.get_channel(config.discord_utils.reports_channel_id)
        self.post_alerts_date.start()
        self.send_popularity_movers.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    @tasks.loop(time=datetime.time(hour=6, minute=0, second=0)) # time in UTC
    async def post_alerts_date(self):
        if (market_utils.market_open_today()):
            await self.alerts_channel.send(f"# :rotating_light: Alerts for {date_utils.format_date_mdy(datetime.datetime.today())} :rotating_light:")

    async def send_earnings_movers(self, gainers):
        today = datetime.datetime.today()
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            earnings_date = sd.StockData.Earnings.get_next_earnings_date(ticker)
            if earnings_date != "N/A":
                if earnings_date == today.date():
                    alert = EarningsMoverAlert(ticker=ticker, channel=self.alerts_channel, gainer_row=row)
                    await alert.send_alert()

    async def send_sec_filing_movers(self, gainers):
        today = datetime.datetime.today()
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            filings = sd.SEC().get_filings_from_today(ticker)
            if filings.size > 0:
                alert = SECFilingMoverAlert(ticker=ticker, channel=self.alerts_channel, gainer_row=row)
                await alert.send_alert()
            await asyncio.sleep(1)

    async def send_watchlist_movers(self, gainers):
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            watchlists = sd.Watchlists().get_watchlists()
            for watchlist in watchlists:
                if watchlist == 'personal':
                    pass
                else:
                    watchlist_tickers = sd.Watchlists().get_tickers_from_watchlist(watchlist)
                    if ticker in watchlist_tickers:
                        alert = WatchlistMoverAlert(ticker=ticker, channel=self.alerts_channel, gainer_row = row, watchlist_id=watchlist)
                        await alert.send_alert()
    
    async def send_unusual_volume_movers(self, volume_movers):
        today = datetime.datetime.today()
        for index, row in volume_movers.iterrows():
            ticker = row['Ticker']
            relative_volume = float(row['Relative Volume'])
            if relative_volume > 25.0: # see that Relative Volume exceeds 20x
                pct_change = 0.0
                change_columns = ["Premarket Change", "% Change", "After Hours Change"]
                for column in change_columns:
                    if column in row.index.values:
                        pct_change = float(row[column])
                market_cap = row['Market Cap']
                if abs(pct_change) >= 10.0 and market_cap > 50000000: # See that stock movement is at least 10% and market cap > 50M
                    alert = VolumeMoverAlert(ticker=ticker, channel=self.alerts_channel, volume_row=row)
                    await alert.send_alert()
                await asyncio.sleep(1)
    
    @tasks.loop(minutes=30)
    async def send_popularity_movers(self):
        top_stocks = sd.ApeWisdom().get_top_stocks()[:50]
        for index, row in top_stocks.iterrows():
            ticker = row['ticker']
            todays_rank = row['rank']
            popularity = sd.StockData.get_historical_popularity(ticker)
            popularity = popularity[popularity['date'] > (datetime.date.today() - datetime.timedelta(days=5))]
            min_rank = 0
            max_rank = 0
            for index, popular_row in popularity.iterrows(): # replace with iterrows logic
                if min_rank < popular_row['rank'] or min_rank == 0:
                    min_rank = popular_row['rank']
                if max_rank > popular_row['rank'] or max_rank == 0:
                    max_rank = popular_row['rank']

            today_is_max = False
            if min_rank < todays_rank:
                min_rank = todays_rank
            if max_rank > todays_rank:
                today_is_max = True
                max_rank = todays_rank
                
        
            if (((float(min_rank) - float(max_rank)) / float(min_rank)) * 100.0 > 50.0) and min_rank > 10 and sd.StockData.validate_ticker(ticker) and today_is_max:
                alert = PopularityAlert(ticker = ticker, 
                                        channel=self.alerts_channel,
                                        todays_popularity=row,
                                        historical_popularity=popularity) 
                await alert.send_alert()

##################
# Alerts Classes #
##################
    
class Alert(Report):
    def __init__(self, ticker, channel):
        self.ticker = ticker
        self.channel = channel
        self.message = self.build_alert()
        self.buttons = self.Buttons(self.ticker, channel)
    
    def build_alert_header(self):
        header = f"# :rotatinglight: {self.ticker} ALERT :rotatinglight:\n\n"
        return header 

    def build_alert(self):
        alert = ''
        alert += self.build_alert_header()
        return alert

    def get_pct_change(self, df_row):
        change_columns = ["Premarket Change", "% Change", "After Hours Change"]
        for column in change_columns:
            if column in df_row.index.values:
                change = df_row[column]
                if change is None:
                    return 0.0
                else:
                    return float(df_row[column])

    async def send_alert(self):
        today = datetime.datetime.today()
        market_period = market_utils.get_market_period()
        message_id = config.discord_utils.get_alert_message_id(date=today.date(), ticker=self.ticker, alert_type=self.alert_type)
        if message_id is not None:
            logger.debug(f"Alert {self.alert_type} already reported for ticker {self.ticker} today")
            pass
        else:
            message = await self.channel.send(self.message, view=self.buttons)
            config.discord_utils.insert_alert_message_id(date=today.date(), ticker=self.ticker, alert_type=self.alert_type, message_id=message.id)
            return message


    class Buttons(discord.ui.View):
            def __init__(self, ticker : str, channel):
                super().__init__(timeout=None)
                self.ticker = ticker
                self.channel = channel
                self.add_item(discord.ui.Button(label="Google it", style=discord.ButtonStyle.url, url = "https://www.google.com/search?q={}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="StockInvest", style=discord.ButtonStyle.url, url = "https://stockinvest.us/stock/{}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="FinViz", style=discord.ButtonStyle.url, url = "https://finviz.com/quote.ashx?t={}".format(self.ticker)))
                self.add_item(discord.ui.Button(label="Yahoo! Finance", style=discord.ButtonStyle.url, url = "https://finance.yahoo.com/quote/{}".format(self.ticker)))

                
            @discord.ui.button(label="Generate report", style=discord.ButtonStyle.primary)
            async def generate_chart(self, interaction:discord.Interaction, button:discord.ui.Button,):
                report = StockReport(ticker=self.ticker, channel=self.channel)
                await report.send_report(interaction, visibility="public")

            @discord.ui.button(label="Get news", style=discord.ButtonStyle.primary)
            async def get_news(self, interaction:discord.Interaction, button:discord.ui.Button):
                news_report = NewsReport(self.ticker)
                await news_report.send_report(interaction)
                await interaction.response.send_message(f"Fetched news for {self.ticker}!", ephemeral=True)

class EarningsMoverAlert(Alert):
    def __init__(self, ticker, channel, gainer_row):
        self.pct_change = self.get_pct_change(gainer_row)
        self.alert_type = "EARNINGS_MOVER"
        super().__init__(ticker, channel)

    def build_alert_header(self):
        header = f"## :rotating_light: Earnings Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.pct_change > 0 else ":small_red_triangle_down:"
        return f"**{self.ticker}** is {symbol} **{"{:.2f}".format(self.pct_change)}%**  {market_utils.get_market_period()} and has earnings today\n\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_earnings_date()
        return alert

class SECFilingMoverAlert(Alert):
    def __init__(self, ticker, channel, gainer_row):
        self.pct_change = self.get_pct_change(gainer_row)
        self.alert_type = "SEC_FILING_MOVER"
        super().__init__(ticker, channel)

    def build_alert_header(self):
        header = f"## :rotating_light: SEC Filing Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.pct_change > 0 else ":small_red_triangle_down:"
        return f"**{self.ticker}** is {symbol} **{"{:.2f}".format(self.pct_change)}%** {market_utils.get_market_period()} and filed with the SEC today\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_todays_sec_filings()
        return alert

class WatchlistMoverAlert(Alert):
    def __init__(self, ticker, channel, gainer_row, watchlist_id):
        self.pct_change = self.get_pct_change(gainer_row)
        self.alert_type = "WATCHLIST_MOVER"
        self.watchlist_id = watchlist_id
        super().__init__(ticker, channel)

    def build_alert_header(self):
        header = f"## :rotating_light: Watchlist Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.pct_change > 0 else ":small_red_triangle_down:"
        return f"**{self.ticker}** is {symbol} **{"{:.2f}".format(self.pct_change)}%**  and is on your **{self.watchlist_id}** watchlist\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        return alert

class VolumeMoverAlert(Alert):
    def __init__(self, ticker, channel, volume_row):
        self.pct_change = self.get_pct_change(volume_row)
        self.alert_type = "VOLUME_MOVER"
        self.volume = volume_row['Volume']
        self.average_volume = volume_row['Average Volume (10 Day)']
        self.relative_volume = volume_row['Relative Volume']
        super().__init__(ticker, channel)

    def build_alert_header(self):
        header = f"## :rotating_light: Volume Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.pct_change > 0 else ":small_red_triangle_down:"
        return f"**{self.ticker}** is {symbol} **{"{:.2f}".format(self.pct_change)}%** with volume up **{"{:.2f} times".format(self.relative_volume)}** the 10-day average\n\n"

    def build_volume_stats(self):
        return f"""## Volume Stats
        - **Today's Volume:** {self.format_large_num(self.volume)}
        - **Average Volume (10 Day):** {self.format_large_num(self.average_volume)}
        - **Relative Volume:** {"{:.2f}x".format(self.relative_volume)}\n\n"""

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_volume_stats()
        return alert

class PopularityAlert(Alert):
    def __init__(self, ticker, channel, todays_popularity, historical_popularity):
        self.alert_type = "POPULARITY"
        self.todays_popularity = todays_popularity
        self.historical_popularity = historical_popularity
        self.popularity_stats = self.get_popularity_stats()
        super().__init__(ticker, channel)
    
    def get_popularity_stats(self):
        stats = {'todays_rank':self.todays_popularity['rank'],
                 'low_rank': None,
                 'low_rank_date':None,
                 'high_rank': None, 
                 'high_rank_date':None}
        popularity = pd.concat([self.todays_popularity, self.historical_popularity], ignore_index=True)
        for index, row in self.historical_popularity.iterrows():
            rank = row['rank']
            date = row['date']
            if stats.get('low_rank') is None or stats.get('low_rank') > rank:
                stats['low_rank'] = rank
                stats['low_rank_date'] = date
            if stats.get('high_rank') is None or stats.get('high_rank') < rank:
                stats['high_rank'] = rank
                stats['high_rank_date'] = date
        if stats.get('low_rank') is None or stats.get('low_rank') > stats.get('todays_rank'):
                stats['low_rank'] = stats.get('todays_rank')
                stats['low_rank_date'] = datetime.date.today()
        if stats.get('high_rank') is None or stats.get('high_rank') < stats.get('todays_rank'):
            stats['high_rank'] = stats.get('todays_rank')
            stats['high_rank_date'] = datetime.date.today()
        return stats
            
    def build_alert_header(self):
        header = f"## :rotating_light: Popularity Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        return f"**{self.ticker}** has moved {self.popularity_stats['high_rank'] - self.popularity_stats['low_rank']} spots between {date_utils.format_date_mdy(self.popularity_stats['low_rank_date'])} **({self.popularity_stats['low_rank']})** and {date_utils.format_date_mdy(self.popularity_stats['high_rank_date'])} **({self.popularity_stats['high_rank']})** \n\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_popularity()
        return alert


#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Alerts(bot))
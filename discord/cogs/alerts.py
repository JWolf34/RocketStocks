import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
import reports
import stockdata as sd
import config
from config import utils
import datetime
import logging

# Logging configuration
logger = logging.getLogger(__name__)

class Alerts(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.alerts_channel=self.bot.get_channel(config.get_alerts_channel_id())
        #self.send_earnings_movers.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    #@tasks.loop(minutes=5)
    async def send_earnings_movers(self, gainers):
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            earnings_date = sd.StockData.Earnings.get_next_earnings_date(ticker)
            if utils.format_date_mdy(earnings_date) == utils.format_date_mdy(datetime.datetime.today()):
                alert = EarningsMoverAlert(ticker=ticker, channel=self.alerts_channel, pct_change= row['% Change'])
                await alert.send_alert()

##################
# Alerts Classes #
##################
    
class Alert():
    def __init__(self, ticker, channel):
        self.ticker = ticker
        self.channel = channel
        self.message = self.build_alert()
    
    def build_alert_header(self):
        header = f"# :rotatinglight: {self.ticker} ALERT :rotatinglight:\n\n"
        return header 

    def build_alert(self):
        alert = ''
        alert += self.build_alert_header()
        return alert

    def build_earnings_date(self):
        earnings_info = sd.StockData.Earnings.get_next_earnings_info(self.ticker)
        message = "**Earnings Date:** "
        message += f"{earnings_info['date'].iloc[0].strftime("%m/%d/%Y")}, "
        earnings_time = earnings_info['time'].iloc[0]
        if "pre-market" in earnings_time:
            message += "before market open"
        elif "after-hours" in earnings_time:
            message += "after market close"
        else:
            message += "time not specified"

        return message + "\n\n"


    def build_recent_sec_filings(self):
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

    async def send_alert(self):
        message = await self.channel.send(self.message)
        return message

class EarningsMoverAlert(Alert):
    def __init__(self, ticker, channel, pct_change):
        self.pct_change = pct_change
        self.market_time="premarket"
        super().__init__(ticker, channel)

    def build_alert_header(self):
        header = f"## :rotating_light: EARNINGS MOVER: {self.ticker} :rotating_light:\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.pct_change > 0 else ":small_red_triangle_down:"
        return f"**Movement:** {symbol} {self.pct_change}% {self.market_time}\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_earnings_date()
        return alert

        

#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Alerts(bot))
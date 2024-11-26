import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
from reports import Report
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
        today = datetime.datetime.today()
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            earnings_date = sd.StockData.Earnings.get_next_earnings_date(ticker)
            if earnings_date != "N/A":
                if earnings_date == today.date():
                    alert = EarningsMoverAlert(ticker=ticker, channel=self.alerts_channel, pct_change= float(row['% Change'].strip("%")))
                    await alert.send_alert()

    async def send_sec_filing_movers(self, gainers):
        today = datetime.datetime.today()
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            filings = sd.SEC().get_filings_from_today(ticker)
            if filings.size > 0:
                alert = SECFilingMoverAlert(ticker=ticker, channel=self.alerts_channel, pct_change= float(row['% Change'].strip("%")))
                await alert.send_alert()


##################
# Alerts Classes #
##################
    
class Alert(Report):
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

    async def send_alert(self):
        message = await self.channel.send(self.message)
        return message

class EarningsMoverAlert(Alert):
    def __init__(self, ticker, channel, pct_change):
        self.pct_change = pct_change
        super().__init__(ticker, channel)

    def build_alert_header(self):
        header = f"## :rotating_light: Earnings Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        symbol = ":green_circle:" if self.pct_change > 0 else ":small_red_triangle_down:"
        return f"**{self.ticker}** is up **{self.pct_change}%** {utils.get_market_period()} and has earnings today\n\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_earnings_date()
        return alert

class SECFilingMoverAlert(Alert):
    def __init__(self, ticker, channel, pct_change):
        self.pct_change = pct_change
        super().__init__(ticker, channel)

    def build_alert_header(self):
        header = f"## :rotating_light: SEC Filing Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        return f"**{self.ticker}** is up **{self.pct_change}%** {utils.get_market_period()} and filed with the SEC today\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_todays_sec_filings()
        return alert

        

#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Alerts(bot))
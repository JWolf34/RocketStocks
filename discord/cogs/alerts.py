import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
from reports import Report
from reports import StockReport
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
        self.reports_channel= self.bot.get_channel(config.get_reports_channel_id())
        self.post_alerts_date.start()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")


    @tasks.loop(time=datetime.time(hour=12, minute=30, second=0)) # time in UTC
    async def post_alerts_date(self):
        now = datetime.datetime.now()
        if (now.weekday() < 5):
            await self.alerts_channel.send(f"# :rotating_light: Alerts for {utils.format_date_mdy(now.date())} :rotating_light:")

    async def send_earnings_movers(self, gainers):
        today = datetime.datetime.today()
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            earnings_date = sd.StockData.Earnings.get_next_earnings_date(ticker)
            if earnings_date != "N/A":
                if earnings_date == today.date():
                    change_columns = ["Premarket Change", "% Change", "After Hours Change"]
                    pct_change = 0.0
                    for column in change_columns:
                        if column in row.index:
                            pct_change = float(row[column].strip("%"))
                            break
                    alert = EarningsMoverAlert(ticker=ticker, channel=self.alerts_channel, pct_change= pct_change)
                    await alert.send_alert()

    async def send_sec_filing_movers(self, gainers):
        today = datetime.datetime.today()
        for index, row in gainers.iterrows():
            ticker = row['Ticker']
            filings = sd.SEC().get_filings_from_today(ticker)
            if filings.size > 0:
                change_columns = ["Premarket Change", "% Change", "After Hours Change"]
                pct_change = 0.0
                for column in change_columns:
                    if column in row.index:
                        pct_change = float(row[column].strip("%"))
                        break
                alert = SECFilingMoverAlert(ticker=ticker, channel=self.alerts_channel, pct_change= pct_change)
                await alert.send_alert()
    
    async def send_unusual_volume_movers(self, volume_movers):
        today = datetime.datetime.today()
        for index, row in volume_movers.iterrows():
            ticker = row['Ticker']
            if float(row['Relative Volume']) > 100.0:
                alert = VolumeMoverAlert(ticker=ticker, channel=self.alerts_channel, volume_row=row)
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
        if utils.in_premarket() or utils.in_intraday() or utils.in_afterhours():
            today = datetime.datetime.today()
            market_period = utils.get_market_period()
            message_id = config.get_alert_message_id(date=today.date(), ticker=self.ticker, alert_type=self.alert_type)
            if message_id is not None:
                logger.debug(f"Alert {self.alert_type} already reported for ticker {self.ticker} today")
                pass
            else:
                message = await self.channel.send(self.message, view=self.buttons)
                config.insert_alert_message_id(date=today.date(), ticker=self.ticker, alert_type=self.alert_type, message_id=message.id)
                return message
        else: 
            # Outside market hours
            pass

    class Buttons(discord.ui.View):
            def __init__(self, ticker : str, channel):
                super().__init__()
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
    def __init__(self, ticker, channel, pct_change):
        self.pct_change = pct_change
        self.alert_type = "EARNINGS_MOVER"
        super().__init__(ticker, channel)

    def build_alert_header(self):
        header = f"## :rotating_light: Earnings Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
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
        self.alert_type = "SEC_FILING_MOVER"
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

class VolumeMoverAlert(Alert):
    def __init__(self, ticker, channel, volume_row):
        self.pct_change = volume_row['% Change']
        self.alert_type = "VOLUME_MOVER"
        self.volume = volume_row['Volume']
        self.average_volume = volume_row['Average Volume (10 Day)']
        self.relative_volume = volume_row['Relative Volume']
        super().__init__(ticker, channel)

    def build_alert_header(self):
        header = f"## :rotating_light: Volume Mover: {self.ticker}\n\n\n"
        return header

    def build_todays_change(self):
        return f"**{self.ticker}** is up **{self.pct_change}%** with relative volume **{"{:.2f} times".format(self.relative_volume)}** the normal today\n\n"

    def build_stats(self):
        return f"### Stats\n**Today's Volume:** {self.volume}\n**Average Volume:** {self.average_volume}\n**Relative Volume:** {self.relative_volume}x\n\n"

    def build_alert(self):
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_stats()
        return alert


#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Alerts(bot))
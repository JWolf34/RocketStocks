import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
import config
import logging

# Logging configuration
logger = logging.getLogger(__name__)

class Alerts(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.alerts_channel=self.bot.get_channel(config.get_alerts_channel_id())

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

##################
# Alerts Classes #
##################
    
class Alert():
    def __init__(self, ticker, channel):
        self.ticker = ticker
        self.channel = channel
        self.message = self.build_alert()
    
    def build_alert_header(self):
        header = f"# :rotatinglight: {ticker} ALERT :rotatinglight:\n\n"
        return header 

    def build_alert(self):
        alert = ''
        alert += self.build_alert_header()
        return alert

class EarningsMoverAlert(Alert):
    def __init__(self, ticker, channel):
        super().__init__(ticker, channel)
        

#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Alerts(bot))
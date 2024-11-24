import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
from reports import GainerReport
import config
import logging

# Logging configuration
logger = logging.getLogger(__name__)

class Tests(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.gainers_channel = self.bot.get_channel(config.get_gainers_channel_id())
        

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    ########################
    # Test & Help Commands #
    ########################
   
    @commands.is_owner()
    @app_commands.command(name = "test-gainer-reports", description= "Test posting premarket gainer reports",)
    async def test_premarket_reports(self, interaction: discord.Interaction):
        logger.info("/test-premarket-reports function called by user {}".format(interaction.user.name))
        await interaction.response.defer(ephemeral=True)

        report = GainerReport(self.gainers_channel)
        if report.get_market_period() == "EOD":
            await interaction.followup.send("Market is closed - cannot post gainer reports", ephemeral=True)
        else:
            await report.send_report()
            await interaction.followup.send("Gainer reports test complete!", ephemeral=True)

#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Tests(bot))
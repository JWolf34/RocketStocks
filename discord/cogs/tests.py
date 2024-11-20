import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
import logging

# Logging configuration
logger = logging.getLogger(__name__)

class Tests(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    ########################
    # Test & Help Commands #
    ########################
   
    @app_commands.command(name = "test-gainer-reports", description= "Test posting premarket gainer reports",)
    async def test_premarket_reports(self, interaction: discord.Interaction):
        logger.info("/test-premarket-reports function called by user {}".format(interaction.user.name))
        await interaction.response.defer(ephemeral=True)

        report = GainerReport()
        await report.send_report()

        await interaction.followup.send("Posted premarket reports", ephemeral=True)




#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Tests(bot))
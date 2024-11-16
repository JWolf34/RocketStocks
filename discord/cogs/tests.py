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

    @app_commands.command(name = "help", description= "Show help on the bot's commands",)
    async def help(self, interaction: discord.Interaction):
        logger.info("/help function called by user {}".format(interaction.user.name))
        embed = discord.Embed()
        embed.title = 'RocketStocks Help'
        for command in client.tree.get_commands():
            embed.add_field(name=command.name, value=command.description)
        await interaction.response.send_message(embed=embed)
    

    @app_commands.command(name = "test-daily-download-analyze-data", description= "Test running the logic for daily data download and indicator generation",)
    async def test_daily_download_analyze_data(self, interaction: discord.Interaction):
        logger.info("/test-daily-download-analyze-data function called by user {}".format(interaction.user.name))
        await interaction.response.send_message("Running daily download and analysis", ephemeral=True)
        download_data_thread = threading.Thread(target=sd.daily_download_analyze_data)
        download_data_thread.start()

    @app_commands.command(name = "test-minute-download-data", description= "Test running the logic for weekly minute-by-minute data download",)
    async def test_minutes_download_data(self, interaction: discord.Interaction):
        logger.info("/test-minute-download-data function called by user {}".format(interaction.user.name))

        await interaction.response.send_message("Running daily download and analysis", ephemeral=True)
        download_data_thread = threading.Thread(target=sd.minute_download_data)
        download_data_thread.start()

    @app_commands.command(name = "test-run-strategy-report", description= "Test running the strategy report",)
    async def test_run_strategy_report(self, interaction: discord.Interaction):
        logger.info("/test-run-strategy-report function called by user {}".format(interaction.user.name))
        await interaction.response.defer(ephemeral=True)

        await send_strategy_reports()

        await interaction.followup.send("Posted strategy report", ephemeral=True)

    @app_commands.command(name = "test-run-watchlist-report", description= "Test running the strategy report",)
    async def test_run_watchlist_report(self, interaction: discord.Interaction):
        logger.info("/test-run-watchlist-report function called by user {}".format(interaction.user.name))
        await interaction.response.defer(ephemeral=True)

        await send_watchlist_reports()

        await interaction.followup.send("Posted watchlist report", ephemeral=True)

   
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
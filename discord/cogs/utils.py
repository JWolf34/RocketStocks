import discord
from discord import app_commands
from discord.ext import commands
import stockdata as sd
import logging

# Logging configuration
logger = logging.getLogger(__name__)

class Utils(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    @app_commands.command(name='sync', description='Sync bot commands to the server')
    @commands.is_owner()
    async def sync(self, interaction:discord.Interaction):
        await self.bot.tree.sync()
        await interaction.response.send_message("Bot commands synced!", ephemeral=True)

    @app_commands.command(name = "help", description= "Show help on the bot's commands",)
    async def help(self, interaction: discord.Interaction):
        logger.info("/help function called by user {}".format(interaction.user.name))
        embed = discord.Embed()
        embed.title = 'RocketStocks Help'
        for command in client.tree.get_commands():
            embed.add_field(name=command.name, value=command.description)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name='force-update-5m-data', description="Forcefully update the 5m price history db table")
    @commands.is_owner()
    async def force_update_5m_data(self, interaction:discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        logger.info("/force-update-5m-data function called by user {}".format(interaction.user.name))
        sd.StockData.update_5m_price_history(override_schedule=True)
        await interaction.followup.send("5m price history table updated")
        




#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Utils(bot))
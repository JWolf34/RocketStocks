import discord
from discord import app_commands
from discord.ext import commands
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


#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Utils(bot))
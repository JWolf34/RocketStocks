import discord
from discord import app_commands
from discord.ext import commands
import logging


# Logging configuration
logger = logging.getLogger(__name__)

class Utils(commands.Cog):
    def __init__(self, bot:commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")
    
    @app_commands.command(name='sync', description='Sync bot commands to the server')
    @app_commands.checks.has_permissions(administrator=True)
    async def sync(self, interaction:discord.Interaction):
        """Sync bot commands to Discord's servers. Use this after adding or removing an app command"""
        logger.info("/sync command called by user {}".format(interaction.user.name))
        await self.bot.tree.sync()
        await interaction.response.send_message("Bot commands synced!", ephemeral=True)
        logger.info("Bot commands synced!")

    @app_commands.command(name = "help", description= "Show help on the bot's commands",)
    async def help(self, interaction: discord.Interaction):
        """Post message in discord with documentation on the bot's commands"""
        logger.info("/help function called by user {}".format(interaction.user.name))
        embeds = []

        # Iterate over each cog and generate embed with cog's commands
        for cog_name, cog in self.bot.cogs.items():
            commands = cog.get_app_commands()
            if commands:
                embed = discord.Embed()
                embed.title = cog_name
                for command in commands:
                    # Only show commands that the user meets checks for
                    if all([check(interaction) for check in command.checks]):
                        embed.add_field(name=f"/{command.name}", value=command.description)
                # Only add embed if at least command is available to that user in a cog
                if embed.fields:
                    embeds.append(embed)
        await interaction.response.send_message(embeds=embeds, ephemeral=True)

    




#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Utils(bot))
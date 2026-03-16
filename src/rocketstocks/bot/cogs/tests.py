"""Tests cog — retained for backwards compatibility; admin commands moved to admin.py."""
import logging

from discord.ext import commands

logger = logging.getLogger(__name__)


class Tests(commands.Cog):
    def __init__(self, bot: commands.Bot, stock_data):
        self.bot = bot
        self.stock_data = stock_data

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")


async def setup(bot):
    await bot.add_cog(Tests(bot, bot.stock_data))

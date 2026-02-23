import discord
from discord import app_commands
from discord.ext import commands
from src.rocketstocks.data.stockdata import StockData
from rocketstocks.core.utils.market import market_utils
from rocketstocks.core.config.settings import screeners_channel_id
import logging

logger = logging.getLogger(__name__)


class Tests(commands.Cog):
    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data
        self.gainers_channel = self.bot.get_channel(screeners_channel_id)

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.command(name="test-gainer-reports", description="Test posting premarket gainer reports",)
    async def test_premarket_reports(self, interaction: discord.Interaction):
        logger.info(f"/test-premarket-reports function called by user {interaction.user.name}")
        await interaction.response.defer(ephemeral=True)
        reports = self.bot.get_cog("Reports")
        if market_utils().get_market_period() == "EOD":
            await interaction.followup.send("Market is closed - cannot post gainer reports", ephemeral=True)
        else:
            await interaction.followup.send("Gainer reports test complete!", ephemeral=True)

    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.command(name='force-update-5m-data', description="Forcefully update the 5m price history db table")
    async def force_update_5m_data(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/force-update-5m-data function called by user {interaction.user.name}")
        tickers = self.stock_data.tickers.get_all_tickers()
        await self.stock_data.price_history.update_5m_price_history(tickers)
        await interaction.followup.send("5m price history table updated")

    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.command(name='force-update-daily-data', description="Forcefully update the 5m price history db table")
    async def force_update_daily_data(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/force-update-daily-data function called by user {interaction.user.name}")
        tickers = self.stock_data.tickers.get_all_tickers()
        await self.stock_data.price_history.update_daily_price_history(tickers)
        await interaction.followup.send("Daily price history table updated")


async def setup(bot):
    await bot.add_cog(Tests(bot, bot.stock_data))

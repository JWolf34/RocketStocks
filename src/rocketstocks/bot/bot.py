import os
import pathlib
import logging
import discord
from discord.ext import commands
from rocketstocks.data.stock_data import StockData
from rocketstocks.data.db import Postgres
from rocketstocks.core.config.secrets import secrets
from rocketstocks.core.config.paths import validate_path, datapaths

logger = logging.getLogger(__name__)

intents = discord.Intents.default()
bot = commands.Bot(command_prefix='$', intents=intents)
token = secrets.discord_token


def _attach_discord_file_handler() -> None:
    """Route discord library logs to the file handler (WARNING+ only)."""
    discord_logger = logging.getLogger("discord")
    discord_logger.setLevel(logging.WARNING)
    for handler in logging.getLogger().handlers:
        if handler.name == "file":
            discord_logger.addHandler(handler)
            break


async def load():
    """Dynamically load all cog modules from the bot/cogs/ package directory."""
    cogs_dir = pathlib.Path(__file__).parent / 'cogs'
    for filename in os.listdir(cogs_dir):
        if filename.endswith(".py") and not filename.startswith("_"):
            await bot.load_extension(f"rocketstocks.bot.cogs.{filename[:-3]}")
    logger.info("Loaded extensions")


def run_bot(stock_data: StockData):
    _attach_discord_file_handler()

    @bot.event
    async def on_ready():
        logger.info("RocketStocks bot ready!")
        await load()
        # Create database tables that do not exist and ensure data paths exist
        Postgres().create_tables()
        validate_path(datapaths.attachments_path)

    bot.stock_data = stock_data
    bot.run(token)

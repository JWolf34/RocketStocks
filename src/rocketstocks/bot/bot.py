import os
import pathlib
import logging
import discord
from discord.ext import commands
from rocketstocks.data.stockdata import StockData
from rocketstocks.data.db import Postgres
from rocketstocks.core.config.secrets import secrets
from rocketstocks.core.config.paths import validate_path, datapaths
from rocketstocks.core.notifications import EventEmitter, NotificationConfig

logger = logging.getLogger(__name__)


def create_bot() -> commands.Bot:
    """Create and return a new Bot instance."""
    intents = discord.Intents.default()
    return commands.Bot(command_prefix='$', intents=intents)


def _attach_discord_file_handler() -> None:
    """Route discord library logs to the file handler (WARNING+ only)."""
    discord_logger = logging.getLogger("discord")
    discord_logger.setLevel(logging.WARNING)
    for handler in logging.getLogger().handlers:
        if handler.name == "file":
            discord_logger.addHandler(handler)
            break


async def load(bot: commands.Bot):
    """Dynamically load all cog modules from the bot/cogs/ package directory."""
    cogs_dir = pathlib.Path(__file__).parent / 'cogs'
    for filename in os.listdir(cogs_dir):
        if filename.endswith(".py") and not filename.startswith("_"):
            await bot.load_extension(f"rocketstocks.bot.cogs.{filename[:-3]}")
    logger.info("Loaded extensions")


def run_bot(stock_data: StockData, emitter: EventEmitter, notification_config: NotificationConfig):
    _attach_discord_file_handler()
    bot = create_bot()

    @bot.event
    async def on_ready():
        logger.info("RocketStocks bot ready!")
        await load(bot)
        # Create database tables that do not exist and ensure data paths exist
        Postgres().create_tables()
        validate_path(datapaths.attachments_path)

    bot.stock_data = stock_data
    bot.emitter = emitter
    bot.notification_config = notification_config
    bot.run(secrets.discord_token)

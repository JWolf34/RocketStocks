import os
import pathlib
import logging
import discord
from discord.ext import commands
from rocketstocks.data.stockdata import StockData
from rocketstocks.data.channel_config import REPORTS, ALERTS, SCREENERS, CHARTS, NOTIFICATIONS
from rocketstocks.data.schema import create_tables
from rocketstocks.core.config.secrets import secrets
from rocketstocks.core.config.paths import validate_path, datapaths
from rocketstocks.core.notifications import EventEmitter, NotificationConfig

logger = logging.getLogger(__name__)

_LEGACY_ENV_MAP = {
    REPORTS: "REPORTS_CHANNEL_ID",
    ALERTS: "ALERTS_CHANNEL_ID",
    SCREENERS: "SCREENERS_CHANNEL_ID",
    CHARTS: "CHARTS_CHANNEL_ID",
    NOTIFICATIONS: "NOTIFICATIONS_CHANNEL_ID",
}


class RocketStocksBot(commands.Bot):
    """discord.py Bot subclass with per-guild channel routing helpers."""

    stock_data: StockData
    emitter: EventEmitter
    notification_config: NotificationConfig

    def get_channel_for_guild(self, guild_id: int, config_type: str) -> discord.TextChannel | None:
        """Return the configured TextChannel for (guild_id, config_type), or None."""
        channel_id = self.stock_data.channel_config.get_channel_id(guild_id, config_type)
        if channel_id is None:
            return None
        channel = self.get_channel(channel_id)
        if channel is None:
            logger.warning(f"Channel {channel_id} for guild={guild_id} type={config_type} not in cache")
        return channel

    def iter_channels(self, config_type: str):
        """Yield (guild_id, TextChannel) for every guild that has config_type configured."""
        for guild_id, channel_id in self.stock_data.channel_config.get_all_guilds_for_type(config_type):
            channel = self.get_channel(channel_id)
            if channel is not None:
                yield guild_id, channel
            else:
                logger.warning(f"Skipping guild={guild_id} type={config_type}: channel {channel_id} not in cache")


def create_bot() -> RocketStocksBot:
    """Create and return a new RocketStocksBot instance."""
    intents = discord.Intents.default()
    return RocketStocksBot(command_prefix='$', intents=intents)


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


async def _migrate_legacy_channel_config(bot: RocketStocksBot) -> None:
    """One-time migration: seed channel_config from legacy env vars.

    For every guild the bot is currently in that has no channel config at all,
    reads the 5 old channel env vars from os.getenv() and inserts them.
    Safe to call on every startup — guilds that already have config are skipped.
    """
    channel_map = {
        config_type: os.getenv(env_var)
        for config_type, env_var in _LEGACY_ENV_MAP.items()
    }

    # If none of the legacy vars are set, skip entirely
    if not any(channel_map.values()):
        return

    for guild in bot.guilds:
        existing = bot.stock_data.channel_config.get_all_for_guild(guild.id)
        if existing:
            logger.debug(f"Migration: guild={guild.id} already has config, skipping")
            continue

        for config_type, channel_id_str in channel_map.items():
            if channel_id_str and channel_id_str != '0':
                channel_id = int(channel_id_str)
                bot.stock_data.channel_config.upsert_channel(guild.id, config_type, channel_id)
                logger.info(f"Migration: seeded {config_type}={channel_id} for guild={guild.id} ({guild.name})")


def run_bot(stock_data: StockData, emitter: EventEmitter, notification_config: NotificationConfig):
    _attach_discord_file_handler()
    bot = create_bot()

    @bot.event
    async def on_ready():
        logger.info("RocketStocks bot ready!")
        await load(bot)
        # Create database tables that do not exist
        create_tables(bot.stock_data.db)
        validate_path(datapaths.attachments_path)
        # Seed channel config from legacy env vars (no-op once migrated)
        await _migrate_legacy_channel_config(bot)
        # Sync slash commands to all guilds
        await bot.tree.sync()

    bot.stock_data = stock_data
    bot.emitter = emitter
    bot.notification_config = notification_config
    bot.run(secrets.discord_token)

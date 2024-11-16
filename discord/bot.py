import sys
sys.path.append('../RocketStocks')
import os
import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
import asyncio
import stockdata as sd
import analysis as an
import datetime as dt
import threading
import logging
import numpy as np
import strategies
import math
import datetime
import json
from table2ascii import table2ascii

# Logging configuration
logger = logging.getLogger(__name__)

# Paths for writing data
ATTACHMENTS_PATH = "discord/attachments"
DAILY_DATA_PATH = "data/CSV/daily"
MINUTE_DATA_PATH = "data/CSV/minute"
UTILS_PATH = "data/utils"

##################
# Init Functions #
##################

def run_bot():

    intents = discord.Intents.default()
    bot = commands.Bot(command_prefix='$', intents=intents)

    def get_bot_token():
        logger.debug("Fetching Discord bot token")
        try:
            token = os.getenv('DISCORD_TOKEN')
            logger.debug("Successfully fetched token")
            return token
        except Exception as e:
            logger.exception("Failed to fetch Discord bot token\n{}".format(e))
            return ""

    async def load():
        for filename in os.listdir("./discord/cogs"):
            if filename.endswith(".py"):
                await bot.load_extension(f"cogs.{filename[:-3]}")

    @bot.event
    async def on_ready():
        print("Bot ready!")
        await load()

    bot.run(get_bot_token())

if __name__ == "__main__":
    run_bot()


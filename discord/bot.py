import sys
sys.path.append('../RocketStocks')
import os
import discord
from discord.ext import commands
import asyncio
import utils
import logging


# Logging configuration
logger = logging.getLogger(__name__)

discord_logger = logging.getLogger('discord')
for handler in logger.handlers:
    if handler.name == 'file':
        discord_logger.addHandler(handler)

# Bot setup
intents = discord.Intents.default()
bot = commands.Bot(command_prefix='$', intents=intents)
token = utils.secrets.discord_token

# Load cogs
async def load():
    for filename in os.listdir("./discord/cogs"):
        if filename.endswith(".py"):
            await bot.load_extension(f"cogs.{filename[:-3]}")
    logger.info("Loaded extensions")

# Start bot
def run_bot(stock_data):
    @bot.event
    async def on_ready():
        logger.info("RocketStocks bot ready!")
        await load()
        utils.bot_setup()
    
    bot.stock_data = stock_data # StockData
    bot.run(token)




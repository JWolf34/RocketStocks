import sys
sys.path.append('../RocketStocks')
import os
import discord
from discord.ext import commands
import asyncio
import config
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
token = config.secrets.discord_token

# Load cogs
async def load():
    for filename in os.listdir("./discord/cogs"):
        if filename.endswith(".py"):
            await bot.load_extension(f"cogs.{filename[:-3]}")
    logger.info("Loaded extensions")

# Start bot
def run_bot():
    @bot.event
    async def on_ready():
        logger.info("RocketStocks bot ready!")
        await load()
        config.bot_setup()
    
    
    bot.run(token)




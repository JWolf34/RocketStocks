import sys
sys.path.append('../RocketStocks')
import os
import discord
from discord import app_commands
from discord.ext import commands
import asyncio
import config
import logging


# Logging configuration
logger = logging.getLogger(__name__)

intents = discord.Intents.default()
bot = commands.Bot(command_prefix='$', intents=intents)
token = config.get_discord_token()

async def load():
    for filename in os.listdir("./discord/cogs"):
        if filename.endswith(".py"):
            await bot.load_extension(f"cogs.{filename[:-3]}")
    logger.info("Loaded extensions")

def run_bot():
    @bot.event
    async def on_ready():
        logger.info("RocketStocks bot ready!")
        await load()
    
    
    bot.run(token)




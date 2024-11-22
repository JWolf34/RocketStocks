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

def run_bot():

    intents = discord.Intents.default()
    bot = commands.Bot(command_prefix='$', intents=intents)
    token = config.get_discord_token()

    async def load():
        for filename in os.listdir("./discord/cogs"):
            if filename.endswith(".py"):
                logger.info(f"Loading {filename}")
                await bot.load_extension(f"cogs.{filename[:-3]}")

    @bot.event
    async def on_ready():
        logger.info("RocketStocks bot ready!")
        await load()
        #await bot.tree.sync()
    
    bot.run(token)

if __name__ == "__main__":
    run_bot()


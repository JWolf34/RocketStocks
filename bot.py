import os
import discord
from discord import app_commands
from discord.ext import commands
import asyncio
import json
import yfinance
import responses

async def send_message(message, user_message, is_private):
    try:
        response = ''
    except Exception as e:
        print(e)

def run_bot():
    with open('config.json') as config_file:
        data = json.load(config_file)

    TOKEN = data['discord-token']

    intents = discord.Intents.default()
    client = commands.Bot(command_prefix='$', intents=intents)

    @client.event
    async def on_ready():
        try:
            await client.tree.sync()
            print(f"Synced {len(synced)} command(s)")
        except Exception as e:
            print(e)
        print('Connected!')

    @client.tree.command(name = "addticker", description= "Add a new stock ticker for the bot to watch",)
    @app_commands.describe(ticker = "Ticker to add to watchlist")
    async def addticker(interaction: discord.Interaction, ticker: str):

        await interaction.response.send_message(ticker)
        '''
        with open ("tickers.txt", "a") as tickers:
            tickers.write(arg)
        ctx.send("Added ticker: " + arg)
        '''
    
    client.run(TOKEN)

if __name__ == "__main__":
    run_bot()

    
    
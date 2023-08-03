import sys
sys.path.append('../RocketStocks')
import os
import discord
from discord import app_commands
from discord.ext import commands
import asyncio
import json
import yfinance as yf
import stockdata as sd

async def send_message(message, user_message, is_private):
    try:
        response = ''
    except Exception as e:
        print(e)

def run_bot():
    with open('discord/config.json') as config_file:
        data = json.load(config_file)

    TOKEN = data['discord-token']

    intents = discord.Intents.default()
    client = commands.Bot(command_prefix='$', intents=intents)

    @client.event
    async def on_ready():
        try:
            await client.tree.sync()
        except Exception as e:
            print(e)
        print('Connected!')

    @client.tree.command(name = "addticker", description= "Add a new stock ticker for the bot to watch",)
    @app_commands.describe(ticker = "Ticker to add to watchlist")
    async def addticker(interaction: discord.Interaction, ticker: str):

        tickers = open('data/tickers.txt', 'a')
        tickers.write(ticker + "\n")
        await interaction.response.send_message("Added " + ticker + "  to the watchlist")

    @client.tree.command(name = "removeticker", description= "Remove a stock ticker from the watchlist",)
    @app_commands.describe(ticker = "Ticker to remove from watchlist")
    async def removeticker(interaction: discord.Interaction, ticker: str):

        symbols = sd.get_tickers()

        message = ticker + " does not exist in the watchlist"
        with open("discord/tickers.txt", 'w') as watchlist:
            for symbol in symbols:
                if symbol != ticker:
                    watchlist.write(symbol + "\n")
                else:
                    message =  "Removed " + ticker + " from the watchlist"

        await interaction.response.send_message(message)

    @client.tree.command(name = "watchlist", description= "List the tickers on the watchlist",)
    async def watchlist(interaction: discord.Interaction):
        tickers = sd.get_tickers()
        message = "Watchlist: " + ', '.join(tickers)
        await interaction.response.send_message(message)

    @client.tree.command(name = "news-all", description= "Get the news on all the tickets on your watchlist",)
    async def newsall(interaction: discord.Interaction):
        tickers = sd.get_tickers()
        embed = sd.get_news(tickers)
        await interaction.response.send_message(embed=embed)

    @client.tree.command(name = "news", description= "Get the news from all tickers given in a comma-separated list",)
    async def news(interaction: discord.Interaction, tickers: str):
        tickerList = []
        for ticker in tickers.split(','):
            tickerList.append(ticker)

        embed = discord.Embed()
        message = ''
        for ticker in tickerList:
            message += sd.get_news(ticker)
        embed.description = message
        await interaction.response.send_message(embed=embed)

    @client.tree.command(name = "fetch", description= "Returns data file for input ticker",)
    async def fetch(interaction: discord.Interaction, ticker: str):
        try:
            file = discord.File("data/" + ticker + ".csv")
            await interaction.response.send_message(file=file, content= "Data file for " + ticker)
        except Exception:
            await interaction.response.send_message("No data file for " + ticker + " available")
        

    client.run(TOKEN)
    

if __name__ == "__main__":
    run_bot()

    
    
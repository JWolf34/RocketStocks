import sys
sys.path.append('../RocketStocks')
import os
import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
import asyncio
import json
import yfinance as yf
import stockdata as sd
import analysis as an
import datetime as dt



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
            send_reports.start()
        except Exception as e:
            print(e)
        print('Connected!')

    @client.tree.command(name = "addticker", description= "Add a new stock ticker for the bot to watch",)
    @app_commands.describe(ticker = "Ticker to add to watchlist")
    async def addticker(interaction: discord.Interaction, ticker: str):

        tickers = open('data/tickers.txt', 'a')
        tickers.write("\n" + ticker)
        tickers.close()
        await interaction.response.send_message("Added " + ticker + "  to the watchlist")

    @client.tree.command(name = "removeticker", description= "Remove a stock ticker from the watchlist",)
    @app_commands.describe(ticker = "Ticker to remove from watchlist")
    async def removeticker(interaction: discord.Interaction, ticker: str):

        symbols = sd.get_tickers()

        message = ticker + " does not exist in the watchlist"
        with open("data/tickers.txt", 'w') as watchlist:
            for i in range(0, len(symbols)):
                if symbols[i] != ticker:
                    if i == len(symbols) - 1:
                        watchlist.write(symbols[i])
                    else:
                        watchlist.write(symbols[i] + "\n")
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
    
    @tasks.loop(hours=24)  
    async def send_reports():

        # Configure channel to send reports to
        channel = await client.fetch_channel('1150890013471555705')
        
        for ticker in sd.get_tickers():

            # Get techincal indicator charts and convert them to a list of discord File objects
            files = sd.fetch_charts(ticker)
            for i in range(0, len(files)):
                files[i] = discord.File(files[i])

            # Append message based on analysis of indicators

            message = "**" + ticker + " Analysis " + dt.date.today().strftime("%m/%d/%Y") + "**\n\n"

            analysis = sd.fetch_analysis(ticker)

            for indicator in analysis:
                message += indicator
            
            await channel.send(message, files=files)
        
    client.run(TOKEN)
    

if __name__ == "__main__":
    run_bot()

    
    
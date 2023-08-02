import sys
sys.path.append('..')
import os
import discord
from discord import app_commands
from discord.ext import commands
import asyncio
import json
import yfinance as yf
import rocketstocks.stockdata as sd

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

        tickers = open('tickers.txt', 'a')
        tickers.write(ticker + "\n")
        await interaction.response.send_message("Added " + ticker + "  to the watchlist")

    @client.tree.command(name = "removeticker", description= "Remove a stock ticker from the watchlist",)
    @app_commands.describe(ticker = "Ticker to remove from watchlist")
    async def removeticker(interaction: discord.Interaction, ticker: str):

        with open("tickers.txt", 'r') as watchlist:
            symbols = watchlist.read().splitlines()

        message = ticker + " does not exist in the watchlist"
        with open("tickers.txt", 'w') as watchlist:
            for symbol in symbols:
                if symbol != ticker:
                    watchlist.write(symbol + "\n")
                else:
                    message =  "Removed " + ticker + " from the watchlist"

        await interaction.response.send_message(message)

    @client.tree.command(name = "watchlist", description= "List the tickers on the watchlist",)
    async def watchlist(interaction: discord.Interaction):
        tickers = open("tickers.txt", 'r').read().splitlines()
        message = "Watchlist: " + ','.join(tickers)
        await interaction.response.send_message(message)

    @client.tree.command(name = "news-all", description= "Get the news on all the tickets on your watchlist",)
    async def newsall(interaction: discord.Interaction):
        tickers = open("tickers.txt").read().splitlines()
        embed = get_news(tickers)
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


    def get_news(tickers):
        embed = discord.Embed()
        message = ''
        for ticker in tickers:
            stock = yf.Ticker(ticker)
            articles = stock.news
            uuids_txt = open("tickers.txt", 'a')
            uuids_values = open("tickers.txt", 'r').read().splitlines()
            titles = []
            links = []
            for article in articles:
                if article['uuid'] in uuids_values:
                    pass
                else: 
                    titles.append(article['title']) 
                    links.append(article['link'])
            if len(titles) > 0:
                description = ''
                for i in range(0, len(titles)):
                    description += "[" + titles[i] + "]" + "(" + links[i] + ")" + "\n"
                
                message += ticker + ": \n" + description + "\n"
                
        embed.description = message
        return embed
        

    client.run(TOKEN)
    

if __name__ == "__main__":
    run_bot()

    
    
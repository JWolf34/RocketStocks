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

    @client.tree.command(name = "addticker", description= "Add a new stock ticker for the bot to watch and generate analysis for the ticker",)
    @app_commands.describe(ticker = "Ticker to add to watchlist")
    @app_commands.choices(watchlist =[
        app_commands.Choice(name = "global", value = 'global'),
        app_commands.Choice(name = "personal", value = 'personal')
    ])
    async def addticker(interaction: discord.Interaction, ticker: str, watchlist: app_commands.Choice[str]):
        ticker = ticker.upper()
        if(sd.validate_ticker(ticker)):
            await interaction.response.defer(ephemeral=True)
            if watchlist.value == 'personal':
                user_id = interaction.user.id
                if not (os.path.isdir("watchlists/{}".format(user_id))):
                    os.makedirs("watchlists/{}".format(user_id))
                    file = open("watchlists/{}/watchlist.txt".format(user_id), 'a')
                    file.close()

                symbols = sd.get_tickers(user_id)
                if (ticker in symbols):
                    await interaction.followup.send(ticker + " is already on your watchlist", ephemeral=True)
                else:
                    symbols.append(ticker)
                    symbols.sort()
                    with open('watchlists/{}/watchlist.txt'.format(user_id), 'w') as watchlist:
                        watchlist.write("\n".join(symbols))
                        an.run_analysis([ticker])
                        await interaction.followup.send("Added " + ticker + " to your watchlist!", ephemeral=True)
            else:
                await interaction.response.defer(ephemeral=True)
                if not (os.path.isdir("watchlists/global")):
                    os.makedirs("watchlists/global")
                    file = open("watchlists/global/watchlist.txt", 'a')
                    file.close()
                
                symbols = sd.get_tickers()
                if (ticker in symbols):
                    await interaction.followup.send(ticker + " is already on the global watchlist")
                else: 
                    symbols.append(ticker)
                    symbols.sort()
                    with open('watchlists/global/watchlist.txt', 'w') as watchlist:
                        watchlist.write("\n".join(symbols))
                        an.run_analysis([ticker])
                        await interaction.followup.send("Added " + ticker + " to the global watchlist!")
        else:
            await interaction.response.send_message(ticker + " is not a valid ticker", ephemeral=True)


    @client.tree.command(name = "removeticker", description= "Remove a stock ticker from the watchlist",)
    @app_commands.describe(ticker = "Ticker to remove from watchlist")
    @app_commands.choices(watchlist =[
        app_commands.Choice(name = "global", value = 'global'),
        app_commands.Choice(name = "personal", value = 'personal')
    ])
    async def removeticker(interaction: discord.Interaction, ticker: str, watchlist: app_commands.Choice[str]):
        
        ticker = ticker.upper()
        # Handle personal watchlist use case
        if watchlist.value == 'personal':
            user_id = interaction.user.id
            if not (os.path.isdir("watchlists/{}".format(user_id))):
                os.makedirs("watchlists/{}".format(user_id))
                file = open("watchlists/{}/watchlist.txt".format(user_id), 'a')
                file.close()

            symbols = sd.get_tickers(user_id)
            if (ticker not in symbols):
                await interaction.response.send_message(ticker + " is not on your watchlist", ephemeral=True)
            else:
                symbols.remove(ticker)
                symbols.sort()
                with open('watchlists/{}/watchlist.txt'.format(user_id), 'w') as watchlist:
                    watchlist.write("\n".join(symbols))
                    await interaction.response.send_message("Removed " + ticker + " to your watchlist", ephemeral=True)

        # Handle global watchlist use case            
        else:
            user_id = interaction.user.id
            if not (os.path.isdir("watchlists/global".format(user_id))):
                os.makedirs("watchlists/global".format(user_id))
                file = open("watchlists/global/watchlist.txt".format(user_id), 'a')
                file.close()

            symbols = sd.get_tickers()
            if (ticker not in symbols):
                await interaction.response.send_message(ticker + " is not on the global watchlist")
            else:
                symbols.remove(ticker)
                symbols.sort()
                with open('watchlists/global/watchlist.txt'.format(user_id), 'w') as watchlist:
                    watchlist.write("\n".join(symbols))
                    await interaction.response.send_message("Removed " + ticker + " from the global watchlist")

    @client.tree.command(name = "watchlist", description= "List the tickers on the watchlist",)
    @app_commands.choices(watchlist =[
        app_commands.Choice(name = "global", value = 'global'),
        app_commands.Choice(name = "personal", value = 'personal')
    ])
    async def watchlist(interaction: discord.Interaction, watchlist: app_commands.Choice[str]):
        if watchlist.value == 'personal':
            user_id = interaction.user.id
            tickers = sd.get_tickers(user_id)
            message = "Watchlist: " + ', '.join(tickers)
            await interaction.response.send_message(message, ephemeral=True)
        else:
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
    
    @client.tree.command(name = "run-analysis", description= "Force the bot to run analysis on all tickers in a given watchlist",)
    @app_commands.choices(watchlist =[
        app_commands.Choice(name = "global", value = 'global'),
        app_commands.Choice(name = "personal", value = 'personal')
    ])
    async def runanalysis(interaction: discord.Interaction, watchlist: app_commands.Choice[str]):
        
        if watchlist.value == 'personal':
            user_id = interaction.user.id
            try:
                tickers = sd.get_tickers(user_id)
                await interaction.response.defer(ephemeral=True)
                an.run_analysis(tickers)
                await interaction.followup.send("Analysis complete!")
            except Exception as e:
                await interaction.response.send_message("Analysis failed. Do you have an existing watchlist?", ephemeral=True)
        else: 
            try:
                tickers = sd.get_tickers()
                await interaction.response.defer(ephemeral=True)
                an.run_analysis(tickers)
                await interaction.followup.send("Analysis complete!")
            except Exception as e:
                await interaction.response.send_message("Analysis failed. Is there an invalid ticker on the watchlist?", ephemeral=True)
        
        

    @tasks.loop(hours=24)  
    async def send_reports():

        if (dt.datetime.now().weekday() < 5):
            
            #Send out global reports

            # Configure channel to send reports to
            channel = await client.fetch_channel('1150890013471555705')
            
            for ticker in sd.get_tickers():
                report = build_report(ticker)
                message, files = report.get('message'), report.get('files')
                await channel.send(message, files=files)


        else:
            pass
     
    @send_reports.before_loop
    async def delay_send_reports():
        hour = 15
        minute = 5
        now = dt.datetime.now()
        future = dt.datetime(now.year, now.month, now.day, hour, minute)
        if now.hour >= hour and now.minute > minute:
            future += dt.timedelta(days=1)
        await asyncio.sleep((future-now).seconds)

    @client.tree.command(name = "run-reports", description= "Force the bot to post analysis of a given watchlist",)
    @app_commands.choices(watchlist =[
        app_commands.Choice(name = "global", value = 'global'),
        app_commands.Choice(name = "personal", value = 'personal')
    ])
    async def runreports(interaction: discord.Interaction, watchlist: app_commands.Choice[str]):
        if watchlist.value == 'personal':
            user_id = interaction.user.id
            try:
                tickers = sd.get_tickers(user_id)
                user = interaction.user
                await interaction.response.defer(ephemeral=True)
                for ticker in tickers:
                    report = build_report(ticker)
                    message, files = report.get('message'), report.get('files')
                    await user.send(message, files=files)
                await interaction.followup.send("Reports have been posted!", ephemeral=True)
            except Exception as e:
                await interaction.response.send_message("Reports failed to run. Do you have an existing watchlist?", ephemeral=True)
        else: 
            try:
                channel = await client.fetch_channel('1150890013471555705')
                tickers = sd.get_tickers()
                await interaction.response.defer(ephemeral=True)
                for ticker in tickers:
                    report = build_report(ticker)
                    message, files = report.get('message'), report.get('files')
                    await channel.send(message, files=files)
                await interaction.followup.send("Reports have been posted!")
            except Exception as e:
                await interaction.response.send_message("Reports failed to run. Is there an invalid ticker on thw watchlist?", ephemeral=True)


    @client.tree.command(name = "fetch-reports", description= "Fetch analysis reports of the specified tickers",)
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])        
    async def fetchreports(interaction: discord.interactions, tickers: str, visibility: app_commands.Choice[str]):
        
        await interaction.response.defer(ephemeral=True)

        tickers = tickers.split(',')

        # Validate each ticker in the list is valid
        for ticker in tickers:
            if(not sd.validate_ticker(ticker)):
                tickers.remove(ticker)

        an.run_analysis(tickers)

        if visibility.value == 'private':
            for ticker in tickers:
                report = build_report(ticker)
                message, files = report.get('message'), report.get('files')
                await interaction.user.send(message, files=files)
        else:
            for ticker in tickers:
                report = build_report(ticker)
                message, files = report.get('message'), report.get('files')
                await interaction.channel.send(message, files=files)

        await interaction.followup.send("Fetched reports!", ephemeral=True)
            

    def build_report(ticker):

        # Get techincal indicator charts and convert them to a list of discord File objects
        files = sd.fetch_charts(ticker)
        for i in range(0, len(files)):
            files[i] = discord.File(files[i])

        # Append message based on analysis of indicators

        message = "**" + ticker + " Analysis " + dt.date.today().strftime("%m/%d/%Y") + "**\n\n"

        analysis = sd.fetch_analysis(ticker)

        for indicator in analysis:
            message += indicator
        
        report = {'message':message, 'files':files}

        return report

    @client.tree.command(name = "test-run-reports", description= "Force the bot to post reports in a testing channel",)
    async def run_reports_test(interaction: discord.Interaction):
        # Configure channel to send reports to
        channel = await client.fetch_channel('1113281014677123084')
        for ticker in sd.get_tickers():

            # Get techincal indicator charts and convert them to a list of discord File objects
            files = sd.fetch_charts(ticker)
            for i in range(0, len(files)):
                files[i] = discord.File(files[i])

            # Append message based on analysis of indicators

            message = "\n**" + ticker + " Analysis " + dt.date.today().strftime("%m/%d/%Y") + "**\n\n"

            analysis = sd.fetch_analysis(ticker)

            for indicator in analysis:
                message += indicator
            
            await channel.send(message, files=files)
            
    @client.tree.command(name = "help", description= "Show help on the bot's commands",)
    async def help(interaction: discord.Interaction):
        embed = discord.Embed()
        embed.title = 'RocketStocks Help'
        for command in client.tree.get_commands():
            embed.add_field(name=command.name, value=command.description)
        await interaction.response.send_message(embed=embed)
    client.run(TOKEN)
    

if __name__ == "__main__":
    run_bot()

    
    
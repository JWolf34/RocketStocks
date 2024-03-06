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

    @client.tree.command(name = "addtickers", description= "Add tickers to the selected watchlist",)
    @app_commands.describe(tickers = "Ticker to add to watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.choices(watchlist =[
        app_commands.Choice(name = "global", value = 'global'),
        app_commands.Choice(name = "personal", value = 'personal')
    ])
    async def addtickers(interaction: discord.Interaction, tickers: str, watchlist: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        
        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)

        watchlist_path = ""
        symbols = ""
        message_flavor = ""

        # Get watchlist path and watchlist contents based on value of watchlist input
        if watchlist.value == 'personal':
                user_id = interaction.user.id
                watchlist_path = sd.get_watchlist_path(user_id)
                symbols = sd.get_tickers(user_id)
                message_flavor = "your"
        else:
            watchlist_path = sd.get_watchlist_path()
            symbols = sd.get_tickers()
            message_flavor = "the global"

        # Create watchlist if not present    
        if not (os.path.isdir(watchlist_path)):
            os.makedirs(watchlist_path)
            file = open("{}/watchlist.txt".format(watchlist_path), 'a')
            file.close()
    
        # Add ticker to watchlist if not already present
        for ticker in tickers:
            if (ticker in symbols):
                pass
            else:
                symbols.append(ticker)
                symbols.sort()
                with open('{}/watchlist.txt'.format(watchlist_path), 'w') as watchlist:
                    watchlist.write("\n".join(symbols))
                    watchlist.close()
                    
        
        if len(tickers) > 0 and len(invalid_tickers) > 0:
            await interaction.followup.send("Added {} to {} watchlist but could not add the following tickers: {}".format(", ".join(tickers), message_flavor, ", ".join(invalid_tickers)), ephemeral=True)
        elif len(tickers) > 0:
            await interaction.followup.send("Added {} to {} watchlist!".format(", ".join(tickers), message_flavor), ephemeral=True)
        else:
            await interaction.followup.send("No tickers added to {} watchlist. Invalid tickers: {}".format(message_flavor, ", ".join(invalid_tickers)), ephemeral=True)


    @client.tree.command(name = "removetickers", description= "Remove tickers from the selected watchlist",)
    @app_commands.describe(tickers = "Tickers to remove from watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.choices(watchlist =[
        app_commands.Choice(name = "global", value = 'global'),
        app_commands.Choice(name = "personal", value = 'personal')
    ])
    async def removetickers(interaction: discord.Interaction, tickers: str, watchlist: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        
        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)

        watchlist_path = ""
        symbols = ""
        message_flavor = ""

        # Get watchlist path and watchlist contents based on value of watchlist input
        if watchlist.value == 'personal':
            user_id = interaction.user.id
            watchlist_path = sd.get_watchlist_path(user_id)
            symbols = sd.get_tickers(user_id)
            message_flavor = "your"
        else:
            watchlist_path = sd.get_watchlist_path()
            symbols = sd.get_tickers()
            message_flavor = "the global"\
            
        # Create watchlist if not present    
        if not (os.path.isdir(watchlist_path)):
            os.makedirs(watchlist_path)
            file = open("{}/watchlist.txt".format(watchlist_path), 'a')
            file.close()
            await interaction.followup.send("There are no tickers in {} watchlist. Use /addticker to begin building a watchlist.".format(message_flavor), ephemeral=True)
            return
        # If watchlist is empty, return
        elif len(symbols) == 0:
            await interaction.followup.send("There are no tickers in {} watchlist. Use /addticker to begin building a watchlist.".format(message_flavor), ephemeral=True)
            return
    
        for ticker in tickers:
            if (ticker in symbols):
                symbols.remove(ticker)
            else:
                invalid_tickers.append(ticker)

        for ticker in invalid_tickers:
            if ticker in tickers:
                tickers.remove(ticker)
            
        with open('{}/watchlist.txt'.format(watchlist_path), 'w') as watchlist:
            watchlist.write("\n".join(symbols))
                
        
        if len(tickers) > 0 and len(invalid_tickers) > 0:
            await interaction.followup.send("Removed {} from {} watchlist but could not remove the following tickers: {}".format(", ".join(tickers), message_flavor, ", ".join(invalid_tickers)), ephemeral=True)
        elif len(tickers) > 0:
            await interaction.followup.send("Removed {} from {} watchlist!".format(", ".join(tickers), message_flavor), ephemeral=True)
        else:
            await interaction.followup.send("No tickers removed from {} watchlist. Invalid tickers: {}".format(message_flavor, ", ".join(invalid_tickers)), ephemeral=True)

    @client.tree.command(name = "watchlist", description= "List the tickers on the selected watchlist",)
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
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

    @client.tree.command(name = "set-watchlist", description= "Overwrite a watchlist with the specified tickers",)
    @app_commands.describe(tickers = "Tickers to add to watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.choices(watchlist =[
        app_commands.Choice(name = "global", value = 'global'),
        app_commands.Choice(name = "personal", value = 'personal')
    ])
    async def set_watchlist(interaction: discord.Interaction, tickers: str, watchlist: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)

        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)

        watchlist_path = ""
        symbols = ""
        message_flavor = ""

        # Get watchlist path and watchlist contents based on value of watchlist input
        if watchlist.value == 'personal':
                user_id = interaction.user.id
                watchlist_path = sd.get_watchlist_path(user_id)
                message_flavor = "your"
        else:
            watchlist_path = sd.get_watchlist_path()
            symbols = sd.get_tickers()
            message_flavor = "the global"

        # Create watchlist if not present    
        if not (os.path.isdir(watchlist_path)):
            os.makedirs(watchlist_path)
            file = open("{}/watchlist.txt".format(watchlist_path), 'a')
            file.close()
    
        # Add tickers to watchlist
        with open('{}/watchlist.txt'.format(watchlist_path), 'w') as watchlist:
            watchlist.write("\n".join(tickers))
            watchlist.close()
                    
        
        if len(tickers) > 0 and len(invalid_tickers) > 0:
            await interaction.followup.send("Set {} watchlist to {} but could not add the following tickers: {}".format(message_flavor, ", ".join(tickers), ", ".join(invalid_tickers)), ephemeral=True)
        elif len(tickers) > 0:
            await interaction.followup.send("Set {} watchlist to {}.".format(message_flavor, ", ".join(tickers)), ephemeral=True)
        else:
            await interaction.followup.send("No tickers added to {} watchlist. Invalid tickers: {}".format(message_flavor, ", ".join(invalid_tickers)), ephemeral=True)

    # Are the news commands necessary? See if it's worth implementing them into reports or something
    '''
    @client.tree.command(name = "news-all", description= "Get the news on all the tickers on your watchlist",)
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
        '''

    @client.tree.command(name = "fetch-csv", description= "Returns data file for input ticker. Default: 1 year period.",)
    @app_commands.describe(ticker = "Ticker to return data for")
    @app_commands.describe(period = "Range of the data returned. Valid values: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max. Default: 1y")
    @app_commands.describe(interval = "Range between intraday data. Valid values: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo. Default: 1d")
    async def fetch_csv(interaction: discord.Interaction, ticker: str, period: str = "1y", interval: str = "1d"):
        try:
            sd.download_data_and_update_csv(ticker, period, interval)
            file = discord.File("data/CSV/{}.csv".format(ticker))
            await interaction.response.send_message(file=file, content= "Data file for " + ticker)
        except Exception:
            await interaction.response.send_message("Failed to fetch data file. Please ensure your parameters are valid.")
    
    @client.tree.command(name = "run-analysis", description= "Force the bot to run analysis on all tickers in a given watchlist",)
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.choices(watchlist =[
        app_commands.Choice(name = "global", value = 'global'),
        app_commands.Choice(name = "personal", value = 'personal')
    ])
    async def runanalysis(interaction: discord.Interaction, watchlist: app_commands.Choice[str]):
        tickers = []
        if watchlist.value == 'personal':
            user_id = interaction.user.id
            try:
                tickers = sd.get_tickers(user_id)
            except Exception as e:
                await interaction.response.send_message("Analysis failed. Do you have an existing watchlist?", ephemeral=True)
        else:
            try:
                tickers = sd.get_tickers()
            except Exception as e:
                await interaction.response.send_message("Analysis failed. Do you have an existing watchlist?", ephemeral=True)
        
        #await interaction.response.defer(ephemeral=True)
        an.run_analysis(tickers)
        await interaction.followup.send("Analysis complete!")
            
        
        
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
    @app_commands.describe(watchlist = "Which watchlist to fetch reports for")
    @app_commands.choices(watchlist =[
        app_commands.Choice(name = "global", value = 'global'),
        app_commands.Choice(name = "personal", value = 'personal')
    ])
    async def runreports(interaction: discord.Interaction, watchlist: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        
        tickers = ""
        message = ""
        if watchlist.value == 'personal':
            user_id = interaction.user.id
            tickers = sd.get_tickers(user_id)
        else:
            tickers = sd.get_tickers()

        if len(tickers) == 0:
            message = "No tickers on the watchlist. Use /addticker to build a watchlist."
        else:
            user = interaction.user
            channel = await client.fetch_channel('1150890013471555705')

            an.run_analysis(tickers)

            for ticker in tickers:
                report = build_report(ticker)
                message, files = report.get('message'), report.get('files')
                if watchlist.value == 'personal':
                    await user.send(message, files=files)
                else:
                    await channel.send(message, files=files)
                    
            message = "Reports have been posted!"
        await interaction.followup.send(message, ephemeral=True)


    @client.tree.command(name = "fetch-reports", description= "Fetch analysis reports of the specified tickers",)
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])        
    async def fetchreports(interaction: discord.interactions, tickers: str, visibility: app_commands.Choice[str]):
        
        await interaction.response.defer(ephemeral=True)

        tickers = tickers.split(' ')

        # Validate each ticker in the list is valid
        for ticker in tickers:
            if(not sd.validate_ticker(ticker)):
                tickers.remove(ticker)

        an.run_analysis(tickers)

        if visibility.value == 'private':
            for ticker in tickers:
                report = build_report(ticker)
                message, files, links = report.get('message'), report.get('files'), report.get('links')
                await interaction.user.send(message, files=files, embed=links)
        else:
            for ticker in tickers:
                report = build_report(ticker)
                message, files, links = report.get('message'), report.get('files'), report.get('links')
                await interaction.channel.send(message, files=files, embed=links)

        await interaction.followup.send("Fetched reports!", ephemeral=True)
            

    def build_report(ticker):

        # Get techincal indicator charts and convert them to a list of discord File objects
        files = sd.fetch_charts(ticker)
        for i in range(0, len(files)):
            files[i] = discord.File(files[i])

        # Append message based on analysis of indicators
        message = "**" + ticker + " Report " + dt.date.today().strftime("%m/%d/%Y") + "**\n"
        links = get_ticker_links(ticker)
        message += " | ".join(links) + "\n\n"

        # Append day's summary to message
        summary = sd.get_days_summary(ticker)
        message += "**Summary** \n| "
        for col in summary.keys():
            message += "**{}:** {}".format(col, f"{summary[col]:,.2f}")
            message += " | "

        message += "\n"

        # Append next earnings date to message
        message += "*Next earnings date:* {}\n\n".format(sd.get_next_earnings_date(ticker))


        analysis = sd.fetch_analysis(ticker)

        message += "**Analysis**\n"
        for indicator in analysis:
            message += indicator
        
        report = {'message':message, 'files':files, 'embed':links}

        return report

    def get_ticker_links(ticker):

        links = []
        stockinvest = "[StockInvest](https://stockinvest.us/stock/{})".format(ticker)
        links.append(stockinvest)
        finviz = "[FinViz](https://finviz.com/quote.ashx?t={})".format(ticker)
        links.append(finviz)
        yahoo = "[Yahoo! Finance](https://finance.yahoo.com/quote/{})".format(ticker)
        links.append(yahoo)

        return links



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

    
    
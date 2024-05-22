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
import threading
import logging

# Logging configuration
logger = logging.getLogger(__name__)

# Paths for writing data
ATTACHMENTS_PATH = "discord/attachments"
DAILY_DATA_PATH = "data/CSV/daily"

##################
# Init Functions #
##################

def get_bot_token():
    logger.debug("Fetching Discord bot token")
    try:
        token = os.getenv('DISCORD_TOKEN')
        logger.debug("Successfully fetched token")
        return token
    except Exception as e:
        logger.exception("Failed to fetch Discord bot token\n{}".format(e))
        return ""

def get_reports_channel_id():
    try:
        channel_id = os.getenv("REPORTS_CHANNEL_ID")
        logger.debug("Reports channel ID is {}".format(channel_id))
        return channel_id
    except Exception as e:
        logger.exception("Failed to fetch reports channel ID\n{}".format(e))
        return ""

def run_bot():
    TOKEN = get_bot_token()

    intents = discord.Intents.default()
    client = commands.Bot(command_prefix='$', intents=intents)

    @client.event
    async def on_ready():
        try:
            await client.tree.sync()
            send_reports.start()
        except Exception as e:
            logger.exception("Encountered error waiting for on-ready signal from bot\n{}".format(e))
        logger.info("Bot connected! ")

    async def sync_commands():
        logger.info("Syncing tree commands")
        await client.tree.sync()
        logger.info("Commands synced!")

    ########################
    # Watchlist Management #
    ########################

    async def watchlist_options(interaction: discord.Interaction, current: str):
        watchlists = sd.get_watchlists()
        return [
            app_commands.Choice(name = watchlist, value= watchlist)
            for watchlist in watchlists if current.lower() in watchlist.lower()
        ]

    @client.tree.command(name = "add-tickers", description= "Add tickers to the selected watchlist",)
    @app_commands.describe(tickers = "Ticker to add to watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def addtickers(interaction: discord.Interaction, tickers: str, watchlist: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/add-tickers function called by user {}".format(interaction.user.name))
        
        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)

        message_flavor = watchlist
        is_personal = False

        # Set message flavor based on value of watchlist argument
        logger.debug("Selected watchlist is '{}'".format(watchlist))
        watchlist_id = watchlist

        if watchlist == 'personal':
            watchlist_id = str(interaction.user.id)
            is_personal = True
            message_flavor = "your"

        symbols = sd.get_tickers_from_watchlist(watchlist_id)
        duplicate_tickers = [x for x in tickers if x in symbols]
        tickers = [x for x in tickers if x not in symbols]

    
        # Update watchlist with new tickers
        sd.update_watchlist(watchlist_id=watchlist_id, tickers=symbols + tickers)
        
        message = ''
        if len(tickers) > 0:
            message = "Added {} to {} watchlist!".format(", ".join(tickers), message_flavor)
        else:
            message = "No tickers added to {} watchlist.".format(message_flavor)
        if len(invalid_tickers) > 0:
                message += " Invalid tickers: {}.".format(", ".join(invalid_tickers))
        if len(duplicate_tickers) > 0:
            message += " Duplicate tickers: {}".format(", ".join(duplicate_tickers))
        
        await interaction.followup.send(message, ephemeral=is_personal)
        

    @client.tree.command(name = "remove-tickers", description= "Remove tickers from the selected watchlist",)
    @app_commands.describe(tickers = "Tickers to remove from watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def removetickers(interaction: discord.Interaction, tickers: str, watchlist: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/remove-tickers function called by user {}".format(interaction.user.name))
        
    # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)

        message_flavor = watchlist
        is_personal = False

        # Set message flavor based on value of watchlist argument
        logger.debug("Selected watchlist is '{}'".format(watchlist))
        watchlist_id = watchlist

        if watchlist == 'personal':
            watchlist_id = str(interaction.user.id)
            is_personal = True
            message_flavor = "your"

        symbols = sd.get_tickers_from_watchlist(watchlist_id)
        
    
        # If watchlist is empty, return
        if len(symbols) == 0:
            await interaction.followup.send("There are no tickers in {} watchlist. Use /add-tickers or /create-watchlist to begin building a watchlist.".format(message_flavor), ephemeral=is_personal)
            
        else:
            # Identify input tickers not in the watchlist
            NA_tickers = [ticker for ticker in tickers if ticker not in symbols]
            tickers = [ticker for ticker in tickers if ticker not in NA_tickers]
            
            
            # Update watchlist without input tickers                   
            sd.update_watchlist(watchlist_id=watchlist_id, tickers=[ticker for ticker in symbols if ticker not in tickers])                   
            
            message = ''
            if len(tickers) > 0:
                message = "Removed {} from {} watchlist!".format(", ".join(tickers), message_flavor)
            else:
                message = "No tickers removed {} watchlist.".format(message_flavor)
            if len(invalid_tickers) > 0:
                    message += " Invalid tickers: {}.".format(", ".join(invalid_tickers))
            if len(NA_tickers) > 0:
                message += " Tickers not in watchlist: {}".format(", ".join(NA_tickers))
        
            await interaction.followup.send(message, ephemeral=is_personal)
            
                      
        

    @client.tree.command(name = "watchlist", description= "List the tickers on the selected watchlist",)
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def watchlist(interaction: discord.Interaction, watchlist: str):
        logger.info("/watchlist function called by user {}".format(interaction.user.name))
    
        logger.debug("Selected watchlist is '{}'".format(watchlist))
        is_personal = False
        watchlist_id = watchlist
        if watchlist == 'personal':
            watchlist_id = str(interaction.user.id)
            is_personal = True
        
        tickers = sd.get_tickers_from_watchlist(watchlist_id)
        await interaction.response.send_message("Watchlist: " + ', '.join(tickers), ephemeral=is_personal)

    @client.tree.command(name = "set-watchlist", description= "Overwrite a watchlist with the specified tickers",)
    @app_commands.describe(tickers = "Tickers to add to watchlist (separated by spaces)")
    @app_commands.describe(watchlist = "Which watchlist you want to make changes to")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def set_watchlist(interaction: discord.Interaction, tickers: str, watchlist: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/set-watchlist function called by user {}".format(interaction.user.name))

        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)

        symbols = []
        message_flavor = watchlist
        is_personal = False
        watchlist_id = watchlist

        # Get watchlist path and watchlist contents based on value of watchlist input
        if watchlist == 'personal':
                watchlist_id = interaction.user.id
                message_flavor = "your"
                is_personal = True

                  
        try:
            # Update watchlist with new tickers         
            sd.update_watchlist(watchlist_id=watchlist_id, tickers=tickers)
            if len(tickers) > 0 and len(invalid_tickers) > 0:
                await interaction.followup.send("Set {} watchlist to {} but could not add the following tickers: {}".format(message_flavor, ", ".join(tickers), ", ".join(invalid_tickers)), ephemeral=is_personal)
            elif len(tickers) > 0:
                await interaction.followup.send("Set {} watchlist to {}.".format(message_flavor, ", ".join(tickers)), ephemeral=is_personal)
            else:
                await interaction.followup.send("No tickers added to {} watchlist. Invalid tickers: {}".format(message_flavor, ", ".join(invalid_tickers)), ephemeral=is_personal)
            
        except FileNotFoundError as e:
            logger.exception("Encountered FileNotFoundError when attempting to set watchlist '{}'".format(watchlist))
            await interaction.followup.send("Watchlist '{}' does not exist".format(watchlist), ephemeral=False)            
        
        

    @client.tree.command(name = "create-watchlist", description= "List the tickers on the selected watchlist",)
    @app_commands.describe(watchlist = "Name of the watchlist to create")
    @app_commands.describe(tickers = "Tickers to add to watchlist (separated by spaces)")
    async def create_watchlist(interaction: discord.Interaction, watchlist: str, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/create-watchlist function called by user {}".format(interaction.user.name))

        # Parse list from ticker input and identify invalid tickers
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)

        sd.create_watchlist(watchlist_id=watchlist, tickers=tickers)
        await interaction.followup.send("Created watchlist '{}' with tickers: ".format(watchlist) + ', '.join(tickers), ephemeral=False)

    @client.tree.command(name = "delete-watchlist", description= "Delete a watchlist",)
    @app_commands.describe(watchlist = "Watchlist to delete")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    async def delete_watchlist(interaction: discord.Interaction, watchlist: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/delete-watchlist function called by user {}".format(interaction.user.name))

        try:
            sd.delete_watchlist(watchlist_id=watchlist)
            await interaction.followup.send("Deleted watchlist '{}'".format(watchlist), ephemeral=False)
        except FileNotFoundError as e:
            logger.exception("Encountered FileNotFoundError when attempting to delete watchlist '{}'".format(watchlist))
            await interaction.followup.send("Watchlist '{}' does not exist".format(watchlist), ephemeral=False)

    ######################################################
    # 'Fetch' functions for returning data saved to disk #
    ######################################################

    @client.tree.command(name = "fetch-csv", description= "Returns data file for input ticker. Default: 1 year period.",)
    @app_commands.describe(tickers = "Tickers to return data for (separated by spaces)")
    async def fetch_csv(interaction: discord.Interaction, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/fetch-csv function called by user {}".format(interaction.user.name))
        logger.debug("Data file(s) for {} requested".format(tickers))
        try:
            files = []
            tickers, invalid_tickers = sd.get_list_from_tickers(tickers)
            for ticker in tickers:
                if not sd.daily_data_up_to_date(sd.fetch_daily_data(ticker)):
                    sd.download_analyze_data(ticker)
                file = discord.File("{}/{}.csv".format(DAILY_DATA_PATH,ticker))
                await interaction.user.send(content = "Data file for {}".format(ticker), file=file)
            if len(invalid_tickers) > 0:
                await interaction.followup.send("Fetched data files for {}. Invalid tickers:".format(", ".join(tickers), ", ".join(invalid_tickers)), ephemeral=True)
            else:
                await interaction.followup.send("Fetched data files for {}".format(", ".join(tickers)), ephemeral=True)

        except Exception as e:
            logger.exception("Failed to fetch data file with following exception:\n{}".format(e))
            await interaction.followup.send("Failed to fetch data files. Please ensure your parameters are valid.")
    
    @client.tree.command(name = "fetch-financials", description= "Fetch financial reports of the specified tickers ",)
    @app_commands.describe(tickers = "Tickers to return financials for (separated by spaces)")
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])        
    async def fetch_financials(interaction: discord.interactions, tickers: str, visibility: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/fetch-financials function called by user {}".format(interaction.user.name))
        logger.debug("Financials requested for {}".format(tickers))


        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)

        if(len(tickers) > 0):
            for ticker in tickers:
                files = sd.fetch_financials(ticker)
                for i in range(0, len(files)):
                    files[i] = discord.File(files[i])
                if visibility.value == 'private':
                    await interaction.user.send("Financials for {}".format(ticker), files=files)
                else:
                    await interaction.channel.send("Financials for {}".format(ticker), files=files)

            await interaction.followup.send("Posted financials for {}".format(",".join(tickers)), ephemeral=True)
        else:
            logger.warning("Found no valid tickers in {} to fetch fincancials for".format(tickers))
            await interaction.followup.send("No valid tickers in {}".format(",".join(invalid_tickers)), ephemeral=True)
            
    ########################        
    # Analysis and Reports #
    ########################

    # Plot graph for the selected tickers
    @client.tree.command(name = "plot-charts", description= "Plot selected graphs for the selected tickers",)
    @app_commands.describe(tickers = "Tickers to return charts for (separated by spaces)")
    @app_commands.describe(chart = "Charts to return for the specified tickers")
    @app_commands.choices(chart = [app_commands.Choice(name=x, value=x) for x in sorted(an.get_plots().keys())])
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])
    @app_commands.describe(display_signals = "True to plot buy/sell signals, False to not plot signals") 
    @app_commands.choices(display_signals=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])
    @app_commands.describe(num_days = "Number of days (data points) to plot on the chart (Default: 365)")
    @app_commands.describe(plot_type = "Style in which to plot the Close price on the chart")
    @app_commands.choices(plot_type = [app_commands.Choice(name = x, value= x) for x in an.get_plot_types()])
    @app_commands.describe(style = " Style in which to generate the chart")
    @app_commands.choices(style = [app_commands.Choice(name = x, value = x) for x in an.get_plot_styles()])
    @app_commands.describe(show_volume= "True to show volume plot, False to not plot volume") 
    @app_commands.choices(show_volume=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    async def plot_charts(interaction: discord.interactions, tickers: str, chart: app_commands.Choice[str], visibility: app_commands.Choice[str],
                    display_signals: app_commands.Choice[str] = 'True',
                    num_days: int = 365, 
                    plot_type: app_commands.Choice[str] = 'line', 
                    style: app_commands.Choice[str] = 'tradingview',
                    show_volume: app_commands.Choice[str] = 'False'):

        await interaction.response.defer(ephemeral=True)
        logger.info("/plot-chart function called by user {}".format(interaction.user.name))

        # Validate optional parameters
        if isinstance(display_signals, app_commands.Choice):
            display_signals = display_signals.value
        if isinstance(plot_type, app_commands.Choice):
            plot_type= plot_type.value
        if isinstance(style, app_commands.Choice):
            style=style.value
        if isinstance(show_volume, app_commands.Choice):
            show_volume = show_volume.value
        
        

        # Clean up data for plotting
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)
        chart = chart.value
        visibility = visibility.value
        chart_info = an.get_plot(chart)

        savefilepath_root = "{}/{}".format(ATTACHMENTS_PATH, "plots")

        # Generate indicator chart for tickers
        for ticker in tickers:
            files = []
            message  = ''

            # Fetch data file for ticker
            data = sd.fetch_daily_data(ticker)
            if data.size == 0:
                sd.download_analyze_data(ticker)
                data = sd.fetch_daily_data(ticker)

            plot_success, plot_message = an.plot(ticker=ticker,
                        data=data,
                        indicator_name=chart,
                        display_signals=eval(display_signals),
                        num_days=num_days,
                        plot_type=plot_type,
                        style=style,
                        show_volume=eval(show_volume),
                        savefilepath_root=savefilepath_root
                        )
            if not plot_success:
                message = "Failed to generate strategy '{}' for ticker {}. ".format(chart, ticker) + plot_message
                logger.error(message)
                await interaction.followup.send(message)
                return
            files.append(discord.File(savefilepath_root+ "/{}/{}.png".format(ticker, chart_info['abbreviation'])))
        
            if visibility == 'private':
                await interaction.user.send(message, files=files)
            else:
                await interaction.channel.send(message, files=files)

        await interaction.followup.send("Finished generating charts")



    # Plot strategy for the selected tickers
    @client.tree.command(name = "plot-strategy", description= "Plot selected graphs for the selected tickers",)
    @app_commands.describe(tickers = "Tickers to return charts for (separated by spaces)")
    @app_commands.describe(strategy = "Strategy to generate charts for")
    @app_commands.choices(strategy = [app_commands.Choice(name=x, value=x) for x in sorted(an.get_strategies().keys())])
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])
    @app_commands.describe(display_signals = "True to plot buy/sell signals, False to not plot signals") 
    @app_commands.choices(display_signals=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])
    @app_commands.describe(num_days = "Number of days (data points) to plot on the chart (Default: 365)")
    @app_commands.describe(plot_type = "Style in which to plot the Close price on the chart")
    @app_commands.choices(plot_type = [app_commands.Choice(name = x, value= x) for x in an.get_plot_types()])
    @app_commands.describe(style = " Style in which to generate the chart")
    @app_commands.choices(style = [app_commands.Choice(name = x, value = x) for x in an.get_plot_styles()])
    @app_commands.describe(show_volume= "True to show volume plot, False to not plot volume") 
    @app_commands.choices(show_volume=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    async def plot_strategy(interaction: discord.interactions, tickers: str, strategy: app_commands.Choice[str], visibility: app_commands.Choice[str],
                    display_signals: app_commands.Choice[str] = 'True',
                    num_days: int = 365, 
                    plot_type: app_commands.Choice[str] = 'line', 
                    style: app_commands.Choice[str] = 'tradingview',
                    show_volume: app_commands.Choice[str] = 'False'):

        await interaction.response.defer(ephemeral=True)
        logger.info("/plot-strategy function called by user {}".format(interaction.user.name))

        # Validate optional parameters
        if isinstance(display_signals, app_commands.Choice):
            display_signals = display_signals.value
        if isinstance(plot_type, app_commands.Choice):
            plot_type= plot_type.value
        if isinstance(style, app_commands.Choice):
            style=style.value
        if isinstance(show_volume, app_commands.Choice):
            show_volume = show_volume.value
        
    
        visibility = visibility.value

        # Clean up data for plotting
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)
        strategy = an.get_strategy(strategy.value)
        charts = strategy.plots
        savefilepath_root = "{}/{}/{}".format(ATTACHMENTS_PATH, "plots/strategies",strategy.abbreviation)

        # Generate indicator charts for each ticker and append them to 'files'
        for ticker in tickers:
            files = []
            message  = ''

            # Fetch data file for ticker
            data = sd.fetch_daily_data(ticker)
            if data.size == 0:
                sd.download_analyze_data(ticker)
                data = sd.fetch_daily_data(ticker)

            for chart in charts:
                plot_success, plot_message = an.plot(ticker=ticker,
                            data=data,
                            indicator_name=chart,
                            display_signals=eval(display_signals),
                            num_days=num_days,
                            plot_type=plot_type,
                            style=style,
                            show_volume=eval(show_volume),
                            savefilepath_root=savefilepath_root
                            )
                if not plot_success:
                    message = "Failed to generate strategy '{}' for ticker {}. ".format(strategy.name, ticker) + plot_message
                    logger.error(message)
                    await interaction.followup.send(message)
                    return
                files.append(discord.File(savefilepath_root+ "/{}/{}.png".format(ticker, an.get_plot(chart)['abbreviation'])))
        
            # Generate Strategy chart
            plot_success, plot_message = an.plot(ticker=ticker,
                        data=data,
                        indicator_name=strategy.name,
                        display_signals=True,
                        num_days=num_days,
                        plot_type=plot_type,
                        style=style,
                        show_volume=eval(show_volume),
                        savefilepath_root=savefilepath_root,
                        title= "{} {} Strategy".format(ticker, strategy.name),
                        is_strategy=True
                        )
                
                
                
            message = plot_message
            files.append(discord.File(savefilepath_root+ "/{}/{}.png".format(ticker, strategy.abbreviation)))
        
            if visibility == 'private':
                await interaction.user.send(message, files=files)
            else:
                await interaction.channel.send(message, files=files)
            
                

        await interaction.followup.send("Finished generating charts")

    # Plot strategy for the selected watchlist
    @client.tree.command(name = "run-strategy", description= "Plot selected graphs for the selected watchlist",)
    @app_commands.describe(watchlist = "Watchlist to plot the strategy against")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    @app_commands.describe(strategy = "Strategy to generate charts for")
    @app_commands.choices(strategy = [app_commands.Choice(name=x, value=x) for x in sorted(an.get_strategies().keys())])
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])
    @app_commands.describe(display_signals = "True to plot buy/sell signals, False to not plot signals") 
    @app_commands.choices(display_signals=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])
    @app_commands.describe(num_days = "Number of days (data points) to plot on the chart (Default: 365)")
    @app_commands.describe(plot_type = "Style in which to plot the Close price on the chart")
    @app_commands.choices(plot_type = [app_commands.Choice(name = x, value= x) for x in an.get_plot_types()])
    @app_commands.describe(style = " Style in which to generate the chart")
    @app_commands.choices(style = [app_commands.Choice(name = x, value = x) for x in an.get_plot_styles()])
    @app_commands.describe(show_volume= "True to show volume plot, False to not plot volume") 
    @app_commands.choices(show_volume=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])      
    async def run_strategy(interaction: discord.interactions, watchlist: str, strategy: app_commands.Choice[str], visibility: app_commands.Choice[str],
                    display_signals: app_commands.Choice[str] = 'True',
                    num_days: int = 365, 
                    plot_type: app_commands.Choice[str] = 'line', 
                    style: app_commands.Choice[str] = 'tradingview',
                    show_volume: app_commands.Choice[str] = 'False'):
        
        await interaction.response.defer(ephemeral=True)
        logger.info("/run-strategy function called by user {}".format(interaction.user.name))

        # Validate optional parameters
        if isinstance(display_signals, app_commands.Choice):
            display_signals = display_signals.value
        if isinstance(plot_type, app_commands.Choice):
            plot_type= plot_type.value
        if isinstance(style, app_commands.Choice):
            style=style.value
        if isinstance(show_volume, app_commands.Choice):
            show_volume = show_volume.value
        
    
        visibility = visibility.value

        # Clean up data for plotting
        tickers = sd.get_tickers_from_watchlist(watchlist)
        strategy = an.get_strategy(strategy.value)
        charts = strategy.plots
        savefilepath_root = "{}/{}/{}".format(ATTACHMENTS_PATH, "plots/strategies",strategy.abbreviation)

        # Generate indicator charts for each ticker and append them to 'files'
        for ticker in tickers:
            files = []
            message  = ''

            # Fetch data file for ticker
            data = sd.fetch_daily_data(ticker)
            if data.size == 0:
                sd.download_analyze_data(ticker)
                data = sd.fetch_daily_data(ticker)

            for chart in charts:
                plot_success, plot_message = an.plot(ticker=ticker,
                            data=data,
                            indicator_name=chart,
                            display_signals=eval(display_signals),
                            num_days=num_days,
                            plot_type=plot_type,
                            style=style,
                            show_volume=eval(show_volume),
                            savefilepath_root=savefilepath_root
                            )
                if not plot_success:
                    message = "Failed to generate strategy '{}' for ticker {}. ".format(strategy.name, ticker) + plot_message
                    logger.error(message)
                    await interaction.followup.send(message)
                    return
                files.append(discord.File(savefilepath_root+ "/{}/{}.png".format(ticker, an.get_plot(chart)['abbreviation'])))
        
            # Generate Strategy chart
            plot_success, plot_message = an.plot(ticker=ticker,
                        data=data,
                        indicator_name=strategy.name,
                        display_signals=True,
                        num_days=num_days,
                        plot_type=plot_type,
                        style=style,
                        show_volume=eval(show_volume),
                        savefilepath_root=savefilepath_root,
                        title= "{} {} Strategy".format(ticker, strategy.name),
                        is_strategy=True
                        )
                
                
                
            message = plot_message
            files.append(discord.File(savefilepath_root+ "/{}/{}.png".format(ticker, strategy.abbreviation)))
        
            if visibility == 'private':
                await interaction.user.send(message, files=files)
            else:
                await interaction.channel.send(message, files=files)
            
                

        await interaction.followup.send("Finished generating charts")
        
    
    # Send daily reports for stocks on the global watchlist to the reports channel
    @tasks.loop(hours=24)  
    async def send_reports():
        
        if (dt.datetime.now().weekday() < 5):
            logger.info("********** [SENDING REPORTS] **********")

            # Configure channel to send reports to
            channel = await client.fetch_channel(get_reports_channel_id())
            
            an.run_analysis(sd.get_tickers_from_watchlist())
            for ticker in sd.get_tickers_from_watchlist():
                report = build_ticker_report(ticker)
                message, files = report.get('message'), report.get('files')
                await channel.send(message, files=files)
                

            # Daily scoring logic 
            #daily_scores = an.get_masterlist_scores()
            #daily_summary_message = build_daily_summary
            #await channel.send(daily_summary_message, file=discord.File("{}/daily_rankings.csv".format(ATTACHMENTS_PATH)))

            logger.info("********** [FINISHED SENDING REPORTS] **********")
        else:
            pass

    # Configure delay before sending daily reports to send at the same time daily
    @send_reports.before_loop
    async def delay_send_reports():
        
        hour = 6
        minute = 30
        now = dt.datetime.now()

        future = dt.datetime(now.year, now.month, now.day, hour, minute)
        if now.hour >= hour and now.minute > minute:
            future += dt.timedelta(days=1)
        
        time_to_reports = dt.timedelta(seconds=(future-now).seconds)
        logger.info("Sending reports in {}".format(time_to_reports))
        await asyncio.sleep(time_to_reports.seconds)
            
    @client.tree.command(name = "run-reports", description= "Post analysis of a given watchlist (use /fetch-reports for individual or non-watchlist stocks)",)
    @app_commands.describe(watchlist = "Which watchlist to fetch reports for")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])
    async def runreports(interaction: discord.Interaction, watchlist: str, visibility: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/run-reports function called by user {}".format(interaction.user.name))
        logger.debug("Selected watchlist is '{}'".format(watchlist))
        
        
        message = ""
        watchlist_id = watchlist

        # Populate tickers based on value of watchlist
        if watchlist == 'personal':
            watchlist_id = str(interaction.user.id)
        

        tickers = sd.get_tickers_from_watchlist(watchlist_id)

        if len(tickers) == 0:
            # Empty watchlist
            logger.warning("Selected watchlist '{}' is empty".format(watchlist))
            message = "No tickers on the watchlist. Use /addticker to build a watchlist."
        else:
            user = interaction.user
            channel = await client.fetch_channel(get_reports_channel_id())

            an.run_analysis(tickers)

            # Build reports and send messages
            logger.info("Running reports on tickers {}".format(tickers))
            for ticker in tickers:
                logger.info("Processing ticker {}".format(ticker))
                report = build_ticker_report(ticker)
                message, files = report.get('message'), report.get('files')

                if visibility.value == 'private':
                    await interaction.user.send(message, files=files)
                else:
                    await interaction.channel.send(message, files=files)
                logger.info("Posted report for ticker {}".format(ticker))
                    
            message = "Reports have been posted!"
            logger.info("Reports have been posted")
        await interaction.followup.send(message, ephemeral=True)

    
    @client.tree.command(name = "fetch-reports", description= "Fetch analysis reports of the specified tickers (use /run-reports to analyze a watchlist)",)
    @app_commands.describe(tickers = "Tickers to post reports for (separated by spaces)")
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])        
    async def fetchreports(interaction: discord.interactions, tickers: str, visibility: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/fetch-reports function called by user {}".format(interaction.user.name))
        

        # Validate each ticker in the list is valid
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)
        logger.debug("Validated tickers {} | Invalid tickers: {}".format(tickers, invalid_tickers))

        an.run_analysis(tickers)

        logger.info("Fetching reports for tickers {}".format(tickers))
        # Build reports and send messages
        for ticker in tickers:
            logger.info("Processing ticker {}".format(ticker))
            report = build_ticker_report(ticker)
            message, files, links = report.get('message'), report.get('files'), report.get('links')
            if visibility.value == 'private':
                await interaction.user.send(message, files=files, embed=links)
            else:
                await interaction.channel.send(message, files=files, embed=links)
            logger.info("Report posted for ticker {}".format(ticker))
        if len(invalid_tickers) > 0:
            await interaction.followup.send("Fetched reports for {}. Failed to fetch reports for {}.".format(", ".join(tickers), ", ".join(invalid_tickers)), ephemeral=True)
        else:
            logger.info("Reports have been posted")
            await interaction.followup.send("Fetched reports!", ephemeral=True)

    ###########################
    # Report Helper Functions #
    ###########################

    def build_ticker_report(ticker):
        logger.info("Building report for ticker {}".format(ticker))

        # Get techincal indicator charts and convert them to a list of discord File objects
        files = sd.fetch_charts(ticker)
        for i in range(0, len(files)):
            files[i] = discord.File(files[i])

        # Append ticker name, today's date, and external links to message
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

        # Append analysis to mesage
        analysis = sd.fetch_analysis(ticker)

        message += "**Indicator Analysis**\n"
        for indicator in analysis:
            message += indicator

        message += "\n"
        
        # Append strategy analysis to message
        strategies = an.get_strategies()
        if len(strategies) > 0:
            message += "**Strategy Analysis**\n"
            for strategy in strategies:
                data = sd.fetch_daily_data(ticker)
                strategy_name = strategies[strategy].name
                signals = strategies[strategy].signals
                buy_threshold = strategies[strategy].buy_threshold
                sell_threshold = strategies[strategy].sell_threshold
                try:
                    score_evaluation = an.score_eval(
                        an.signals_score(data, signals),
                        buy_threshold, 
                        sell_threshold)
                except KeyError as e:
                    logger.exception("Encountered Key Error when evaluating strategy '{}' fot ticker '{}':\n{}".format(strategy_name, ticker, e))
                    score_evaluation = "N/A"
                message += "{}: **{}**\n".format(strategy_name, score_evaluation)
            
            report = {'message':message, 'files':files, 'embed':links}

            return report
    
    def build_strategy_report(ticker):
        logger.info("Building strategy report for ticker {}".format(ticker))
        message = ''
        strategies = an.get_strategies()
        if len(strategies) == 0:
            logger.debug("No strategies available - return empty report")
            return message
        else:
            message = "**Strategies**\n"
        
            for strategy in strategies:
                logger.debug("Applying strategy '{}' on ticker '{}'".format(strategy.name, ticker))
                score = an.signals_score(sd.fetch_daily_data(ticker), strategy.signals)
                message += "{}: **{}**".format(strategy.name, an.score_eval(score, strategy.buy_threshold, strategy.sell_threshold))

        return message
    
    def build_daily_summary():
        logger.info("Building daily summary report")
        
        # TODO - scoring and strategy signals
        daily_scores = an.get_masterlist_scores()
        BUY_THRESHOLD = 1.00
        daily_rankings_message = '**Daily Stock Rankings**\n\n'

        daily_rankings_message += "**SMA 10/50 Strategy**\n"
        for col in daily_scores.columns:
            try:
                if float(col) >= 1.00:
                    daily_rankings_message += "**{}**\n".format(col)
                    daily_rankings_message += " ".join(daily_scores[col].dropna()) + "\n\n"
            except ValueError as e:
                #Index column is invalid
                pass

    def get_ticker_links(ticker):
        logger.debug("Building links to external sites for ticker {}".format(ticker))

        links = []
        stockinvest = "[StockInvest](https://stockinvest.us/stock/{})".format(ticker)
        links.append(stockinvest)
        finviz = "[FinViz](https://finviz.com/quote.ashx?t={})".format(ticker)
        links.append(finviz)
        yahoo = "[Yahoo! Finance](https://finance.yahoo.com/quote/{})".format(ticker)
        links.append(yahoo)
        tradingview = "[TradingView](https://www.tradingview.com/chart/?symbol={})".format(ticker)
        links.append(tradingview)

        return links
         
    ########################
    # Test & Help Commands #
    ########################

    @client.tree.command(name = "help", description= "Show help on the bot's commands",)
    async def help(interaction: discord.Interaction):
        logger.info("/help function called by user {}".format(interaction.user.name))
        embed = discord.Embed()
        embed.title = 'RocketStocks Help'
        for command in client.tree.get_commands():
            embed.add_field(name=command.name, value=command.description)
        await interaction.response.send_message(embed=embed)
    
    @client.tree.command(name = "test-daily-download-analyze-data", description= "Test running the logic for daily data download and indicator generation",)
    async def test_daily_download_analyze_data(interaction: discord.Interaction):
        logger.info("/test-daily-download-analyze-data function called by user {}".format(interaction.user.name))
        await interaction.response.send_message("Running daily download and analysis", ephemeral=True)
        download_data_thread = threading.Thread(target=sd.daily_download_analyze_data)
        download_data_thread.start()

    @client.tree.command(name = "test-minute-download-data", description= "Test running the logic for weekly minute-by-minute data download",)
    async def test_minutes_download_data(interaction: discord.Interaction):
        logger.info("/test-minute-download-data function called by user {}".format(interaction.user.name))

        await interaction.response.send_message("Running daily download and analysis", ephemeral=True)
        download_data_thread = threading.Thread(target=sd.minute_download_data)
        download_data_thread.start()

    @client.tree.command(name = "fetch-logs", description= "Return the log file for the bot",)
    async def fetch_logs(interaction: discord.Interaction):
        logger.info("/fetch-logs function called by user {}".format(interaction.user.name))
        log_file = discord.File("logs/rocketstocks.log")
        await interaction.user.send(content = "Log file for RocketStocks :rocket:",file=log_file)
        await interaction.response.send_message("Log file has been sent", ephemeral=True)


    client.run(TOKEN)
    
if __name__ == "__main__":
    run_bot()

    
    
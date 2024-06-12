import sys
sys.path.append('../RocketStocks')
import os
import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
import asyncio
import stockdata as sd
import analysis as an
import datetime as dt
import threading
import logging
import numpy as np
import strategies

# Logging configuration
logger = logging.getLogger(__name__)

# Paths for writing data
ATTACHMENTS_PATH = "discord/attachments"
DAILY_DATA_PATH = "data/CSV/daily"
MINUTE_DATA_PATH = "data/CSV/minute"
UTILS_PATH = "data/utils"

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
    @app_commands.describe(type="Type of data file to return - daily data or minute-by-minute data")
    @app_commands.choices(type=[
        app_commands.Choice(name='daily', value='daily'),
        app_commands.Choice(name='minute', value='minute')
    ])
    async def fetch_csv(interaction: discord.Interaction, tickers: str, type: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/fetch-csv function called by user {}".format(interaction.user.name))
        logger.debug("Data file(s) for {} requested".format(tickers))

        type = type.value
        
        files = []
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)
        try:
            for ticker in tickers:
                if type == 'daily':
                    data = sd.fetch_daily_data(ticker)
                    csv_path = "{}/{}.csv".format(DAILY_DATA_PATH,ticker)
                    if not sd.daily_data_up_to_date(data):
                        sd.download_analyze_data(ticker)
                else:
                    data = sd.fetch_minute_data(ticker)
                    csv_path = "{}/{}.csv".format(MINUTE_DATA_PATH,ticker)
                    data = sd.download_data(ticker, period='7d', interval='1m')
                    sd.update_csv(data, ticker, MINUTE_DATA_PATH)

                file = discord.File(csv_path)
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
            
    ############
    # Plotting #
    ############

    async def chart_options(interaction: discord.Interaction, current: str):
        charts = strategies.get_strategies().keys()
        return [
            app_commands.Choice(name = chart, value= chart)
            for chart in charts if current.lower() in chart.lower()
        ]

    # Plot graphs for the selected tickers
    @client.tree.command(name = "plot-charts", description= "Plot selected graphs for the selected tickers",)
    @app_commands.describe(tickers = "Tickers to return charts for (separated by spaces)")
    @app_commands.describe(chart = "Charts to return for the specified tickers")
    @app_commands.autocomplete(chart=chart_options,)
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
    @app_commands.describe(timeframe = "Timeframe to plot the chart against (Default: 1y)")
    @app_commands.choices(timeframe = [app_commands.Choice(name = x, value= x) for x in an.get_plot_timeframes()])

    @app_commands.describe(plot_type = "Style in which to plot the Close price on the chart")
    @app_commands.choices(plot_type = [app_commands.Choice(name = x, value= x) for x in an.get_plot_types()])
    @app_commands.describe(style = " Style in which to generate the chart")
    @app_commands.choices(style = [app_commands.Choice(name = x, value = x) for x in an.get_plot_styles()])
    @app_commands.describe(show_volume= "True to show volume plot, False to not plot volume") 
    @app_commands.choices(show_volume=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(linear_regression = "Plot linear regression over Close")
    @app_commands.choices(linear_regression=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(midpoint = "Plot midpoint over Close")
    @app_commands.choices(midpoint =[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(ohlc4 = "Plot OHLVC4 over Close")
    @app_commands.choices(ohlc4 =[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(zscore = "Plot ZScore indicator")
    @app_commands.choices(zscore=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(cumulative_log_return = "Plot Cumulative Log Return")
    @app_commands.choices(cumulative_log_return=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(squeeze = "Plot Squeeze indicator")
    @app_commands.choices(linear_regression=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(archer_moving_averages = "Plot Archer Moving Averages over Close (not recommended when plotting another MA indicator)")
    @app_commands.choices(archer_moving_averages=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(archer_obv = "Plot Archer OBV indicator")
    @app_commands.choices(archer_obv=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ]) 
    async def plot_charts(interaction: discord.interactions, tickers: str, chart: str, visibility: app_commands.Choice[str],
                    display_signals: app_commands.Choice[str] = 'True',
                    timeframe: str = "1y", 
                    plot_type: app_commands.Choice[str] = 'candle', 
                    style: app_commands.Choice[str] = 'tradingview',
                    show_volume: app_commands.Choice[str] = 'False',
                    linear_regression: app_commands.Choice[str] = 'False',
                    midpoint: app_commands.Choice[str] = 'False',
                    ohlc4: app_commands.Choice[str] = 'False',
                    zscore: app_commands.Choice[str] = 'False',
                    cumulative_log_return: app_commands.Choice[str] = 'False',
                    squeeze: app_commands.Choice[str] = 'False',
                    archer_moving_averages: app_commands.Choice[str] = 'False',
                    archer_obv: app_commands.Choice[str] = 'False'):

        await interaction.response.defer(ephemeral=True)
        logger.info("/plot-chart function called by user {}".format(interaction.user.name))
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)

        # Generate indicator chart for tickers
        for ticker in tickers:
            await send_charts(**locals())

        await interaction.followup.send("Finished generating charts")

    # Plot graphs for the selected watchlist
    @client.tree.command(name = "run-charts", description= "Plot selected graphs for the selected tickers",)
    @app_commands.describe(watchlist = "Watchlist to plot the strategy against")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    @app_commands.describe(chart = "Charts to return for the specified tickers")
    @app_commands.autocomplete(chart=chart_options,)
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
    @app_commands.describe(timeframe = "Timeframe to plot the chart against (Default: 1y)")
    @app_commands.choices(timeframe = [app_commands.Choice(name = x, value= x) for x in an.get_plot_timeframes()])

    @app_commands.describe(plot_type = "Style in which to plot the Close price on the chart")
    @app_commands.choices(plot_type = [app_commands.Choice(name = x, value= x) for x in an.get_plot_types()])
    @app_commands.describe(style = " Style in which to generate the chart")
    @app_commands.choices(style = [app_commands.Choice(name = x, value = x) for x in an.get_plot_styles()])
    @app_commands.describe(show_volume= "True to show volume plot, False to not plot volume") 
    @app_commands.choices(show_volume=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])
    @app_commands.describe(linear_regression = "Plot linear regression over Close")
    @app_commands.choices(linear_regression=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(midpoint = "Plot midpoint over Close")
    @app_commands.choices(midpoint =[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(ohlc4 = "Plot OHLVC4 over Close")
    @app_commands.choices(ohlc4 =[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(zscore = "Plot ZScore indicator")
    @app_commands.choices(zscore=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(cumulative_log_return = "Plot Cumulative Log Return")
    @app_commands.choices(cumulative_log_return=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(squeeze = "Plot Squeeze indicator")
    @app_commands.choices(linear_regression=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(archer_moving_averages = "Plot Archer Moving Averages over Close (not recommended when plotting another MA indicator)")
    @app_commands.choices(archer_moving_averages=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(archer_obv = "Plot Archer OBV indicator")
    @app_commands.choices(archer_obv=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    async def run_charts(interaction: discord.interactions, watchlist: str, chart: str, visibility: app_commands.Choice[str],
                    display_signals: app_commands.Choice[str] = 'True',
                    timeframe: app_commands.Choice[str] = "1y", 
                    plot_type: app_commands.Choice[str] = 'candle', 
                    style: app_commands.Choice[str] = 'tradingview',
                    show_volume: app_commands.Choice[str] = 'False',
                    linear_regression: app_commands.Choice[str] = 'False',
                    midpoint: app_commands.Choice[str] = 'False',
                    ohlc4: app_commands.Choice[str] = 'False',
                    zscore: app_commands.Choice[str] = 'False',
                    cumulative_log_return: app_commands.Choice[str] = 'False',
                    squeeze: app_commands.Choice[str] = 'False',
                    archer_moving_averages: app_commands.Choice[str] = 'False',
                    archer_obv: app_commands.Choice[str] = 'False'):

        await interaction.response.defer(ephemeral=True)
        logger.info("/run-charts function called by user {}".format(interaction.user.name))
        watchlist_id =  watchlist
        if watchlist == 'personal':
            watchlist_id = interaction.user.id
        tickers = sd.get_tickers_from_watchlist(watchlist_id)

        # Generate indicator chart for tickers
        for ticker in tickers:
            await send_charts(**locals())

        await interaction.followup.send("Finished generating charts")


    async def send_charts(interaction: discord.Interaction, ticker, **kwargs):
            
            # Parse ticker and data fields
            plot_args = {}
            plot_args['ticker'] = ticker

            data = sd.fetch_daily_data(ticker)
            if data.size == 0 or not sd.daily_data_up_to_date(data):
                sd.download_analyze_data(ticker)
                data = sd.fetch_daily_data(ticker)
            plot_args['df'] = data
            plot_args['title'] = ticker
            plot_args['verbose'] = True
            visibility = kwargs.pop("visibility").value

            # Parse strategy and signal information
            strategy = strategies.get_strategy(kwargs.pop('chart'))()
            plot_args['strategy'] = strategy
            plot_args['long_trend'] = strategy.signals(data)
            plot_name = strategy.name
            
             # Args for saving plot as PNG
            plot_args['savepath'] = "{}/{}".format(ATTACHMENTS_PATH, "plots")
            plot_args['filename'] = strategy.short_name

            # Parse optional plot args
            tsignals = kwargs.pop('display_signals')
            plot_args['tsignals'] = eval(tsignals) if isinstance(tsignals, str) else eval(tsignals.value)
            plot_args['plot_returns'] = plot_args['tsignals']

            type = kwargs.pop('plot_type')
            plot_args['type'] = type if isinstance(type, str) else type.value

            style = kwargs.pop('style')
            plot_args['style'] = style if isinstance(style, str) else style.value

            timeframe = kwargs.pop('timeframe', '1y')
            plot_args['last'] = an.recent_bars(data, timeframe) if isinstance(timeframe, str) else an.recent_bars(data, timeframe.value)

            # Parse additional charts to plot
            linreg = kwargs.pop('linear_regression')
            plot_args['linreg'] = eval(linreg) if isinstance(linreg, str) else eval(linreg.value)

            midpoint = kwargs.pop('midpoint')
            plot_args['midpoint'] = eval(midpoint) if isinstance(midpoint, str) else eval(midpoint.value)

            olhc4 = kwargs.pop('ohlc4')
            plot_args['olhc4'] = eval(olhc4) if isinstance(olhc4, str) else eval(olhc4.value)
        
            zscore = kwargs.pop('zscore')
            plot_args['zscore'] = eval(zscore) if isinstance(zscore, str) else eval(zscore.value)

            clr = kwargs.pop('cumulative_log_return')
            plot_args['clr'] = eval(clr) if isinstance(clr, str) else eval(clr.value)

            squeeze = kwargs.pop('squeeze')
            plot_args['squeeze'] = eval(squeeze) if isinstance(squeeze, str) else eval(squeeze.value)

            archermas = kwargs.pop('archer_moving_averages')
            plot_args['archermas'] = eval(archermas) if isinstance(archermas, str) else eval(archermas.value)

            archerobv = kwargs.pop('archer_obv')
            plot_args['archerobv'] = eval(archerobv) if isinstance(archerobv, str) else eval(archerobv.value)

            # Set true for indicators to plot
            for indicator in strategy.indicators:
                plot_args[indicator] = True

            # Override plot args with strategy-specific configs
            plot_args = strategy.override_chart_args(plot_args)

            files = []
            message  = '{} for {} over last {} days'.format(strategy.name, ticker, plot_args['last'])

            chart = an.Chart(**plot_args)
            if chart is None:
                message = "Failed to plot '{}' for ticker {}. ".format(plot_name, ticker) 
                logger.error(message)
                if visibility == 'private':
                    await interaction.user.send(message)
                else:
                    await interaction.channel.send(message)
                return None
            files.append(discord.File(plot_args['savepath']+ "/{}/{}.png".format(ticker, strategy.short_name)))
            if visibility == 'private':
                await interaction.user.send(message, files=files)
            else:
                await interaction.channel.send(message, files=files)

    ###############
    # Backtesting #
    ###############
    
    async def backtest_options(interaction: discord.Interaction, current: str):
        charts = strategies.get_combination_strategies()
        bt_strategies = [chart().name for chart in charts]
        return [
            app_commands.Choice(name = strategy, value= strategy)
            for strategy in bt_strategies if current.lower() in strategy.lower()
        ]

    # Generate backtests for the selected tickers
    @client.tree.command(name = "fetch-backtests", description= "Generate backtests for the selected tickers",)
    @app_commands.describe(tickers = "Tickers to return charts for (separated by spaces)")
    @app_commands.describe(strategy = "Strategy to run the backtest against")
    @app_commands.autocomplete(strategy=backtest_options,)
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])
    @app_commands.describe(cash = "Amount of captial to start the backtest with (Default: 10,000)")
    @app_commands.describe(timeframe = "Timeframe to plot the chart against (Default: 10y)")
    @app_commands.choices(timeframe = [app_commands.Choice(name = x, value= x) for x in an.get_plot_timeframes()])
    @app_commands.describe(stats_only = "True to only return stats of backtest, False to return interactive backtest HTML file (Default: False)")
    @app_commands.choices(stats_only=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(summary_only = "True to only post backtest summary, False to post individual backtest stats (Default: False)")
    @app_commands.choices(summary_only=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  

    async def fetch_backtests(interaction: discord.interactions, tickers: str, strategy: str, visibility: app_commands.Choice[str],
                    cash: int = 10000,
                    timeframe: str = "1y", 
                    stats_only: str = "False", 
                    summary_only: str = "False"):

        await interaction.response.defer(ephemeral=True)
        logger.info("/fetch-backtests function called by user {}".format(interaction.user.name))
        tickers, invalid_tickers = sd.get_list_from_tickers(tickers)

        # Generate backtest stats
        backtest_stats = {}
        for ticker in tickers:
            stats = await send_backtest(**locals())
            backtest_stats[ticker] = stats

        await send_backtest_summary(interaction, backtest_stats, tickers, strategy, visibility.value)

        await interaction.followup.send("Finished generating backtests")

    # Generate backtests for tickers in the selected watchlists
    @client.tree.command(name = "run-backtests", description= "Generate backtests for the selected tickers",)
    @app_commands.describe(watchlist = "Watchlist to plot the strategy against")
    @app_commands.autocomplete(watchlist=watchlist_options,)
    @app_commands.describe(strategy = "Strategy to run the backtest against")
    @app_commands.autocomplete(strategy=backtest_options,)
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])
    @app_commands.describe(cash = "Amount of captial to start the backtest with (Default: 10,000)")
    @app_commands.describe(timeframe = "Timeframe to plot the chart against (Default: 10y)")
    @app_commands.choices(timeframe = [app_commands.Choice(name = x, value= x) for x in an.get_plot_timeframes()])
    @app_commands.describe(stats_only = "True to only return stats of backtest, False to return interactive backtest HTML file (Default: False)")
    @app_commands.choices(stats_only=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  
    @app_commands.describe(summary_only = "True to only post backtest summary, False to post individual backtest stats (Default: False)")
    @app_commands.choices(summary_only=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False") 
    ])  

    async def run_backtests(interaction: discord.interactions, watchlist: str, strategy: str, visibility: app_commands.Choice[str],
                    cash: int = 10000,
                    timeframe: str = "10y", 
                    stats_only: str = "False",
                    summary_only: str = "False"):

        await interaction.response.defer(ephemeral=True)
        logger.info("/run-backtests function called by user {}".format(interaction.user.name))
        watchlist_id =  watchlist
        if watchlist == 'personal':
            watchlist_id = interaction.user.id
        tickers = sd.get_tickers_from_watchlist(watchlist_id)

        # Generate backtest stats
        backtest_stats = {}
        for ticker in tickers:
            stats = await send_backtest(**locals())
            backtest_stats[ticker] = stats

        await send_backtest_summary(interaction, backtest_stats, tickers, strategy, visibility.value)

        await interaction.followup.send("Finished generating backtests")

    
    async def send_backtest(interaction: discord.Interaction, ticker, **kwargs):
        
        # Parse ticker and data fields
        backtest_args = {}
        backtest_args['ticker'] = ticker

        data = sd.fetch_daily_data(ticker)
        if data.size == 0 or not sd.daily_data_up_to_date(data):
            sd.download_analyze_data(ticker)
            data = sd.fetch_daily_data(ticker)
        timeframe = kwargs.pop('timeframe', '10y')
        last = an.recent_bars(data, tf=timeframe) if isinstance(timeframe, str) else an.recent_bars(data, tf=timeframe.value)
        data = data.tail(last)
        backtest_args['data'] = data
        visibility = kwargs.pop("visibility").value

        # Parse strategy
        strategy = strategies.get_strategy(kwargs.pop('strategy'))()
        
            # Args for saving backtest HTML file 
        backtest_args['filepathroot'] = "{}/{}".format(ATTACHMENTS_PATH, "backtests")

        # Parse optional arguments

        cash = kwargs.pop('cash', 10000)
        backtest_args['cash'] = cash

        

        stats_only = kwargs.pop('stats_only', "False")
        backtest_args['stats_only'] = eval(stats_only) if isinstance(stats_only, str) else eval(stats_only.value)

        summary_only = kwargs.pop('summary_only', 'False')
        summary_only = eval(summary_only) if isinstance(summary_only, str) else eval(summary_only.value)
        if summary_only:
            backtest_args['stats_only'] = True
        files = []
        message  = '### Backtest of {} for {} over last {} days'.format(strategy.name, ticker, last)

        stats = strategy.backtest(**backtest_args)
        if stats is None:
            message = "Failed to plot '{}' for ticker {}. ".format(plot_name, ticker) 
            logger.error(message)
            if visibility == 'private':
                await interaction.user.send(message)
            else:
                await interaction.channel.send(message)
            return None
        message += "\n ```{}```".format(stats)
        if not backtest_args['stats_only']:
            files.append(discord.File(backtest_args['filepathroot']+ "/{}/{}_{}.html".format(ticker, ticker, strategy.short_name)))
        if not summary_only:
            if visibility == 'private':
                await interaction.user.send(message, files=files)
            else:
                await interaction.channel.send(message, files=files)
        return stats

    async def send_backtest_summary(interaction : discord.Interaction, backtest_stats, tickers, strategy_name, visibility):

        total_return = 0.0
        highest_return = 0.0
        highest_return_ticker = ''
        lowest_return  = 0.0
        lowest_return_ticker = ''
        
        for ticker in tickers:
            stats = backtest_stats.get(ticker)
            return_value = stats.get('Return [%]')
            total_return += return_value    

            if return_value > highest_return:
                highest_return = return_value
                highest_return_ticker = ticker
                if lowest_return == 0.0:
                    lowest_return = return_value
                    lowest_return_ticker = ticker
            if return_value < lowest_return:
                lowest_return = return_value
                lowest_return_ticker = ticker
                if highest_return == 0.0:
                    highest_return = return_value
                    highest_return_ticker = ticker

        message = "## Backtest Summary\n**Strategy:** {}\n**Tickers:** {}\n".format(strategy_name, ", ".join(tickers))
        message += "**Average Return**: {:2f}%\n".format(total_return/len(tickers))
        message += "**Highest Return**: {:2f}% ({})\n".format(highest_return, highest_return_ticker)
        message += "**Lowest Return**: {:2f}% ({})".format(lowest_return, lowest_return_ticker)

        if visibility == 'private':
            await interaction.user.send(message)
        else:
            await interaction.channel.send(message)



        
    ###########
    # Reports #
    ###########

    # Send daily reports for stocks on the global watchlist to the reports channel
    @tasks.loop(hours=24)  
    async def send_reports():
        
        if (dt.datetime.now().weekday() < 5):

            await send_watchlist_reports()    
            await send_strategy_reports()

        else:
            pass
    
    # Send ticker reports to reports channel for tickers on the 'daily-reports' watchlist if it exists
    async def send_watchlist_reports():
        
        # Configure channel to send reports to
        channel = await client.fetch_channel(get_reports_channel_id())
        

        watchlist = sd.get_tickers_from_watchlist('daily-reports')
        if len(watchlist) == 0:
            logger.info("No tickers found in the 'daily-reports' watchlist. No reports will be posted.")
            await channel.send("No tickers exist in the 'daily-reports' watchlist. Add tickers to this watchlist to receive daily reports")
        else:
            
            logger.info("********** [SENDING DAILY REPORTS] **********")
            logger.info("Tickers {} found in 'daily-reports' watchlist".format(watchlist))
            an.run_analysis(watchlist)
            await channel.send("## Daily Reports {}".format(dt.date.today().strftime("%m/%d/%Y")))
            for ticker in watchlist:
                report = build_ticker_report(ticker)
                message, files = report.get('message'), report.get('files')
                await channel.send(message, files=files)
            logger.info("********** [FINISHED SENDING DAILY REPORTS] **********")

    # Generate and send strategy reports to the reports channel
    async def send_strategy_reports():
        
        channel = await client.fetch_channel(get_reports_channel_id())
        reports = {}
        
        logger.info("********** [SENDING STRATEGY REPORTS] **********")
        for strategy in strategies.get_combination_strategies():
            strategy = strategy()
            message, file = build_strategy_report(strategy)
            reports[strategy.name] = {'message':message, 'file':file}

        
        await channel.send("## Strategy Report {}".format(dt.date.today().strftime("%m/%d/%Y")))
        for strategy_name, report in reports.items():
            await channel.send(report.get('message'), file=report.get('file'))

        logger.info("********** [FINISHED SENDING STRATEGY REPORTS] **********")


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

    # Build report for selected ticker to be posted
    def build_ticker_report(ticker):
        logger.info("Building report for ticker {}".format(ticker))

        data = sd.fetch_daily_data(ticker)

        # Header
        def build_report_header():

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
            
            # Append ticker name, today's date, and external links to message
            message = "## " + ticker + " Report " + dt.date.today().strftime("%m/%d/%Y") + "\n"
            links = get_ticker_links(ticker)
            message += " | ".join(links)
            return message + "\n"

        # Ticker Info
        def build_ticker_info():
            message = "### Ticker Info\n"
            try:
                ticker_data = sd.get_all_tickers_data().loc[ticker]
            except KeyError as e:
                logger.exception("Encountered KeyError when collecting ticker info:\n{}".format(e))
                sd.add_to_all_tickers(ticker)
                ticker_data = sd.get_all_tickers_data().loc[ticker]
        
            message += "**Name:** {}\n".format(ticker_data['Name'] if ticker_data['Name'] is not np.nan else "N/A")
            message += "**Sector:** {}\n".format(ticker_data['Sector']if ticker_data['Sector'] is not np.nan else "N/A")
            message += "**Industry:** {}\n".format(ticker_data['Industry'] if ticker_data['Industry'] is not np.nan else "N/A")
            message += "**Market Cap:** {}\n".format(("$" + "{:,}".format(ticker_data['Market Cap'])) if ticker_data['Market Cap'] is not np.nan else "N/A") 
            message += "**Country:** {}\n".format(ticker_data['Country'] if ticker_data['Country'] is not np.nan else "N/A")
            message += "**Next earnings date:** {}".format(sd.get_next_earnings_date(ticker))
            
            return message + "\n"

        # Daily Summary
        def build_daily_summary():
            # Append day's summary to message
            summary = sd.get_days_summary(data)
            message = "### Summary \n| "
            for col in summary.keys():
                message += "**{}:** {}".format(col, f"{summary[col]:,.2f}")
                message += " | "

            return message + "\n"

        # Indicator analysis
        def build_indicator_analysis():
            # Append analysis to mesage
            #analysis = sd.fetch_analysis(ticker)

            message = "### Indicator Analysis\n"
            for strategy in strategies.get_indicator_strategies():
                strategy = strategy()
                signals = an.get_ta_signals(strategy.signals(data))
                
                message += "{}: ".format(strategy.name)

                if signals.TS_Entries.iloc[-1]:
                    message += "**BUY\n**"
                elif signals.TS_Trends.iloc[-1]:
                    message += "**HOLD\n**"
                else:
                    message += "**SELL\n**"

            #message += analysis
            return message + "\n"

        # Strategy analysis
        def build_strategy_analysis():
            # Append strategy analysis to message
            message = ''

            message += "### Strategy Analysis\n"
            for strategy in strategies.get_combination_strategies():
                strategy = strategy()
                signals = an.get_ta_signals(strategy.signals(data))
                
                message += "{}: ".format(strategy.name)

                if signals.TS_Entries.iloc[-1]:
                    message += "**BUY\n**"
                elif signals.TS_Trends.iloc[-1]:
                    message += "**HOLD\n**"
                else:
                    message += "**SELL\n**"
            return message

        # Collect chart files
        def build_chart_files():
            # Get techincal indicator charts and convert them to a list of discord File objects
            files = sd.fetch_charts(ticker)
            for i in range(0, len(files)):
                files[i] = discord.File(files[i])
            return files

        message = build_report_header()
        message += build_ticker_info()
        message += build_daily_summary()
        message += build_indicator_analysis()
        message += build_strategy_analysis()
        files = build_chart_files()


        report = {'message':message, 'files':files} #, 'embed':links}

        return report
    
    # Build report for selected strategy against all tickers
    def build_strategy_report(strategy):
        logger.info("Building strategy report for strategy '{}'".format(strategy.name))
        watchlist_tickers = sd.get_tickers_from_all_watchlists()
        an.generate_strategy_scores(strategy)
        buys = an.get_strategy_scores(strategy)['BUY'].dropna()

        # Append watchlist tickers to report
        message = "## {} BUYS\n**Watchlist tickers: ** ".format(strategy.name)
        for index, ticker in buys.items():
            if ticker in watchlist_tickers:
                message += "{} ".format(ticker)

        # Append non-watchlist tickers to report
        message += "\n**Non-watchlist tickers: **"
        for index, ticker in buys.items():
            if ticker not in watchlist_tickers:
                message += "{} ".format(ticker)
            if len(message) >= 1990:
                break
        message += "\n\n"

        file = discord.File(an.get_strategy_score_filepath(strategy))

        return message, file
         
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

    @client.tree.command(name = "test-run-strategy-report", description= "Test running the strategy report",)
    async def test_run_strategy_report(interaction: discord.Interaction):
        logger.info("/test-run-strategy-report function called by user {}".format(interaction.user.name))
        await interaction.response.defer(ephemeral=True)

        await send_strategy_reports()

        await interaction.followup.send("Posted strategy report", ephemeral=True)

    @client.tree.command(name = "test-run-watchlist-report", description= "Test running the strategy report",)
    async def test_run_watchlist_report(interaction: discord.Interaction):
        logger.info("/test-run-watchlist-report function called by user {}".format(interaction.user.name))
        await interaction.response.defer(ephemeral=True)

        await send_watchlist_reports()

        await interaction.followup.send("Posted watchlist report", ephemeral=True)
    
    @client.tree.command(name = "fetch-all-tickers-csv", description= "Return CSV with data on all tickers the bot runs analysis on",)
    async def fetch_all_tickers_csv(interaction: discord.Interaction):
        logger.info("/fetch-all-tickers-csv function called by user {}".format(interaction.user.name))
        csv_file = discord.File("{}/all_tickers.csv".format(UTILS_PATH))
        await interaction.user.send(content = "All tickers",file=csv_file)
        await interaction.response.send_message("CSV file has been sent", ephemeral=True)

    @client.tree.command(name = "fetch-logs", description= "Return the log file for the bot",)
    async def fetch_logs(interaction: discord.Interaction):
        logger.info("/fetch-logs function called by user {}".format(interaction.user.name))
        log_file = discord.File("logs/rocketstocks.log")
        await interaction.user.send(content = "Log file for RocketStocks :rocket:",file=log_file)
        await interaction.response.send_("Log file has been sent", ephemeral=True)


    client.run(TOKEN)
    
if __name__ == "__main__":
    run_bot()

    
    
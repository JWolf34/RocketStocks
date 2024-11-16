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
import math
import datetime
import json
from table2ascii import table2ascii

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

intents = discord.Intents.default()
client = commands.Bot(command_prefix='$', intents=intents)

def get_bot_token():
    logger.debug("Fetching Discord bot token")
    try:
        token = os.getenv('DISCORD_TOKEN')
        logger.debug("Successfully fetched token")
        return token
    except Exception as e:
        logger.exception("Failed to fetch Discord bot token\n{}".format(e))
        return ""

async def load():
    for filename in os.listdir("./discord/cogs"):
        if filename.endswith(".py"):
            await client.load_extension(f"cogs.{filename[:-3]}")

async def main():
    async with client:
        await load()
        await client.start(get_bot_token())

asyncio.run(main())


def get_reports_channel_id():
    try:
        channel_id = os.getenv("REPORTS_CHANNEL_ID")
        logger.debug("Reports channel ID is {}".format(channel_id))
        return channel_id
    except Exception as e:
        logger.exception("Failed to fetch reports channel ID\n{}".format(e))
        return ""

def get_alerts_channel_id():
    try:
        channel_id = os.getenv("ALERTS_CHANNEL_ID")
        logger.debug("Alerts channel ID is {}".format(channel_id))
        return channel_id
    except Exception as e:
        logger.exception("Failed to fetch alerts channel ID\n{}".format(e))
        return ""


def run_bot():
    TOKEN = get_bot_token()

    #intents = discord.Intents.default()
    #client = commands.Bot(command_prefix='$', intents=intents)

    @client.event
    async def on_ready():
        try:
            await client.tree.sync()
            #send_reports.start()
            send_gainer_reports.start()
        except Exception as e:
            logger.exception("Encountered error waiting for on-ready signal from bot\n{}".format(e))
        logger.info("Bot connected! ")

    async def sync_commands():
        logger.info("Syncing tree commands")
        await client.tree.sync()
        logger.info("Commands synced!")

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


# Config

def get_config():
    try:
        config = open("config.json")
        data = json.load(config)
        return data 
    except FileNotFoundError as e:
        write_config({})

def write_config(data):
    with open("config.json", 'w') as config_file:
        json.dump(data, config_file)
        
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

    @client.tree.command(name = "test-gainer-reports", description= "Test posting premarket gainer reports",)
    async def test_premarket_reports(interaction: discord.Interaction):
        logger.info("/test-premarket-reports function called by user {}".format(interaction.user.name))
        await interaction.response.defer(ephemeral=True)

        report = GainerReport()
        await report.send_report()

        await interaction.followup.send("Posted premarket reports", ephemeral=True)

    #client.run(TOKEN)
    
#if __name__ == "__main__":
    #run_bot()

    
    
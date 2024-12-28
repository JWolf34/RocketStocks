import sys
sys.path.append("../RocketStocks/discord/cogs")
from watchlists import Watchlists
import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
import stockdata as sd
import analysis as an
import strategies
import logging

# Logging configuration
logger = logging.getLogger(__name__)

class Charts(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    async def chart_options(self, interaction: discord.Interaction, current: str):
        charts = strategies.get_strategies().keys()
        return [
            app_commands.Choice(name = chart, value= chart)
            for chart in charts if current.lower() in chart.lower()
        ]

    # Plot graphs for the selected tickers
    @app_commands.command(name = "plot-charts", description= "Plot selected graphs for the selected tickers",)
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
    @commands.is_owner()
    async def plot_charts(self, interaction: discord.interactions, tickers: str, chart: str, visibility: app_commands.Choice[str],
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
        plot_args = locals()
        plot_args.pop("self", None)
        plot_args.pop("interaction", None)
        plot_args.pop("ticker", None)
        for ticker in tickers:
            await self.send_charts(interaction, ticker, **plot_args)

        await interaction.followup.send("Finished generating charts")

    # Plot graphs for the selected watchlist
    @app_commands.command(name = "run-charts", description= "Plot selected graphs for the selected tickers",)
    @app_commands.describe(watchlist = "Watchlist to plot the strategy against")
    @app_commands.autocomplete(watchlist=Watchlists.watchlist_options,)
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
    @commands.is_owner()
    async def run_charts(self, interaction: discord.interactions, watchlist: str, chart: str, visibility: app_commands.Choice[str],
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
        plot_args = locals()
        plot_args.pop("self", None)
        plot_args.pop("interaction", None)
        plot_args.pop("ticker", None)
        for ticker in tickers:
            await self.send_charts(interaction, ticker, **plot_args)

        await interaction.followup.send("Finished generating charts")


    async def send_charts(self, interaction: discord.Interaction, ticker, **kwargs):
            
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
    
    async def backtest_options(self, interaction: discord.Interaction, current: str):
        charts = strategies.get_combination_strategies()
        bt_strategies = [chart().name for chart in charts]
        return [
            app_commands.Choice(name = strategy, value= strategy)
            for strategy in bt_strategies if current.lower() in strategy.lower()
        ]

    # Generate backtests for the selected tickers
    @app_commands.command(name = "fetch-backtests", description= "Generate backtests for the selected tickers",)
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
    @commands.is_owner()
    async def fetch_backtests(self, interaction: discord.interactions, tickers: str, strategy: str, visibility: app_commands.Choice[str],
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
    @app_commands.command(name = "run-backtests", description= "Generate backtests for the selected tickers",)
    @app_commands.describe(watchlist = "Watchlist to plot the strategy against")
    @app_commands.autocomplete(watchlist=Watchlists.watchlist_options,)
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
    @commands.is_owner()
    async def run_backtests(self, interaction: discord.interactions, watchlist: str, strategy: str, visibility: app_commands.Choice[str],
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

    
    async def send_backtest(self, interaction: discord.Interaction, ticker, **kwargs):
        
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

    async def send_backtest_summary(self, interaction : discord.Interaction, backtest_stats, tickers, strategy_name, visibility):

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

    
#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Charts(bot))
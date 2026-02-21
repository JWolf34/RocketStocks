import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
from rocketstocks.bot.cogs.watchlists import Watchlists
from rocketstocks.data.stock_data import StockData
from rocketstocks.core.charting.helpers import get_plot_timeframes, get_plot_types, get_plot_styles
import logging

logger = logging.getLogger(__name__)


class Charts(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    '''
    async def chart_options(self, interaction: discord.Interaction, current: str):
        charts = []
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
    @app_commands.choices(timeframe = [app_commands.Choice(name = x, value= x) for x in get_plot_timeframes()])

    @app_commands.describe(plot_type = "Style in which to plot the Close price on the chart")
    @app_commands.choices(plot_type = [app_commands.Choice(name = x, value= x) for x in get_plot_types()])
    @app_commands.describe(style = " Style in which to generate the chart")
    @app_commands.choices(style = [app_commands.Choice(name = x, value = x) for x in get_plot_styles()])
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
    @app_commands.choices(squeeze=[
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

        plot_args = locals()
        plot_args.pop("self", None)
        plot_args.pop("interaction", None)
        plot_args.pop("ticker", None)
        for ticker in tickers:
            await self.send_charts(interaction, ticker, **plot_args)

        await interaction.followup.send("Finished generating charts")
    '''


async def setup(bot):
    await bot.add_cog(Charts(bot))

import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
import stockdata as sd
import config
import logging

# Logging configuration
logger = logging.getLogger(__name__)

class Data(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    @app_commands.command(name = "fetch-csv", description= "Returns data file for input ticker. Default: 1 year period.",)
    @app_commands.describe(tickers = "Tickers to return data for (separated by spaces)")
    @app_commands.describe(type="Type of data file to return - daily data or minute-by-minute data")
    @app_commands.choices(type=[
        app_commands.Choice(name='daily', value='daily'),
        app_commands.Choice(name='minute', value='minute')
    ])
    async def fetch_csv(self, interaction: discord.Interaction, tickers: str, type: app_commands.Choice[str]):
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
                    csv_path = "{}/{}.csv".format(config.get_daily_data_path(),ticker)
                    if not sd.daily_data_up_to_date(data):
                        sd.download_analyze_data(ticker)
                else:
                    data = sd.fetch_minute_data(ticker)
                    csv_path = "{}/{}.csv".format(config.get_minute_data_path(),ticker)
                    data = sd.download_data(ticker, period='7d', interval='1m')
                    sd.update_csv(data, ticker, config.get_minute_data_path())

                file = discord.File(csv_path)
                await interaction.user.send(content = "Data file for {}".format(ticker), file=file)

            if len(invalid_tickers) > 0:
                await interaction.followup.send("Fetched data files for {}. Invalid tickers:".format(", ".join(tickers), ", ".join(invalid_tickers)), ephemeral=True)
            else:
                await interaction.followup.send("Fetched data files for {}".format(", ".join(tickers)), ephemeral=True)


        except Exception as e:
            logger.exception("Failed to fetch data file with following exception:\n{}".format(e))
            await interaction.followup.send("Failed to fetch data files. Please ensure your parameters are valid.")
    
    @app_commands.command(name = "fetch-financials", description= "Fetch financial reports of the specified tickers ",)
    @app_commands.describe(tickers = "Tickers to return financials for (separated by spaces)")
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])        
    async def fetch_financials(self, interaction: discord.interactions, tickers: str, visibility: app_commands.Choice[str]):
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

    @app_commands.command(name = "fetch-logs", description= "Return the log file for the bot",)
    async def fetch_logs(self, interaction: discord.Interaction):
        logger.info("/fetch-logs function called by user {}".format(interaction.user.name))
        log_file = discord.File("logs/rocketstocks.log")
        await interaction.user.send(content = "Log file for RocketStocks :rocket:",file=log_file)
        await interaction.response.send_message("Log file has been sent", ephemeral=True)

    @app_commands.command(name = "fetch-all-tickers-csv", description= "Return CSV with data on all tickers the bot runs analysis on",)
    async def fetch_all_tickers_csv(self, interaction: discord.Interaction):
        logger.info("/fetch-all-tickers-csv function called by user {}".format(interaction.user.name))
        csv_file = discord.File("{}/all_tickers.csv".format(config.get_utils_path()))
        await interaction.user.send(content = "All tickers",file=csv_file)
        await interaction.response.send_message("CSV file has been sent", ephemeral=True)

            

#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Data(bot))
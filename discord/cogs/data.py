import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks
import stockdata as sd
import config
import csv
import logging
import json
from table2ascii import table2ascii, Alignment, PresetStyle

# Logging configuration
logger = logging.getLogger(__name__)

class Data(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.reports_channel=self.bot.get_channel(config.get_reports_channel_id())
        

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    @app_commands.command(name = "csv", description= "Returns data file for input ticker. Default: 1 year period.",)
    @app_commands.describe(tickers = "Tickers to return data for (separated by spaces)")
    @app_commands.describe(frequency="Type of data file to return - daily data or minute-by-minute data")
    @app_commands.choices(frequency=[
        app_commands.Choice(name='daily', value='daily'),
        app_commands.Choice(name='5m', value='5m')
    ])
    async def csv(self, interaction: discord.Interaction, tickers: str, frequency: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/csv function called by user {}".format(interaction.user.name))
        logger.debug("Data file(s) for {} requested".format(tickers))

        frequency = frequency.value
        
        files = []
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)
        try:
            for ticker in tickers:
                if frequency == 'daily':
                    data = sd.StockData.fetch_daily_price_history(ticker)
                    if data.size > 0:
                        pass
                    else:
                        data = sd.Schwab().get_daily_price_history(ticker)
                else:
                    data = sd.StockData.fetch_5m_price_history(ticker)
                    if data.size > 0:
                        pass
                    else:
                        data = sd.Schwab().get_5m_price_history(ticker)
                message = ""
                file = None
                if data is not None:
                    message = f"Data file for {ticker}"
                    filepath = f"{config.get_attachments_path()}/{ticker}_{frequency}_data.csv"
                    data.to_csv(filepath, index=False)
                    file = discord.File(filepath)
                else:
                    message = f"Could not fetch data for ticker {ticker}"
                
                await interaction.user.send(content = message, file=file)

            if len(invalid_tickers) > 0:
                await interaction.followup.send("Fetched data files for {}. Invalid tickers:".format(", ".join(tickers), ", ".join(invalid_tickers)), ephemeral=True)
            else:
                await interaction.followup.send("Fetched data files for {}".format(", ".join(tickers)), ephemeral=True)


        except Exception as e:
            logger.exception("Failed to fetch data file with following exception:\n{}".format(e))
            await interaction.followup.send("Failed to fetch data files. Please ensure your parameters are valid.")
    
    @app_commands.command(name = "financials", description= "Fetch financial reports of the specified tickers ",)
    @app_commands.describe(tickers = "Tickers to return financials for (separated by spaces)")
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])        
    async def financials(self, interaction: discord.interactions, tickers: str, visibility: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/financials function called by user {}".format(interaction.user.name))
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

    @app_commands.command(name = "logs", description= "Return the log file for the bot",)
    async def logs(self, interaction: discord.Interaction):
        logger.info("/logs function called by user {}".format(interaction.user.name))
        log_file = discord.File("logs/rocketstocks.log")
        await interaction.user.send(content = "Log file for RocketStocks :rocket:",file=log_file)
        await interaction.response.send_message("Log file has been sent", ephemeral=True)

    @app_commands.command(name = "all-tickers-info", description= "Return CSV with data on all tickers the bot runs analysis on",)
    async def all_tickers_csv(self, interaction: discord.Interaction):
        logger.info("/all-tickers-into function called by user {}".format(interaction.user.name))
        data = sd.StockData.get_all_ticker_info()
        filepath = f"{config.get_attachments_path()}/all-tickers-info.csv"
        data.to_csv(filepath)
        csv_file = discord.File(filepath)       
        await interaction.user.send(content = "All tickers",file=csv_file)
        await interaction.response.send_message("CSV file has been sent", ephemeral=True)
        
    @app_commands.command(name = "eps", description= "Returns recent EPS data for the input tickers",)
    @app_commands.describe(tickers = "Tickers to return EPS data for (separated by spaces)")
    @app_commands.describe(visibility = "'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility =[
        app_commands.Choice(name = "private", value = 'private'),
        app_commands.Choice(name = "public", value = 'public')
    ])
    async def eps(self, interaction: discord.Interaction, tickers: str, visibility: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info("/eps function called by user {}".format(interaction.user.name))
        sd.validate_path(config.get_attachments_path())
        message = ""
        file = None
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)
        for ticker in tickers:
            eps = sd.StockData.Earnings.get_historical_earnings(ticker)
            if eps.size > 0:
                filepath = f"{config.get_attachments_path()}/{ticker}_eps.csv"
                eps.to_csv(filepath, index=False)
                file = discord.File(filepath)
                eps_table = table2ascii(
                    header = eps.columns.tolist(),
                    body = eps.values.tolist()[-4:],
                    style=PresetStyle.thick,
                    alignments=[Alignment.LEFT, Alignment.LEFT, Alignment.LEFT, Alignment.LEFT]
                )
                message = f"## Recent EPS for {ticker}\n ```{eps_table}```"
            else:
                message = f"Could not retrieve EPS data for ticker {ticker}"
            if visibility.value == "private":
                message = await interaction.user.send(message, files=[file])
            else:
                message = await self.reports_channel.send(message, files=[file])

        # Follow-up
        follow_up = ""
        if message is not None: # Message was generated
            follow_up = f"Posted EPS for tickers [{", ".join(tickers)}]({message.jump_url})!"
            if len(invalid_tickers) > 0: # More than one invalid ticke input
                follow_up += f" Invalid tickers: {", ".join(invalid_tickers)}"
        if len(tickers) == 0: # No valid tickers input
            follow_up = f"No valid tickers input: {", ".join(invalid_tickers)}"
        await interaction.followup.send(follow_up, ephemeral=True)
    
    @app_commands.command(name = "form", description= "Returns links to latest form of requested type",)
    @app_commands.describe(tickers = "Tickers to return SEC forms for (separated by spaces)")
    @app_commands.describe(form = "The form type to get a link to (10-K, 10-Q, 8-K, etc)")
    async def form(self, interaction: discord.Interaction, tickers: str, form:str):
        await interaction.response.defer()
        logger.info("/form function called by user {}".format(interaction.user.name))
        
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)
        sec = sd.SEC()
        message = ""
        for ticker in tickers:
            recent_filings = sec.get_recent_filings(ticker)
            target_filing = None
            for index, filing in recent_filings.iterrows():
                if filing['form'] == form:
                    target_filing = filing
                    break
            if target_filing is None:
                message += f"No form {form} found for ticker {ticker}\n"
            else:
                # Need to make universal date conversion function and make SEC module reference CIK value from database
                message += f"[{ticker} Form {form} - Filed {sd.StockData.Earnings.format_earnings_date(target_filing['filingDate'])}]({sec.get_link_to_filing(ticker, target_filing)})\n"
        await interaction.followup.send(message)
    
    @app_commands.command(name="fundamentals", description="Return JSON files of fundamental data for desired tickers")
    @app_commands.describe(tickers = "Tickers to return SEC forms for (separated by spaces)")
    async def fundamentals(self, interaction: discord.Interaction, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/fundamentals function called by user {interaction.user.name}")
        message = None
        file = None
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)
        for ticker in tickers:
            fundamentals = sd.Schwab().get_fundamentals(ticker)
            
            if fundamentals is not None:
                filepath = f"{config.get_attachments_path()}/{ticker}_fundamentals.json"
                with open(filepath, 'w') as json_file:
                    json.dump(fundamentals, json_file)
                file = discord.File(filepath)
                message = f"Fundamentals for ticker {ticker}"
            else:
                message = f"Could not retrieve fundamentals for ticker {ticker}"
            message = await interaction.user.send(content=message, file=file)
        
        # Follow-up
        follow_up = ""
        if message is not None: # Message was generated
            follow_up = f"Posted fundamentals for tickers [{", ".join(tickers)}]({message.jump_url})!"
            if len(invalid_tickers) > 0: # More than one invalid ticke input
                follow_up += f" Invalid tickers: {", ".join(invalid_tickers)}"
        if len(tickers) == 0: # No valid tickers input
            follow_up = f"No valid tickers input: {", ".join(invalid_tickers)}"
        await interaction.followup.send(follow_up, ephemeral=True)

    @app_commands.command(name="options", description="Return JSON files of fundamental data for desired tickers")
    @app_commands.describe(tickers = "Tickers to return SEC forms for (separated by spaces)")
    async def options(self, interaction: discord.Interaction, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/fundamentals function called by user {interaction.user.name}")
        message = None
        file = None
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)
        for ticker in tickers:
            fundamentals = sd.Schwab().get_options_chain(ticker)
            
            if fundamentals is not None:
                filepath = f"{config.get_attachments_path()}/{ticker}_options_chain.json"
                with open(filepath, 'w') as json_file:
                    json.dump(fundamentals, json_file)
                file = discord.File(filepath)
                message = f"Options chain for ticker {ticker}"
            else:
                message = f"Could not retrieve options chain for ticker {ticker}"
            message = await interaction.user.send(content=message, file=file)
        
        # Follow-up
        follow_up = ""
        if message is not None: # Message was generated
            follow_up = f"Posted otpions chains for tickers [{", ".join(tickers)}]({message.jump_url})!"
            if len(invalid_tickers) > 0: # More than one invalid ticke input
                follow_up += f" Invalid tickers: {", ".join(invalid_tickers)}"
        if len(tickers) == 0: # No valid tickers input
            follow_up = f"No valid tickers input: {", ".join(invalid_tickers)}"
        await interaction.followup.send(follow_up, ephemeral=True)


#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Data(bot))
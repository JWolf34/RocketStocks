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
        self.reports_channel=self.bot.get_channel(config.discord_utils.reports_channel_id)
        

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
        logger.info("Data file(s) for {} requested".format(tickers))

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
                    filepath = f"{config.datapaths.attachments_path}/{ticker}_{frequency}_data.csv"
                    data.to_csv(filepath, index=False)
                    file = discord.File(filepath)
                else:
                    message = f"Could not fetch data for ticker {ticker}"
                
                message = await interaction.user.send(content = message, file=file)

            if len(invalid_tickers) > 0:
                logger.info(f"Sent data files for tickers {tickers}. Invalid tickers: {invalid_tickers}")
                await interaction.followup.send("Fetched data files for [{}]({}). Invalid tickers: {}".format(", ".join(tickers), message.jump_url, ", ".join(invalid_tickers)), ephemeral=True)
            else:
                logger.info(f"Sent data files for tickers {tickers}")
                await interaction.followup.send("Fetched data files for [{}]({})".format(", ".join(tickers), message.jump_url), ephemeral=True)


        except Exception as e:
            logger.exception("Failed to fetch data file with following exception:\n{}".format(e))
            await interaction.followup.send("Failed to fetch data files. Please ensure your parameters are valid.")
    
    @app_commands.command(name = "financials", description= "Fetch financial reports of the specified tickers ",)
    @app_commands.describe(tickers = "Tickers to return financials for (separated by spaces)")       
    async def financials(self, interaction: discord.interactions, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/financials function called by user {}".format(interaction.user.name))
        logger.info("Financials requested for {}".format(tickers))


        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)

        message = None
        for ticker in tickers:
            files = []
            financials = sd.StockData.fetch_financials(ticker)
            for statement, data in financials.items():
                filepath = f"{config.datapaths.attachments_path}/{ticker}_{statement}.csv"
                data.to_csv(filepath)
                files.append(discord.File(filepath))
            
            message = await interaction.user.send("Financials for {}".format(ticker), files=files)
      
        # Follow-up
        follow_up = ""
        if message is not None: # Message was generated
            logger.info(f"Provided financials for tickers {tickers}")
            follow_up = f"Posted financials for tickers [{", ".join(tickers)}]({message.jump_url})!"
            if len(invalid_tickers) > 0: # More than one invalid ticke input
                follow_up += f" Invalid tickers: {", ".join(invalid_tickers)}"
        if len(tickers) == 0: # No valid tickers input
            logger.info("No valid tickers input. No fincancials returned")
            follow_up = f"No valid tickers input: {", ".join(invalid_tickers)}"
        await interaction.followup.send(follow_up, ephemeral=True)
        
    @app_commands.command(name = "logs", description= "Return the log file for the bot",)
    async def logs(self, interaction: discord.Interaction):
        logger.info("/logs function called by user {}".format(interaction.user.name))
        log_file = discord.File("logs/rocketstocks.log")
        await interaction.user.send(content = "Log file for RocketStocks :rocket:",file=log_file)
        await interaction.response.send_message("Log file has been sent", ephemeral=True)
        logger.info("Log file successfully sent")

    @app_commands.command(name = "all-tickers-info", description= "Return CSV with data on all tickers the bot runs analysis on",)
    async def all_tickers_csv(self, interaction: discord.Interaction):
        logger.info("/all-tickers-into function called by user {}".format(interaction.user.name))
        data = sd.StockData.get_all_ticker_info()
        filepath = f"{config.datapaths.attachments_path}/all-tickers-info.csv"
        data.to_csv(filepath)
        csv_file = discord.File(filepath)       
        await interaction.user.send(content = "All tickers",file=csv_file)
        await interaction.response.send_message("CSV file has been sent", ephemeral=True)
        logger.info(f"Provided data file for all {len(data)} tickers")
        
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
        logger.info(f"EPS requested for tickers {tickers}")
        sd.validate_path(config.datapaths.attachments_path)
        message = ""
        file = None
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)
        for ticker in tickers:
            eps = sd.StockData.Earnings.get_historical_earnings(ticker)
            if eps.size > 0:
                filepath = f"{config.datapaths.attachments_path}/{ticker}_eps.csv"
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
            logger.info(f"Provided EPS data for tickers {tickers}")
            follow_up = f"Posted EPS for tickers [{", ".join(tickers)}]({message.jump_url})!"
            if len(invalid_tickers) > 0: # More than one invalid ticke input
                follow_up += f" Invalid tickers: {", ".join(invalid_tickers)}"
        if len(tickers) == 0: # No valid tickers input
            logger.info(f"No valid tickers input. No EPS data provided")
            follow_up = f"No valid tickers input: {", ".join(invalid_tickers)}"
        await interaction.followup.send(follow_up, ephemeral=True)
    
    @app_commands.command(name = "form", description= "Returns links to latest form of requested type",)
    @app_commands.describe(tickers = "Tickers to return SEC forms for (separated by spaces)")
    @app_commands.describe(form = "The form type to get a link to (10-K, 10-Q, 8-K, etc)")
    async def form(self, interaction: discord.Interaction, tickers: str, form:str):
        await interaction.response.defer()
        logger.info("/form function called by user {}".format(interaction.user.name))
        logger.info(f"Form {form} requested for tickers {tickers}")
        
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
                filing_date = config.date_utils.format_date_mdy(target_filing['filingDate'])
                sec_link = sec.get_link_to_filing(ticker, target_filing)
                message += f"[{ticker} Form {form} - Filed {filing_date}]({sec_link})\n"
        await interaction.followup.send(message)
        logger.info(f"Form {form} provided for tickers {tickers}")
    
    @app_commands.command(name="fundamentals", description="Return JSON files of fundamental data for desired tickers")
    @app_commands.describe(tickers = "Tickers to return SEC forms for (separated by spaces)")
    async def fundamentals(self, interaction: discord.Interaction, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/fundamentals function called by user {interaction.user.name}")
        logger.info(f"Fundamentals requested for tickers {tickers}")
        message = None
        file = None
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)
        for ticker in tickers:
            fundamentals = sd.Schwab().get_fundamentals(ticker)
            
            if fundamentals is not None:
                filepath = f"{config.datapaths.attachments_path}/{ticker}_fundamentals.json"
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
            logger.info(f"Fundamentals provided for tickers {tickers}")
            follow_up = f"Posted fundamentals for tickers [{", ".join(tickers)}]({message.jump_url})!"
            if len(invalid_tickers) > 0: # More than one invalid ticke input
                follow_up += f" Invalid tickers: {", ".join(invalid_tickers)}"
        if len(tickers) == 0: # No valid tickers input
            logger.info("No valid tickers input. No fincancials provided")
            follow_up = f"No valid tickers input: {", ".join(invalid_tickers)}"
        await interaction.followup.send(follow_up, ephemeral=True)

    @app_commands.command(name="options", description="Return JSON files of fundamental data for desired tickers")
    @app_commands.describe(tickers = "Tickers to return SEC forms for (separated by spaces)")
    async def options(self, interaction: discord.Interaction, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/options function called by user {interaction.user.name}")
        logger.info(f"Options chain(s) requested for tickers {tickers}")
        message = None
        file = None
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)
        for ticker in tickers:
            options = sd.Schwab().get_options_chain(ticker)
            
            if options is not None:
                filepath = f"{config.datapaths.attachments_path}/{ticker}_options_chain.json"
                with open(filepath, 'w') as json_file:
                    json.dump(options, json_file)
                file = discord.File(filepath)
                message = f"Options chain for ticker {ticker}"
            else:
                message = f"Could not retrieve options chain for ticker {ticker}"
            message = await interaction.user.send(content=message, file=file)
        
        # Follow-up
        follow_up = ""
        if message is not None: # Message was generated
            logger.info(f"Options chain(s) provided for tickers {tickers}")
            follow_up = f"Posted options chains for tickers [{", ".join(tickers)}]({message.jump_url})!"
            if len(invalid_tickers) > 0: # More than one invalid ticke input
                follow_up += f" Invalid tickers: {", ".join(invalid_tickers)}"
        if len(tickers) == 0: # No valid tickers input
            logger.info("No valid tickers input. No options chains provided")
            follow_up = f"No valid tickers input: {", ".join(invalid_tickers)}"
        await interaction.followup.send(follow_up, ephemeral=True)

    @app_commands.command(name="popularity", description="Return historical popularity of desired tickers in CSV format")
    @app_commands.describe(tickers = "Tickers to return popularity for (separated by spaces)")
    async def popularity(self, interaction: discord.Interaction, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/popularity function called by user {}".format(interaction.user.name))
        logger.info(f"Historical popularity requested for tickers {tickers}")
        files = []
        tickers, invalid_tickers = sd.StockData.get_valid_tickers(tickers)
        try:
            for ticker in tickers:
                message = ""
                file = None
                data = sd.StockData.get_historical_popularity(ticker=ticker)  
                if data.size:
                    message = f"Popularity for {ticker}"
                    filepath = f"{config.datapaths.attachments_path}/{ticker}_popularity.csv"
                    data.to_csv(filepath, index=False)
                    file = discord.File(filepath)
                else:
                    message = f"No popularity data available for ticker {ticker}"
                
                message = await interaction.user.send(content = message, file=file)

            if len(invalid_tickers) > 0:
                logger.info(f"Provided popualrity for tickers {tickers}. Invalid tickers: {invalid_tickers}")
                await interaction.followup.send("Fetched popularity for [{}]({}). Invalid tickers: {}".format(", ".join(tickers), message.jump_url, ", ".join(invalid_tickers)), ephemeral=True)
            else:
                logger.info(f"Provided popualrity for tickers {tickers}")
                await interaction.followup.send("Fetched popularity for [{}]({})".format(", ".join(tickers), message.jump_url), ephemeral=True)


        except Exception as e:
            logger.exception("Failed to fetch popularity with following exception:\n{}".format(e))
            await interaction.followup.send("Failed to popularity reports. Please ensure your parameters are valid.")

    async def politician_options(self, interaction: discord.Interaction, current: str):
        politicians = sd.CapitolTrades.all_politicians()
        names = [politician['name'] for politician in politicians]
        return [
            app_commands.Choice(name = p_name, value=p_name)
            for p_name in names if current.lower() in p_name.lower()
        ][:25]

    @app_commands.command(name="politician-trades", description="Return trades for target politician recorded on Capitol Trades")
    @app_commands.describe(politician_name = "Politician to return trades for")
    @app_commands.autocomplete(politician_name=politician_options,)
    async def politician_trades(self, interaction: discord.Interaction, politician_name:str):
        await interaction.response.defer(ephemeral=True)
        logger.info("/politician-trades function called by user {}".format(interaction.user.name))
        logger.info(f"Trades requested for politician {politician_name}")
        politician_names = [politician['name'] for politician in sd.CapitolTrades.all_politicians()]
        if politician_name not in politician_names:
            await interaction.followup.send("Invalid politician name provided. Please select a valid name from the list available.", ephemeral=True)
        else:
            politician = sd.CapitolTrades.politician(name=politician_name)
            trades = sd.CapitolTrades.trades(pid=politician['politician_id'])

            message = ""
            file = None
            if not trades.empty:
                message = f"Trades made by {politician_name} - [Capitol Trades](<https://www.capitoltrades.com/politicians/{politician['politician_id']}>)"
                filepath = f"{config.datapaths.attachments_path}/trades_{politician['name'].lower().replace(" ",'_')}.csv"
                trades.to_csv(filepath, index=False)
                file = discord.File(filepath)
            else:
                message = f"No trades found for {politician_name}"
            
            message = await interaction.followup.send(content = message, file=file)




#########        
# Setup #
#########

async def setup(bot):
    await bot.add_cog(Data(bot))
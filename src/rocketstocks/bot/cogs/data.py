import discord
from discord import app_commands
from discord.ext import commands
from src.rocketstocks.data.stockdata import StockData
from rocketstocks.data.channel_config import REPORTS
from rocketstocks.core.config.paths import datapaths
from rocketstocks.core.utils.formatting import ticker_string
from rocketstocks.core.utils.dates import date_utils
import logging
import json
from table2ascii import table2ascii, Alignment, PresetStyle
import zipfile
import os

logger = logging.getLogger(__name__)


class Data(commands.Cog):
    """Cog for returning data to the user, such as JSON or CSV files"""
    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    @app_commands.command(name="csv", description="Returns data file for input ticker. Default: 1 year period.",)
    @app_commands.describe(tickers="Tickers to return data for (separated by spaces)")
    @app_commands.describe(frequency="Type of data file to return - daily data or minute-by-minute data")
    @app_commands.choices(frequency=[
        app_commands.Choice(name='daily', value='daily'),
        app_commands.Choice(name='5m', value='5m')
    ])
    async def csv(self, interaction: discord.Interaction, tickers: str, frequency: app_commands.Choice[str]):
        """Return CSV file of requested frequency of the requested ticker"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/csv function called by user {interaction.user.name}")

        frequency = frequency.value
        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Data file(s) requested for {tickers}")

        for ticker in tickers:
            if frequency == 'daily':
                data = await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
            else:
                data = await self.stock_data.price_history.fetch_5m_price_history(ticker=ticker)
            message = ""
            file = None
            if not data.empty:
                message = f"{frequency.capitalize()} data file for {ticker}"
                filepath = f"{datapaths.attachments_path}/{ticker}_{frequency}_data.csv"
                data.to_csv(filepath, index=False)
                file = discord.File(filepath)
            else:
                message = f"Could not fetch price data for ticker `{ticker}`"

            message = await interaction.user.send(content=message, file=file)

        if tickers:
            message = f"Fetched {frequency} data files for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            message = f"Could not fetch {frequency} data files."
        if invalid_tickers:
            message += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(message, ephemeral=True)

    @app_commands.command(name="financials", description="Fetch financial reports of the specified tickers",)
    @app_commands.describe(tickers="Tickers to return financials for (separated by spaces)")
    async def financials(self, interaction: discord.interactions, tickers: str):
        """Return latest financials on input tickers in JSON format"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/financials function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Financials requested for {tickers}")

        for ticker in tickers:
            files = []
            financials = self.stock_data.fetch_financials(ticker)
            for statement, data in financials.items():
                filepath = f"{datapaths.attachments_path}/{ticker}_{statement}.csv"
                data.to_csv(filepath)
                files.append(discord.File(filepath))

            message = await interaction.user.send("Financials for {}".format(ticker), files=files)
            logger.info(f"Posted financials for ticker '{ticker}'")

        if tickers:
            message = f"Fetched financials for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            message = f"Could not fetch financials."
        if invalid_tickers:
            message += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(message, ephemeral=True)

    @app_commands.command(name="logs", description="Return the log file for the bot",)
    async def logs(self, interaction: discord.Interaction):
        """Return latest log file and ZIP file of all log files for the bot"""
        logger.info(f"/logs function called by user {interaction.user.name}")

        files = []

        log_file = discord.File("logs/rocketstocks.log")
        files.append(log_file)

        logs_zip = zipfile.ZipFile(f"{datapaths.attachments_path}/logs.zip", 'w', zipfile.ZIP_DEFLATED)
        for log in os.listdir("logs"):
            logs_zip.write(f"logs/{log}")
        logs_zip.close()
        files.append(discord.File(f"{datapaths.attachments_path}/logs.zip"))

        await interaction.user.send(content="Log file for RocketStocks :rocket:", files=files)
        await interaction.response.send_message("Log file has been sent", ephemeral=True)
        logger.info("Log file sent successfully")

    @app_commands.command(name="all-tickers-info", description="Return CSV with data on all tickers the bot runs analysis on",)
    async def all_tickers_csv(self, interaction: discord.Interaction):
        """Return CSV file with contents of 'tickers' table in database"""
        logger.info(f"/all-tickers-into function called by user {interaction.user.name}")
        data = await self.stock_data.tickers.get_all_ticker_info()
        filepath = f"{datapaths.attachments_path}/all-tickers-info.csv"
        data.to_csv(filepath)
        csv_file = discord.File(filepath)
        await interaction.user.send(content="All tickers", file=csv_file)
        await interaction.response.send_message("CSV file has been sent", ephemeral=True)
        logger.info(f"Provided data file for all {len(data)} tickers")

    @app_commands.command(name="earnings", description="Returns recent earnings data for the input tickers",)
    @app_commands.describe(tickers="Tickers to return EPS data for (separated by spaces)")
    @app_commands.describe(visibility="'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility=[
        app_commands.Choice(name="private", value='private'),
        app_commands.Choice(name="public", value='public')
    ])
    async def earnings(self, interaction: discord.Interaction, tickers: str, visibility: app_commands.Choice[str]):
        """Return hisorical earnings data in CSV formats for input tickers and post recent earnings data in message"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/earnings function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Earnings requested for {tickers}")

        column_map = {'date': 'Date Reported',
                      'eps': 'EPS',
                      'surprise': 'Surprise',
                      'epsforecast': 'Estimate',
                      'fiscalquarterending': 'Quarter'}

        for ticker in tickers:
            eps = await self.stock_data.earnings.get_historical_earnings(ticker)
            if not eps.empty:
                filepath = f"{datapaths.attachments_path}/{ticker}_eps.csv"
                eps.to_csv(filepath, index=False)
                file = discord.File(filepath)

                eps = eps.iloc[::-1].head(12)
                eps = eps.filter(list(column_map.keys()))
                eps = eps.rename(columns=column_map)
                eps_table = table2ascii(
                    header=eps.columns.tolist(),
                    body=eps.values.tolist(),
                    style=PresetStyle.thick
                )
                message = f"**Earnings for {ticker}**\n ```{eps_table}```"
            else:
                message = f"Could not retrieve EPS data for ticker `{ticker}`"
            if visibility.value == "private":
                message = await interaction.user.send(message, files=[file])
            else:
                channel = self.bot.get_channel_for_guild(interaction.guild_id, REPORTS)
                if channel is None:
                    await interaction.followup.send("Use `/setup` to configure the reports channel.", ephemeral=True)
                    return
                message = await channel.send(message, files=[file])

        if tickers:
            message = f"Fetched EPS data for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            message = f"Could not fetch EPS data."
        if invalid_tickers:
            message += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(message, ephemeral=True)

    @app_commands.command(name="form", description="Returns link to latest SEC form of requested type",)
    @app_commands.describe(tickers="Tickers to return SEC forms for (separated by spaces)")
    @app_commands.describe(form="The form type to get a link to (10-K, 10-Q, 8-K, etc)")
    async def form(self, interaction: discord.Interaction, tickers: str, form: str):
        """Return links to latest SEC forms of given type for input tickers"""
        await interaction.response.defer()
        logger.info(f"/form function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)

        message = ""
        for ticker in tickers:
            recent_filings = await self.stock_data.sec.get_recent_filings(ticker=ticker, latest=250)
            target_filing = None
            for index, filing in recent_filings.iterrows():
                if filing['form'] == form:
                    target_filing = filing
                    break
            if target_filing is None:
                message += f"No form {form} found for ticker `{ticker}`\n"
            else:
                filing_date = date_utils.format_date_mdy(target_filing['filingDate'])
                sec_link = target_filing['link']
                message += f"[{ticker} Form {form} - Filed {filing_date}]({sec_link})\n"

        if not message:
            message = f"No form {form} found for given tickers {ticker_string(tickers)}"
        else:
            message = f"Form {form} for tickers {ticker_string(tickers)}:\n\n" + message
            if invalid_tickers:
                message += f"\n\nInvalid tickers: {ticker_string(invalid_tickers)}"
        await interaction.followup.send(message)
        logger.info(f"Form {form} provided for tickers {tickers}")

    @app_commands.command(name="fundamentals", description="Return fundamental data for desired tickers in JSON format")
    @app_commands.describe(tickers="Tickers to return fundamentals for (separated by spaces)")
    async def fundamentals(self, interaction: discord.Interaction, tickers: str):
        """Return fundamentals in JSON format for input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/fundamentals function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Fundamentals requested for tickers {tickers}")

        for ticker in tickers:
            fundamentals = await self.stock_data.schwab.get_fundamentals(tickers=tickers)

            if fundamentals:
                filepath = f"{datapaths.attachments_path}/{ticker}_fundamentals.json"
                with open(filepath, 'w') as json_file:
                    json.dump(fundamentals, json_file)
                file = discord.File(filepath)
                message = f"Fundamentals for ticker `{ticker}`"
            else:
                message = f"Could not retrieve fundamentals for ticker `{ticker}`"
            message = await interaction.user.send(content=message, file=file)

        if tickers:
            message = f"Fetched fundamentals for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            message = f"Could not fetch fundamentals."
        if invalid_tickers:
            message += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(message, ephemeral=True)

    @app_commands.command(name="options", description="Return options chains for desired tickers in JSON format")
    @app_commands.describe(tickers="Tickers to return options chains for (separated by spaces)")
    async def options(self, interaction: discord.Interaction, tickers: str):
        """Return options chains in JSON format for input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/options function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Options chain(s) requested for tickers {tickers}")

        for ticker in tickers:
            options = await self.stock_data.schwab.get_options_chain(ticker)

            if options:
                filepath = f"{datapaths.attachments_path}/{ticker}_options_chain.json"
                with open(filepath, 'w') as json_file:
                    json.dump(options, json_file)
                file = discord.File(filepath)
                message = f"Options chain for ticker `{ticker}`"
            else:
                message = f"Could not retrieve options chain for ticker `{ticker}`"
            message = await interaction.user.send(content=message, file=file)

        if tickers:
            message = f"Fetched options chains for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            message = f"Could not fetch options chains."
        if invalid_tickers:
            message += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(message, ephemeral=True)

    @app_commands.command(name="popularity", description="Return historical popularity of desired tickers in CSV format")
    @app_commands.describe(tickers="Tickers to return popularity for (separated by spaces)")
    async def popularity(self, interaction: discord.Interaction, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/popularity function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Historical popularity requested for tickers {tickers}")

        for ticker in tickers:
            data = await self.stock_data.popularity.fetch_popularity(ticker=ticker)
            if not data.empty:
                message = f"Popularity for `{ticker}`"
                filepath = f"{datapaths.attachments_path}/{ticker}_popularity.csv"
                data.to_csv(filepath, index=False)
                file = discord.File(filepath)
            else:
                message = f"No popularity data available for ticker `{ticker}`"

            message = await interaction.user.send(content=message, file=file)

        if tickers:
            message = f"Fetched popularity data for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            message = f"Could not fetch popularity data."
        if invalid_tickers:
            message += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(message, ephemeral=True)


async def setup(bot):
    await bot.add_cog(Data(bot, bot.stock_data))

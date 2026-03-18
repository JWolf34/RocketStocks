import asyncio
import discord
from discord import app_commands
from discord.ext import commands
from src.rocketstocks.data.stockdata import StockData
from rocketstocks.data.channel_config import REPORTS
from rocketstocks.data.clients.schwab import SchwabTokenError
from rocketstocks.core.config.paths import datapaths
from rocketstocks.core.utils.formatting import ticker_string
from rocketstocks.core.utils.dates import format_date_mdy
import logging
import json
from table2ascii import table2ascii, PresetStyle

logger = logging.getLogger(__name__)


def _write_json(path, obj):
    with open(path, 'w') as f:
        json.dump(obj, f)


class Data(commands.Cog):
    """Cog for returning data to the user, such as JSON or CSV files"""
    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    data_group = app_commands.Group(name="data", description="Fetch financial data for tickers")

    @data_group.command(name="price", description="Returns data file for input ticker. Default: 1 year period.")
    @app_commands.describe(tickers="Tickers to return data for (separated by spaces)")
    @app_commands.describe(frequency="Type of data file to return - daily data or minute-by-minute data")
    @app_commands.choices(frequency=[
        app_commands.Choice(name='daily', value='daily'),
        app_commands.Choice(name='5m', value='5m')
    ])
    async def data_price(self, interaction: discord.Interaction, tickers: str, frequency: app_commands.Choice[str]):
        """Return CSV file of requested frequency of the requested ticker"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data price function called by user {interaction.user.name}")

        frequency = frequency.value
        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Data file(s) requested for {tickers}")

        message = None
        for ticker in tickers:
            if frequency == 'daily':
                data = await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
            else:
                data = await self.stock_data.price_history.fetch_5m_price_history(ticker=ticker)
            dm_content = ""
            file = None
            if not data.empty:
                dm_content = f"{frequency.capitalize()} data file for {ticker}"
                filepath = f"{datapaths.attachments_path}/{ticker}_{frequency}_data.csv"
                await asyncio.to_thread(data.to_csv, filepath, index=False)
                file = discord.File(filepath)
            else:
                dm_content = f"Could not fetch price data for ticker `{ticker}`"

            try:
                if file:
                    message = await interaction.user.send(content=dm_content, file=file)
                else:
                    message = await interaction.user.send(content=dm_content)
            except discord.Forbidden:
                await interaction.followup.send(
                    "Couldn't send DM — please enable DMs from server members in your privacy settings.",
                    ephemeral=True,
                )
                return

        if tickers and message is not None:
            followup = f"Fetched {frequency} data files for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            followup = f"Could not fetch {frequency} data files."
        if invalid_tickers:
            followup += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(followup, ephemeral=True)

    @data_group.command(name="financials", description="Fetch financial reports of the specified tickers")
    @app_commands.describe(tickers="Tickers to return financials for (separated by spaces)")
    async def data_financials(self, interaction: discord.Interaction, tickers: str):
        """Return latest financials on input tickers in JSON format"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data financials function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info("Financials requested for {}".format(tickers))

        message = None
        for ticker in tickers:
            files = []
            financials = await asyncio.to_thread(self.stock_data.fetch_financials, ticker)
            for statement, data in financials.items():
                filepath = f"{datapaths.attachments_path}/{ticker}_{statement}.csv"
                await asyncio.to_thread(data.to_csv, filepath)
                files.append(discord.File(filepath))

            try:
                message = await interaction.user.send("Financials for {}".format(ticker), files=files)
            except discord.Forbidden:
                await interaction.followup.send(
                    "Couldn't send DM — please enable DMs from server members in your privacy settings.",
                    ephemeral=True,
                )
                return
            logger.info(f"Posted financials for ticker '{ticker}'")

        if tickers and message is not None:
            followup = f"Fetched financials for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            followup = "Could not fetch financials."
        if invalid_tickers:
            followup += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(followup, ephemeral=True)

    @data_group.command(name="tickers", description="Return CSV with data on all tickers the bot runs analysis on")
    async def data_tickers(self, interaction: discord.Interaction):
        """Return CSV file with contents of 'tickers' table in database"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data tickers function called by user {interaction.user.name}")
        data = await self.stock_data.tickers.get_all_ticker_info()
        filepath = f"{datapaths.attachments_path}/all-tickers-info.csv"
        await asyncio.to_thread(data.to_csv, filepath)
        csv_file = discord.File(filepath)
        try:
            await interaction.user.send(content="All tickers", file=csv_file)
        except discord.Forbidden:
            await interaction.followup.send(
                "Couldn't send DM — please enable DMs from server members in your privacy settings.",
                ephemeral=True,
            )
            return
        await interaction.followup.send("CSV file has been sent", ephemeral=True)
        logger.info(f"Provided data file for all {len(data)} tickers")

    @data_group.command(name="earnings", description="Returns recent earnings data for the input tickers")
    @app_commands.describe(tickers="Tickers to return EPS data for (separated by spaces)")
    @app_commands.describe(visibility="'private' to send to DMs, 'public' to send to the channel")
    @app_commands.choices(visibility=[
        app_commands.Choice(name="private", value='private'),
        app_commands.Choice(name="public", value='public')
    ])
    async def data_earnings(self, interaction: discord.Interaction, tickers: str, visibility: app_commands.Choice[str]):
        """Return historical earnings data in CSV formats for input tickers and post recent earnings data in message"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data earnings function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Earnings requested for {tickers}")

        column_map = {'date': 'Date Reported',
                      'eps': 'EPS',
                      'surprise': 'Surprise',
                      'epsforecast': 'Estimate',
                      'fiscalquarterending': 'Quarter'}

        message = None
        for ticker in tickers:
            eps = await self.stock_data.earnings.get_historical_earnings(ticker)
            file = None
            if not eps.empty:
                filepath = f"{datapaths.attachments_path}/{ticker}_eps.csv"
                await asyncio.to_thread(eps.to_csv, filepath, index=False)
                file = discord.File(filepath)

                eps = eps.iloc[::-1].head(12)
                eps = eps.filter(list(column_map.keys()))
                eps = eps.rename(columns=column_map)
                eps_table = table2ascii(
                    header=eps.columns.tolist(),
                    body=eps.values.tolist(),
                    style=PresetStyle.thick
                )
                dm_content = f"**Earnings for {ticker}**\n ```{eps_table}```"
            else:
                dm_content = f"Could not retrieve EPS data for ticker `{ticker}`"
            if visibility.value == "private":
                try:
                    message = await interaction.user.send(dm_content, files=[file] if file else [])
                except discord.Forbidden:
                    await interaction.followup.send(
                        "Couldn't send DM — please enable DMs from server members in your privacy settings.",
                        ephemeral=True,
                    )
                    return
            else:
                channel = self.bot.get_channel_for_guild(interaction.guild_id, REPORTS)
                if channel is None:
                    await interaction.followup.send("Use `/server setup` to configure the reports channel.", ephemeral=True)
                    return
                message = await channel.send(dm_content, files=[file] if file else [])

        if tickers and message is not None:
            followup = f"Fetched EPS data for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            followup = "Could not fetch EPS data."
        if invalid_tickers:
            followup += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(followup, ephemeral=True)

    @data_group.command(name="sec-filing", description="Returns link to latest SEC form of requested type")
    @app_commands.describe(tickers="Tickers to return SEC forms for (separated by spaces)")
    @app_commands.describe(form="The form type to get a link to (10-K, 10-Q, 8-K, etc)")
    async def data_sec_filing(self, interaction: discord.Interaction, tickers: str, form: str):
        """Return links to latest SEC forms of given type for input tickers"""
        await interaction.response.defer()
        logger.info(f"/data sec-filing function called by user {interaction.user.name}")

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
                filing_date = format_date_mdy(target_filing['filingDate'])
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

    @data_group.command(name="fundamentals", description="Return fundamental data for desired tickers in JSON format")
    @app_commands.describe(tickers="Tickers to return fundamentals for (separated by spaces)")
    async def data_fundamentals(self, interaction: discord.Interaction, tickers: str):
        """Return fundamentals in JSON format for input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data fundamentals function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Fundamentals requested for tickers {tickers}")

        message = None
        for ticker in tickers:
            file = None
            try:
                fundamentals = await self.stock_data.schwab.get_fundamentals(tickers=[ticker])
            except SchwabTokenError:
                await interaction.followup.send(
                    "Schwab authentication required — use `/schwab auth`.", ephemeral=True
                )
                return

            if fundamentals:
                filepath = f"{datapaths.attachments_path}/{ticker}_fundamentals.json"
                await asyncio.to_thread(_write_json, filepath, fundamentals)
                file = discord.File(filepath)
                dm_content = f"Fundamentals for ticker `{ticker}`"
            else:
                dm_content = f"Could not retrieve fundamentals for ticker `{ticker}`"

            try:
                if file:
                    message = await interaction.user.send(content=dm_content, file=file)
                else:
                    message = await interaction.user.send(content=dm_content)
            except discord.Forbidden:
                await interaction.followup.send(
                    "Couldn't send DM — please enable DMs from server members in your privacy settings.",
                    ephemeral=True,
                )
                return

        if tickers and message is not None:
            followup = f"Fetched fundamentals for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            followup = "Could not fetch fundamentals."
        if invalid_tickers:
            followup += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(followup, ephemeral=True)

    @data_group.command(name="options", description="Return options chains for desired tickers in JSON format")
    @app_commands.describe(tickers="Tickers to return options chains for (separated by spaces)")
    async def data_options(self, interaction: discord.Interaction, tickers: str):
        """Return options chains in JSON format for input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data options function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Options chain(s) requested for tickers {tickers}")

        message = None
        for ticker in tickers:
            file = None
            try:
                options = await self.stock_data.schwab.get_options_chain(ticker)
            except SchwabTokenError:
                await interaction.followup.send(
                    "Schwab authentication required — use `/schwab auth`.", ephemeral=True
                )
                return

            if options:
                filepath = f"{datapaths.attachments_path}/{ticker}_options_chain.json"
                await asyncio.to_thread(_write_json, filepath, options)
                file = discord.File(filepath)
                dm_content = f"Options chain for ticker `{ticker}`"
            else:
                dm_content = f"Could not retrieve options chain for ticker `{ticker}`"

            try:
                if file:
                    message = await interaction.user.send(content=dm_content, file=file)
                else:
                    message = await interaction.user.send(content=dm_content)
            except discord.Forbidden:
                await interaction.followup.send(
                    "Couldn't send DM — please enable DMs from server members in your privacy settings.",
                    ephemeral=True,
                )
                return

        if tickers and message is not None:
            followup = f"Fetched options chains for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            followup = "Could not fetch options chains."
        if invalid_tickers:
            followup += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(followup, ephemeral=True)

    @data_group.command(name="popularity", description="Return historical popularity of desired tickers in CSV format")
    @app_commands.describe(tickers="Tickers to return popularity for (separated by spaces)")
    async def data_popularity(self, interaction: discord.Interaction, tickers: str):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data popularity function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info(f"Historical popularity requested for tickers {tickers}")

        message = None
        for ticker in tickers:
            file = None
            data = await self.stock_data.popularity.fetch_popularity(ticker=ticker)
            if not data.empty:
                dm_content = f"Popularity for `{ticker}`"
                filepath = f"{datapaths.attachments_path}/{ticker}_popularity.csv"
                await asyncio.to_thread(data.to_csv, filepath, index=False)
                file = discord.File(filepath)
            else:
                dm_content = f"No popularity data available for ticker `{ticker}`"

            try:
                if file:
                    message = await interaction.user.send(content=dm_content, file=file)
                else:
                    message = await interaction.user.send(content=dm_content)
            except discord.Forbidden:
                await interaction.followup.send(
                    "Couldn't send DM — please enable DMs from server members in your privacy settings.",
                    ephemeral=True,
                )
                return

        if tickers and message is not None:
            followup = f"Fetched popularity data for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            followup = "Could not fetch popularity data."
        if invalid_tickers:
            followup += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(followup, ephemeral=True)

    @data_group.command(name="quote", description="Return real-time quote for desired tickers")
    @app_commands.describe(tickers="Tickers to return quotes for (separated by spaces)")
    async def data_quote(self, interaction: discord.Interaction, tickers: str):
        """Return real-time quotes for input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data quote function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        if not tickers:
            await interaction.followup.send("No valid tickers provided.", ephemeral=True)
            return

        try:
            quotes = await self.stock_data.schwab.get_quotes(tickers)
        except SchwabTokenError:
            await interaction.followup.send(
                "Schwab authentication required — use `/schwab-auth`.", ephemeral=True
            )
            return

        embed = discord.Embed(title="Real-Time Quotes", color=discord.Color.blue())
        for ticker in tickers:
            quote_data = quotes.get(ticker, {})
            q = quote_data.get('quote', {})
            r = quote_data.get('regular', {})
            last_price = r.get('regularMarketLastPrice') or q.get('lastPrice', 'N/A')
            change = q.get('netChange', 'N/A')
            change_pct = q.get('netPercentChange', 'N/A')
            bid = q.get('bidPrice', 'N/A')
            ask = q.get('askPrice', 'N/A')
            volume = q.get('totalVolume', 'N/A')
            open_price = q.get('openPrice', 'N/A')
            high = q.get('highPrice', 'N/A')
            low = q.get('lowPrice', 'N/A')
            if isinstance(change, (int, float)) and isinstance(change_pct, (int, float)):
                change_str = f"{change:+.2f} ({change_pct:+.2f}%)"
            else:
                change_str = "N/A"
            volume_str = f"{volume:,}" if isinstance(volume, (int, float)) else str(volume)
            value = (
                f"**Last:** ${last_price}\n"
                f"**Change:** {change_str}\n"
                f"**Bid × Ask:** ${bid} × ${ask}\n"
                f"**Volume:** {volume_str}\n"
                f"**Open / High / Low:** ${open_price} / ${high} / ${low}"
            )
            embed.add_field(name=ticker, value=value, inline=False)

        if invalid_tickers:
            embed.set_footer(text=f"Invalid tickers: {ticker_string(invalid_tickers)}")

        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="upcoming-earnings", description="Return upcoming earnings info for desired tickers")
    @app_commands.describe(tickers="Tickers to return upcoming earnings for (separated by spaces)")
    async def data_upcoming_earnings(self, interaction: discord.Interaction, tickers: str):
        """Return upcoming earnings info for input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data upcoming-earnings function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        if not tickers:
            await interaction.followup.send("No valid tickers provided.", ephemeral=True)
            return

        embed = discord.Embed(title="Upcoming Earnings", color=discord.Color.green())
        for ticker in tickers:
            info = await self.stock_data.earnings.get_next_earnings_info(ticker)
            if info is None:
                embed.add_field(name=ticker, value="No upcoming earnings found.", inline=False)
            else:
                timing = info.get('time', 'N/A')
                timing_label = {"pre": "Before Market", "after": "After Market"}.get(timing, timing)
                value = (
                    f"**Date:** {info.get('date', 'N/A')}\n"
                    f"**When:** {timing_label}\n"
                    f"**EPS Forecast:** {info.get('eps_forecast', 'N/A')}\n"
                    f"**Estimates:** {info.get('no_of_ests', 'N/A')}\n"
                    f"**Last Year EPS:** {info.get('last_year_eps', 'N/A')}"
                )
                embed.add_field(name=ticker, value=value, inline=False)

        if invalid_tickers:
            embed.set_footer(text=f"Invalid tickers: {ticker_string(invalid_tickers)}")

        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="stats", description="Return statistical profile for desired tickers")
    @app_commands.describe(tickers="Tickers to return stats for (separated by spaces)")
    async def data_stats(self, interaction: discord.Interaction, tickers: str):
        """Return ticker statistical profile for input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data stats function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        if not tickers:
            await interaction.followup.send("No valid tickers provided.", ephemeral=True)
            return

        embed = discord.Embed(title="Ticker Stats", color=discord.Color.purple())
        for ticker in tickers:
            stats = await self.stock_data.ticker_stats.get_stats(ticker)
            if stats is None:
                embed.add_field(name=ticker, value="No stats available. Run the classify job to populate.", inline=False)
            else:
                mkt_cap = stats.get('market_cap')
                mkt_cap_str = f"${mkt_cap / 1e9:.1f}B" if mkt_cap else "N/A"
                value = (
                    f"**Classification:** {stats.get('classification', 'N/A')}\n"
                    f"**Market Cap:** {mkt_cap_str}\n"
                    f"**Volatility 20d:** {stats.get('volatility_20d', 'N/A')}\n"
                    f"**Mean Return 20d/60d:** {stats.get('mean_return_20d', 'N/A')} / {stats.get('mean_return_60d', 'N/A')}\n"
                    f"**Std Return 20d/60d:** {stats.get('std_return_20d', 'N/A')} / {stats.get('std_return_60d', 'N/A')}\n"
                    f"**Avg RVOL 20d:** {stats.get('avg_rvol_20d', 'N/A')}\n"
                    f"**BB Upper/Mid/Lower:** {stats.get('bb_upper', 'N/A')} / {stats.get('bb_mid', 'N/A')} / {stats.get('bb_lower', 'N/A')}\n"
                    f"**Updated:** {stats.get('updated_at', 'N/A')}"
                )
                embed.add_field(name=ticker, value=value, inline=False)

        if invalid_tickers:
            embed.set_footer(text=f"Invalid tickers: {ticker_string(invalid_tickers)}")

        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="movers", description="Return top 10 daily price movers")
    async def data_movers(self, interaction: discord.Interaction):
        """Return top 10 daily price movers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data movers function called by user {interaction.user.name}")

        try:
            movers_data = await self.stock_data.schwab.get_movers()
        except SchwabTokenError:
            await interaction.followup.send(
                "Schwab authentication required — use `/schwab-auth`.", ephemeral=True
            )
            return

        screeners = movers_data.get('screeners', []) if movers_data else []
        embed = discord.Embed(title="Top 10 Daily Movers", color=discord.Color.gold())
        if not screeners:
            embed.description = "No mover data available."
        else:
            for mover in screeners[:10]:
                ticker = mover.get('symbol', 'N/A')
                last_price = mover.get('lastPrice', 'N/A')
                change_pct = mover.get('percentChange', 'N/A')
                volume = mover.get('totalVolume', 'N/A')
                change_pct_str = f"{change_pct:+.2f}%" if isinstance(change_pct, (int, float)) else str(change_pct)
                volume_str = f"{volume:,}" if isinstance(volume, (int, float)) else str(volume)
                embed.add_field(
                    name=f"{ticker}  {change_pct_str}",
                    value=f"**Price:** ${last_price}  |  **Volume:** {volume_str}",
                    inline=False,
                )

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(Data(bot, bot.stock_data))

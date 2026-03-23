import asyncio
import pandas as pd
import discord
from discord import app_commands
from discord.ext import commands
from src.rocketstocks.data.stockdata import StockData
from rocketstocks.data.channel_config import REPORTS
from rocketstocks.data.clients.schwab import SchwabTokenError, SchwabRateLimitError
from rocketstocks.core.config.paths import datapaths
from rocketstocks.core.utils.formatting import ticker_string
import logging
import json
from rocketstocks.bot.senders.embed_utils import spec_to_embed
from rocketstocks.core.content.models import (
    QuoteData, UpcomingEarningsData, TickerStatsData, MoverData,
    PriceSnapshotData, FinancialHighlightsData, FundamentalsSnapshotData,
    OptionsSummaryData, PopularitySnapshotData, TickersSummaryData,
    EarningsTableData, SecFilingData,
    AnalystData, OwnershipData, InsiderData, ShortInterestData,
    NewsData, EarningsForecastData, OnDemandScreenerData,
)
from rocketstocks.core.content.data.quote_card import QuoteCard
from rocketstocks.core.content.data.upcoming_earnings_card import UpcomingEarningsCard
from rocketstocks.core.content.data.stats_card import StatsCard
from rocketstocks.core.content.data.movers_card import MoversCard
from rocketstocks.core.content.data.price_snapshot import PriceSnapshot
from rocketstocks.core.content.data.financial_highlights import FinancialHighlights
from rocketstocks.core.content.data.fundamentals_snapshot import FundamentalsSnapshot
from rocketstocks.core.content.data.options_summary import OptionsSummary
from rocketstocks.core.content.data.popularity_snapshot import PopularitySnapshot
from rocketstocks.core.content.data.tickers_summary import TickersSummary
from rocketstocks.core.content.data.earnings_card import EarningsCard
from rocketstocks.core.content.data.sec_filing_card import SecFilingCard
from rocketstocks.core.content.data.analyst_card import AnalystCard
from rocketstocks.core.content.data.ownership_card import OwnershipCard
from rocketstocks.core.content.data.insider_card import InsiderCard
from rocketstocks.core.content.data.short_interest_card import ShortInterestCard
from rocketstocks.core.content.data.news_card import NewsCard
from rocketstocks.core.content.data.forecast_card import ForecastCard
from rocketstocks.core.content.data.on_demand_screener import OnDemandScreener

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

    data_group = app_commands.Group(name="data", description="Look up stock data — prices, earnings, quotes, and more")

    @data_group.command(name="price", description="Get daily or 5-min OHLCV price data as a CSV (DM'd to you)")
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
            try:
                if frequency == 'daily':
                    data = await self.stock_data.price_history.fetch_daily_price_history(ticker=ticker)
                else:
                    data = await self.stock_data.price_history.fetch_5m_price_history(ticker=ticker)
            except Exception:
                logger.error(f"[data_price] Failed to fetch {frequency} data for '{ticker}'", exc_info=True)
                data = pd.DataFrame()
            dm_content = ""
            file = None
            if not data.empty:
                dm_content = f"{frequency.capitalize()} data file for {ticker}"
                filepath = f"{datapaths.attachments_path}/{ticker}_{frequency}_data.csv"
                await asyncio.to_thread(data.to_csv, filepath, index=False)
                file = discord.File(filepath)
            else:
                dm_content = f"Could not fetch price data for ticker `{ticker}`"

            # Build snapshot embed — fetch live quote if daily; skip gracefully on auth error
            snapshot_embed = None
            if not data.empty:
                quote = None
                if frequency == 'daily':
                    try:
                        quote = await self.stock_data.schwab.get_quote(ticker)
                    except (SchwabTokenError, SchwabRateLimitError, Exception):
                        pass
                snap_data = PriceSnapshotData(
                    ticker=ticker,
                    daily_price_history=data,
                    frequency=frequency,
                    quote=quote,
                )
                snapshot_embed = spec_to_embed(PriceSnapshot(snap_data).build())

            try:
                if file and snapshot_embed:
                    message = await interaction.user.send(content=dm_content, file=file, embed=snapshot_embed)
                elif file:
                    message = await interaction.user.send(content=dm_content, file=file)
                elif snapshot_embed:
                    message = await interaction.user.send(content=dm_content, embed=snapshot_embed)
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

    @data_group.command(name="financials", description="Get income, balance sheet, and cash flow CSVs (DM'd to you)")
    @app_commands.describe(tickers="Tickers to return financials for (separated by spaces)")
    async def data_financials(self, interaction: discord.Interaction, tickers: str):
        """Return latest financials on input tickers in JSON format"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data financials function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        logger.info("Financials requested for {}".format(tickers))

        message = None
        for ticker in tickers:
            try:
                files = []
                financials = await asyncio.to_thread(self.stock_data.yfinance.get_financials, ticker)
                for statement, data in financials.items():
                    filepath = f"{datapaths.attachments_path}/{ticker}_{statement}.csv"
                    await asyncio.to_thread(data.to_csv, filepath)
                    files.append(discord.File(filepath))
            except Exception:
                logger.error(f"[data_financials] Failed to fetch financials for '{ticker}'", exc_info=True)
                files = []

            highlights_embed = None
            if files:
                try:
                    hi_data = FinancialHighlightsData(ticker=ticker, financials=financials)
                    highlights_embed = spec_to_embed(FinancialHighlights(hi_data).build())
                except Exception:
                    logger.debug(f"[data_financials] Failed to build highlights embed for '{ticker}'", exc_info=True)

            try:
                send_kwargs = {"content": f"Financials for {ticker}", "files": files}
                if highlights_embed:
                    send_kwargs["embed"] = highlights_embed
                message = await interaction.user.send(**send_kwargs)
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

    @data_group.command(name="tickers", description="Get a CSV of every ticker the bot tracks (DM'd to you)")
    async def data_tickers(self, interaction: discord.Interaction):
        """Return CSV file with contents of 'tickers' table in database"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data tickers function called by user {interaction.user.name}")
        data = await self.stock_data.tickers.get_all_ticker_info()
        filepath = f"{datapaths.attachments_path}/all-tickers-info.csv"
        await asyncio.to_thread(data.to_csv, filepath)
        csv_file = discord.File(filepath)
        summary_embed = spec_to_embed(TickersSummary(TickersSummaryData(tickers_df=data)).build())
        try:
            await interaction.user.send(content="All tickers", file=csv_file, embed=summary_embed)
        except discord.Forbidden:
            await interaction.followup.send(
                "Couldn't send DM — please enable DMs from server members in your privacy settings.",
                ephemeral=True,
            )
            return
        await interaction.followup.send("CSV file has been sent", ephemeral=True)
        logger.info(f"Provided data file for all {len(data)} tickers")

    @data_group.command(name="earnings", description="Get historical EPS data as a table and CSV")
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

        message = None
        for ticker in tickers:
            eps = await self.stock_data.earnings.get_historical_earnings(ticker)
            file = None
            embed = None
            if not eps.empty:
                filepath = f"{datapaths.attachments_path}/{ticker}_eps.csv"
                await asyncio.to_thread(eps.to_csv, filepath, index=False)
                file = discord.File(filepath)
                embed = spec_to_embed(EarningsCard(EarningsTableData(ticker=ticker, historical_earnings=eps)).build())
                dm_content = f"Earnings history for `{ticker}`"
            else:
                dm_content = f"Could not retrieve EPS data for ticker `{ticker}`"
            if visibility.value == "private":
                try:
                    send_kwargs = {"content": dm_content, "files": [file] if file else []}
                    if embed:
                        send_kwargs["embed"] = embed
                    message = await interaction.user.send(**send_kwargs)
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
                send_kwargs = {"content": dm_content, "files": [file] if file else []}
                if embed:
                    send_kwargs["embed"] = embed
                message = await channel.send(**send_kwargs)

        if tickers and message is not None:
            followup = f"Fetched EPS data for tickers [{ticker_string(tickers)}]({message.jump_url})."
        else:
            followup = "Could not fetch EPS data."
        if invalid_tickers:
            followup += f" Invalid tickers: {ticker_string(invalid_tickers)}"

        await interaction.followup.send(followup, ephemeral=True)

    @data_group.command(name="sec-filing", description="Get a link to the latest SEC filing (10-K, 10-Q, 8-K, etc.)")
    @app_commands.describe(tickers="Tickers to return SEC forms for (separated by spaces)")
    @app_commands.describe(form="The form type to get a link to (10-K, 10-Q, 8-K, etc)")
    async def data_sec_filing(self, interaction: discord.Interaction, tickers: str, form: str):
        """Return links to latest SEC forms of given type for input tickers"""
        await interaction.response.defer()
        logger.info(f"/data sec-filing function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)

        filings = {}
        for ticker in tickers:
            recent_filings = await self.stock_data.sec.get_recent_filings(ticker=ticker, latest=250)
            target_filing = None
            for _, filing in recent_filings.iterrows():
                if filing['form'] == form:
                    target_filing = filing.to_dict()
                    break
            filings[ticker] = target_filing

        embed = spec_to_embed(SecFilingCard(SecFilingData(tickers=tickers, filings=filings, form=form)).build())
        footer_parts = []
        if invalid_tickers:
            footer_parts.append(f"Invalid tickers: {ticker_string(invalid_tickers)}")
        content = " · ".join(footer_parts) if footer_parts else None
        await interaction.followup.send(content=content, embed=embed)
        logger.info(f"Form {form} provided for tickers {tickers}")

    @data_group.command(name="fundamentals", description="Get fundamental data (P/E, EPS, beta, etc.) as JSON (DM'd to you)")
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
                fundamentals = await asyncio.wait_for(
                    self.stock_data.schwab.get_fundamentals(tickers=[ticker]),
                    timeout=15.0,
                )
            except asyncio.TimeoutError:
                logger.warning(f"[data_fundamentals] Schwab timed out for '{ticker}'")
                await interaction.followup.send(
                    f"Request timed out for ticker `{ticker}` — try again shortly.", ephemeral=True
                )
                return
            except SchwabRateLimitError:
                await interaction.followup.send(
                    "Schwab API rate limit exceeded — please wait a moment and try again.", ephemeral=True
                )
                return
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
                snap_embed = spec_to_embed(FundamentalsSnapshot(
                    FundamentalsSnapshotData(ticker=ticker, fundamentals=fundamentals)
                ).build())
            else:
                dm_content = f"Could not retrieve fundamentals for ticker `{ticker}`"
                snap_embed = None

            try:
                send_kwargs = {"content": dm_content}
                if file:
                    send_kwargs["file"] = file
                if snap_embed:
                    send_kwargs["embed"] = snap_embed
                message = await interaction.user.send(**send_kwargs)
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

    @data_group.command(name="options", description="Get full options chain data as JSON (DM'd to you)")
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
                options = await asyncio.wait_for(
                    self.stock_data.schwab.get_options_chain(ticker),
                    timeout=15.0,
                )
            except asyncio.TimeoutError:
                logger.warning(f"[data_options] Schwab timed out for '{ticker}'")
                await interaction.followup.send(
                    f"Request timed out for ticker `{ticker}` — try again shortly.", ephemeral=True
                )
                return
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
                current_price = options.get('underlyingPrice')
                snap_embed = spec_to_embed(OptionsSummary(
                    OptionsSummaryData(ticker=ticker, options_chain=options, current_price=current_price)
                ).build())
            else:
                dm_content = f"Could not retrieve options chain for ticker `{ticker}`"
                snap_embed = None

            try:
                send_kwargs = {"content": dm_content}
                if file:
                    send_kwargs["file"] = file
                if snap_embed:
                    send_kwargs["embed"] = snap_embed
                message = await interaction.user.send(**send_kwargs)
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

    @data_group.command(name="popularity", description="Get historical social-media mention data as CSV (DM'd to you)")
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
                snap_embed = spec_to_embed(PopularitySnapshot(
                    PopularitySnapshotData(ticker=ticker, popularity=data)
                ).build())
            else:
                dm_content = f"No popularity data available for ticker `{ticker}`"
                snap_embed = None

            try:
                send_kwargs = {"content": dm_content}
                if file:
                    send_kwargs["file"] = file
                if snap_embed:
                    send_kwargs["embed"] = snap_embed
                message = await interaction.user.send(**send_kwargs)
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

    @data_group.command(name="quote", description="Get a real-time quote with price, change, bid/ask, and volume")
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
        except SchwabRateLimitError:
            await interaction.followup.send(
                "Schwab API rate limit exceeded — please wait a moment and try again.", ephemeral=True
            )
            return
        except SchwabTokenError:
            await interaction.followup.send(
                "Schwab authentication required — use `/schwab auth`.", ephemeral=True
            )
            return

        data = QuoteData(tickers=tickers, quotes=quotes, invalid_tickers=invalid_tickers)
        embed = spec_to_embed(QuoteCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="upcoming-earnings", description="See the next earnings date, EPS forecast, and analyst estimates")
    @app_commands.describe(tickers="Tickers to return upcoming earnings for (separated by spaces)")
    async def data_upcoming_earnings(self, interaction: discord.Interaction, tickers: str):
        """Return upcoming earnings info for input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data upcoming-earnings function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        if not tickers:
            await interaction.followup.send("No valid tickers provided.", ephemeral=True)
            return

        earnings_info = {}
        for ticker in tickers:
            earnings_info[ticker] = await self.stock_data.earnings.get_next_earnings_info(ticker)

        data = UpcomingEarningsData(tickers=tickers, earnings_info=earnings_info, invalid_tickers=invalid_tickers)
        embed = spec_to_embed(UpcomingEarningsCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="stats", description="See volatility, classification, Bollinger Bands, and return stats")
    @app_commands.describe(tickers="Tickers to return stats for (separated by spaces)")
    async def data_stats(self, interaction: discord.Interaction, tickers: str):
        """Return ticker statistical profile for input tickers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data stats function called by user {interaction.user.name}")

        tickers, invalid_tickers = await self.stock_data.tickers.parse_valid_tickers(tickers)
        if not tickers:
            await interaction.followup.send("No valid tickers provided.", ephemeral=True)
            return

        stats_dict = {}
        for ticker in tickers:
            stats_dict[ticker] = await self.stock_data.ticker_stats.get_stats(ticker)

        data = TickerStatsData(tickers=tickers, stats=stats_dict, invalid_tickers=invalid_tickers)
        embed = spec_to_embed(StatsCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="movers", description="See today's top 10 stock price movers")
    async def data_movers(self, interaction: discord.Interaction):
        """Return top 10 daily price movers"""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data movers function called by user {interaction.user.name}")

        try:
            movers_data = await self.stock_data.schwab.get_movers()
        except SchwabRateLimitError:
            await interaction.followup.send(
                "Schwab API rate limit exceeded — please wait a moment and try again.", ephemeral=True
            )
            return
        except SchwabTokenError:
            await interaction.followup.send(
                "Schwab authentication required — use `/schwab auth`.", ephemeral=True
            )
            return

        screeners = movers_data.get('screeners', []) if movers_data else []
        data = MoverData(direction='gainers', screeners=screeners)
        embed = spec_to_embed(MoversCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)


    @data_group.command(name="analyst", description="See analyst price targets, ratings, and recent upgrades/downgrades")
    @app_commands.describe(ticker="Ticker to look up")
    async def data_analyst(self, interaction: discord.Interaction, ticker: str):
        """Return analyst consensus embed for a single ticker."""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data analyst called by {interaction.user.name} for {ticker}")

        tickers, invalid = await self.stock_data.tickers.parse_valid_tickers(ticker)
        if not tickers:
            await interaction.followup.send(f"Could not fetch data for: {ticker_string(invalid)}", ephemeral=True)
            return

        t = tickers[0]
        price_targets = await asyncio.to_thread(self.stock_data.yfinance.get_analyst_price_targets, t)
        recommendations = await asyncio.to_thread(self.stock_data.yfinance.get_recommendations_summary, t)
        upgrades_downgrades = await asyncio.to_thread(self.stock_data.yfinance.get_upgrades_downgrades, t)

        data = AnalystData(
            ticker=t,
            price_targets=price_targets,
            recommendations=recommendations,
            upgrades_downgrades=upgrades_downgrades,
        )
        embed = spec_to_embed(AnalystCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="ownership", description="See institutional and insider ownership breakdown")
    @app_commands.describe(ticker="Ticker to look up")
    async def data_ownership(self, interaction: discord.Interaction, ticker: str):
        """Return ownership breakdown embed for a single ticker."""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data ownership called by {interaction.user.name} for {ticker}")

        tickers, invalid = await self.stock_data.tickers.parse_valid_tickers(ticker)
        if not tickers:
            await interaction.followup.send(f"Could not fetch data for: {ticker_string(invalid)}", ephemeral=True)
            return

        t = tickers[0]
        institutional = await asyncio.to_thread(self.stock_data.yfinance.get_institutional_holders, t)
        major = await asyncio.to_thread(self.stock_data.yfinance.get_major_holders, t)

        data = OwnershipData(ticker=t, institutional_holders=institutional, major_holders=major)
        embed = spec_to_embed(OwnershipCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="insider", description="See recent insider transactions and purchase activity")
    @app_commands.describe(ticker="Ticker to look up")
    async def data_insider(self, interaction: discord.Interaction, ticker: str):
        """Return insider activity embed for a single ticker."""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data insider called by {interaction.user.name} for {ticker}")

        tickers, invalid = await self.stock_data.tickers.parse_valid_tickers(ticker)
        if not tickers:
            await interaction.followup.send(f"Could not fetch data for: {ticker_string(invalid)}", ephemeral=True)
            return

        t = tickers[0]
        transactions = await asyncio.to_thread(self.stock_data.yfinance.get_insider_transactions, t)
        purchases = await asyncio.to_thread(self.stock_data.yfinance.get_insider_purchases, t)

        data = InsiderData(ticker=t, insider_transactions=transactions, insider_purchases=purchases)
        embed = spec_to_embed(InsiderCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="short-interest", description="See short interest ratio, % of float, and shares short")
    @app_commands.describe(ticker="Ticker to look up")
    async def data_short_interest(self, interaction: discord.Interaction, ticker: str):
        """Return short interest embed for a single ticker (sourced from Schwab fundamentals)."""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data short-interest called by {interaction.user.name} for {ticker}")

        tickers, invalid = await self.stock_data.tickers.parse_valid_tickers(ticker)
        if not tickers:
            await interaction.followup.send(f"Could not fetch data for: {ticker_string(invalid)}", ephemeral=True)
            return

        t = tickers[0]
        try:
            raw = await self.stock_data.schwab.get_fundamentals([t])
        except (SchwabTokenError, SchwabRateLimitError) as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
            return

        fund = {}
        instruments = raw.get('instruments', []) if raw else []
        if instruments:
            fund = instruments[0].get('fundamental', {})

        data = ShortInterestData(
            ticker=t,
            short_interest_ratio=fund.get('shortInterestToFloat') or fund.get('shortIntRatio'),
            short_interest_shares=fund.get('shortInterestShares') or fund.get('shortInt'),
            short_percent_of_float=fund.get('shortPercentOfFloat') or fund.get('shortInterestToFloat'),
            shares_outstanding=fund.get('sharesOutstanding'),
        )
        embed = spec_to_embed(ShortInterestCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)


    @data_group.command(name="news", description="Get the latest news headlines for one or more tickers")
    @app_commands.describe(tickers="Tickers to fetch news for (separated by spaces)")
    async def data_news(self, interaction: discord.Interaction, tickers: str):
        """Return latest news headlines for input tickers."""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data news called by {interaction.user.name} for {tickers}")

        valid_tickers, invalid = await self.stock_data.tickers.parse_valid_tickers(tickers)
        if not valid_tickers:
            await interaction.followup.send(f"Could not fetch data for: {ticker_string(invalid)}", ephemeral=True)
            return

        news_results = {}
        for ticker in valid_tickers:
            try:
                news_results[ticker] = await asyncio.to_thread(
                    self.stock_data.news.get_news, ticker
                )
            except Exception:
                logger.warning(f"[data_news] Failed to fetch news for {ticker}", exc_info=True)
                news_results[ticker] = None

        data = NewsData(tickers=valid_tickers, news_results=news_results)
        embed = spec_to_embed(NewsCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="forecast", description="Get quarterly and annual EPS forecasts from NASDAQ")
    @app_commands.describe(ticker="Ticker to fetch forecast for")
    async def data_forecast(self, interaction: discord.Interaction, ticker: str):
        """Return earnings forecast embed for a single ticker."""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data forecast called by {interaction.user.name} for {ticker}")

        tickers, invalid = await self.stock_data.tickers.parse_valid_tickers(ticker)
        if not tickers:
            await interaction.followup.send(f"Could not fetch data for: {ticker_string(invalid)}", ephemeral=True)
            return

        t = tickers[0]
        try:
            quarterly = await asyncio.to_thread(
                self.stock_data.nasdaq.get_earnings_forecast_quarterly, t
            )
            yearly = await asyncio.to_thread(
                self.stock_data.nasdaq.get_earnings_forecast_yearly, t
            )
        except Exception:
            logger.warning(f"[data_forecast] Failed to fetch forecast for {t}", exc_info=True)
            quarterly, yearly = pd.DataFrame(), pd.DataFrame()

        data = EarningsForecastData(ticker=t, quarterly_forecast=quarterly, yearly_forecast=yearly)
        embed = spec_to_embed(ForecastCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="screener", description="Run an on-demand TradingView screener")
    @app_commands.describe(screener_type="Which screener to run")
    @app_commands.choices(screener_type=[
        app_commands.Choice(name='Premarket Gainers', value='premarket'),
        app_commands.Choice(name='Intraday Gainers', value='intraday'),
        app_commands.Choice(name='Unusual Volume', value='unusual-volume'),
    ])
    async def data_screener(self, interaction: discord.Interaction, screener_type: app_commands.Choice[str]):
        """Return an on-demand screener embed."""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data screener called by {interaction.user.name} — type: {screener_type.value}")

        try:
            if screener_type.value == 'premarket':
                raw = await asyncio.to_thread(self.stock_data.trading_view.get_premarket_gainers)
            elif screener_type.value == 'intraday':
                raw = await asyncio.to_thread(self.stock_data.trading_view.get_intraday_gainers)
            else:
                raw = await asyncio.to_thread(self.stock_data.trading_view.get_unusual_volume_movers)
        except Exception:
            logger.warning(f"[data_screener] TradingView call failed", exc_info=True)
            raw = pd.DataFrame()

        data = OnDemandScreenerData(screener_type=screener_type.value, data=raw)
        embed = spec_to_embed(OnDemandScreener(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)

    @data_group.command(name="losers", description="See today's top 10 stock price losers")
    async def data_losers(self, interaction: discord.Interaction):
        """Return top 10 daily price losers."""
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/data losers called by {interaction.user.name}")

        schwab_client = self.stock_data.schwab.client
        if schwab_client is None:
            await interaction.followup.send(
                "Schwab authentication required — use `/schwab auth`.", ephemeral=True
            )
            return

        try:
            movers_data = await self.stock_data.schwab.get_movers(
                sort_order=schwab_client.Movers.SortOrder.PERCENT_CHANGE_DOWN
            )
        except SchwabRateLimitError:
            await interaction.followup.send(
                "Schwab API rate limit exceeded — please wait a moment and try again.", ephemeral=True
            )
            return
        except SchwabTokenError:
            await interaction.followup.send(
                "Schwab authentication required — use `/schwab auth`.", ephemeral=True
            )
            return

        screeners = movers_data.get('screeners', []) if movers_data else []
        data = MoverData(direction='losers', screeners=screeners)
        embed = spec_to_embed(MoversCard(data).build())
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(Data(bot, bot.stock_data))

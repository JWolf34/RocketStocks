import datetime
import logging

import discord
import pandas as pd
from discord import app_commands
from discord.ext import commands

from rocketstocks.bot.senders.alert_sender import _spec_to_embed
from rocketstocks.core.content.alerts.earnings_alert import EarningsMoverAlert
from rocketstocks.core.content.alerts.politician_alert import PoliticianTradeAlert
from rocketstocks.core.content.alerts.popularity_alert import PopularityAlert
from rocketstocks.core.content.alerts.sec_filing_alert import SECFilingMoverAlert
from rocketstocks.core.content.alerts.volume_alert import VolumeMoverAlert
from rocketstocks.core.content.alerts.volume_spike_alert import VolumeSpikeAlert
from rocketstocks.core.content.alerts.watchlist_alert import WatchlistMoverAlert
from rocketstocks.core.content.models import (
    EarningsMoverData,
    PoliticianTradeAlertData,
    PopularityAlertData,
    SECFilingData,
    VolumeMoverData,
    VolumeSpikeData,
    WatchlistMoverData,
)
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.core.utils.market import market_utils
from src.rocketstocks.data.stockdata import StockData

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dummy data helpers
# ---------------------------------------------------------------------------

def _dummy_quote(pct_change: float = 5.32, price: float = 178.42) -> dict:
    return {
        'quote': {
            'netPercentChange': pct_change,
            'openPrice': round(price * 0.98, 2),
            'highPrice': round(price * 1.02, 2),
            'lowPrice': round(price * 0.97, 2),
            'totalVolume': 1_250_000,
        },
        'regular': {
            'regularMarketLastPrice': price,
        },
        'reference': {
            'exchangeName': 'NASDAQ',
            'isShortable': True,
            'isHardToBorrow': False,
        },
        'assetSubType': 'COE',
    }


def _dummy_ticker_info(name: str = 'Acme Corp') -> dict:
    return {
        'name': name,
        'sector': 'Technology',
        'industry': 'Software',
        'country': 'USA',
    }


def _dummy_daily_price_history(days: int = 90) -> pd.DataFrame:
    today = datetime.date.today()
    all_dates = [today - datetime.timedelta(days=i) for i in range(days, 0, -1)]
    business_dates = [d for d in all_dates if d.weekday() < 5]
    n = len(business_dates)
    base = 178.42
    closes = [round(base + (i - n // 2) * 0.5, 2) for i in range(n)]
    return pd.DataFrame({
        'date': business_dates,
        'open': [round(c * 0.99, 2) for c in closes],
        'high': [round(c * 1.02, 2) for c in closes],
        'low': [round(c * 0.97, 2) for c in closes],
        'close': closes,
        'volume': [1_000_000 + i * 5_000 for i in range(n)],
    })


def _dummy_historical_earnings() -> pd.DataFrame:
    today = datetime.date.today()
    return pd.DataFrame({
        'date': [
            today - datetime.timedelta(days=365),
            today - datetime.timedelta(days=270),
            today - datetime.timedelta(days=180),
            today - datetime.timedelta(days=90),
        ],
        'eps': [1.20, 1.35, 1.52, 1.68],
        'epsforecast': [1.15, 1.30, 1.45, 1.60],
        'surprise': [4.35, 3.85, 4.83, 5.00],
        'fiscalquarterending': ['Q1 2024', 'Q2 2024', 'Q3 2024', 'Q4 2024'],
    })


def _dummy_next_earnings_info() -> dict:
    return {
        'date': datetime.date.today(),
        'time': 'after-hours',
        'eps_forecast': '1.82',
        'fiscal_quarter_ending': 'Q1 2025',
        'no_of_ests': 15,
        'last_year_rpt_dt': str(datetime.date.today() - datetime.timedelta(days=365)),
        'last_year_eps': '1.68',
    }


def _dummy_sec_filings() -> pd.DataFrame:
    today_str = datetime.datetime.today().strftime("%Y-%m-%d")
    return pd.DataFrame({
        'filingDate': [today_str, today_str],
        'form': ['10-K', '8-K'],
        'link': ['https://www.sec.gov', 'https://www.sec.gov'],
    })


def _dummy_popularity() -> pd.DataFrame:
    """Create popularity rows spanning the past 6 days, including an entry at
    the exact round-down-30-min timestamp for today (required by PopularityAlert)."""
    now = date_utils.round_down_nearest_minute(30)
    rows = []
    for day_offset in range(6):
        dt = now - datetime.timedelta(days=day_offset)
        rank = max(5, 55 - day_offset * 8)
        rows.append({'datetime': dt, 'rank': rank})
        # Extra entry earlier in the same day
        morning = dt.replace(hour=9, minute=30, second=0, microsecond=0)
        rows.append({'datetime': morning, 'rank': rank + 5})
    return pd.DataFrame(rows)


def _dummy_politician_trades() -> pd.DataFrame:
    return pd.DataFrame({
        'ticker': ['NVDA', 'AAPL', 'MSFT'],
        'type': ['Buy', 'Sell', 'Buy'],
        'amount': ['$1M–$5M', '$500K–$1M', '$250K–$500K'],
        'date': [str(datetime.date.today())] * 3,
    })


def _build_dummy_alert(alert_type: str):
    """Return a fully-constructed Alert instance populated with dummy data."""
    ticker = 'ACME'
    ticker_info = _dummy_ticker_info('Acme Corporation')
    quote = _dummy_quote()

    if alert_type == 'earnings_mover':
        return EarningsMoverAlert(EarningsMoverData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            next_earnings_info=_dummy_next_earnings_info(),
            historical_earnings=_dummy_historical_earnings(),
        ))

    if alert_type == 'volume_mover':
        return VolumeMoverAlert(VolumeMoverData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            rvol=3.72,
            daily_price_history=_dummy_daily_price_history(),
        ))

    if alert_type == 'volume_spike':
        return VolumeSpikeAlert(VolumeSpikeData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            rvol_at_time=4.21,
            avg_vol_at_time=150_000,
            time='10:30 AM',
        ))

    if alert_type == 'watchlist_mover':
        return WatchlistMoverAlert(WatchlistMoverData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            watchlist='Tech',
        ))

    if alert_type == 'sec_filing':
        return SECFilingMoverAlert(SECFilingData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            recent_sec_filings=_dummy_sec_filings(),
        ))

    if alert_type == 'popularity':
        return PopularityAlert(PopularityAlertData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            popularity=_dummy_popularity(),
        ))

    if alert_type == 'politician_trade':
        return PoliticianTradeAlert(PoliticianTradeAlertData(
            politician={'name': 'Jane Smith', 'party': 'Independent', 'state': 'California', 'politician_id': 'jane-smith'},
            trades=_dummy_politician_trades(),
        ))

    raise ValueError(f"Unknown alert type: {alert_type!r}")


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------

class Tests(commands.Cog):
    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.command(name="test-gainer-reports", description="Test posting premarket gainer reports",)
    async def test_premarket_reports(self, interaction: discord.Interaction):
        logger.info(f"/test-premarket-reports function called by user {interaction.user.name}")
        await interaction.response.defer(ephemeral=True)
        reports = self.bot.get_cog("Reports")
        if market_utils().get_market_period() == "EOD":
            await interaction.followup.send("Market is closed - cannot post gainer reports", ephemeral=True)
        else:
            await interaction.followup.send("Gainer reports test complete!", ephemeral=True)

    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.command(name='force-update-5m-data', description="Forcefully update the 5m price history db table")
    async def force_update_5m_data(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/force-update-5m-data function called by user {interaction.user.name}")
        tickers = self.stock_data.tickers.get_all_tickers()
        await self.stock_data.price_history.update_5m_price_history(tickers)
        await interaction.followup.send("5m price history table updated")

    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.command(name='force-update-daily-data', description="Forcefully update the 5m price history db table")
    async def force_update_daily_data(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/force-update-daily-data function called by user {interaction.user.name}")
        tickers = self.stock_data.tickers.get_all_tickers()
        await self.stock_data.price_history.update_daily_price_history(tickers)
        await interaction.followup.send("Daily price history table updated")

    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.command(name="test-alert", description="Send a test alert to your DMs with dummy data")
    @app_commands.describe(alert_type="The type of alert to preview")
    @app_commands.choices(alert_type=[
        app_commands.Choice(name="Earnings Mover",   value="earnings_mover"),
        app_commands.Choice(name="Volume Mover",     value="volume_mover"),
        app_commands.Choice(name="Volume Spike",     value="volume_spike"),
        app_commands.Choice(name="Watchlist Mover",  value="watchlist_mover"),
        app_commands.Choice(name="SEC Filing Mover", value="sec_filing"),
        app_commands.Choice(name="Popularity Mover", value="popularity"),
        app_commands.Choice(name="Politician Trade", value="politician_trade"),
    ])
    async def test_alert(self, interaction: discord.Interaction, alert_type: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/test-alert [{alert_type.value}] called by {interaction.user.name}")

        try:
            alert = _build_dummy_alert(alert_type.value)
        except Exception as exc:
            logger.exception(f"Failed to build dummy alert for type {alert_type.value!r}")
            await interaction.followup.send(f"Error building alert: {exc}", ephemeral=True)
            return

        try:
            spec = alert.build_embed_spec()
            embed = _spec_to_embed(spec)
            await interaction.followup.send(embed=embed)
        except NotImplementedError:
            msg = alert.build_alert()
            await interaction.followup.send(msg, ephemeral=True)
        except Exception as exc:
            logger.exception(f"Failed to send test alert DM to {interaction.user.name}")
            await interaction.followup.send(f"Error sending DM: {exc}", ephemeral=True)
            return

async def setup(bot):
    await bot.add_cog(Tests(bot, bot.stock_data))

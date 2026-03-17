"""Admin cog — administrator-only commands for testing, debugging, and data management."""
import datetime
import logging
import zipfile
import os

import discord
import pandas as pd
from discord import app_commands
from discord.ext import commands

from rocketstocks.bot.senders.embed_utils import spec_to_embed
from rocketstocks.core.config.paths import datapaths
from rocketstocks.core.content.alerts.earnings_alert import EarningsMoverAlert
from rocketstocks.core.content.alerts.watchlist_alert import WatchlistMoverAlert
from rocketstocks.core.content.alerts.popularity_surge_alert import PopularitySurgeAlert
from rocketstocks.core.content.alerts.momentum_confirmation_alert import MomentumConfirmationAlert
from rocketstocks.core.content.alerts.market_alert import MarketAlert
from rocketstocks.core.content.models import (
    EarningsMoverData,
    EarningsSpotlightData,
    GainerScreenerData,
    NewsReportData,
    PoliticianReportData,
    PopularityReportData,
    PopularityScreenerData,
    StockReportData,
    VolumeScreenerData,
    WatchlistMoverData,
    WeeklyEarningsData,
    PopularitySurgeData,
    MomentumConfirmationData,
    MarketAlertData,
)
from rocketstocks.core.content.reports.earnings_report import EarningsSpotlightReport
from rocketstocks.core.content.reports.news_report import NewsReport
from rocketstocks.core.content.reports.politician_report import PoliticianReport
from rocketstocks.core.content.reports.popularity_report import PopularityReport
from rocketstocks.core.content.reports.stock_report import StockReport
from rocketstocks.core.content.screeners.earnings_screener import WeeklyEarningsScreener
from rocketstocks.core.content.screeners.gainer_screener import GainerScreener
from rocketstocks.core.content.screeners.popularity_screener import PopularityScreener
from rocketstocks.core.content.screeners.volume_screener import VolumeScreener
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.core.utils.market import market_utils
from src.rocketstocks.data.stockdata import StockData

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dummy data helpers
# ---------------------------------------------------------------------------

def _dummy_quote(pct_change: float = 5.32, price: float = 178.42, ticker: str = 'ACME') -> dict:
    return {
        'symbol': ticker,
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


def _dummy_popularity() -> pd.DataFrame:
    """Create popularity rows spanning the past 6 days for surge detection."""
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


def _dummy_weekly_earnings_data() -> WeeklyEarningsData:
    """Dummy WeeklyEarningsData with 8 stocks spread across the next 5 business days."""
    today = datetime.date.today()
    business_days: list[datetime.date] = []
    d = today
    while len(business_days) < 5:
        if d.weekday() < 5:
            business_days.append(d)
        d += datetime.timedelta(days=1)

    one_year = datetime.timedelta(days=365)
    rows = [
        {'date': business_days[0], 'ticker': 'AAPL',  'time': 'after-hours',  'fiscal_quarter_ending': 'Q1 2025', 'eps_forecast': '1.82', 'no_of_ests': 15, 'last_year_eps': '1.68', 'last_year_rpt_dt': str(business_days[0] - one_year)},
        {'date': business_days[0], 'ticker': 'MSFT',  'time': 'after-hours',  'fiscal_quarter_ending': 'Q1 2025', 'eps_forecast': '2.94', 'no_of_ests': 20, 'last_year_eps': '2.73', 'last_year_rpt_dt': str(business_days[0] - one_year)},
        {'date': business_days[1], 'ticker': 'GOOGL', 'time': 'after-hours',  'fiscal_quarter_ending': 'Q1 2025', 'eps_forecast': '1.59', 'no_of_ests': 12, 'last_year_eps': '1.42', 'last_year_rpt_dt': str(business_days[1] - one_year)},
        {'date': business_days[1], 'ticker': 'META',  'time': 'after-hours',  'fiscal_quarter_ending': 'Q1 2025', 'eps_forecast': '4.32', 'no_of_ests': 18, 'last_year_eps': '3.79', 'last_year_rpt_dt': str(business_days[1] - one_year)},
        {'date': business_days[2], 'ticker': 'AMZN',  'time': 'after-hours',  'fiscal_quarter_ending': 'Q1 2025', 'eps_forecast': '0.89', 'no_of_ests': 22, 'last_year_eps': '0.73', 'last_year_rpt_dt': str(business_days[2] - one_year)},
        {'date': business_days[3], 'ticker': 'NVDA',  'time': 'after-hours',  'fiscal_quarter_ending': 'Q1 2025', 'eps_forecast': '5.56', 'no_of_ests': 25, 'last_year_eps': '4.94', 'last_year_rpt_dt': str(business_days[3] - one_year)},
        {'date': business_days[3], 'ticker': 'TSLA',  'time': 'after-hours',  'fiscal_quarter_ending': 'Q1 2025', 'eps_forecast': '0.62', 'no_of_ests': 17, 'last_year_eps': '0.53', 'last_year_rpt_dt': str(business_days[3] - one_year)},
        {'date': business_days[4], 'ticker': 'JPM',   'time': 'pre-market',   'fiscal_quarter_ending': 'Q1 2025', 'eps_forecast': '4.11', 'no_of_ests': 14, 'last_year_eps': '3.97', 'last_year_rpt_dt': str(business_days[4] - one_year)},
    ]
    return WeeklyEarningsData(
        upcoming_earnings=pd.DataFrame(rows),
        watchlist_tickers=['AAPL', 'NVDA', 'TSLA'],
    )


def _dummy_fundamentals() -> dict:
    return {
        'instruments': [{'fundamental': {
            'marketCap': 2_900_000_000_000,
            'eps': 6.42,
            'epsTTM': 6.42,
            'peRatio': 29.42,
            'beta': 1.24,
            'dividendAmount': 0.96,
        }}]
    }


def _dummy_price_history(rows: int = 250) -> pd.DataFrame:
    dates = [datetime.date.today() - datetime.timedelta(days=i) for i in range(rows)]
    dates.reverse()
    return pd.DataFrame({
        'date': dates,
        'open': [180.0] * rows,
        'high': [195.0] * rows,
        'low': [175.0] * rows,
        'close': [188.9] * rows,
        'volume': [50_000_000] * rows,
    })


def _dummy_earnings_df() -> pd.DataFrame:
    return pd.DataFrame({
        'date': [datetime.date(2025, 10, 31), datetime.date(2025, 7, 31)],
        'eps': [1.58, 1.40],
        'surprise': [3.1, 1.8],
        'epsforecast': [1.53, 1.38],
        'fiscalquarterending': ['Sep 2025', 'Jun 2025'],
    })


def _dummy_gainer_data() -> GainerScreenerData:
    gainers = pd.DataFrame({
        'name':             ['AAPL', 'NVDA', 'TSLA', 'META', 'AMZN', 'GOOGL', 'MSFT'],
        'change':           [8.42, 6.75, 5.91, 4.83, 4.21, 3.67, 3.12],
        'close':            [188.90, 142.30, 248.75, 512.40, 198.60, 178.90, 415.20],
        'volume':           [52_000_000, 38_000_000, 29_000_000, 21_000_000, 18_500_000, 15_200_000, 14_800_000],
        'market_cap_basic': [2.9e12, 3.5e12, 0.8e12, 1.3e12, 1.9e12, 2.2e12, 3.1e12],
    })
    return GainerScreenerData(market_period='intraday', gainers=gainers)


def _dummy_volume_data() -> VolumeScreenerData:
    unusual = pd.DataFrame({
        'name':                     ['GME', 'AMC', 'BBBY', 'PLTR', 'RIVN', 'LCID', 'SOFI'],
        'close':                    [22.40, 5.80, 1.24, 18.90, 12.40, 2.85, 7.30],
        'change':                   [14.2, 9.8, 6.1, 5.4, 4.7, 3.9, 3.2],
        'relative_volume_10d_calc': [8.4, 6.1, 5.7, 4.3, 3.9, 3.5, 3.1],
        'volume':                   [48_000_000, 32_000_000, 28_000_000, 22_000_000, 19_000_000, 16_500_000, 14_000_000],
        'average_volume_10d_calc':  [5_700_000, 5_200_000, 4_900_000, 5_100_000, 4_900_000, 4_700_000, 4_500_000],
        'market_cap_basic':         [8e9, 1.2e9, 0.1e9, 40e9, 12e9, 5e9, 8e9],
    })
    return VolumeScreenerData(unusual_volume=unusual)


def _dummy_popularity_screener_data() -> PopularityScreenerData:
    popular = pd.DataFrame({
        'rank':             list(range(1, 16)),
        'ticker':           ['TSLA', 'AAPL', 'NVDA', 'GME', 'AMC', 'PLTR', 'AMZN', 'META', 'MSFT', 'GOOGL', 'RIVN', 'SOFI', 'NIO', 'BBBY', 'SPY'],
        'mentions':         [2840, 2210, 1980, 1750, 1520, 1380, 1240, 1190, 1050, 980, 870, 810, 760, 720, 690],
        'rank_24h_ago':     [2, 1, 4, 3, 6, 5, 8, 7, 10, 9, 12, 11, 14, 13, 16],
        'mentions_24h_ago': [2100, 2350, 1600, 1900, 1350, 1420, 1100, 1230, 900, 1020, 750, 850, 680, 770, 610],
    })
    return PopularityScreenerData(popular_stocks=popular)


def _build_dummy_screener(screener_type: str):
    """Return a fully-constructed screener instance populated with dummy data."""
    if screener_type == 'gainers':
        return GainerScreener(_dummy_gainer_data())
    if screener_type == 'volume':
        return VolumeScreener(_dummy_volume_data())
    if screener_type == 'popularity':
        return PopularityScreener(_dummy_popularity_screener_data())
    if screener_type == 'earnings':
        return WeeklyEarningsScreener(_dummy_weekly_earnings_data())
    raise ValueError(f"Unknown screener type: {screener_type!r}")


def _build_dummy_report(report_type: str):
    """Return a fully-constructed report instance populated with dummy data."""
    ticker_info = _dummy_ticker_info('Apple Inc')
    if report_type == 'stock':
        return StockReport(data=StockReportData(
            ticker='AAPL',
            ticker_info=ticker_info,
            quote=_dummy_quote(pct_change=2.5, price=188.9, ticker='AAPL'),
            fundamentals=_dummy_fundamentals(),
            daily_price_history=_dummy_price_history(),
            popularity=pd.DataFrame(),
            historical_earnings=_dummy_earnings_df(),
            next_earnings_info={},
            recent_sec_filings=pd.DataFrame(),
        ))
    if report_type == 'earnings':
        return EarningsSpotlightReport(data=EarningsSpotlightData(
            ticker='NVDA',
            ticker_info=ticker_info,
            quote=_dummy_quote(pct_change=1.5, price=188.9, ticker='NVDA'),
            fundamentals=_dummy_fundamentals(),
            daily_price_history=_dummy_price_history(),
            historical_earnings=_dummy_earnings_df(),
            next_earnings_info={
                'date': datetime.date(2026, 3, 15),
                'time': 'after-hours',
                'fiscal_quarter_ending': 'Jan 2026',
                'eps_forecast': '0.89',
                'no_of_ests': '42',
                'last_year_rpt_dt': '2025-02-26',
                'last_year_eps': '0.76',
            },
        ))
    if report_type == 'news':
        news = {'articles': [
            {
                'title': f'Sample Article {i + 1}',
                'url': f'https://example.com/article-{i + 1}',
                'source': {'name': 'Reuters'},
                'publishedAt': '2026-02-27T10:00:00Z',
            }
            for i in range(5)
        ]}
        return NewsReport(data=NewsReportData(query='AAPL', news=news))
    if report_type == 'popularity':
        df = pd.DataFrame({
            'rank': range(1, 21),
            'ticker': [f'TK{i}' for i in range(1, 21)],
            'mentions': [100 - i for i in range(20)],
            'rank_24h_ago': range(2, 22),
            'mentions_24h_ago': [90 - i for i in range(20)],
        })
        return PopularityReport(data=PopularityReportData(popular_stocks=df, filter='all'))
    if report_type == 'politician':
        politician = {
            'name': 'Nancy Pelosi',
            'party': 'Democrat',
            'state': 'California',
            'politician_id': 'demo-pelosi',
        }
        trades_df = pd.DataFrame({
            'Ticker': ['NVDA', 'AAPL', 'MSFT'],
            'Published Date': ['2026-01-10', '2026-01-05', '2025-12-20'],
            'Order Type': ['Purchase', 'Sale', 'Purchase'],
            'Order Size': ['$500K - $1M', '$250K - $500K', '$1M - $5M'],
            'Filed After': ['5 days', '12 days', '30 days'],
        })
        return PoliticianReport(data=PoliticianReportData(
            politician=politician,
            trades=trades_df,
            politician_facts={'Net Worth': '$120M', 'In Office Since': '1987'},
        ))
    raise ValueError(f"Unknown report type: {report_type!r}")


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

    if alert_type == 'watchlist_mover':
        return WatchlistMoverAlert(WatchlistMoverData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            watchlist='Tech',
        ))

    if alert_type == 'popularity_surge':
        from rocketstocks.core.analysis.popularity_signals import (
            PopularitySurgeResult, SurgeType,
        )
        surge_result = PopularitySurgeResult(
            ticker=ticker,
            is_surging=True,
            surge_types=[SurgeType.MENTION_SURGE, SurgeType.RANK_JUMP],
            current_rank=45,
            rank_24h_ago=180,
            rank_change=135,
            mentions=3200,
            mentions_24h_ago=900,
            mention_ratio=3.56,
            rank_velocity=-12.0,
            rank_velocity_zscore=-2.8,
        )
        return PopularitySurgeAlert(PopularitySurgeData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            surge_result=surge_result,
        ))

    if alert_type == 'momentum_confirmation':
        from rocketstocks.core.analysis.alert_strategy import AlertTriggerResult
        from rocketstocks.core.analysis.classification import StockClass
        trigger = AlertTriggerResult(
            should_alert=True,
            classification=StockClass.STANDARD,
            zscore=3.1,
            percentile=97.5,
            bb_position=None,
            confluence_count=None,
            confluence_total=None,
            confluence_details=None,
            volume_zscore=2.8,
            signal_type='unusual_move',
        )
        return MomentumConfirmationAlert(MomentumConfirmationData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            surge_flagged_at=datetime.datetime.now() - datetime.timedelta(hours=2),
            surge_types=['mention_surge', 'rank_jump'],
            price_at_flag=170.0,
            price_change_since_flag=5.0,
            surge_alert_message_id=None,
            trigger_result=trigger,
        ))

    if alert_type == 'market_alert':
        from rocketstocks.core.analysis.alert_strategy import AlertTriggerResult
        from rocketstocks.core.analysis.classification import StockClass
        from rocketstocks.core.analysis.composite_score import CompositeScoreResult
        trigger = AlertTriggerResult(
            should_alert=True,
            classification=StockClass.VOLATILE,
            zscore=3.5,
            percentile=98.5,
            bb_position=None,
            confluence_count=None,
            confluence_total=None,
            confluence_details=None,
            volume_zscore=4.2,
            signal_type='unusual_move',
        )
        composite = CompositeScoreResult(
            composite_score=3.1,
            should_alert=True,
            volume_component=4.2,
            price_component=3.5,
            cross_signal_component=0.0,
            classification_component=2.0,
            trigger_result=trigger,
            dominant_signal='volume',
        )
        return MarketAlert(MarketAlertData(
            ticker=ticker,
            ticker_info=ticker_info,
            quote=quote,
            composite_result=composite,
            rvol=4.5,
        ))

    raise ValueError(f"Unknown alert type: {alert_type!r}")


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------

class Admin(commands.Cog):
    """Administrator-only commands for testing, debugging, and data management."""

    def __init__(self, bot: commands.Bot, stock_data: StockData):
        self.bot = bot
        self.stock_data = stock_data

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f"Cog {__name__} loaded!")

    admin_group = app_commands.Group(
        name="admin",
        description="Administrator commands for testing and data management",
        default_permissions=discord.Permissions(administrator=True),
    )

    @admin_group.command(name="logs", description="Return the log file for the bot")
    @app_commands.checks.has_permissions(administrator=True)
    async def admin_logs(self, interaction: discord.Interaction):
        """Return latest log file and ZIP file of all log files for the bot"""
        logger.info(f"/admin logs function called by user {interaction.user.name}")

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

    @admin_group.command(name="update-5m", description="Forcefully update the 5m price history db table")
    @app_commands.checks.has_permissions(administrator=True)
    async def admin_update_5m(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/admin update-5m function called by user {interaction.user.name}")
        tickers = self.stock_data.tickers.get_all_tickers()
        await self.stock_data.price_history.update_5m_price_history(tickers)
        await interaction.followup.send("5m price history table updated")

    @admin_group.command(name="update-daily", description="Forcefully update the daily price history db table")
    @app_commands.checks.has_permissions(administrator=True)
    async def admin_update_daily(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/admin update-daily function called by user {interaction.user.name}")
        tickers = self.stock_data.tickers.get_all_tickers()
        await self.stock_data.price_history.update_daily_price_history(tickers)
        await interaction.followup.send("Daily price history table updated")

    @admin_group.command(name="test-alert", description="Send a test alert embed with dummy data")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(alert_type="The type of alert to preview")
    @app_commands.choices(alert_type=[
        app_commands.Choice(name="Earnings Mover",         value="earnings_mover"),
        app_commands.Choice(name="Watchlist Mover",        value="watchlist_mover"),
        app_commands.Choice(name="Popularity Surge",       value="popularity_surge"),
        app_commands.Choice(name="Momentum Confirmation",  value="momentum_confirmation"),
        app_commands.Choice(name="Market Alert",           value="market_alert"),
    ])
    async def admin_test_alert(self, interaction: discord.Interaction, alert_type: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/admin test-alert [{alert_type.value}] called by {interaction.user.name}")

        try:
            alert = _build_dummy_alert(alert_type.value)
        except Exception as exc:
            logger.exception(f"Failed to build dummy alert for type {alert_type.value!r}")
            await interaction.followup.send(f"Error building alert: {exc}", ephemeral=True)
            return

        try:
            embed = spec_to_embed(alert.build())
            await interaction.followup.send(embed=embed)
        except Exception as exc:
            logger.exception(f"Failed to send test alert to {interaction.user.name}")
            await interaction.followup.send(f"Error sending embed: {exc}", ephemeral=True)
            return

    @admin_group.command(name="test-screener", description="Preview a screener embed with dummy data (ephemeral)")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(screener="The type of screener to preview")
    @app_commands.choices(screener=[
        app_commands.Choice(name="gainers",         value="gainers"),
        app_commands.Choice(name="unusual-volume",  value="volume"),
        app_commands.Choice(name="popularity",      value="popularity"),
        app_commands.Choice(name="weekly-earnings", value="earnings"),
    ])
    async def admin_test_screener(self, interaction: discord.Interaction, screener: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/admin test-screener [{screener.value}] called by {interaction.user.name}")

        try:
            content = _build_dummy_screener(screener.value)
        except Exception as exc:
            logger.exception(f"Failed to build dummy screener for type {screener.value!r}")
            await interaction.followup.send(f"Error building screener: {exc}", ephemeral=True)
            return

        try:
            embed = spec_to_embed(content.build())
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as exc:
            logger.exception(f"Failed to send test screener to {interaction.user.name}")
            await interaction.followup.send(f"Error sending screener: {exc}", ephemeral=True)

    @admin_group.command(name="test-report", description="Preview a report embed with dummy data (ephemeral)")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(report="The type of report to preview")
    @app_commands.choices(report=[
        app_commands.Choice(name="stock",              value="stock"),
        app_commands.Choice(name="earnings-spotlight", value="earnings"),
        app_commands.Choice(name="news",               value="news"),
        app_commands.Choice(name="popularity",         value="popularity"),
        app_commands.Choice(name="politician",         value="politician"),
    ])
    async def admin_test_report(self, interaction: discord.Interaction, report: app_commands.Choice[str]):
        await interaction.response.defer(ephemeral=True)
        logger.info(f"/admin test-report [{report.value}] called by {interaction.user.name}")

        try:
            content = _build_dummy_report(report.value)
        except Exception as exc:
            logger.exception(f"Failed to build dummy report for type {report.value!r}")
            await interaction.followup.send(f"Error building report: {exc}", ephemeral=True)
            return

        try:
            embed = spec_to_embed(content.build())
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as exc:
            logger.exception(f"Failed to send test report to {interaction.user.name}")
            await interaction.followup.send(f"Error sending report: {exc}", ephemeral=True)


    async def cog_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message(
                "You need Administrator permissions to use this command.", ephemeral=True
            )
        else:
            raise error


async def setup(bot):
    await bot.add_cog(Admin(bot, bot.stock_data))

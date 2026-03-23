"""Typed dataclasses for all content type data inputs.

Each content class declares exactly what data it needs via its corresponding dataclass.
This replaces the ~13 optional kwargs on Report.__init__ with explicit, type-checked fields.
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field

import pandas as pd


# ---------------------------------------------------------------------------
# Discord-agnostic embed colors (no discord import needed)
# ---------------------------------------------------------------------------

COLOR_GREEN  = 0x2ecc71
COLOR_RED    = 0xe74c3c
COLOR_ORANGE = 0xe67e22
COLOR_BLUE   = 0x3498db
COLOR_PURPLE = 0x9b59b6
COLOR_INDIGO = 0x3f51b5
COLOR_TEAL   = 0x1abc9c
COLOR_GOLD   = 0xf1c40f
COLOR_PINK   = 0xe91e63
COLOR_CYAN   = 0x00bcd4
COLOR_AMBER  = 0xf39c12


# ---------------------------------------------------------------------------
# Discord-agnostic embed specification
# ---------------------------------------------------------------------------

@dataclass
class EmbedField:
    name: str
    value: str
    inline: bool = False


@dataclass
class EmbedSpec:
    title: str
    description: str
    color: int
    fields: list[EmbedField] = field(default_factory=list)
    footer: str | None = None
    timestamp: bool = False
    url: str | None = None
    thumbnail_url: str | None = None


# ---------------------------------------------------------------------------
# Base / shared
# ---------------------------------------------------------------------------

@dataclass
class TickerData:
    """Minimal ticker data shared by most stock content types."""
    ticker: str
    ticker_info: dict
    quote: dict


# ---------------------------------------------------------------------------
# Report models
# ---------------------------------------------------------------------------

@dataclass
class StockReportData(TickerData):
    fundamentals: dict
    daily_price_history: pd.DataFrame
    popularity: pd.DataFrame
    historical_earnings: pd.DataFrame
    next_earnings_info: dict
    recent_sec_filings: pd.DataFrame
    recent_alerts: list = field(default_factory=list)  # list[dict] with keys date, alert_type, url


@dataclass
class FullStockReportData(StockReportData):
    """Extended stock report data for /report ticker detail=full."""
    price_targets: dict | None = None
    recommendations: pd.DataFrame = field(default_factory=pd.DataFrame)
    upgrades_downgrades: pd.DataFrame = field(default_factory=pd.DataFrame)
    short_interest_ratio: float | None = None
    short_interest_shares: float | None = None
    short_percent_of_float: float | None = None
    shares_outstanding: float | None = None
    quarterly_forecast: pd.DataFrame = field(default_factory=pd.DataFrame)
    yearly_forecast: pd.DataFrame = field(default_factory=pd.DataFrame)
    classification: str | None = None
    volatility_20d: float | None = None


@dataclass
class ComparisonReportData:
    """Data for /report compare — side-by-side ticker comparison."""
    tickers: list            # ordered list; benchmark_ticker is last if present
    quotes: dict             # {ticker: schwab_quote_dict}
    fundamentals: dict       # {ticker: schwab_fundamentals_dict | None}
    daily_price_histories: dict  # {ticker: pd.DataFrame}
    popularities: dict       # {ticker: pd.DataFrame}
    ticker_infos: dict       # {ticker: info_dict}
    stats: dict              # {ticker: stats_dict | None}
    benchmark_ticker: str | None = None


@dataclass
class NewsReportData:
    query: str
    news: dict


@dataclass
class EarningsSpotlightData(TickerData):
    fundamentals: dict
    daily_price_history: pd.DataFrame
    historical_earnings: pd.DataFrame
    next_earnings_info: dict


@dataclass
class PopularityReportData:
    popular_stocks: pd.DataFrame
    filter: str


@dataclass
class PoliticianReportData:
    politician: dict
    trades: pd.DataFrame
    politician_facts: dict


@dataclass
class AlertSummaryData:
    since_dt: datetime.datetime   # resolved boundary datetime
    label: str                    # human-readable label for the embed title/footer
    alerts: list                  # each dict: {date, ticker, alert_type, messageid, alert_data}


# ---------------------------------------------------------------------------
# Screener models
# ---------------------------------------------------------------------------

@dataclass
class GainerScreenerData:
    market_period: str
    gainers: pd.DataFrame


@dataclass
class VolumeScreenerData:
    unusual_volume: pd.DataFrame


@dataclass
class PopularityScreenerData:
    popular_stocks: pd.DataFrame


@dataclass
class WeeklyEarningsData:
    upcoming_earnings: pd.DataFrame
    watchlist_tickers: list


# ---------------------------------------------------------------------------
# Alert models
# ---------------------------------------------------------------------------

@dataclass
class PopularitySurgeData(TickerData):
    surge_result: object  # PopularitySurgeResult (object to avoid circular import)
    popularity_history: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass
class MomentumConfirmationData(TickerData):
    surge_flagged_at: object  # datetime
    surge_types: list = field(default_factory=list)
    price_at_flag: float | None = None
    price_change_since_flag: float | None = None
    surge_alert_message_id: int | None = None
    daily_price_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    trigger_result: object | None = None  # AlertTriggerResult | None


@dataclass
class MarketAlertData(TickerData):
    composite_result: object  # CompositeScoreResult
    daily_price_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    rvol: float | None = None


@dataclass
class MarketMoverData(TickerData):
    composite_result: object   # CompositeScoreResult
    signal_detected_at: object  # datetime
    confirmation_reason: str   # 'sustained', 'price_accelerating', 'volume_accelerating', 'volume_extreme'
    signal_observations: int
    price_velocity: float | None = None
    price_acceleration: float | None = None
    volume_velocity: float | None = None
    volume_acceleration: float | None = None
    daily_price_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    rvol: float | None = None


@dataclass
class EarningsResultData(TickerData):
    eps_actual: float
    eps_estimate: float | None
    surprise_pct: float | None
    historical_earnings: pd.DataFrame
    next_earnings_info: dict
    daily_price_history: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass
class EarningsMoverData(TickerData):
    next_earnings_info: dict
    historical_earnings: pd.DataFrame
    daily_price_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    trigger_result: object | None = None   # AlertTriggerResult | None
    eps_actual: float | None = None
    eps_estimate: float | None = None
    surprise_pct: float | None = None


@dataclass
class WatchlistMoverData(TickerData):
    watchlist: str
    daily_price_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    trigger_result: object | None = None   # AlertTriggerResult | None


# ---------------------------------------------------------------------------
# Data cog models
# ---------------------------------------------------------------------------

@dataclass
class QuoteData:
    """Data for /data quote — real-time quotes for one or more tickers."""
    tickers: list
    quotes: dict                     # {ticker: schwab_quote_dict}
    invalid_tickers: list = field(default_factory=list)


@dataclass
class UpcomingEarningsData:
    """Data for /data upcoming-earnings."""
    tickers: list
    earnings_info: dict              # {ticker: earnings_info_dict | None}
    invalid_tickers: list = field(default_factory=list)


@dataclass
class TickerStatsData:
    """Data for /data stats."""
    tickers: list
    stats: dict                      # {ticker: stats_dict | None}
    invalid_tickers: list = field(default_factory=list)


@dataclass
class MoverData:
    """Data for /data movers and /data losers."""
    direction: str                   # 'gainers' or 'losers'
    screeners: list                  # list of mover dicts from Schwab


# ---------------------------------------------------------------------------
# Data cog snapshot models (Phase 2)
# ---------------------------------------------------------------------------

@dataclass
class PriceSnapshotData:
    """Data for the price snapshot embed in /data price."""
    ticker: str
    daily_price_history: pd.DataFrame
    frequency: str                   # 'daily' or '5m'
    quote: dict | None = None        # Schwab quote dict — optional (may be unavailable)


@dataclass
class FinancialHighlightsData:
    """Data for the financial highlights embed in /data financials."""
    ticker: str
    financials: dict                 # yfinance financials dict (income/balance/cash DataFrames)


@dataclass
class FundamentalsSnapshotData:
    """Data for the fundamentals snapshot embed in /data fundamentals."""
    ticker: str
    fundamentals: dict               # Schwab fundamentals JSON response


@dataclass
class OptionsSummaryData:
    """Data for the options summary embed in /data options."""
    ticker: str
    options_chain: dict              # Schwab options chain JSON response
    current_price: float | None = None


@dataclass
class PopularitySnapshotData:
    """Data for the popularity snapshot embed in /data popularity."""
    ticker: str
    popularity: pd.DataFrame


@dataclass
class TickersSummaryData:
    """Data for the tickers summary embed in /data tickers."""
    tickers_df: pd.DataFrame


@dataclass
class EarningsTableData:
    """Data for the earnings history embed in /data earnings."""
    ticker: str
    historical_earnings: pd.DataFrame


@dataclass
class SecFilingData:
    """Data for the SEC filing embed in /data sec-filing."""
    tickers: list
    filings: dict                    # {ticker: filing_dict | None}
    form: str


# ---------------------------------------------------------------------------
# Data cog Phase 3 models — YFinance analyst / ownership / insider / short
# ---------------------------------------------------------------------------

@dataclass
class AnalystData:
    """Data for /data analyst."""
    ticker: str
    price_targets: dict | None
    recommendations: pd.DataFrame
    upgrades_downgrades: pd.DataFrame


@dataclass
class OwnershipData:
    """Data for /data ownership."""
    ticker: str
    institutional_holders: pd.DataFrame
    major_holders: pd.DataFrame


@dataclass
class InsiderData:
    """Data for /data insider."""
    ticker: str
    insider_transactions: pd.DataFrame
    insider_purchases: pd.DataFrame


@dataclass
class ShortInterestData:
    """Data for /data short-interest."""
    ticker: str
    short_interest_ratio: float | None
    short_interest_shares: float | None
    short_percent_of_float: float | None
    shares_outstanding: float | None


# ---------------------------------------------------------------------------
# Data cog Phase 4 models — News, Forecast, Screener, Losers
# ---------------------------------------------------------------------------

@dataclass
class NewsData:
    """Data for /data news — News API results per ticker."""
    tickers: list
    news_results: dict          # {ticker: news_api_response_dict}


@dataclass
class EarningsForecastData:
    """Data for /data forecast — NASDAQ quarterly and yearly EPS forecasts."""
    ticker: str
    quarterly_forecast: pd.DataFrame
    yearly_forecast: pd.DataFrame


@dataclass
class OnDemandScreenerData:
    """Data for /data screener — on-demand TradingView screener result."""
    screener_type: str          # 'premarket', 'intraday', or 'unusual-volume'
    data: pd.DataFrame


@dataclass
class TechnicalReportData(TickerData):
    """Data for /report technical — deep-dive technical analysis."""
    daily_price_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    stats: dict | None = None


@dataclass
class OptionsReportData(TickerData):
    """Data for /report options — full options chain analysis."""
    options_chain: dict = field(default_factory=dict)
    daily_price_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    iv_history: pd.DataFrame = field(default_factory=pd.DataFrame)  # for IV Rank (Phase 5)



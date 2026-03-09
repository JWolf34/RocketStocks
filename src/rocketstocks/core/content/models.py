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
class EarningsMoverData(TickerData):
    next_earnings_info: dict
    historical_earnings: pd.DataFrame
    daily_price_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    trigger_result: object | None = None   # AlertTriggerResult | None


@dataclass
class WatchlistMoverData(TickerData):
    watchlist: str
    daily_price_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    trigger_result: object | None = None   # AlertTriggerResult | None



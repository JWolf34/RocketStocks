"""Typed dataclasses for all content type data inputs.

Each content class declares exactly what data it needs via its corresponding dataclass.
This replaces the ~13 optional kwargs on Report.__init__ with explicit, type-checked fields.
"""
from __future__ import annotations

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
class EarningsMoverData(TickerData):
    next_earnings_info: dict
    historical_earnings: pd.DataFrame


@dataclass
class VolumeMoverData(TickerData):
    rvol: float
    daily_price_history: pd.DataFrame


@dataclass
class VolumeSpikeData(TickerData):
    rvol_at_time: float
    avg_vol_at_time: float
    time: str


@dataclass
class WatchlistMoverData(TickerData):
    watchlist: str


@dataclass
class SECFilingData(TickerData):
    recent_sec_filings: pd.DataFrame


@dataclass
class PopularityAlertData(TickerData):
    popularity: pd.DataFrame


@dataclass
class PoliticianTradeAlertData:
    politician: dict
    trades: pd.DataFrame

"""Typed dataclasses for all content type data inputs.

Each content class declares exactly what data it needs via its corresponding dataclass.
This replaces the ~13 optional kwargs on Report.__init__ with explicit, type-checked fields.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


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

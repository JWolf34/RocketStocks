"""Batch dataclasses and BatchAPI — multi-field parallel fetches."""
import asyncio
import datetime
import logging
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class TickerBatch:
    """All data for a single ticker deep-dive."""
    info: dict = field(default_factory=dict)
    quote: dict = field(default_factory=dict)
    schwab_fundamentals: dict = field(default_factory=dict)
    yfinance_financials: dict = field(default_factory=dict)
    daily_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    five_min_history: pd.DataFrame | None = None
    options_chain: dict = field(default_factory=dict)
    iv_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    eps_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    next_earnings: dict | None = None
    analyst_targets: dict | None = None
    recommendations: pd.DataFrame = field(default_factory=pd.DataFrame)
    float_data: dict = field(default_factory=dict)
    insider_transactions: pd.DataFrame = field(default_factory=pd.DataFrame)
    popularity: pd.DataFrame = field(default_factory=pd.DataFrame)
    recent_filings: pd.DataFrame = field(default_factory=pd.DataFrame)
    errors: dict[str, Exception] = field(default_factory=dict)


@dataclass
class PeerComparisonBatch:
    """Daily history, fundamentals, and stats for a set of tickers and benchmarks."""
    daily_histories: dict[str, pd.DataFrame] = field(default_factory=dict)
    schwab_fundamentals: dict[str, dict] = field(default_factory=dict)
    ticker_stats: dict[str, dict | None] = field(default_factory=dict)
    errors: dict[str, Exception] = field(default_factory=dict)


@dataclass
class OptionsSnapshot:
    """Options chain, IV history, quote, and stats for a single ticker."""
    chain: dict = field(default_factory=dict)
    iv_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    quote: dict = field(default_factory=dict)
    ticker_stats: dict | None = None
    errors: dict[str, Exception] = field(default_factory=dict)


@dataclass
class FundamentalsSnapshot:
    """Full fundamental picture for a single ticker."""
    financials: dict = field(default_factory=dict)
    eps_history: pd.DataFrame = field(default_factory=pd.DataFrame)
    analyst_targets: dict | None = None
    recommendations: pd.DataFrame = field(default_factory=pd.DataFrame)
    upgrades_downgrades: pd.DataFrame = field(default_factory=pd.DataFrame)
    insider_transactions: pd.DataFrame = field(default_factory=pd.DataFrame)
    errors: dict[str, Exception] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Internal gather helper
# ---------------------------------------------------------------------------

async def _gather_fields(
    tasks: dict,
    defaults: dict,
) -> tuple[dict, dict]:
    """Run all tasks concurrently via asyncio.gather(return_exceptions=True).

    Returns (field_values, errors). Failed tasks land in errors under their
    field name; the corresponding default value is used in field_values so
    the dataclass can always be constructed.
    """
    names = list(tasks.keys())
    coros = list(tasks.values())
    results = await asyncio.gather(*coros, return_exceptions=True)
    values: dict = {}
    errors: dict = {}
    for name, result in zip(names, results):
        if isinstance(result, BaseException):
            errors[name] = result
            values[name] = defaults.get(name)
        else:
            values[name] = result
    return values, errors


# ---------------------------------------------------------------------------
# BatchAPI
# ---------------------------------------------------------------------------

class BatchAPI:
    """High-level batch methods that fan out multiple fetches in parallel."""

    def __init__(self, api):
        self._api = api

    async def get_deep_dive(
        self,
        ticker: str,
        history_days: int = 365,
        intraday_days: int = 5,
    ) -> TickerBatch:
        """Fetch all single-ticker data fields in parallel.

        A single yfinance or Schwab blip never breaks the whole batch — each
        field failure is isolated into batch.errors[field_name].
        """
        logger.debug(f"get_deep_dive({ticker!r}, history_days={history_days}, intraday_days={intraday_days})")
        today = datetime.date.today()
        start_daily = today - datetime.timedelta(days=history_days)
        now = datetime.datetime.now()
        start_intraday = now - datetime.timedelta(days=intraday_days)

        tasks = {
            "info": self._api.get_ticker_info(ticker),
            "quote": self._api.get_quote(ticker),
            "schwab_fundamentals": self._api.get_schwab_fundamentals(ticker),
            "yfinance_financials": asyncio.to_thread(self._api.get_financials, ticker),
            "daily_history": self._api.get_daily_history(ticker, start_daily, today),
            "five_min_history": self._api.get_5m_history(ticker, start_intraday, now),
            "options_chain": self._api.get_options_chain(ticker),
            "iv_history": self._api.get_iv_history(ticker),
            "eps_history": self._api.get_eps_history(ticker),
            "next_earnings": self._api.get_next_earnings(ticker),
            "analyst_targets": asyncio.to_thread(self._api.get_analyst_price_targets, ticker),
            "recommendations": asyncio.to_thread(self._api.get_recommendations, ticker),
            "float_data": asyncio.to_thread(self._api.get_float_data, ticker),
            "insider_transactions": asyncio.to_thread(self._api.get_insider_transactions, ticker),
            "popularity": self._api.get_popularity(ticker),
            "recent_filings": self._api.get_recent_filings(ticker),
        }
        defaults = {
            "info": {}, "quote": {}, "schwab_fundamentals": {}, "yfinance_financials": {},
            "daily_history": pd.DataFrame(), "five_min_history": None, "options_chain": {},
            "iv_history": pd.DataFrame(), "eps_history": pd.DataFrame(),
            "next_earnings": None, "analyst_targets": None, "recommendations": pd.DataFrame(),
            "float_data": {}, "insider_transactions": pd.DataFrame(),
            "popularity": pd.DataFrame(), "recent_filings": pd.DataFrame(),
        }
        v, errors = await _gather_fields(tasks, defaults)
        return TickerBatch(
            info=v["info"],
            quote=v["quote"],
            schwab_fundamentals=v["schwab_fundamentals"],
            yfinance_financials=v["yfinance_financials"],
            daily_history=v["daily_history"],
            five_min_history=v["five_min_history"],
            options_chain=v["options_chain"],
            iv_history=v["iv_history"],
            eps_history=v["eps_history"],
            next_earnings=v["next_earnings"],
            analyst_targets=v["analyst_targets"],
            recommendations=v["recommendations"],
            float_data=v["float_data"],
            insider_transactions=v["insider_transactions"],
            popularity=v["popularity"],
            recent_filings=v["recent_filings"],
            errors=errors,
        )

    async def get_peer_comparison(
        self,
        tickers: list[str],
        benchmarks: list[str] | None = None,
        days: int = 365,
    ) -> PeerComparisonBatch:
        """Fetch daily history, fundamentals, and stats for tickers + benchmarks."""
        if benchmarks is None:
            benchmarks = ["SPY"]
        logger.debug(f"get_peer_comparison({tickers}, benchmarks={benchmarks}, days={days})")
        all_tickers = list(dict.fromkeys(tickers + benchmarks))  # deduplicate, preserve order
        today = datetime.date.today()
        start = today - datetime.timedelta(days=days)

        tasks = {
            "daily_histories": self._api.get_daily_histories(all_tickers, start, today),
            "schwab_fundamentals": self._api.get_schwab_fundamentals_batch(all_tickers),
            "ticker_stats": self._api.get_ticker_stats_batch(all_tickers),
        }
        defaults = {
            "daily_histories": {}, "schwab_fundamentals": {}, "ticker_stats": {},
        }
        v, errors = await _gather_fields(tasks, defaults)
        return PeerComparisonBatch(
            daily_histories=v["daily_histories"],
            schwab_fundamentals=v["schwab_fundamentals"],
            ticker_stats=v["ticker_stats"],
            errors=errors,
        )

    async def get_options_snapshot(self, ticker: str) -> OptionsSnapshot:
        """Fetch options chain, IV history, quote, and stats for a single ticker."""
        logger.debug(f"get_options_snapshot({ticker!r})")
        tasks = {
            "chain": self._api.get_options_chain(ticker),
            "iv_history": self._api.get_iv_history(ticker),
            "quote": self._api.get_quote(ticker),
            "ticker_stats": self._api.get_ticker_stats(ticker),
        }
        defaults = {
            "chain": {}, "iv_history": pd.DataFrame(), "quote": {}, "ticker_stats": None,
        }
        v, errors = await _gather_fields(tasks, defaults)
        return OptionsSnapshot(
            chain=v["chain"],
            iv_history=v["iv_history"],
            quote=v["quote"],
            ticker_stats=v["ticker_stats"],
            errors=errors,
        )

    async def get_fundamentals_snapshot(self, ticker: str) -> FundamentalsSnapshot:
        """Fetch all fundamental data fields in parallel."""
        logger.debug(f"get_fundamentals_snapshot({ticker!r})")
        tasks = {
            "financials": asyncio.to_thread(self._api.get_financials, ticker),
            "eps_history": self._api.get_eps_history(ticker),
            "analyst_targets": asyncio.to_thread(self._api.get_analyst_price_targets, ticker),
            "recommendations": asyncio.to_thread(self._api.get_recommendations, ticker),
            "upgrades_downgrades": asyncio.to_thread(self._api.get_upgrades_downgrades, ticker),
            "insider_transactions": asyncio.to_thread(self._api.get_insider_transactions, ticker),
        }
        defaults = {
            "financials": {}, "eps_history": pd.DataFrame(), "analyst_targets": None,
            "recommendations": pd.DataFrame(), "upgrades_downgrades": pd.DataFrame(),
            "insider_transactions": pd.DataFrame(),
        }
        v, errors = await _gather_fields(tasks, defaults)
        return FundamentalsSnapshot(
            financials=v["financials"],
            eps_history=v["eps_history"],
            analyst_targets=v["analyst_targets"],
            recommendations=v["recommendations"],
            upgrades_downgrades=v["upgrades_downgrades"],
            insider_transactions=v["insider_transactions"],
            errors=errors,
        )

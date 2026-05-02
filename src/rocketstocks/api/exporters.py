"""File writers for rocketstocks-data export — pure functions, no API calls."""
import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_DAILY_COLS = ['ticker', 'open', 'high', 'low', 'close', 'volume', 'date']
_5M_COLS = ['ticker', 'open', 'high', 'low', 'close', 'volume', 'datetime']
_POPULARITY_COLS = [
    'datetime', 'rank', 'ticker', 'name',
    'mentions', 'upvotes', 'rank_24h_ago', 'mentions_24h_ago',
]
_FINANCIALS_MAP = {
    'income_statement':           '_income_statement.csv',
    'quarterly_income_statement': '_quarterly_income_statement.csv',
    'balance_sheet':              '_balance_sheet.csv',
    'quarterly_balance_sheet':    '_quarterly_balance_sheet.csv',
    'cash_flow':                  '_cash_flow.csv',
    'quarterly_cash_flow':        '_quarterly_cash_flow.csv',
}


def write_daily_csv(df: pd.DataFrame, dest_dir: Path, ticker: str) -> Path:
    """Write daily OHLCV to TICKER_daily_data.csv with Cowork column order."""
    path = dest_dir / f"{ticker}_daily_data.csv"
    df.reindex(columns=_DAILY_COLS).to_csv(path, index=False)
    return path


def write_5m_csv(df: pd.DataFrame, dest_dir: Path, ticker: str) -> Path:
    """Write 5-minute OHLCV to TICKER_5m_data.csv with Cowork column order."""
    path = dest_dir / f"{ticker}_5m_data.csv"
    df.reindex(columns=_5M_COLS).to_csv(path, index=False)
    return path


def write_options_json(data: dict, dest_dir: Path, ticker: str) -> Path:
    """Write raw Schwab options chain to TICKER_options_chain.json."""
    path = dest_dir / f"{ticker}_options_chain.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def write_fundamentals_json(data: dict, dest_dir: Path, ticker: str) -> Path:
    """Write raw Schwab fundamentals to TICKER_fundamentals.json."""
    path = dest_dir / f"{ticker}_fundamentals.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def write_financials_csvs(financials: dict, dest_dir: Path, ticker: str) -> list[Path]:
    """Write all six yfinance financial statement CSVs.

    Returns the list of paths actually written (skips empty DataFrames).
    """
    written = []
    for key, suffix in _FINANCIALS_MAP.items():
        df = financials.get(key)
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            logger.debug(f"Skipping empty financials key {key!r} for {ticker}")
            continue
        path = dest_dir / f"{ticker}{suffix}"
        df.to_csv(path)
        written.append(path)
    return written


def write_eps_csv(df: pd.DataFrame, dest_dir: Path, ticker: str) -> Path:
    """Write EPS history to TICKER_eps.csv."""
    path = dest_dir / f"{ticker}_eps.csv"
    df.to_csv(path, index=False)
    return path


def write_popularity_csv(df: pd.DataFrame, dest_dir: Path, ticker: str) -> Path:
    """Write popularity history to TICKER_popularity.csv with Cowork column order."""
    path = dest_dir / f"{ticker}_popularity.csv"
    df.reindex(columns=_POPULARITY_COLS).to_csv(path, index=False)
    return path

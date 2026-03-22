"""YFinance API client — financial statements and earnings data."""
import datetime
import logging

import numpy as np
import pandas as pd
import yfinance as yf
from ratelimit import limits, sleep_and_retry

logger = logging.getLogger(__name__)


class YFinanceClient:
    """Thin wrapper around yfinance with rate limiting."""

    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_earnings_result(self, ticker: str) -> dict | None:
        """Return today's reported EPS result for *ticker*, or None if not yet available.

        Checks yfinance ``earnings_dates`` for a row matching today's date with a
        populated ``Reported EPS`` value.  Returns a dict with keys:
        ``eps_actual``, ``eps_estimate``, ``surprise_pct``.
        """
        try:
            stock = yf.Ticker(ticker)
            df = stock.earnings_dates
            if df is None or df.empty:
                return None

            today = datetime.date.today()
            # earnings_dates index is a timezone-aware DatetimeTZDtype; normalise to date
            df = df.copy()
            df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
            today_dt = pd.Timestamp(today)

            if today_dt not in df.index:
                return None

            row = df.loc[today_dt]
            # Handle duplicate index (multiple rows for same date) — take first
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]

            eps_actual = row.get('Reported EPS')
            if eps_actual is None or (isinstance(eps_actual, float) and np.isnan(eps_actual)):
                return None

            eps_estimate = row.get('EPS Estimate')
            surprise_pct = row.get('Surprise(%)')

            return {
                'eps_actual': float(eps_actual),
                'eps_estimate': float(eps_estimate) if eps_estimate is not None and not (isinstance(eps_estimate, float) and np.isnan(eps_estimate)) else None,
                'surprise_pct': float(surprise_pct) if surprise_pct is not None and not (isinstance(surprise_pct, float) and np.isnan(surprise_pct)) else None,
            }
        except Exception:
            logger.warning(f"get_earnings_result({ticker}) failed", exc_info=True)
            return None

    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_financials(self, ticker: str) -> dict:
        """Return latest available financial statements for *ticker* from Yahoo Finance."""
        logger.info(f"Fetching financials for ticker {ticker}")
        stock = yf.Ticker(ticker)
        return {
            'income_statement': stock.income_stmt,
            'quarterly_income_statement': stock.quarterly_income_stmt,
            'balance_sheet': stock.balance_sheet,
            'quarterly_balance_sheet': stock.quarterly_balance_sheet,
            'cash_flow': stock.cashflow,
            'quarterly_cash_flow': stock.quarterly_cashflow,
        }

    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_analyst_price_targets(self, ticker: str) -> dict | None:
        """Return analyst price targets for *ticker*, or None on failure."""
        try:
            return yf.Ticker(ticker).analyst_price_targets
        except Exception:
            logger.warning(f"get_analyst_price_targets({ticker}) failed", exc_info=True)
            return None

    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_recommendations_summary(self, ticker: str) -> pd.DataFrame:
        """Return analyst recommendations summary for *ticker*."""
        try:
            result = yf.Ticker(ticker).recommendations_summary
            return result if result is not None else pd.DataFrame()
        except Exception:
            logger.warning(f"get_recommendations_summary({ticker}) failed", exc_info=True)
            return pd.DataFrame()

    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_upgrades_downgrades(self, ticker: str) -> pd.DataFrame:
        """Return recent analyst upgrades/downgrades for *ticker*."""
        try:
            result = yf.Ticker(ticker).upgrades_downgrades
            return result if result is not None else pd.DataFrame()
        except Exception:
            logger.warning(f"get_upgrades_downgrades({ticker}) failed", exc_info=True)
            return pd.DataFrame()

    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_institutional_holders(self, ticker: str) -> pd.DataFrame:
        """Return institutional holders for *ticker*."""
        try:
            result = yf.Ticker(ticker).institutional_holders
            return result if result is not None else pd.DataFrame()
        except Exception:
            logger.warning(f"get_institutional_holders({ticker}) failed", exc_info=True)
            return pd.DataFrame()

    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_major_holders(self, ticker: str) -> pd.DataFrame:
        """Return major holders breakdown for *ticker*."""
        try:
            result = yf.Ticker(ticker).major_holders
            return result if result is not None else pd.DataFrame()
        except Exception:
            logger.warning(f"get_major_holders({ticker}) failed", exc_info=True)
            return pd.DataFrame()

    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_insider_transactions(self, ticker: str) -> pd.DataFrame:
        """Return insider transactions for *ticker*."""
        try:
            result = yf.Ticker(ticker).insider_transactions
            return result if result is not None else pd.DataFrame()
        except Exception:
            logger.warning(f"get_insider_transactions({ticker}) failed", exc_info=True)
            return pd.DataFrame()

    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_insider_purchases(self, ticker: str) -> pd.DataFrame:
        """Return insider purchases summary for *ticker*."""
        try:
            result = yf.Ticker(ticker).insider_purchases
            return result if result is not None else pd.DataFrame()
        except Exception:
            logger.warning(f"get_insider_purchases({ticker}) failed", exc_info=True)
            return pd.DataFrame()

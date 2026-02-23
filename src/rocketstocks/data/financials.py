"""Standalone function for fetching financial statements via yfinance."""
import logging

import yfinance as yf

logger = logging.getLogger(__name__)


def fetch_financials(ticker: str) -> dict:
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

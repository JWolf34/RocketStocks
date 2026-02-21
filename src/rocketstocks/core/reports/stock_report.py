import logging
import pandas as pd
from rocketstocks.core.reports.base import Report

logger = logging.getLogger(__name__)


class StockReport(Report):
    """Report subclass containing information on the report's ticker"""

    def __init__(self, ticker_info: dict, daily_price_history: pd.DataFrame, popularity: pd.DataFrame,
                 recent_sec_filings: pd.DataFrame, historical_earnings: pd.DataFrame,
                 next_earnings_info: dict, quote: dict, fundamentals: dict):
        super().__init__(
            ticker_info=ticker_info,
            daily_price_history=daily_price_history,
            popularity=popularity,
            recent_sec_filings=recent_sec_filings,
            historical_earnings=historical_earnings,
            next_earnings_info=next_earnings_info,
            quote=quote,
            fundamentals=fundamentals,
        )

    def build_report(self):
        """Build complete stock report content string"""
        logger.debug("Building Stock Report...")
        report = ''
        report += self.build_report_header()
        report += self.build_ticker_info()
        report += self.build_daily_summary()
        report += self.build_performance()
        report += self.build_fundamentals()
        report += self.build_popularity()
        report += self.build_recent_earnings()
        report += self.build_recent_SEC_filings()
        return report

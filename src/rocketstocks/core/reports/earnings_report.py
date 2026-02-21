import logging
import pandas as pd
from rocketstocks.core.reports.base import Report

logger = logging.getLogger(__name__)


class EarningsSpotlightReport(Report):
    """Report subclass to post spotlight on random stock reporting earnings today"""

    def __init__(self, ticker_info: pd.DataFrame, daily_price_history: pd.DataFrame,
                 next_earnings_info: pd.DataFrame, historical_earnings: pd.DataFrame,
                 quote: dict, fundamentals: dict):
        super().__init__(
            ticker_info=ticker_info,
            daily_price_history=daily_price_history,
            next_earnings_info=next_earnings_info,
            historical_earnings=historical_earnings,
            quote=quote,
            fundamentals=fundamentals,
        )

    def build_report_header(self):
        """Overrides the parent function to generate custom header"""
        logger.debug("Building Earnings Spotlight Report header...")
        return f"# :bulb: Earnings Spotlight: {self.ticker}\n\n"

    def build_report(self):
        """Build complete earnings spotlight report content string"""
        logger.debug("Building Earnings Spotlight Report...")
        report = ""
        report += self.build_report_header()
        report += self.build_earnings_date()
        report += self.build_ticker_info()
        report += self.build_fundamentals()
        report += self.build_performance()
        report += self.build_upcoming_earnings_summary()
        report += self.build_recent_earnings()
        return report

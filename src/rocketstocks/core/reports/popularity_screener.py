import datetime
import logging
import pandas as pd
from rocketstocks.core.reports.screener import Screener
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


class PopularityScreener(Screener):
    """Screener subclass to post popularity rankings for stocks"""

    def __init__(self, popular_stocks: pd.DataFrame):
        column_map = {
            'rank': 'Rank',
            'ticker': 'Ticker',
            'mentions': 'Mentions',
            'rank_24h_ago': 'Rank 24H Ago',
            'mentions_24h_ago': 'Mentions 24H Ago',
        }

        super().__init__(
            screener_type="popular-stocks",
            data=popular_stocks,
            column_map=column_map,
        )

    def build_report_header(self):
        """Overrides the parent function to generate custom header"""
        logger.debug(f"Building '{self.screener_type}' screener header...")
        now = datetime.datetime.now(tz=date_utils.timezone())
        header = "### :rotating_light: Popular Stocks {} (Updated {})\n\n".format(
            now.date().strftime("%m/%d/%Y"),
            date_utils.round_down_nearest_minute(30).astimezone(date_utils.timezone()).strftime("%I:%M %p"),
        )
        return header

    def build_report(self):
        """Build complete popularity screener content string"""
        logger.debug(f"Building '{self.screener_type}' screener...")
        report = ""
        report += self.build_report_header()
        report += self.build_df_table(df=self.data[:20])
        return report

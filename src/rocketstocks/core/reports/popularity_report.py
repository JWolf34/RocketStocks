import datetime
import logging
import pandas as pd
from rocketstocks.core.reports.base import Report
from rocketstocks.core.config.paths import datapaths

logger = logging.getLogger(__name__)


class PopularityReport(Report):
    """Report subclass for posting the most recent popularity ranking for stocks in the input filter"""

    def __init__(self, popular_stocks: pd.DataFrame, filter: str):
        super().__init__()
        self.popular_stocks = popular_stocks
        self.filter = filter
        self.column_map = {
            'rank': 'Rank',
            'ticker': 'Ticker',
            'mentions': 'Mentions',
            'rank_24h_ago': 'Rank 24H Ago',
            'mentions_24h_ago': 'Mentions 24H Ago',
        }

        self.filepath = f"{datapaths.attachments_path}/popular-stocks_{filter}_{datetime.datetime.today().strftime('%m-%d-%Y')}.csv"
        self.write_df_to_file(df=self.popular_stocks, filepath=self.filepath)

    def format_columns(self):
        """Format all columns in self.data per column map - rename and drop columns accordingly"""
        self.popular_stocks = self.popular_stocks.filter(list(self.column_map.keys()))
        self.popular_stocks = self.popular_stocks.rename(columns=self.column_map)

    def build_report_header(self):
        """Overrides the parent function to generate custom header"""
        logger.debug("Building Popularity Report header...")
        return f"# Most Popular Stocks ({self.filter}) {datetime.datetime.today().strftime('%m/%d/%Y')}\n\n"

    def build_report(self):
        """Build complete popularity report content string"""
        logger.debug("Building Popularity Report...")
        report = ''
        report += self.build_report_header()
        report += self.build_df_table(self.popular_stocks.drop(columns=['name'])[:20])
        return report

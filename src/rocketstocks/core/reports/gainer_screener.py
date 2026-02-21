import datetime
import logging
import pandas as pd
from rocketstocks.core.reports.screener import Screener
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


class GainerScreener(Screener):
    """Screener subclass for posting premarket/intraday/postmarket gainers"""

    def __init__(self, market_period: str, gainers: pd.DataFrame):
        self.market_period = market_period

        if self.market_period == 'premarket':
            column_map = {
                'name': 'Ticker',
                'premarket_change': 'Change (%)',
                'premarket_close': 'Price',
                'close': 'Prev Close',
                'premarket_volume': 'Pre Market Volume',
                'market_cap_basic': 'Market Cap',
            }
        elif self.market_period == 'intraday':
            column_map = {
                'name': 'Ticker',
                'change': 'Change (%)',
                'close': 'Price',
                'volume': 'Volume',
                'market_cap_basic': 'Market Cap',
            }
        elif self.market_period == 'aftermarket':
            column_map = {
                'name': 'Ticker',
                'postmarket_change': 'Change (%)',
                'postmarket_close': 'Price',
                'close': 'Price at Close',
                'postmarket_volume': 'After Hours Volume',
                'market_cap_basic': 'Market Cap',
            }
        else:
            column_map = {}

        super().__init__(
            screener_type=f"{self.market_period}-gainers",
            data=gainers,
            column_map=column_map,
        )

    def format_columns(self):
        """Extends the parent function to format Volume, Market Cap, and % Change columns"""
        super().format_columns()

        volume_cols = self.data.filter(like='Volume').columns.to_list()
        for volume_col in volume_cols:
            self.data[volume_col] = self.data[volume_col].apply(lambda x: self.format_large_num(x))

        self.data['Market Cap'] = self.data['Market Cap'].apply(lambda x: self.format_large_num(x))
        self.data['Change (%)'] = self.data['Change (%)'].apply(
            lambda x: "{:.2f}%".format(float(x)) if x is not None else 0.00
        )

    def build_report_header(self):
        """Overrides the parent function to generate custom header"""
        logger.debug(f"Building '{self.screener_type}' screener header...")
        now = datetime.datetime.now(tz=date_utils.timezone())
        header = "### :rotating_light: {} Gainers {} (Updated {})\n\n".format(
            "Pre-market" if self.market_period == 'premarket'
            else "Intraday" if self.market_period == 'intraday'
            else "After Hours" if self.market_period == 'aftermarket'
            else "",
            now.date().strftime("%m/%d/%Y"),
            now.strftime("%I:%M %p"),
        )
        return header

    def build_report(self):
        """Build complete gainer screener content string"""
        logger.debug(f"Building '{self.screener_type}' screener...")
        report = ""
        report += self.build_report_header()
        report += self.build_df_table(self.data[:15])
        return report

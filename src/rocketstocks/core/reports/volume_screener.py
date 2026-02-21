import datetime
import logging
import pandas as pd
from rocketstocks.core.reports.screener import Screener
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


class VolumeScreener(Screener):
    """Screener subclass to post unusual volume movers in the market"""

    def __init__(self, unusual_volume: pd.DataFrame):
        column_map = {
            'name': 'Ticker',
            'close': 'Price',
            'change': 'Change (%)',
            'relative_volume_10d_calc': 'Relative Volume (10 Day)',
            'volume': 'Volume',
            'average_volume_10d_calc': 'Avg Volume (10 Day)',
            'market_cap_basic': 'Market Cap',
        }

        super().__init__(
            screener_type="unusual-volume",
            data=unusual_volume,
            column_map=column_map,
        )

    def format_columns(self):
        """Extends the parent function to format Volume, Market Cap, % Change, and Relative Volume"""
        super().format_columns()

        volume_cols = self.data.filter(like='Volume').columns.to_list()
        for volume_col in volume_cols:
            self.data[volume_col] = self.data[volume_col].apply(lambda x: self.format_large_num(x))

        self.data['Market Cap'] = self.data['Market Cap'].apply(lambda x: self.format_large_num(x))
        self.data['Change (%)'] = self.data['Change (%)'].apply(
            lambda x: "{:.2f}%".format(float(x)) if x is not None else 0.00
        )
        self.data['Relative Volume (10 Day)'] = self.data['Relative Volume (10 Day)'].apply(
            lambda x: f"{x}x"
        )

    def build_report_header(self):
        """Overrides the parent function to generate custom header"""
        logger.debug(f"Building '{self.screener_type}' screener header...")
        now = datetime.datetime.now(tz=date_utils.timezone())
        header = "### :rotating_light: Unusual Volume {} (Updated {})\n\n".format(
            now.date().strftime("%m/%d/%Y"),
            now.strftime("%I:%M %p"),
        )
        return header

    def build_report(self):
        """Build complete volume screener content string"""
        logger.debug(f"Building '{self.screener_type}' screener...")
        report = ""
        report += self.build_report_header()
        report += self.build_df_table(self.data[:12])
        return report

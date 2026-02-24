import datetime
import logging

from rocketstocks.core.content.formatting import build_df_table, format_large_num
from rocketstocks.core.content.models import GainerScreenerData
from rocketstocks.core.content.screeners.base import Screener
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)

_COLUMN_MAPS = {
    'premarket': {
        'name': 'Ticker',
        'premarket_change': 'Change (%)',
        'premarket_close': 'Price',
        'close': 'Prev Close',
        'premarket_volume': 'Pre Market Volume',
        'market_cap_basic': 'Market Cap',
    },
    'intraday': {
        'name': 'Ticker',
        'change': 'Change (%)',
        'close': 'Price',
        'volume': 'Volume',
        'market_cap_basic': 'Market Cap',
    },
    'aftermarket': {
        'name': 'Ticker',
        'postmarket_change': 'Change (%)',
        'postmarket_close': 'Price',
        'close': 'Price at Close',
        'postmarket_volume': 'After Hours Volume',
        'market_cap_basic': 'Market Cap',
    },
}


class GainerScreener(Screener):
    """Screener for premarket/intraday/postmarket gainers."""

    def __init__(self, data: GainerScreenerData):
        self.market_period = data.market_period
        column_map = _COLUMN_MAPS.get(data.market_period, {})
        super().__init__(
            screener_type=f"{data.market_period}-gainers",
            data=data.gainers,
            column_map=column_map,
        )
        self._format_extra_columns()

    def _format_extra_columns(self) -> None:
        """Format Volume, Market Cap, and % Change columns (skips if columns absent)."""
        if self.data.empty:
            return

        volume_cols = self.data.filter(like='Volume').columns.to_list()
        for volume_col in volume_cols:
            self.data[volume_col] = self.data[volume_col].apply(lambda x: format_large_num(x))

        if 'Market Cap' in self.data.columns:
            self.data['Market Cap'] = self.data['Market Cap'].apply(lambda x: format_large_num(x))
        if 'Change (%)' in self.data.columns:
            self.data['Change (%)'] = self.data['Change (%)'].apply(
                lambda x: "{:.2f}%".format(float(x)) if x is not None else 0.00
            )

    def build_report(self) -> str:
        logger.debug(f"Building '{self.screener_type}' screener...")
        now = datetime.datetime.now(tz=date_utils.timezone())
        label = (
            "Pre-market" if self.market_period == 'premarket'
            else "Intraday" if self.market_period == 'intraday'
            else "After Hours" if self.market_period == 'aftermarket'
            else ""
        )
        count = len(self.data[:15])
        header = "### :rotating_light: {} Gainers — **{} stocks** · {} (Updated {})\n\n".format(
            label,
            count,
            now.date().strftime("%m/%d/%Y"),
            now.strftime("%I:%M %p"),
        )
        footer = "-# Data via TradingView · {}\n".format(now.strftime("%m/%d/%Y %I:%M %p"))
        return header + build_df_table(self.data[:15]) + "\n" + footer

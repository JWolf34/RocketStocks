import datetime
import logging

from rocketstocks.core.content.formatting import build_df_table, format_large_num
from rocketstocks.core.content.models import COLOR_ORANGE, EmbedSpec, VolumeScreenerData
from rocketstocks.core.content.screeners.base import Screener
from rocketstocks.core.content.sections_card import volume_screener_cards
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)

_COLUMN_MAP = {
    'name': 'Ticker',
    'close': 'Price',
    'change': 'Change (%)',
    'relative_volume_10d_calc': 'Relative Volume (10 Day)',
    'volume': 'Volume',
    'average_volume_10d_calc': 'Avg Volume (10 Day)',
    'market_cap_basic': 'Market Cap',
}


class VolumeScreener(Screener):
    """Screener for unusual volume movers."""

    def __init__(self, data: VolumeScreenerData):
        super().__init__(
            screener_type="unusual-volume",
            data=data.unusual_volume,
            column_map=_COLUMN_MAP,
        )
        self._format_extra_columns()

    def _format_extra_columns(self) -> None:
        """Format Volume, Market Cap, % Change, and Relative Volume columns."""
        volume_cols = self.data.filter(like='Volume').columns.to_list()
        for volume_col in volume_cols:
            self.data[volume_col] = self.data[volume_col].apply(lambda x: format_large_num(x))

        self.data['Market Cap'] = self.data['Market Cap'].apply(lambda x: format_large_num(x))
        self.data['Change (%)'] = self.data['Change (%)'].apply(
            lambda x: "{:.2f}%".format(float(x)) if x is not None else 0.00
        )
        self.data['Relative Volume (10 Day)'] = self.data['Relative Volume (10 Day)'].apply(
            lambda x: f"{x}x"
        )

    def build_report(self) -> str:
        logger.debug(f"Building '{self.screener_type}' screener...")
        now = datetime.datetime.now(tz=date_utils.timezone())
        count = len(self.data[:12])
        header = "🚨 Unusual Volume — **{} stocks** · {} (Updated {})\n\n".format(
            count,
            now.date().strftime("%m/%d/%Y"),
            now.strftime("%I:%M %p"),
        )
        footer = "-# Data via TradingView · {}\n".format(now.strftime("%m/%d/%Y %I:%M %p"))
        return header + build_df_table(self.data[:12]) + "\n" + footer

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug(f"Building '{self.screener_type}' screener EmbedSpec...")
        now = datetime.datetime.now(tz=date_utils.timezone())
        count = len(self.data[:12])
        title = "🚨 Unusual Volume — {} stocks · {} (Updated {})".format(
            count,
            now.date().strftime("%m/%d/%Y"),
            now.strftime("%I:%M %p"),
        )
        description = volume_screener_cards(self.data, limit=20)
        footer = "Data via TradingView · {}".format(now.strftime("%m/%d/%Y %I:%M %p"))
        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_ORANGE,
            footer=footer,
            timestamp=True,
        )

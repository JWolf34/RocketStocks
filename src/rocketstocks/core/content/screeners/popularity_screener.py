import datetime
import logging

from rocketstocks.core.content.formatting import build_df_table
from rocketstocks.core.content.models import PopularityScreenerData
from rocketstocks.core.content.screeners.base import Screener
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)

_COLUMN_MAP = {
    'rank': 'Rank',
    'ticker': 'Ticker',
    'mentions': 'Mentions',
    'rank_24h_ago': 'Rank 24H Ago',
    'mentions_24h_ago': 'Mentions 24H Ago',
}


class PopularityScreener(Screener):
    """Screener for popularity rankings."""

    def __init__(self, data: PopularityScreenerData):
        super().__init__(
            screener_type="popular-stocks",
            data=data.popular_stocks,
            column_map=_COLUMN_MAP,
        )

    def build_report(self) -> str:
        logger.debug(f"Building '{self.screener_type}' screener...")
        now = datetime.datetime.now(tz=date_utils.timezone())
        count = len(self.data[:20])
        updated_time = date_utils.round_down_nearest_minute(30).astimezone(date_utils.timezone()).strftime("%I:%M %p")
        header = "### :rotating_light: Popular Stocks — **{} stocks** · {} (Updated {})\n\n".format(
            count,
            now.date().strftime("%m/%d/%Y"),
            updated_time,
        )
        footer = "-# Data via ApeWisdom · {}\n".format(now.strftime("%m/%d/%Y %I:%M %p"))
        return header + build_df_table(df=self.data[:20]) + "\n" + footer

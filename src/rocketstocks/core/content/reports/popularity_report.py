import datetime
import logging

from rocketstocks.core.config.paths import datapaths
from rocketstocks.core.content.formatting import build_df_table, write_df_to_file
from rocketstocks.core.content.models import PopularityReportData
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)

_COLUMN_MAP = {
    'rank': 'Rank',
    'ticker': 'Ticker',
    'mentions': 'Mentions',
    'rank_24h_ago': 'Rank 24H Ago',
    'mentions_24h_ago': 'Mentions 24H Ago',
}


class PopularityReport:
    """Standalone popularity report."""

    def __init__(self, data: PopularityReportData):
        self.data = data
        self.filepath = (
            f"{datapaths.attachments_path}/popular-stocks_{data.filter}_"
            f"{datetime.datetime.today().strftime('%m-%d-%Y')}.csv"
        )
        write_df_to_file(df=data.popular_stocks, filepath=self.filepath)

        # Format columns for display
        self._display_data = data.popular_stocks.filter(list(_COLUMN_MAP.keys())).rename(columns=_COLUMN_MAP)

    def build_report(self) -> str:
        logger.debug("Building Popularity Report...")
        return (
            sections.popularity_report_header(self.data.filter)
            + build_df_table(self._display_data.drop(columns=['name'], errors='ignore')[:20])
        )

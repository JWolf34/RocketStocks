import logging

from rocketstocks.core.config.paths import datapaths
from rocketstocks.core.content.formatting import write_df_to_file
from rocketstocks.core.content.models import COLOR_TEAL, EmbedSpec, PoliticianReportData
from rocketstocks.core.content.sections_card import politician_info_card, politician_trades_card

logger = logging.getLogger(__name__)


class PoliticianReport:
    """Standalone politician trade history report."""

    def __init__(self, data: PoliticianReportData):
        self.data = data
        self.filepath = f"{datapaths.attachments_path}/{data.politician['politician_id']}_trades.csv"
        write_df_to_file(df=data.trades, filepath=self.filepath)

    def build(self) -> EmbedSpec:
        logger.debug("Building Politician Report embed...")
        title = f"🏛️ Politician Report: {self.data.politician['name']}"

        description = (
            politician_info_card(self.data.politician, self.data.politician_facts)
            + politician_trades_card(self.data.trades)
        )

        if len(description) > 4096:
            description = description[:4093] + '...'

        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_TEAL,
            footer="RocketStocks · politician-report",
            timestamp=True,
        )

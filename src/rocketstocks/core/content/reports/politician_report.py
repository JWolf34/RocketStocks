import logging
import re

from rocketstocks.core.config.paths import datapaths
from rocketstocks.core.content.formatting import write_df_to_file
from rocketstocks.core.content.models import COLOR_BLUE, EmbedSpec, PoliticianReportData
from rocketstocks.core.content import sections
from rocketstocks.core.content.sections_card import politician_trades_card

logger = logging.getLogger(__name__)


class PoliticianReport:
    """Standalone politician trade history report."""

    def __init__(self, data: PoliticianReportData):
        self.data = data
        self.filepath = f"{datapaths.attachments_path}/{data.politician['politician_id']}_trades.csv"
        write_df_to_file(df=data.trades, filepath=self.filepath)

    def build_report(self) -> str:
        logger.debug("Building Politician Report...")
        return (
            sections.politician_report_header(self.data.politician['name'])
            + sections.politician_info_section(self.data.politician, self.data.politician_facts)
            + sections.politician_trades_section(self.data.trades)
        )

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug("Building Politician Report EmbedSpec...")
        title = sections.politician_report_header(
            self.data.politician['name']
        ).splitlines()[0].lstrip('# ').strip()

        # Build body with card-format trades instead of multi-column table
        body = (
            sections.politician_info_section(self.data.politician, self.data.politician_facts)
            + politician_trades_card(self.data.trades)
        )

        # Replace markdown headers with bold text (Discord doesn't render ## in embeds)
        description = re.sub(r'^#{1,3} (.+)$', r'**\1**', body, flags=re.MULTILINE)

        if len(description) > 4096:
            description = description[:4093] + '...'

        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_BLUE,
            footer="RocketStocks · politician-report",
            timestamp=True,
        )

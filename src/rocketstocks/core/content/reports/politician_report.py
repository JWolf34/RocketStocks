import logging

from rocketstocks.core.config.paths import datapaths
from rocketstocks.core.content.formatting import write_df_to_file
from rocketstocks.core.content.models import PoliticianReportData
from rocketstocks.core.content import sections

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

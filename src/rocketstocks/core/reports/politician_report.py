import logging
import pandas as pd
from rocketstocks.core.reports.base import Report
from rocketstocks.core.config.paths import datapaths

logger = logging.getLogger(__name__)


class PoliticianReport(Report):
    """Report subclass for politician trade history"""

    def __init__(self, politician: dict, trades: pd.DataFrame, politician_facts: dict):
        super().__init__(
            politician=politician,
            trades=trades,
            politician_facts=politician_facts,
        )

        self.filepath = f"{datapaths.attachments_path}/{politician['politician_id']}_trades.csv"
        self.write_df_to_file(df=self.trades, filepath=self.filepath)

    def build_report_header(self):
        """Overrides the parent function to generate custom header"""
        logger.debug("Building Politician Report header...")
        return f"# Politician Report: {self.politician['name']}\n"

    def build_report(self):
        """Build complete politician report content string"""
        logger.debug("Building Politician Report...")
        report = ""
        report += self.build_report_header()
        report += self.build_politician_info()
        report += self.build_politician_trades()
        return report

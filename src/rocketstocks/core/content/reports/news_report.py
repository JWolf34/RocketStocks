import logging

from rocketstocks.core.content.models import NewsReportData
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)


class NewsReport:
    """Standalone news report — no base class or ticker required."""

    def __init__(self, data: NewsReportData):
        self.data = data

    def build_report(self) -> str:
        logger.debug("Building News Report...")
        return (
            sections.news_report_header(self.data.query)
            + sections.news_section(self.data.news)
            + '\n'
        )

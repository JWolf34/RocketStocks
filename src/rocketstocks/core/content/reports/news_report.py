import logging

from rocketstocks.core.content.models import COLOR_INDIGO, EmbedSpec, NewsReportData
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

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug("Building News Report EmbedSpec...")
        title = f"News articles for '{self.data.query}'"
        description = sections.news_section(self.data.news)

        if len(description) > 4096:
            description = description[:4093] + '...'

        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_INDIGO,
            footer="RocketStocks · news-report",
            timestamp=True,
        )

import logging
import re

from rocketstocks.core.content.models import COLOR_BLUE, EmbedSpec, NewsReportData
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
        full = self.build_report()
        lines = full.split('\n')
        title = lines[0].lstrip('# ').strip()
        description = '\n'.join(lines[1:]).lstrip('\n')

        # Replace markdown headers with bold text (Discord doesn't render ## in embeds)
        description = re.sub(r'^#{1,3} (.+)$', r'**\1**', description, flags=re.MULTILINE)

        if len(description) > 4096:
            description = description[:4093] + '...'

        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_BLUE,
            footer="RocketStocks · news-report",
            timestamp=True,
        )

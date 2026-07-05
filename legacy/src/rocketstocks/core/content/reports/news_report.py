import logging

from rocketstocks.core.content.models import COLOR_INDIGO, EmbedSpec, NewsReportData
from rocketstocks.core.content.sections_card import news_card

logger = logging.getLogger(__name__)


class NewsReport:
    """Standalone news report — no base class or ticker required."""

    def __init__(self, data: NewsReportData):
        self.data = data

    def build(self) -> EmbedSpec:
        logger.debug("Building News Report embed...")
        title = f"News articles for '{self.data.query}'"
        description = news_card(self.data.news)

        if len(description) > 4096:
            description = description[:4093] + '...'

        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_INDIGO,
            footer="RocketStocks · news-report",
            timestamp=True,
        )

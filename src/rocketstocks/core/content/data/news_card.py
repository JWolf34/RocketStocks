"""NewsCard — news headlines embed for /data news."""
import logging

from rocketstocks.core.content.models import COLOR_INDIGO, EmbedSpec, NewsData
from rocketstocks.core.content.sections_card import news_card

logger = logging.getLogger(__name__)


class NewsCard:
    """Builds a news headlines embed with up to 10 articles per ticker."""

    def __init__(self, data: NewsData):
        self.data = data

    def build(self) -> EmbedSpec:
        sections = []
        for ticker in self.data.tickers:
            news = self.data.news_results.get(ticker)
            if not news or not news.get('articles'):
                sections.append(f"__**{ticker}**__\nNo news found.\n\n")
            else:
                sections.append(f"__**{ticker}**__\n{news_card(news)}\n\n")

        description = "".join(sections).strip()
        if len(description) > 4096:
            description = description[:4093] + '...'
        title = "News: " + ", ".join(self.data.tickers)
        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_INDIGO,
        )

import logging
import datetime

from newsapi import NewsApiClient

from rocketstocks.core.config.settings import settings

logger = logging.getLogger(__name__)


class News:
    def __init__(self):
        self.token = settings.news_api_key
        self.news = NewsApiClient(api_key=self.token)
        # B15 fix: 'eeneral' → 'general'
        self.categories = {
            'Business': 'business',
            'Entertainment': 'entertainment',
            'General': 'general',
            'Health': 'health',
            'Science': 'science',
            'Sports': 'sports',
            'Technology': 'technology',
        }
        self.sort_by = {
            'Relevancy': 'relevancy',
            'Popularity': 'popularity',
            'Publication Time': 'publishedAt',
        }

    def get_sources(self):
        return self.news.get_sources()

    def get_news(self, query, **kwargs):
        logger.debug(f"Fetching news with query '{query}'")
        return self.news.get_everything(q=query, language='en', **kwargs)

    def get_breaking_news(self, query, **kwargs):
        logger.debug(f"Fetching breaking news with query '{query}'")
        return self.news.get_top_headlines(q=query, **kwargs)

import logging
from rocketstocks.core.reports.base import Report
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


class NewsReport(Report):
    """Report subclass to post news articles about the input query"""

    def __init__(self, news, query):
        # NewsReport has no ticker or channel — skip super().__init__()
        self.news = news
        self.query = query

    def build_report_header(self):
        """Return message content for news report header"""
        logger.debug("Building News Report header...")
        header = f"## News articles for '{self.query}'\n"
        return header + "\n"

    def build_news(self):
        """Return message content with up to the top 10 news articles hyperlinked"""
        logger.debug("Building news...")
        report = ''
        for article in self.news['articles'][:10]:
            article_date = date_utils.format_date_from_iso(date=article['publishedAt']).strftime("%m/%d/%y %H:%M:%S EST")
            article_line = f"[{article['title']} - {article['source']['name']} ({article_date})](<{article['url']}>)\n"
            if len(report + article_line) <= 1900:
                report += article_line
            else:
                break
        return report

    def build_report(self):
        """Build complete news report content string"""
        logger.debug("Building News Report...")
        report = ''
        report += self.build_report_header()
        report += self.build_news()
        return report + '\n'

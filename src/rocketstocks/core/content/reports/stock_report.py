import logging

from rocketstocks.core.content.models import StockReportData
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)


class StockReport:
    """Standalone stock report — no base class required."""

    def __init__(self, data: StockReportData):
        self.data = data
        self.ticker = data.ticker

    def build_report(self) -> str:
        logger.debug("Building Stock Report...")
        return (
            sections.report_header(self.data.ticker)
            + sections.ticker_info_section(self.data.ticker_info, self.data.quote)
            + sections.daily_summary_section(self.data.quote)
            + sections.performance_section(self.data.daily_price_history, self.data.quote)
            + sections.fundamentals_section(self.data.fundamentals, self.data.quote)
            + sections.popularity_section(self.data.popularity)
            + sections.recent_earnings_section(self.data.historical_earnings)
            + sections.sec_filings_section(self.data.recent_sec_filings)
        )

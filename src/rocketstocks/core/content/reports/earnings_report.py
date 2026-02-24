import logging

from rocketstocks.core.content.models import EarningsSpotlightData
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)


class EarningsSpotlightReport:
    """Standalone earnings spotlight report."""

    def __init__(self, data: EarningsSpotlightData):
        self.data = data
        self.ticker = data.ticker

    def build_report(self) -> str:
        logger.debug("Building Earnings Spotlight Report...")
        return (
            sections.earnings_spotlight_header(self.data.ticker)
            + sections.earnings_date_section(self.data.ticker, self.data.next_earnings_info)
            + sections.ticker_info_section(self.data.ticker_info, self.data.quote)
            + sections.fundamentals_section(
                self.data.fundamentals, self.data.quote,
                daily_price_history=self.data.daily_price_history,
            )
            + sections.performance_section(self.data.daily_price_history, self.data.quote)
            + sections.technical_signals_section(self.data.daily_price_history)
            + sections.upcoming_earnings_summary_section(self.data.next_earnings_info)
            + sections.recent_earnings_section(self.data.historical_earnings)
        )

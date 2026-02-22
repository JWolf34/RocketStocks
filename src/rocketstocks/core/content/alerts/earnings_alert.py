import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import EarningsMoverData
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)


class EarningsMoverAlert(Alert):
    alert_type = "EARNINGS_MOVER"

    def __init__(self, data: EarningsMoverData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']

    def build_alert(self) -> str:
        logger.debug("Building Earnings Mover Alert...")
        pct_change = self.alert_data['pct_change']
        symbol = "🟢" if pct_change > 0 else "🔻"
        todays_change = f"`{self.data.ticker}` is {symbol} **{pct_change:.2f}%** and reports earnings today\n"
        return (
            sections.alert_header(f"Earnings Mover: {self.data.ticker}")
            + todays_change
            + sections.earnings_date_section(self.data.ticker, self.data.next_earnings_info)
            + sections.recent_earnings_section(self.data.historical_earnings)
        )

import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import SECFilingData
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)


class SECFilingMoverAlert(Alert):
    alert_type = "SEC_FILING_MOVER"

    def __init__(self, data: SECFilingData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']

    def build_alert(self) -> str:
        logger.debug("Building SEC Filing Mover Alert...")
        pct_change = self.alert_data['pct_change']
        symbol = "🟢" if pct_change > 0 else "🔻"
        todays_change = f"**{self.data.ticker}** is {symbol} **{pct_change:.2f}%** and filed with the SEC today\n"
        return (
            sections.alert_header(f"SEC Filing Mover: {self.data.ticker}")
            + todays_change
            + sections.todays_sec_filings_section(self.data.recent_sec_filings)
        )

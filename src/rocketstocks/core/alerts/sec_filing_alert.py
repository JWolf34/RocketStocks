import logging
from rocketstocks.core.alerts.base import Alert

logger = logging.getLogger(__name__)


class SECFilingMoverAlert(Alert):
    def __init__(self, ticker: str, quote: dict):
        super().__init__(
            alert_type="SEC_FILING_MOVER",
            ticker=ticker,
            quote=quote,
        )

    def build_alert_header(self):
        logger.debug("Building alert header...")
        return f"## :rotating_light: SEC Filing Mover: {self.ticker}\n\n\n"

    def build_todays_change(self):
        logger.debug("Building today's change...")
        symbol = "🟢" if self.pct_change > 0 else "🔻"
        return f"**{self.ticker}** is {symbol} **{'{:.2f}'.format(self.pct_change)}%** and filed with the SEC today\n"

    def build_alert(self):
        logger.debug("Building SEC Filing Mover Alert...")
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_todays_sec_filings()
        return alert

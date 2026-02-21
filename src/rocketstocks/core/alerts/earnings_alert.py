import logging
import pandas as pd
from rocketstocks.core.alerts.base import Alert

logger = logging.getLogger(__name__)


class EarningsMoverAlert(Alert):
    def __init__(self, ticker: str, quote: dict, next_earnings_info: dict,
                 historical_earnings: pd.DataFrame):
        super().__init__(
            alert_type="EARNINGS_MOVER",
            ticker=ticker,
            quote=quote,
            next_earnings_info=next_earnings_info,
            historical_earnings=historical_earnings,
        )

    def build_alert_header(self):
        logger.debug("Building alert header...")
        return f"## :rotating_light: Earnings Mover: {self.ticker}\n\n\n"

    def build_todays_change(self):
        logger.debug("Building today's change...")
        message = super().build_todays_change()
        message += " and reports earnings today\n"
        return message

    def build_alert(self):
        logger.debug("Building Earnings Mover Alert...")
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_earnings_date()
        alert += self.build_recent_earnings()
        return alert

import logging
import pandas as pd
from rocketstocks.core.alerts.base import Alert

logger = logging.getLogger(__name__)


class VolumeMoverAlert(Alert):
    """Alert for stocks with high relative volume."""

    def __init__(self, ticker: str, rvol: float, quote: dict, daily_price_history: pd.DataFrame):
        super().__init__(
            alert_type="VOLUME_MOVER",
            ticker=ticker,
            rvol=rvol,
            quote=quote,
            daily_price_history=daily_price_history,
        )

    def build_alert_data(self):
        """Extends parent to include RVOL in alert data."""
        super().build_alert_data()
        self.alert_data['rvol'] = self.rvol

    def build_alert_header(self):
        logger.debug("Building alert header...")
        return f"## :rotating_light: Volume Mover: {self.ticker}\n\n\n"

    def build_todays_change(self):
        """Extends the parent to include RVOL data."""
        logger.debug("Building today's change...")
        message = super().build_todays_change()
        message += f" with volume up **{'{:.2f} times'.format(self.rvol)}** the 10-day average\n"
        return message

    def build_alert(self):
        logger.debug("Building Volume Mover Alert...")
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_volume_stats()
        return alert

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        """Extends parent to also check RVOL change."""
        if super().override_and_edit(prev_alert_data=prev_alert_data):
            return True
        if self.alert_data['rvol'] > (2.0 * prev_alert_data['rvol']):
            return True
        return False

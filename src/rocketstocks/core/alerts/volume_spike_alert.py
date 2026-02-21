import logging
from rocketstocks.core.alerts.base import Alert

logger = logging.getLogger(__name__)


class VolumeSpikeAlert(Alert):
    """Alert for stocks with high relative volume at a specific time of day."""

    def __init__(self, ticker: str, rvol_at_time: float, avg_vol_at_time: float,
                 quote: dict, time: str):
        super().__init__(
            alert_type="VOLUME_SPIKE",
            ticker=ticker,
            rvol_at_time=rvol_at_time,
            avg_vol_at_time=avg_vol_at_time,
            quote=quote,
            time=time,
        )

    def build_alert_data(self):
        """Extends parent to include RVOL_AT_TIME in alert data."""
        super().build_alert_data()
        self.alert_data['rvol_at_time'] = self.rvol_at_time

    def build_alert_header(self):
        logger.debug("Building alert header...")
        return f"## :rotating_light: Volume Spike: {self.ticker}\n\n\n"

    def build_todays_change(self):
        """Extends the parent to include RVOL_AT_TIME data."""
        logger.debug("Building today's change...")
        message = super().build_todays_change()
        message += f" with volume up **{self.rvol_at_time:.2f} times** the normal at this time\n"
        return message

    def build_alert(self):
        logger.debug("Building Volume Spike Alert...")
        alert = ""
        alert += self.build_alert_header()
        alert += self.build_todays_change()
        alert += self.build_volume_stats()
        return alert

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        """Extends parent to also check RVOL_AT_TIME change."""
        if super().override_and_edit(prev_alert_data=prev_alert_data):
            return True
        if self.rvol_at_time > (1.5 * prev_alert_data['rvol_at_time']):
            return True
        return False

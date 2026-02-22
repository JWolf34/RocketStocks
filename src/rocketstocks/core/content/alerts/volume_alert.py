import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import VolumeMoverData
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)


class VolumeMoverAlert(Alert):
    """Alert for stocks with high relative volume."""

    alert_type = "VOLUME_MOVER"

    def __init__(self, data: VolumeMoverData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']
        self.alert_data['rvol'] = data.rvol

    def build_alert(self) -> str:
        logger.debug("Building Volume Mover Alert...")
        pct_change = self.alert_data['pct_change']
        todays_change = (
            sections.todays_change(self.data.ticker, pct_change)
            + f" with volume up **{self.data.rvol:.2f} times** the 10-day average\n"
        )
        return (
            sections.alert_header(f"Volume Mover: {self.data.ticker}")
            + todays_change
            + sections.volume_stats_section(
                quote=self.data.quote,
                daily_price_history=self.data.daily_price_history,
                rvol=self.data.rvol,
            )
        )

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        if super().override_and_edit(prev_alert_data):
            return True
        return self.alert_data['rvol'] > (2.0 * prev_alert_data.get('rvol', 0))

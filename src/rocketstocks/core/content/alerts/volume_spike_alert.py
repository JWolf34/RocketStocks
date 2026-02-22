import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import VolumeSpikeData
from rocketstocks.core.content import sections

logger = logging.getLogger(__name__)


class VolumeSpikeAlert(Alert):
    """Alert for stocks with high relative volume at a specific time of day."""

    alert_type = "VOLUME_SPIKE"

    def __init__(self, data: VolumeSpikeData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']
        self.alert_data['rvol_at_time'] = data.rvol_at_time

    def build_alert(self) -> str:
        logger.debug("Building Volume Spike Alert...")
        pct_change = self.alert_data['pct_change']
        todays_change = (
            sections.todays_change(self.data.ticker, pct_change)
            + f" with volume up **{self.data.rvol_at_time:.2f} times** the normal at this time\n"
        )
        return (
            sections.alert_header(f"Volume Spike: {self.data.ticker}")
            + todays_change
            + sections.volume_stats_section(
                quote=self.data.quote,
                rvol_at_time=self.data.rvol_at_time,
                avg_vol_at_time=self.data.avg_vol_at_time,
                time=self.data.time,
            )
        )

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        if super().override_and_edit(prev_alert_data):
            return True
        return self.data.rvol_at_time > (1.5 * prev_alert_data.get('rvol_at_time', 0))

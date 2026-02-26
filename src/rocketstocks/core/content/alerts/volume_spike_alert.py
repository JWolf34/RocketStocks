import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import (
    COLOR_ORANGE, COLOR_RED,
    VolumeSpikeData, EmbedField, EmbedSpec,
)
from rocketstocks.core.content import sections
from rocketstocks.core.content.formatting import format_large_num

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

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug("Building Volume Spike EmbedSpec...")
        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = (self.data.ticker_info or {}).get('name', self.data.ticker)
        sign = "+" if pct_change > 0 else ""
        volume_at_time = format_large_num(self.data.rvol_at_time * self.data.avg_vol_at_time)

        description = (
            f"**{company_name}** · `{self.data.ticker}` is "
            f"{'🟢' if pct_change > 0 else '🔻'} **{sign}{pct_change:.2f}%** — **${price:.2f}** "
            f"with volume **{self.data.rvol_at_time:.2f}x** the normal at {self.data.time}"
        )

        fields = [
            EmbedField(name="Price", value=f"${price:.2f}", inline=True),
            EmbedField(name="Change", value=f"{sign}{pct_change:.2f}%", inline=True),
            EmbedField(name=f"RVOL at {self.data.time}", value=f"{self.data.rvol_at_time:.2f}x", inline=True),
            EmbedField(name=f"Volume at {self.data.time}", value=volume_at_time, inline=True),
        ]

        return EmbedSpec(
            title=f"🚨 Volume Spike: {self.data.ticker}",
            description=description,
            color=COLOR_ORANGE if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer="RocketStocks · volume-spike",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        if super().override_and_edit(prev_alert_data):
            return True
        return self.data.rvol_at_time > (1.5 * prev_alert_data.get('rvol_at_time', 0))

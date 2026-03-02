import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.alerts.earnings_alert import _stat_fields_from_trigger
from rocketstocks.core.content.models import (
    COLOR_ORANGE, COLOR_RED,
    VolumeSpikeData, EmbedField, EmbedSpec,
)
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

        tr = data.trigger_result
        if tr is not None:
            self.alert_data['zscore'] = tr.zscore
            self.alert_data['percentile'] = tr.percentile
            self.alert_data['classification'] = getattr(tr.classification, 'value', str(tr.classification))
            self.alert_data['signal_type'] = tr.signal_type
            self.alert_data['bb_position'] = tr.bb_position
            self.alert_data['confluence_count'] = tr.confluence_count
            self.alert_data['volume_zscore'] = tr.volume_zscore

    def build(self) -> EmbedSpec:
        logger.debug("Building Volume Spike embed...")
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
            EmbedField(name=f"Avg Vol at {self.data.time}", value=format_large_num(self.data.avg_vol_at_time), inline=True),
        ]

        fields += _stat_fields_from_trigger(self.data.trigger_result)

        return EmbedSpec(
            title=f"🚨 Volume Spike: {self.data.ticker}",
            description=description,
            color=COLOR_ORANGE if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer="RocketStocks · volume-spike",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )

import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.alerts.earnings_alert import _stat_fields_from_trigger
from rocketstocks.core.content.models import (
    COLOR_ORANGE, COLOR_RED,
    VolumeMoverData, EmbedField, EmbedSpec,
)
from rocketstocks.core.content.formatting import format_large_num

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
        logger.debug("Building Volume Mover embed...")
        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = (self.data.ticker_info or {}).get('name', self.data.ticker)
        sign = "+" if pct_change > 0 else ""
        volume = self.data.quote['quote']['totalVolume']

        description = (
            f"**{company_name}** · `{self.data.ticker}` is "
            f"{'🟢' if pct_change > 0 else '🔻'} **{sign}{pct_change:.2f}%** — **${price:.2f}** "
            f"with volume **{self.data.rvol:.2f}x** the 10-day average"
        )

        avg_volume = format_large_num(
            self.data.daily_price_history['volume'].tail(10).mean()
        ) if not self.data.daily_price_history.empty else "N/A"

        fields = [
            EmbedField(name="Price", value=f"${price:.2f}", inline=True),
            EmbedField(name="Change", value=f"{sign}{pct_change:.2f}%", inline=True),
            EmbedField(name="RVOL (10D)", value=f"{self.data.rvol:.2f}x", inline=True),
            EmbedField(name="Volume", value=format_large_num(volume), inline=True),
            EmbedField(name="Avg Volume (10D)", value=avg_volume, inline=True),
        ]

        fields += _stat_fields_from_trigger(self.data.trigger_result)

        return EmbedSpec(
            title=f"🚨 Volume Mover: {self.data.ticker}",
            description=description,
            color=COLOR_ORANGE if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer="RocketStocks · volume-mover",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )

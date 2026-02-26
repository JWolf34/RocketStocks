import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import (
    COLOR_ORANGE, COLOR_RED,
    VolumeMoverData, EmbedField, EmbedSpec,
)
from rocketstocks.core.content import sections
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

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug("Building Volume Mover EmbedSpec...")
        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = (self.data.ticker_info or {}).get('name', self.data.ticker)
        sign = "+" if pct_change > 0 else ""
        volume = self.data.quote['quote']['totalVolume']
        avg_volume_10d = format_large_num(self.data.daily_price_history['volume'].tail(10).mean()) if self.data.daily_price_history is not None and not self.data.daily_price_history.empty else "N/A"

        description = (
            f"**{company_name}** · `{self.data.ticker}` is "
            f"{'🟢' if pct_change > 0 else '🔻'} **{sign}{pct_change:.2f}%** — **${price:.2f}** "
            f"with volume **{self.data.rvol:.2f}x** the 10-day average"
        )

        fields = [
            EmbedField(name="Price", value=f"${price:.2f}", inline=True),
            EmbedField(name="Change", value=f"{sign}{pct_change:.2f}%", inline=True),
            EmbedField(name="RVOL (10D)", value=f"{self.data.rvol:.2f}x", inline=True),
            EmbedField(name="Volume", value=format_large_num(volume), inline=True),
            EmbedField(name="Avg Volume (10D)", value=avg_volume_10d, inline=True),
        ]

        return EmbedSpec(
            title=f"🚨 Volume Mover: {self.data.ticker}",
            description=description,
            color=COLOR_ORANGE if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer="RocketStocks · volume-mover",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )

    def override_and_edit(self, prev_alert_data: dict) -> bool:
        if super().override_and_edit(prev_alert_data):
            return True
        return self.alert_data['rvol'] > (2.0 * prev_alert_data.get('rvol', 0))

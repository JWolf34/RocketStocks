import logging
import math

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.alerts.earnings_alert import _stat_fields_from_trigger
from rocketstocks.core.content.models import (
    COLOR_CYAN, COLOR_RED,
    EmbedField, EmbedSpec,
    MarketMoverData,
)
from rocketstocks.core.utils.market import market_utils

logger = logging.getLogger(__name__)

_CONFIRMATION_LABELS = {
    'sustained': 'Sustained Move',
    'price_accelerating': 'Price Accelerating',
    'volume_accelerating': 'Volume Accelerating',
    'volume_extreme': 'Extreme Volume',
}

_DOMINANT_LABELS = {
    'volume': 'Volume-driven',
    'price': 'Price-driven',
    'mixed': 'Mixed signals',
}


class MarketMoverAlert(Alert):
    alert_type = "MARKET_MOVER"

    @property
    def role_key(self) -> str | None:
        reason = self.alert_data.get('confirmation_reason', '')
        return f"market_mover_{reason}" if reason else None

    def __init__(self, data: MarketMoverData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker

        pct_change = data.quote['quote'].get('netPercentChange', 0.0)
        self.alert_data['pct_change'] = pct_change
        self.alert_data['confirmation_reason'] = data.confirmation_reason
        self.alert_data['signal_observations'] = data.signal_observations
        self.alert_data['signal_detected_at'] = (
            str(data.signal_detected_at) if data.signal_detected_at else None
        )
        self.alert_data['rvol'] = data.rvol

        cr = data.composite_result
        if cr is not None:
            self.alert_data['composite_score'] = cr.composite_score
            self.alert_data['dominant_signal'] = cr.dominant_signal
            self.alert_data['volume_component'] = cr.volume_component
            self.alert_data['price_component'] = cr.price_component
            self.alert_data['cross_signal_component'] = cr.cross_signal_component

            tr = cr.trigger_result
            if tr is not None:
                self.alert_data['zscore'] = tr.zscore
                self.alert_data['percentile'] = tr.percentile
                self.alert_data['classification'] = getattr(
                    tr.classification, 'value', str(tr.classification)
                )
                self.alert_data['signal_type'] = tr.signal_type
                self.alert_data['bb_position'] = tr.bb_position
                self.alert_data['confluence_count'] = tr.confluence_count
                self.alert_data['volume_zscore'] = tr.volume_zscore

        if data.price_velocity is not None:
            self.alert_data['price_velocity'] = data.price_velocity
        if data.price_acceleration is not None:
            self.alert_data['price_acceleration'] = data.price_acceleration
        if data.volume_velocity is not None:
            self.alert_data['volume_velocity'] = data.volume_velocity
        if data.volume_acceleration is not None:
            self.alert_data['volume_acceleration'] = data.volume_acceleration

    def build(self) -> EmbedSpec:
        logger.debug("Building Market Mover embed...")

        pct_change = self.alert_data['pct_change']
        price = market_utils().get_current_price(self.data.quote)
        company_name = (self.data.ticker_info or {}).get('name', self.data.ticker)
        sign = "+" if pct_change > 0 else ""

        confirmation_label = _CONFIRMATION_LABELS.get(
            self.data.confirmation_reason, self.data.confirmation_reason
        )
        dominant_label = _DOMINANT_LABELS.get(
            self.alert_data.get('dominant_signal', ''), 'Unknown'
        )

        description = (
            f"**{company_name}** · `{self.data.ticker}` — {dominant_label}, "
            f"confirmed after {self.data.signal_observations} observation(s) "
            f"({confirmation_label})\n"
            f"{'🟢' if pct_change > 0 else '🔻'} **{sign}{pct_change:.2f}%** — "
            f"**${price:.2f}**"
        )

        color = COLOR_CYAN if pct_change >= 0 else COLOR_RED

        composite_score = self.alert_data.get('composite_score', 0.0)
        fields = [
            EmbedField(name="Price", value=f"${price:.2f}", inline=True),
            EmbedField(name="Change", value=f"{sign}{pct_change:.2f}%", inline=True),
            EmbedField(name="Composite", value=f"{composite_score:.2f}", inline=True),
        ]

        if self.data.rvol is not None and not math.isnan(self.data.rvol):
            fields.append(EmbedField(name="RVOL", value=f"{self.data.rvol:.2f}x", inline=True))

        fields.append(EmbedField(
            name="Confirmation", value=confirmation_label, inline=True
        ))
        fields.append(EmbedField(
            name="Driver", value=dominant_label, inline=True
        ))

        fields += _stat_fields_from_trigger(
            self.data.composite_result.trigger_result
            if self.data.composite_result else None
        )

        # Momentum fields
        for label, key in [
            ("Price Velocity", "price_velocity"),
            ("Price Acceleration", "price_acceleration"),
            ("Vol Velocity", "volume_velocity"),
            ("Vol Acceleration", "volume_acceleration"),
        ]:
            val = self.alert_data.get(key)
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                fields.append(EmbedField(name=label, value=f"{val:+.3f}", inline=True))

        return EmbedSpec(
            title=f"🚀 Market Mover: {self.data.ticker}",
            description=description,
            color=color,
            fields=fields,
            footer="RocketStocks · market-mover",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )

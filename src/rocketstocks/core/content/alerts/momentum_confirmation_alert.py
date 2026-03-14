import logging
import math

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.alerts.earnings_alert import _stat_fields_from_trigger
from rocketstocks.core.content.models import (
    COLOR_GOLD,
    EmbedField, EmbedSpec,
)
from rocketstocks.core.utils.market import market_utils

logger = logging.getLogger(__name__)

_SURGE_TYPE_LABELS = {
    'mention_surge': 'Mention Surge',
    'rank_jump': 'Rank Jump',
    'new_entrant': 'New Entrant',
    'velocity_spike': 'Velocity Spike',
}


class MomentumConfirmationAlert(Alert):
    alert_type = "MOMENTUM_CONFIRMATION"
    role_key = "momentum_confirmed"

    def __init__(self, data):  # data: MomentumConfirmationData
        super().__init__()
        self.data = data
        self.ticker = data.ticker

        pct_change = data.quote['quote'].get('netPercentChange', 0.0)
        self.alert_data['pct_change'] = pct_change
        self.alert_data['price_change_since_flag'] = data.price_change_since_flag
        self.alert_data['surge_flagged_at'] = (
            str(data.surge_flagged_at) if data.surge_flagged_at else None
        )
        self.alert_data['surge_types'] = data.surge_types

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
        logger.debug("Building Momentum Confirmation embed...")

        pct_change = self.alert_data['pct_change']
        price = market_utils().get_current_price(self.data.quote)
        company_name = (self.data.ticker_info or {}).get('name', self.data.ticker)
        sign = "+" if pct_change > 0 else ""

        # Price change since surge was flagged
        price_since_flag = self.data.price_change_since_flag
        since_flag_str = ""
        if (price_since_flag is not None
                and not (isinstance(price_since_flag, float) and math.isnan(price_since_flag))):
            flag_sign = "+" if price_since_flag > 0 else ""
            since_flag_str = f" ({flag_sign}{price_since_flag:.2f}% since surge flagged)"

        surge_types_str = ", ".join(
            _SURGE_TYPE_LABELS.get(st, st) for st in (self.data.surge_types or [])
        ) or "Popularity Surge"

        description = (
            f"**{company_name}** · `{self.data.ticker}` — price/volume confirming earlier "
            f"popularity surge ({surge_types_str})\n"
            f"{'🟢' if pct_change > 0 else '🔻'} **{sign}{pct_change:.2f}%** — "
            f"**${price:.2f}**{since_flag_str}"
        )

        fields = [
            EmbedField(name="Price", value=f"${price:.2f}", inline=True),
            EmbedField(name="Change", value=f"{sign}{pct_change:.2f}%", inline=True),
        ]

        if (price_since_flag is not None
                and not (isinstance(price_since_flag, float) and math.isnan(price_since_flag))):
            flag_sign = "+" if price_since_flag > 0 else ""
            fields.append(EmbedField(
                name="Change Since Flag",
                value=f"{flag_sign}{price_since_flag:.2f}%",
                inline=True,
            ))

        fields += _stat_fields_from_trigger(self.data.trigger_result)

        fields.append(EmbedField(
            name="Original Surge Types",
            value=surge_types_str,
            inline=False,
        ))

        return EmbedSpec(
            title=f"⚡ Momentum Confirmed: {self.data.ticker}",
            description=description,
            color=COLOR_GOLD,
            fields=fields,
            footer="RocketStocks · momentum-confirmation",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )

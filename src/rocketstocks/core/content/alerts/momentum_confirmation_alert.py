import logging
import math

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.alerts.earnings_alert import _stat_fields_from_trigger
from rocketstocks.core.content.models import (
    COLOR_GOLD,
    EmbedField, EmbedSpec,
)
from rocketstocks.core.utils.market import MarketUtils
from rocketstocks.core.utils.formatting import (
    change_emoji, finviz_url, format_signed_pct, get_company_name, is_valid_number,
)

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

        self.populate_trigger_data(self.alert_data, data.trigger_result)

    def build(self) -> EmbedSpec:
        logger.debug("Building Momentum Confirmation embed...")

        pct_change = self.alert_data['pct_change']
        price = MarketUtils().get_current_price(self.data.quote)
        company_name = get_company_name(self.data.ticker_info, self.data.ticker)

        # Price change since surge was flagged
        price_since_flag = self.data.price_change_since_flag
        since_flag_str = ""
        if is_valid_number(price_since_flag):
            since_flag_str = f" ({format_signed_pct(price_since_flag)} since surge flagged)"

        surge_types_str = ", ".join(
            _SURGE_TYPE_LABELS.get(st, st) for st in (self.data.surge_types or [])
        ) or "Popularity Surge"

        description = (
            f"**{company_name}** · `{self.data.ticker}` — price/volume confirming earlier "
            f"popularity surge ({surge_types_str})\n"
            f"{change_emoji(pct_change)} **{format_signed_pct(pct_change)}** — "
            f"**${price:.2f}**{since_flag_str}"
        )

        fields = self.price_change_fields(price, pct_change)

        if is_valid_number(price_since_flag):
            fields.append(EmbedField(
                name="Change Since Flag",
                value=format_signed_pct(price_since_flag),
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
            url=finviz_url(self.data.ticker),
        )

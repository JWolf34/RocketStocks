import datetime
import logging
import math

from rocketstocks.core.content.alerts.base import Alert
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


def _format_duration(seconds: float) -> str:
    """Format a duration in seconds as a human-readable string."""
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes} min"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m" if mins else f"{hours}h"


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

        # Store ConfirmationResult fields if provided
        trigger = data.trigger_result
        if trigger is not None and hasattr(trigger, 'zscore_since_flag'):
            self.alert_data['zscore_since_flag'] = trigger.zscore_since_flag
            self.alert_data['is_sustained'] = trigger.is_sustained
        elif trigger is not None and hasattr(trigger, 'zscore'):
            # Legacy AlertTriggerResult fallback
            self.alert_data['zscore'] = trigger.zscore
            self.alert_data['percentile'] = trigger.percentile

    def build(self) -> EmbedSpec:
        logger.debug("Building Momentum Confirmation embed...")

        pct_change = self.alert_data['pct_change']
        price = MarketUtils().get_current_price(self.data.quote)
        company_name = get_company_name(self.data.ticker_info, self.data.ticker)

        # Time since surge was flagged
        surge_flagged_at = self.data.surge_flagged_at
        elapsed_str = ""
        if surge_flagged_at is not None:
            try:
                now_utc = datetime.datetime.utcnow()
                flagged_dt = (
                    surge_flagged_at if isinstance(surge_flagged_at, datetime.datetime)
                    else datetime.datetime.fromisoformat(str(surge_flagged_at))
                )
                elapsed = (now_utc - flagged_dt).total_seconds()
                elapsed_str = f" · Surge flagged {_format_duration(elapsed)} ago"
            except Exception:
                pass

        # Price change since surge was flagged
        price_since_flag = self.data.price_change_since_flag
        since_flag_str = ""
        if is_valid_number(price_since_flag):
            since_flag_str = f" ({format_signed_pct(price_since_flag)} since surge flagged)"

        surge_types_str = ", ".join(
            _SURGE_TYPE_LABELS.get(st, st) for st in (self.data.surge_types or [])
        ) or "Popularity Surge"

        description = (
            f"**{company_name}** · `{self.data.ticker}` — price confirming earlier "
            f"popularity surge ({surge_types_str}){elapsed_str}\n"
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

        # ConfirmationResult stats
        trigger = self.data.trigger_result
        if trigger is not None and hasattr(trigger, 'zscore_since_flag'):
            zscore_sf = trigger.zscore_since_flag
            if zscore_sf is not None and not (isinstance(zscore_sf, float) and math.isnan(zscore_sf)):
                fields.append(EmbedField(
                    name="Z-Score Since Flag",
                    value=f"{zscore_sf:.2f}σ",
                    inline=True,
                ))
            if trigger.is_sustained is not None:
                fields.append(EmbedField(
                    name="Sustained",
                    value="✅ Yes" if trigger.is_sustained else "⚠️ Mixed",
                    inline=True,
                ))
        elif trigger is not None and hasattr(trigger, 'zscore'):
            # Legacy AlertTriggerResult display
            from rocketstocks.core.content.alerts.earnings_alert import _stat_fields_from_trigger
            fields += _stat_fields_from_trigger(trigger)

        fields.append(EmbedField(
            name="Original Surge Types",
            value=surge_types_str,
            inline=False,
        ))

        # Confidence stat
        confidence_pct = getattr(self.data, 'confidence_pct', None)
        if confidence_pct is not None:
            fields.append(EmbedField(
                name="Signal Confidence (30d)",
                value=f"**{confidence_pct:.1f}%** of popularity surges confirmed",
                inline=True,
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

"""BreakoutAlert — follow-up alert when price confirms a Volume Accumulation signal."""
import datetime
import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED,
    EmbedField, EmbedSpec,
)
from rocketstocks.core.utils.market import MarketUtils
from rocketstocks.core.utils.formatting import (
    finviz_url, format_signed_pct, get_company_name, is_valid_number,
)

logger = logging.getLogger(__name__)


class BreakoutAlert(Alert):
    alert_type = "BREAKOUT"
    role_key = "breakout"

    def __init__(self, data):  # data: BreakoutAlertData
        super().__init__()
        self.data = data
        self.ticker = data.ticker

        pct_change = data.quote['quote'].get('netPercentChange', 0.0)
        self.alert_data['pct_change'] = pct_change
        self.alert_data['price_change_since_flag'] = data.price_change_since_flag
        self.alert_data['signal_detected_at'] = (
            str(data.signal_detected_at) if data.signal_detected_at else None
        )
        self.alert_data['signal_alert_message_id'] = data.signal_alert_message_id
        self.alert_data['price_at_flag'] = data.price_at_flag
        self.alert_data['vol_z_at_signal'] = data.vol_z_at_signal
        self.alert_data['signal_strength'] = data.signal_strength
        self.alert_data['rvol'] = data.rvol

        if data.trigger_result is not None:
            tr = data.trigger_result
            self.alert_data['zscore_since_flag'] = tr.zscore_since_flag
            self.alert_data['is_sustained'] = tr.is_sustained

    def build(self) -> EmbedSpec:
        logger.debug("Building Breakout embed...")

        company_name = get_company_name(self.data.ticker_info, self.data.ticker)
        price = MarketUtils().get_current_price(self.data.quote)
        pct_change = self.alert_data['pct_change']
        price_change_since_flag = self.data.price_change_since_flag

        duration_str = _format_duration(self.data.signal_detected_at)
        flag_price_str = (
            f"${self.data.price_at_flag:.2f}"
            if is_valid_number(self.data.price_at_flag) else "unknown price"
        )
        since_flag_str = (
            f" — {format_signed_pct(price_change_since_flag)} since flagged"
            if is_valid_number(price_change_since_flag) else ""
        )

        description = (
            f"**{company_name}** · `{self.data.ticker}` — volume accumulation flagged "
            f"**{duration_str} ago** at {flag_price_str}{since_flag_str}\n"
            f"Now at **${price:.2f}** ({format_signed_pct(pct_change)})"
        )

        color = COLOR_GREEN if (price_change_since_flag or 0) >= 0 else COLOR_RED

        fields = [
            EmbedField(name="Price Now", value=f"${price:.2f}", inline=True),
        ]

        if is_valid_number(price_change_since_flag):
            fields.append(EmbedField(
                name="Change Since Flag",
                value=format_signed_pct(price_change_since_flag),
                inline=True,
            ))

        fields.append(EmbedField(
            name="Time Since Signal",
            value=duration_str,
            inline=True,
        ))

        if is_valid_number(self.data.rvol):
            fields.append(EmbedField(name="RVOL", value=f"{self.data.rvol:.2f}x", inline=True))

        if is_valid_number(self.data.vol_z_at_signal):
            fields.append(EmbedField(
                name="Vol Z-Score at Signal",
                value=f"{self.data.vol_z_at_signal:+.2f}σ",
                inline=True,
            ))

        if is_valid_number(self.data.price_zscore):
            fields.append(EmbedField(
                name="Price Z-Score",
                value=f"{self.data.price_zscore:+.2f}σ",
                inline=True,
            ))

        if is_valid_number(self.data.divergence_score):
            fields.append(EmbedField(
                name="Divergence Score",
                value=f"{self.data.divergence_score:.2f}",
                inline=True,
            ))

        fields.append(EmbedField(
            name="Signal Strength",
            value=_signal_strength_label(self.data.signal_strength),
            inline=True,
        ))

        if is_valid_number(self.data.confidence_pct):
            fields.append(EmbedField(
                name="Signal Confidence (30d)",
                value=f"**{self.data.confidence_pct:.1f}%** of volume signals confirmed",
                inline=True,
            ))

        if self.data.options_flow:
            from rocketstocks.core.content.alerts.volume_accumulation_alert import options_flow_fields
            fields += options_flow_fields(self.data.options_flow)

        return EmbedSpec(
            title=f"🚀 Breakout: {self.data.ticker}",
            description=description,
            color=color,
            fields=fields,
            footer="RocketStocks · breakout",
            timestamp=True,
            url=finviz_url(self.data.ticker),
        )


def _format_duration(detected_at) -> str:
    """Format time elapsed since detection as a human-readable string."""
    if detected_at is None:
        return "unknown time"
    try:
        elapsed = datetime.datetime.utcnow() - detected_at
        total_seconds = int(elapsed.total_seconds())
        if total_seconds < 60:
            return f"{total_seconds}s"
        elif total_seconds < 3600:
            minutes = total_seconds // 60
            return f"{minutes}m"
        else:
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    except Exception:
        return "unknown time"


def _signal_strength_label(signal_strength: str) -> str:
    labels = {
        'volume_only': 'Volume Only',
        'volume_plus_options': '🔥 Volume + Options',
    }
    return labels.get(signal_strength, signal_strength)

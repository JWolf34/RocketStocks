"""VolumeAccumulationAlert — leading indicator alert for unusual volume without price movement."""
import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import (
    COLOR_BLUE,
    EmbedField, EmbedSpec,
)
from rocketstocks.core.utils.market import MarketUtils
from rocketstocks.core.utils.formatting import (
    finviz_url, format_signed_pct, get_company_name, is_valid_number,
)

logger = logging.getLogger(__name__)


class VolumeAccumulationAlert(Alert):
    alert_type = "VOLUME_ACCUMULATION"
    role_key = "volume_accumulation"

    def __init__(self, data):  # data: VolumeAccumulationAlertData
        super().__init__()
        self.data = data
        self.ticker = data.ticker

        pct_change = data.quote['quote'].get('netPercentChange', 0.0)
        self.alert_data['pct_change'] = pct_change
        self.alert_data['vol_zscore'] = data.vol_zscore
        self.alert_data['price_zscore'] = data.price_zscore
        self.alert_data['rvol'] = data.rvol
        self.alert_data['divergence_score'] = data.divergence_score
        self.alert_data['signal_strength'] = data.signal_strength

    def build(self) -> EmbedSpec:
        logger.debug("Building Volume Accumulation embed...")

        company_name = get_company_name(self.data.ticker_info, self.data.ticker)
        price = MarketUtils().get_current_price(self.data.quote)
        pct_change = self.alert_data['pct_change']

        options_flow = self.data.options_flow

        narrative = (
            f"**{company_name}** · `{self.data.ticker}` is showing unusual volume "
            f"with no significant price movement — RVOL **{self.data.rvol:.1f}x** average"
            f" ({format_signed_pct(pct_change)} · **${price:.2f}**)"
        )
        if options_flow and options_flow.has_unusual_activity:
            dominant_type = _dominant_contract_type(options_flow.unusual_contracts)
            narrative += f"\n⚠️ Unusual **{dominant_type}** activity also detected"

        fields = [
            EmbedField(name="RVOL", value=f"{self.data.rvol:.2f}x", inline=True),
            EmbedField(name="Volume Z-Score", value=f"{self.data.vol_zscore:+.2f}σ", inline=True),
            EmbedField(name="Price Z-Score", value=f"{self.data.price_zscore:+.2f}σ", inline=True),
        ]

        fields += self.price_change_fields(price, pct_change)

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

        if options_flow:
            fields += options_flow_fields(options_flow)

        return EmbedSpec(
            title=f"📊 Volume Accumulation: {self.data.ticker}",
            description=narrative,
            color=COLOR_BLUE,
            fields=fields,
            footer="RocketStocks · volume-accumulation",
            timestamp=True,
            url=finviz_url(self.data.ticker),
        )


def _dominant_contract_type(unusual_contracts: list) -> str:
    """Return 'call' or 'put' based on which type dominates unusual activity."""
    if not unusual_contracts:
        return 'options'
    calls = sum(1 for c in unusual_contracts if c.get('type') == 'call')
    puts = sum(1 for c in unusual_contracts if c.get('type') == 'put')
    return 'call' if calls >= puts else 'put'


def _signal_strength_label(signal_strength: str) -> str:
    labels = {
        'volume_only': 'Volume Only',
        'volume_plus_options': '🔥 Volume + Options',
    }
    return labels.get(signal_strength, signal_strength)


def options_flow_fields(options_flow) -> list:
    """Build EmbedField list for the Options Flow section. Exported for reuse in BreakoutAlert."""
    fields = []

    if options_flow.unusual_contracts:
        contracts_str = ", ".join(
            f"${c['strike']:.0f} {c['type'].upper()} ({c['ratio']:.1f}x OI)"
            for c in options_flow.unusual_contracts[:3]
        )
        fields.append(EmbedField(
            name="Unusual Options",
            value=contracts_str,
            inline=False,
        ))

    if options_flow.put_call_ratio is not None:
        fields.append(EmbedField(
            name="Put/Call Ratio",
            value=f"{options_flow.put_call_ratio:.2f}",
            inline=True,
        ))

    if options_flow.iv_skew_direction:
        skew_labels = {
            'put_skew': '📉 Put Skew',
            'call_skew': '📈 Call Skew',
            'neutral': 'Neutral',
        }
        fields.append(EmbedField(
            name="IV Skew",
            value=skew_labels.get(options_flow.iv_skew_direction, options_flow.iv_skew_direction),
            inline=True,
        ))

    if is_valid_number(options_flow.max_pain):
        fields.append(EmbedField(
            name="Max Pain",
            value=f"${options_flow.max_pain:.2f}",
            inline=True,
        ))

    if is_valid_number(options_flow.flow_score):
        fields.append(EmbedField(
            name="Flow Score",
            value=f"{options_flow.flow_score:.1f}/10",
            inline=True,
        ))

    return fields

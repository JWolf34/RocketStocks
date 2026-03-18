import logging
import math

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED,
    EarningsMoverData, EmbedField, EmbedSpec,
)
from rocketstocks.core.content import sections_card
from rocketstocks.core.utils.formatting import (
    change_emoji, earnings_time_label, finviz_url, format_signed_pct, get_company_name,
)

logger = logging.getLogger(__name__)


def _stat_fields_from_trigger(trigger_result) -> list[EmbedField]:
    """Build embed fields from an AlertTriggerResult (or return empty list)."""
    if trigger_result is None:
        return []
    fields = []
    zscore = trigger_result.zscore
    percentile = trigger_result.percentile
    classification = trigger_result.classification

    if zscore is not None and not (isinstance(zscore, float) and math.isnan(zscore)):
        fields.append(EmbedField(name="Z-Score", value=f"{zscore:.2f}σ", inline=True))
    if percentile is not None and not (isinstance(percentile, float) and math.isnan(percentile)):
        fields.append(EmbedField(name="Move Percentile", value=f"{percentile:.0f}th", inline=True))
    if classification is not None:
        fields.append(EmbedField(name="Class", value=str(classification.value).replace('_', ' ').title(), inline=True))

    # Blue chip extras
    if str(getattr(classification, 'value', '')).lower() == 'blue_chip':
        if trigger_result.bb_position:
            bb_label = {
                'above_upper': 'Above Upper Band',
                'below_lower': 'Below Lower Band',
                'within': 'Within Bands',
            }.get(trigger_result.bb_position, trigger_result.bb_position)
            fields.append(EmbedField(name="BB Position", value=bb_label, inline=True))
        if trigger_result.confluence_count is not None and trigger_result.confluence_total is not None:
            fields.append(EmbedField(
                name="Confluence",
                value=f"{trigger_result.confluence_count}/{trigger_result.confluence_total}",
                inline=True,
            ))
        if trigger_result.signal_type:
            fields.append(EmbedField(
                name="Signal",
                value=trigger_result.signal_type.replace('_', ' ').title(),
                inline=True,
            ))
    return fields


class EarningsMoverAlert(Alert):
    alert_type = "EARNINGS_MOVER"
    role_key = "earnings_mover"

    def __init__(self, data: EarningsMoverData):
        super().__init__()
        self.data = data
        self.ticker = data.ticker
        self.alert_data['pct_change'] = data.quote['quote']['netPercentChange']

        self.populate_trigger_data(self.alert_data, data.trigger_result)

    def build(self) -> EmbedSpec:
        logger.debug("Building Earnings Mover embed...")
        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = get_company_name(self.data.ticker_info, self.data.ticker)

        next_info = self.data.next_earnings_info or {}
        eps_forecast = next_info.get('eps_forecast', 'N/A') or 'N/A'
        time_label = earnings_time_label(next_info.get('time', ''))

        description = (
            f"**{company_name}** · `{self.data.ticker}` is reporting earnings today and is "
            f"{change_emoji(pct_change)} **{format_signed_pct(pct_change)}** — **${price:.2f}**"
        )

        fields = self.price_change_fields(price, pct_change) + [
            EmbedField(name="EPS Forecast", value=str(eps_forecast), inline=True),
            EmbedField(name="Time", value=time_label, inline=True),
        ]

        fields += _stat_fields_from_trigger(self.data.trigger_result)

        if not self.data.historical_earnings.empty:
            fields.append(EmbedField(
                name="Recent Earnings",
                value=sections_card.recent_earnings_card(self.data.historical_earnings, show_header=False),
                inline=False,
            ))

        return EmbedSpec(
            title=f"🚨 Earnings Mover: {self.data.ticker}",
            description=description,
            color=COLOR_GREEN if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer="RocketStocks · earnings-mover",
            timestamp=True,
            url=finviz_url(self.data.ticker),
        )

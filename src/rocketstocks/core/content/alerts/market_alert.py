import logging

from rocketstocks.core.content.alerts.base import Alert
from rocketstocks.core.content.alerts.earnings_alert import _stat_fields_from_trigger
from rocketstocks.core.content.models import (
    COLOR_CYAN, COLOR_RED,
    EmbedField, EmbedSpec,
)
from rocketstocks.core.utils.formatting import (
    change_emoji, finviz_url, format_signed_pct, get_company_name, is_valid_number,
)

logger = logging.getLogger(__name__)


class MarketAlert(Alert):
    alert_type = "MARKET_ALERT"
    role_key = None

    def __init__(self, data):  # data: MarketAlertData
        super().__init__()
        self.data = data
        self.ticker = data.ticker

        pct_change = data.quote['quote'].get('netPercentChange', 0.0)
        self.alert_data['pct_change'] = pct_change

        cr = data.composite_result
        self.alert_data['composite_score'] = cr.composite_score
        self.alert_data['dominant_signal'] = cr.dominant_signal
        self.alert_data['volume_component'] = cr.volume_component
        self.alert_data['price_component'] = cr.price_component
        self.alert_data['cross_signal_component'] = cr.cross_signal_component
        self.alert_data['classification_component'] = cr.classification_component
        self.alert_data['rvol'] = data.rvol

        self.populate_trigger_data(self.alert_data, cr.trigger_result)

    def build(self) -> EmbedSpec:
        logger.debug("Building Market Alert embed...")

        pct_change = self.alert_data['pct_change']
        price = self.data.quote['regular']['regularMarketLastPrice']
        company_name = get_company_name(self.data.ticker_info, self.data.ticker)

        cr = self.data.composite_result
        dominant = cr.dominant_signal

        # Adaptive narrative based on dominant signal
        if dominant == 'volume':
            narrative = f"showing unusual **volume activity** (score: {cr.composite_score:.2f})"
        elif dominant == 'price':
            narrative = f"making a significant **price move** (score: {cr.composite_score:.2f})"
        else:
            narrative = f"showing unusual **mixed activity** (score: {cr.composite_score:.2f})"

        description = (
            f"**{company_name}** · `{self.data.ticker}` is {narrative}\n"
            f"{change_emoji(pct_change)} **{format_signed_pct(pct_change)}** — **${price:.2f}**"
        )

        fields = self.price_change_fields(price, pct_change) + [
            EmbedField(name="Composite Score", value=f"{cr.composite_score:.2f}", inline=True),
        ]

        # Score breakdown
        fields.append(EmbedField(
            name="Score Breakdown",
            value=(
                f"Vol: {cr.volume_component:.2f} · "
                f"Price: {cr.price_component:.2f} · "
                f"Cross: {cr.cross_signal_component:.2f} · "
                f"Class: {cr.classification_component:.2f}"
            ),
            inline=False,
        ))

        # RVOL if available
        rvol = self.data.rvol
        if is_valid_number(rvol):
            fields.append(EmbedField(name="RVOL", value=f"{rvol:.2f}x", inline=True))

        fields += _stat_fields_from_trigger(cr.trigger_result)

        return EmbedSpec(
            title=f"🚨 Market Alert: {self.data.ticker}",
            description=description,
            color=COLOR_CYAN if pct_change > 0 else COLOR_RED,
            fields=fields,
            footer=f"RocketStocks · market-alert · {dominant}",
            timestamp=True,
            url=finviz_url(self.data.ticker),
        )

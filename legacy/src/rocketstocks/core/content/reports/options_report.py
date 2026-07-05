import logging

from rocketstocks.core.content.models import (
    COLOR_GOLD,
    EmbedField,
    EmbedSpec,
    OptionsReportData,
)
from rocketstocks.core.content.sections_card import (
    active_strikes_card,
    greeks_summary_card,
    iv_analysis_card,
    iv_skew_card,
    max_pain_card,
    put_call_card,
    ticker_info_card,
    todays_change_card,
    unusual_options_card,
)
from rocketstocks.core.utils.formatting import finviz_url

logger = logging.getLogger(__name__)


class OptionsReport:
    """Full options chain analysis report for a single ticker."""

    def __init__(self, data: OptionsReportData):
        self.data = data
        self.ticker = data.ticker

    def build(self) -> EmbedSpec:
        logger.debug("Building Options Report embed...")
        d = self.data

        title = f"⚙️ {d.ticker} Options Analysis"

        # Description: ticker info header + today's change
        try:
            header = ticker_info_card(d.ticker_info, d.quote)
            header += '\n' + todays_change_card(d.quote)
        except (KeyError, TypeError):
            header = f"**{d.ticker}**"
        description = header
        if len(description) > 4096:
            description = description[:4093] + '...'

        # Current price from options chain or quote
        try:
            current_price = float(
                d.options_chain.get('underlyingPrice')
                or d.quote['regular']['regularMarketLastPrice']
            )
        except (KeyError, TypeError, ValueError):
            current_price = None

        fields = [
            EmbedField(
                name="Implied Volatility",
                value=iv_analysis_card(d.options_chain, d.daily_price_history, d.iv_history),
            ),
            EmbedField(
                name="Put / Call",
                value=put_call_card(d.options_chain),
            ),
            EmbedField(
                name="Max Pain",
                value=max_pain_card(d.options_chain, current_price),
            ),
            EmbedField(
                name="IV Skew",
                value=iv_skew_card(d.options_chain, current_price),
            ),
            EmbedField(
                name="Unusual Activity",
                value=unusual_options_card(d.options_chain),
            ),
            EmbedField(
                name="Most Active Strikes",
                value=active_strikes_card(d.options_chain, current_price),
            ),
            EmbedField(
                name="ATM Greeks",
                value=greeks_summary_card(d.options_chain, current_price),
            ),
        ]

        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_GOLD,
            fields=fields,
            footer="RocketStocks · options",
            timestamp=True,
            url=finviz_url(d.ticker),
        )

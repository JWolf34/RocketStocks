import logging

from rocketstocks.core.content.models import (
    COLOR_TEAL,
    EmbedField,
    EmbedSpec,
    TechnicalReportData,
)
from rocketstocks.core.content.sections_card import (
    classification_card,
    key_levels_card,
    momentum_detail_card,
    signal_confluence_card,
    ticker_info_card,
    todays_change_card,
    trend_analysis_card,
    volatility_analysis_card,
    volume_analysis_card,
)
from rocketstocks.core.utils.formatting import finviz_url

logger = logging.getLogger(__name__)


class TechnicalReport:
    """Deep-dive technical analysis report for a single ticker."""

    def __init__(self, data: TechnicalReportData):
        self.data = data
        self.ticker = data.ticker

    def build(self) -> EmbedSpec:
        logger.debug("Building Technical Report embed...")
        d = self.data

        title = f"📈 {d.ticker} Technical Analysis"

        # Description: ticker info + today's change + classification
        try:
            header = ticker_info_card(d.ticker_info, d.quote)
            header += '\n' + todays_change_card(d.quote)
        except (KeyError, TypeError):
            header = f"**{d.ticker}**"

        classification = d.stats.get('classification') if d.stats else None
        volatility_20d = d.stats.get('volatility_20d') if d.stats else None
        class_str = classification_card(classification, volatility_20d)
        description = header + ('\n\n' + class_str.strip('\n') if class_str else '')
        if len(description) > 4096:
            description = description[:4093] + '...'

        # Current price for distance calculations
        try:
            current_price = float(d.quote['regular']['regularMarketLastPrice'])
        except (KeyError, TypeError):
            current_price = None

        # Current volume from quote
        try:
            current_volume = float(d.quote['quote']['totalVolume'])
        except (KeyError, TypeError):
            current_volume = None

        fields = [
            EmbedField(
                name="Trend Analysis",
                value=trend_analysis_card(d.daily_price_history, current_price),
            ),
            EmbedField(
                name="Momentum",
                value=momentum_detail_card(d.daily_price_history),
            ),
            EmbedField(
                name="Volatility",
                value=volatility_analysis_card(d.daily_price_history, current_price),
            ),
            EmbedField(
                name="Volume",
                value=volume_analysis_card(d.daily_price_history, current_volume),
            ),
            EmbedField(
                name="Key Levels",
                value=key_levels_card(d.daily_price_history, current_price),
            ),
            EmbedField(
                name="Signal Confluence",
                value=signal_confluence_card(d.daily_price_history, current_price),
            ),
        ]

        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_TEAL,
            fields=fields,
            footer="RocketStocks · technical",
            timestamp=True,
            url=finviz_url(d.ticker),
        )

import logging

from rocketstocks.core.content.models import (
    COLOR_GOLD,
    EmbedSpec, EarningsSpotlightData,
)
from rocketstocks.core.content.sections_card import (
    ohlcv_card, recent_earnings_card,
    performance_card, fundamentals_card, technical_signals_card,
    upcoming_earnings_card, earnings_date_section,
    ticker_info_description, todays_change_description,
)

logger = logging.getLogger(__name__)


class EarningsSpotlightReport:
    """Standalone earnings spotlight report."""

    def __init__(self, data: EarningsSpotlightData):
        self.data = data
        self.ticker = data.ticker

    def build(self) -> EmbedSpec:
        logger.debug("Building Earnings Spotlight embed...")
        color = COLOR_GOLD
        title = f"💡 Earnings Spotlight: {self.data.ticker}"

        # Compact one-liner header at top of description
        compact_header = ticker_info_description(self.data.ticker_info, self.data.quote)
        compact_header += '\n' + todays_change_description(self.data.quote)

        # Build body section by section — card format throughout
        body = (
            earnings_date_section(self.data.ticker, self.data.next_earnings_info)
            + ohlcv_card(self.data.quote)
            + performance_card(self.data.daily_price_history, self.data.quote)
            + fundamentals_card(
                self.data.fundamentals, self.data.quote,
                daily_price_history=self.data.daily_price_history,
            )
            + technical_signals_card(self.data.daily_price_history)
            + upcoming_earnings_card(self.data.next_earnings_info)
            + recent_earnings_card(self.data.historical_earnings)
        )

        description = compact_header + '\n\n' + body

        if len(description) > 4096:
            description = description[:4093] + '...'

        return EmbedSpec(
            title=title,
            description=description,
            color=color,
            footer="RocketStocks · earnings-spotlight",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )

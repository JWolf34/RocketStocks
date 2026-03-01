import logging

from rocketstocks.core.content.models import (
    COLOR_GOLD,
    EmbedSpec, EarningsSpotlightData,
)
from rocketstocks.core.content import sections
from rocketstocks.core.content.sections_card import (
    ohlcv_card, recent_earnings_card,
    performance_card, fundamentals_card, technical_signals_card,
    upcoming_earnings_card,
)
from rocketstocks.core.content.sections_embed import (
    ticker_info_description,
    todays_change_description,
)

logger = logging.getLogger(__name__)


class EarningsSpotlightReport:
    """Standalone earnings spotlight report."""

    def __init__(self, data: EarningsSpotlightData):
        self.data = data
        self.ticker = data.ticker

    def build_report(self) -> str:
        logger.debug("Building Earnings Spotlight Report...")
        return (
            sections.earnings_spotlight_header(self.data.ticker)
            + sections.earnings_date_section(self.data.ticker, self.data.next_earnings_info)
            + sections.ticker_info_section(self.data.ticker_info, self.data.quote)
            + sections.fundamentals_section(
                self.data.fundamentals, self.data.quote,
                daily_price_history=self.data.daily_price_history,
            )
            + sections.performance_section(self.data.daily_price_history, self.data.quote)
            + sections.technical_signals_section(self.data.daily_price_history)
            + sections.upcoming_earnings_summary_section(self.data.next_earnings_info)
            + sections.recent_earnings_section(self.data.historical_earnings)
        )

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug("Building Earnings Spotlight EmbedSpec...")
        color = COLOR_GOLD

        title = sections.earnings_spotlight_header(self.data.ticker).splitlines()[0].lstrip('# ').strip()

        # Compact one-liner header at top of description
        compact_header = ticker_info_description(self.data.ticker_info, self.data.quote)
        compact_header += '\n' + todays_change_description(self.data.quote)

        # Build body section by section — card format throughout
        body = (
            sections.earnings_date_section(self.data.ticker, self.data.next_earnings_info)
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

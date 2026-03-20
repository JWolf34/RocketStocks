"""Earnings result report — posted after a company reports actual EPS."""
import logging

from rocketstocks.core.content.models import (
    COLOR_GREEN, COLOR_RED,
    EmbedSpec, EarningsResultData,
)
from rocketstocks.core.content.sections_card import (
    earnings_result_card, ohlcv_card, performance_card,
    recent_earnings_card, upcoming_earnings_card,
    ticker_info_card, todays_change_card,
)
from rocketstocks.core.utils.formatting import finviz_url

logger = logging.getLogger(__name__)


class EarningsResultReport:
    """Report posted to the reports channel when a company's EPS result becomes available."""

    def __init__(self, data: EarningsResultData):
        self.data = data
        self.ticker = data.ticker

    def build(self) -> EmbedSpec:
        logger.debug(f"Building EarningsResultReport for {self.ticker}")
        beat = (self.data.surprise_pct or 0) >= 0
        color = COLOR_GREEN if beat else COLOR_RED
        indicator = "✅ Beat" if beat else "❌ Missed"
        title = f"📊 Earnings Report: {self.ticker} — {indicator}"

        compact_header = ticker_info_card(self.data.ticker_info, self.data.quote)
        compact_header += '\n' + todays_change_card(self.data.quote)

        body = (
            earnings_result_card(self.data.eps_actual, self.data.eps_estimate, self.data.surprise_pct)
            + ohlcv_card(self.data.quote, self.data.daily_price_history)
            + performance_card(self.data.daily_price_history, self.data.quote)
            + recent_earnings_card(self.data.historical_earnings)
            + upcoming_earnings_card(self.data.next_earnings_info)
        )

        description = compact_header + '\n\n' + body
        if len(description) > 4096:
            description = description[:4093] + '...'

        return EmbedSpec(
            title=title,
            description=description,
            color=color,
            footer="RocketStocks · earnings-result",
            timestamp=True,
            url=finviz_url(self.ticker),
        )

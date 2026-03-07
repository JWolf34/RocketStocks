import logging

from rocketstocks.core.content.models import (
    COLOR_BLUE,
    EmbedSpec, StockReportData,
)
from rocketstocks.core.content.sections_card import (
    ohlcv_card, recent_earnings_card,
    performance_card, fundamentals_card, technical_signals_card,
    popularity_card, sec_filings_card, recent_alerts_card,
    ticker_info_card, todays_change_card,
)

logger = logging.getLogger(__name__)


class StockReport:
    """Standalone stock report — no base class required."""

    def __init__(self, data: StockReportData):
        self.data = data
        self.ticker = data.ticker

    def build(self) -> EmbedSpec:
        logger.debug("Building Stock Report embed...")
        color = COLOR_BLUE
        title = f"📊 {self.data.ticker} Stock Report"

        # Compact one-liner header at top of description
        compact_header = ticker_info_card(self.data.ticker_info, self.data.quote)
        compact_header += '\n' + todays_change_card(self.data.quote)

        # Build body section by section — card format throughout
        body = (
            ohlcv_card(self.data.quote)
            + performance_card(self.data.daily_price_history, self.data.quote)
            + fundamentals_card(
                self.data.fundamentals, self.data.quote,
                daily_price_history=self.data.daily_price_history,
            )
            + technical_signals_card(self.data.daily_price_history)
            + popularity_card(self.data.popularity)
            + recent_earnings_card(self.data.historical_earnings)
            + sec_filings_card(self.data.recent_sec_filings)
            + recent_alerts_card(self.data.recent_alerts)
        )

        description = compact_header + '\n\n' + body

        # Truncate to Discord's 4096 embed description limit
        if len(description) > 4096:
            description = description[:4093] + '...'

        return EmbedSpec(
            title=title,
            description=description,
            color=color,
            footer="RocketStocks · stock-report",
            timestamp=True,
            url=f"https://finviz.com/quote.ashx?t={self.data.ticker}",
        )

import logging
import re

from rocketstocks.core.content.models import (
    COLOR_BLUE, COLOR_GREEN, COLOR_RED,
    EmbedSpec, StockReportData,
)
from rocketstocks.core.content import sections
from rocketstocks.core.content.sections_embed import (
    ticker_info_description,
    todays_change_description,
)

logger = logging.getLogger(__name__)


class StockReport:
    """Standalone stock report — no base class required."""

    def __init__(self, data: StockReportData):
        self.data = data
        self.ticker = data.ticker

    def build_report(self) -> str:
        logger.debug("Building Stock Report...")
        return (
            sections.report_header(self.data.ticker)
            + sections.ticker_info_section(self.data.ticker_info, self.data.quote)
            + sections.daily_summary_section(self.data.quote)
            + sections.performance_section(self.data.daily_price_history, self.data.quote)
            + sections.fundamentals_section(
                self.data.fundamentals, self.data.quote,
                daily_price_history=self.data.daily_price_history,
            )
            + sections.technical_signals_section(self.data.daily_price_history)
            + sections.popularity_section(self.data.popularity)
            + sections.recent_earnings_section(self.data.historical_earnings)
            + sections.sec_filings_section(self.data.recent_sec_filings)
        )

    def build_embed_spec(self) -> EmbedSpec:
        logger.debug("Building Stock Report EmbedSpec...")
        pct_change = self.data.quote['quote'].get('netPercentChange', 0)
        color = COLOR_GREEN if pct_change > 0 else COLOR_RED if pct_change < 0 else COLOR_BLUE

        full = self.build_report()
        # First non-empty line is the title; drop it from body
        lines = full.split('\n')
        title = lines[0].lstrip('# ').strip()
        body = '\n'.join(lines[1:]).lstrip('\n')

        # Replace markdown headers with bold text (Discord doesn't render ## in embeds)
        body = re.sub(r'^#{1,3} (.+)$', r'**\1**', body, flags=re.MULTILINE)

        # Compact one-liner header: name · ticker · sector · exchange + today's change
        compact_header = ticker_info_description(self.data.ticker_info, self.data.quote)
        compact_header += '\n' + todays_change_description(self.data.quote)

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

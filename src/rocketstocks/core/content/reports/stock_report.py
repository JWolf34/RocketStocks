import logging

from rocketstocks.core.content.models import (
    COLOR_BLUE,
    EmbedField, EmbedSpec, FullStockReportData, StockReportData,
)
from rocketstocks.core.content.sections_card import (
    ohlcv_card, recent_earnings_card,
    performance_card, fundamentals_card, technical_signals_card,
    popularity_card, sec_filings_card, recent_alerts_card,
    ticker_info_card, todays_change_card,
    classification_card, analyst_card, short_interest_card, earnings_forecast_card,
)
from rocketstocks.core.utils.formatting import finviz_url

logger = logging.getLogger(__name__)


def _field_value(card_text: str, limit: int = 1024) -> str:
    """Strip the __**Header**__ first line from a card string for use as EmbedField value."""
    lines = card_text.strip('\n').split('\n', 1)
    if len(lines) > 1 and lines[0].startswith('__**'):
        content = lines[1].strip('\n')
    else:
        content = card_text.strip('\n')
    if len(content) > limit:
        content = content[:limit - 3] + '...'
    return content or '\u200b'


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
            ohlcv_card(self.data.quote, self.data.daily_price_history)
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
            url=finviz_url(self.data.ticker),
        )


class FullStockReport:
    """Enhanced stock report with analyst consensus, short interest, and earnings forecast."""

    def __init__(self, data: FullStockReportData):
        self.data = data
        self.ticker = data.ticker

    def build(self) -> EmbedSpec:
        logger.debug("Building Full Stock Report embed...")
        title = f"📊 {self.data.ticker} Full Stock Report"

        compact_header = ticker_info_card(self.data.ticker_info, self.data.quote)
        compact_header += '\n' + todays_change_card(self.data.quote)

        body = (
            ohlcv_card(self.data.quote, self.data.daily_price_history)
            + performance_card(self.data.daily_price_history, self.data.quote)
            + fundamentals_card(
                self.data.fundamentals, self.data.quote,
                daily_price_history=self.data.daily_price_history,
            )
            + classification_card(self.data.classification, self.data.volatility_20d)
            + technical_signals_card(self.data.daily_price_history)
            + popularity_card(self.data.popularity)
            + recent_earnings_card(self.data.historical_earnings)
            + sec_filings_card(self.data.recent_sec_filings)
            + recent_alerts_card(self.data.recent_alerts)
        )

        description = compact_header + '\n\n' + body
        if len(description) > 4096:
            description = description[:4093] + '...'

        fields = [
            EmbedField(
                name="Analyst Consensus",
                value=_field_value(analyst_card(
                    self.data.price_targets,
                    self.data.recommendations,
                    self.data.upgrades_downgrades,
                )),
            ),
            EmbedField(
                name="Earnings Forecast",
                value=_field_value(earnings_forecast_card(
                    self.data.quarterly_forecast,
                    self.data.yearly_forecast,
                )),
            ),
            EmbedField(
                name="Short Interest",
                value=_field_value(short_interest_card(
                    self.data.short_interest_ratio,
                    self.data.short_interest_shares,
                    self.data.short_percent_of_float,
                    self.data.shares_outstanding,
                )),
            ),
        ]

        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_BLUE,
            fields=fields,
            footer="RocketStocks · stock-report",
            timestamp=True,
            url=finviz_url(self.data.ticker),
        )

import logging

from rocketstocks.core.content.models import (
    COLOR_INDIGO,
    ComparisonReportData,
    EmbedField,
    EmbedSpec,
)
from rocketstocks.core.content.sections_card import (
    comparison_performance_card,
    comparison_popularity_card,
    comparison_price_volume_card,
    comparison_technicals_card,
    comparison_valuation_card,
)

logger = logging.getLogger(__name__)


class ComparisonReport:
    """Side-by-side comparison of up to 5 tickers with an optional benchmark."""

    def __init__(self, data: ComparisonReportData):
        self.data = data

    def build(self) -> EmbedSpec:
        logger.debug("Building Comparison Report embed...")
        d = self.data
        tickers = d.tickers

        # Title — keep it short for the embed header
        ticker_list = ' · '.join(tickers)
        title = f"📊 Comparison: {ticker_list}"
        if len(title) > 256:
            title = "📊 Stock Comparison"

        # Description — company names per ticker
        desc_lines = []
        for ticker in tickers:
            info = (d.ticker_infos or {}).get(ticker) or {}
            name = info.get('name', '')
            if name and name != 'NaN':
                desc_lines.append(f"**{ticker}** — {name}")
            else:
                desc_lines.append(f"**{ticker}**")
        if d.benchmark_ticker and d.benchmark_ticker in tickers:
            desc_lines.append(f"\n*Benchmark: **{d.benchmark_ticker}** (marked B in Performance)*")
        description = '\n'.join(desc_lines)
        if len(description) > 4096:
            description = description[:4093] + '...'

        # Build EmbedFields for each category
        fields = []

        pv = comparison_price_volume_card(tickers, d.quotes)
        fields.append(EmbedField(name="Price & Volume", value=pv))

        perf = comparison_performance_card(tickers, d.quotes, d.daily_price_histories, d.benchmark_ticker)
        fields.append(EmbedField(name="Performance", value=perf))

        val = comparison_valuation_card(tickers, d.fundamentals)
        fields.append(EmbedField(name="Valuation", value=val))

        tech = comparison_technicals_card(tickers, d.daily_price_histories)
        fields.append(EmbedField(name="Technicals", value=tech))

        pop = comparison_popularity_card(tickers, d.popularities)
        if pop:
            fields.append(EmbedField(name="Popularity", value=pop))

        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_INDIGO,
            fields=fields,
            footer="RocketStocks · compare",
            timestamp=True,
        )

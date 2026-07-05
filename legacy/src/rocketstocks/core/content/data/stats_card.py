"""StatsCard — ticker statistical profile embed for /data stats."""
import logging

from rocketstocks.core.content.models import COLOR_PURPLE, EmbedField, EmbedSpec, TickerStatsData
from rocketstocks.core.content.formatting import format_large_num
from rocketstocks.core.utils.formatting import ticker_string

logger = logging.getLogger(__name__)


def _pct(val, decimals=2) -> str:
    """Format a percentage value, returning 'N/A' for None."""
    if val is None:
        return "N/A"
    try:
        return f"{float(val):.{decimals}f}%"
    except (TypeError, ValueError):
        return "N/A"


def _num(val, decimals=2) -> str:
    """Format a numeric value, returning 'N/A' for None."""
    if val is None:
        return "N/A"
    try:
        return f"{float(val):.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


def _price(val) -> str:
    """Format a price value, returning 'N/A' for None."""
    if val is None:
        return "N/A"
    try:
        return f"${float(val):.2f}"
    except (TypeError, ValueError):
        return "N/A"


class StatsCard:
    """Builds a ticker stats embed for one or more tickers."""

    def __init__(self, data: TickerStatsData):
        self.data = data

    def build(self) -> EmbedSpec:
        fields = []
        for ticker in self.data.tickers:
            stats = self.data.stats.get(ticker)
            if stats is None:
                fields.append(EmbedField(
                    name=ticker,
                    value="No stats available. Run the classify job to populate.",
                    inline=False,
                ))
            else:
                mkt_cap = stats.get('market_cap')
                mkt_cap_str = format_large_num(mkt_cap) if mkt_cap else "N/A"
                classification = (stats.get('classification') or 'N/A').replace('_', ' ').title()
                updated = stats.get('updated_at', 'N/A')
                value = (
                    f"**Classification:** {classification}\n"
                    f"**Market Cap:** {mkt_cap_str}\n"
                    f"**Volatility 20d:** {_pct(stats.get('volatility_20d'))}\n"
                    f"**Mean Return 20d/60d:** {_pct(stats.get('mean_return_20d'))} / {_pct(stats.get('mean_return_60d'))}\n"
                    f"**Std Return 20d/60d:** {_pct(stats.get('std_return_20d'))} / {_pct(stats.get('std_return_60d'))}\n"
                    f"**Avg RVOL 20d:** {_num(stats.get('avg_rvol_20d'))}x\n"
                    f"**BB Upper/Mid/Lower:** {_price(stats.get('bb_upper'))} / {_price(stats.get('bb_mid'))} / {_price(stats.get('bb_lower'))}\n"
                    f"**Updated:** {updated}"
                )
                fields.append(EmbedField(name=ticker, value=value, inline=False))

        footer = None
        if self.data.invalid_tickers:
            footer = f"Invalid tickers: {ticker_string(self.data.invalid_tickers)}"

        return EmbedSpec(
            title="Ticker Stats",
            description="",
            color=COLOR_PURPLE,
            fields=fields,
            footer=footer,
        )

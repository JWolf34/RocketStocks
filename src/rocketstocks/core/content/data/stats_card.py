"""StatsCard — ticker statistical profile embed for /data stats."""
import logging

from rocketstocks.core.content.models import COLOR_PURPLE, EmbedField, EmbedSpec, TickerStatsData
from rocketstocks.core.utils.formatting import ticker_string

logger = logging.getLogger(__name__)


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
                mkt_cap_str = f"${mkt_cap / 1e9:.1f}B" if mkt_cap else "N/A"
                value = (
                    f"**Classification:** {stats.get('classification', 'N/A')}\n"
                    f"**Market Cap:** {mkt_cap_str}\n"
                    f"**Volatility 20d:** {stats.get('volatility_20d', 'N/A')}\n"
                    f"**Mean Return 20d/60d:** {stats.get('mean_return_20d', 'N/A')} / {stats.get('mean_return_60d', 'N/A')}\n"
                    f"**Std Return 20d/60d:** {stats.get('std_return_20d', 'N/A')} / {stats.get('std_return_60d', 'N/A')}\n"
                    f"**Avg RVOL 20d:** {stats.get('avg_rvol_20d', 'N/A')}\n"
                    f"**BB Upper/Mid/Lower:** {stats.get('bb_upper', 'N/A')} / {stats.get('bb_mid', 'N/A')} / {stats.get('bb_lower', 'N/A')}\n"
                    f"**Updated:** {stats.get('updated_at', 'N/A')}"
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

"""QuoteCard — real-time quote embed for /data quote."""
import logging

from rocketstocks.core.content.models import COLOR_BLUE, EmbedField, EmbedSpec, QuoteData
from rocketstocks.core.utils.formatting import ticker_string

logger = logging.getLogger(__name__)


class QuoteCard:
    """Builds a real-time quote embed for one or more tickers."""

    def __init__(self, data: QuoteData):
        self.data = data

    def build(self) -> EmbedSpec:
        fields = []
        for ticker in self.data.tickers:
            quote_data = self.data.quotes.get(ticker, {})
            q = quote_data.get('quote', {})
            r = quote_data.get('regular', {})

            last_price = r.get('regularMarketLastPrice') or q.get('lastPrice', 'N/A')
            change = q.get('netChange', 'N/A')
            change_pct = q.get('netPercentChange', 'N/A')
            bid = q.get('bidPrice', 'N/A')
            ask = q.get('askPrice', 'N/A')
            volume = q.get('totalVolume', 'N/A')
            open_price = q.get('openPrice', 'N/A')
            high = q.get('highPrice', 'N/A')
            low = q.get('lowPrice', 'N/A')

            if isinstance(change, (int, float)) and isinstance(change_pct, (int, float)):
                change_str = f"{change:+.2f} ({change_pct:+.2f}%)"
            else:
                change_str = "N/A"

            volume_str = f"{volume:,}" if isinstance(volume, (int, float)) else str(volume)

            value = (
                f"**Last:** ${last_price}\n"
                f"**Change:** {change_str}\n"
                f"**Bid × Ask:** ${bid} × ${ask}\n"
                f"**Volume:** {volume_str}\n"
                f"**Open / High / Low:** ${open_price} / ${high} / ${low}"
            )
            fields.append(EmbedField(name=ticker, value=value, inline=False))

        footer = None
        if self.data.invalid_tickers:
            footer = f"Invalid tickers: {ticker_string(self.data.invalid_tickers)}"

        return EmbedSpec(
            title="Real-Time Quotes",
            description="",
            color=COLOR_BLUE,
            fields=fields,
            footer=footer,
        )

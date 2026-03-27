"""TradeQuote content class — quote embed shown before trade confirmation."""
import logging

from rocketstocks.core.content.models import (
    COLOR_AMBER,
    EmbedField,
    EmbedSpec,
    TradeQuoteData,
)

logger = logging.getLogger(__name__)


class TradeQuote:
    """Embed shown to the user before they confirm or cancel a trade."""

    def __init__(self, data: TradeQuoteData):
        self.data = data

    def build(self) -> EmbedSpec:
        d = self.data
        side_label = "Buy" if d.side == "BUY" else "Sell"
        title = f"Paper Trade — {side_label} {d.ticker}"
        description = (
            f"**{d.ticker_name}**\n"
            f"Confirm your order below. This quote expires in **60 seconds**."
        )
        fields = [
            EmbedField(name="Side", value=d.side, inline=True),
            EmbedField(name="Shares", value=f"{d.shares:,}", inline=True),
            EmbedField(name="Price", value=f"${d.price:,.2f}", inline=True),
            EmbedField(name="Total", value=f"${d.total:,.2f}", inline=True),
            EmbedField(name="Cash After", value=f"${d.cash_after:,.2f}", inline=True),
        ]
        return EmbedSpec(
            title=title,
            description=description,
            color=COLOR_AMBER,
            fields=fields,
            timestamp=True,
        )

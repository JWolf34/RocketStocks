"""TradeConfirmation content class — embed shown after trade execution or queuing."""
import logging

from rocketstocks.core.content.models import (
    COLOR_GREEN,
    COLOR_RED,
    EmbedField,
    EmbedSpec,
    TradeConfirmationData,
)

logger = logging.getLogger(__name__)


class TradeConfirmation:
    """Embed shown after a trade is executed (intraday) or queued (off-hours)."""

    def __init__(self, data: TradeConfirmationData):
        self.data = data

    def build(self) -> EmbedSpec:
        d = self.data
        color = COLOR_GREEN if d.side == "BUY" else COLOR_RED
        side_label = "Buy" if d.side == "BUY" else "Sell"

        if d.was_queued:
            title = f"Order Queued — {side_label} {d.ticker}"
            description = (
                f"**{d.ticker_name}**\n"
                f"Market is closed. Your order will execute at market open."
            )
        else:
            title = f"Order Executed — {side_label} {d.ticker}"
            description = f"**{d.ticker_name}**"

        fields = [
            EmbedField(name="Side", value=d.side, inline=True),
            EmbedField(name="Shares", value=f"{d.shares:,}", inline=True),
            EmbedField(name="Price", value=f"${d.price:,.2f}", inline=True),
            EmbedField(name="Total", value=f"${d.total:,.2f}", inline=True),
            EmbedField(name="Cash Remaining", value=f"${d.cash_remaining:,.2f}", inline=True),
        ]
        return EmbedSpec(
            title=title,
            description=description,
            color=color,
            fields=fields,
            timestamp=True,
        )

"""TradeAnnouncement content class — public trade embed posted to TRADE channel."""
import logging

from rocketstocks.core.content.models import (
    COLOR_GREEN,
    COLOR_RED,
    EmbedField,
    EmbedSpec,
    TradeAnnouncementData,
)

logger = logging.getLogger(__name__)


class TradeAnnouncement:
    """Compact embed posted to the public TRADE channel when a trade executes or is queued."""

    def __init__(self, data: TradeAnnouncementData):
        self.data = data

    def build(self) -> EmbedSpec:
        d = self.data
        color = COLOR_GREEN if d.side == "BUY" else COLOR_RED
        side_label = "bought" if d.side == "BUY" else "sold"
        status = "queued" if d.was_queued else "executed"

        title = f"Trade {status.title()} — {d.ticker}"
        description = (
            f"**{d.user_name}** {side_label} **{d.shares:,} shares** of "
            f"**{d.ticker_name}** ({d.ticker})"
        )
        if d.was_queued:
            description += "\n_Will execute at market open._"

        fields = [
            EmbedField(name="Side", value=d.side, inline=True),
            EmbedField(name="Shares", value=f"{d.shares:,}", inline=True),
            EmbedField(name="Price", value=f"${d.price:,.2f}", inline=True),
            EmbedField(name="Total", value=f"${d.total:,.2f}", inline=True),
        ]

        return EmbedSpec(
            title=title,
            description=description,
            color=color,
            fields=fields,
            timestamp=True,
        )

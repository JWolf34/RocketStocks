"""TradeHistory content class — /trade history embed."""
import logging

from rocketstocks.core.content.models import (
    COLOR_INDIGO,
    EmbedField,
    EmbedSpec,
    TradeHistoryData,
)

logger = logging.getLogger(__name__)


def _format_transaction(tx: dict) -> str:
    side = tx.get('side', '')
    ticker = tx.get('ticker', '')
    shares = tx.get('shares', 0)
    price = tx.get('price', 0.0)
    total = tx.get('total', 0.0)
    executed_at = tx.get('executed_at')
    date_str = executed_at.strftime("%m/%d %H:%M") if executed_at else "?"
    side_icon = "🟢" if side == "BUY" else "🔴"
    return (
        f"{side_icon} **{ticker}** — {side} {shares:,} shares @ ${price:,.2f}  "
        f"(${total:,.2f})  `{date_str}`"
    )


class TradeHistory:
    """Embed showing a user's recent paper trading transactions."""

    def __init__(self, data: TradeHistoryData):
        self.data = data

    def build(self) -> EmbedSpec:
        d = self.data
        description = f"**{d.user_name}'s Recent Trades**"

        if not d.transactions:
            return EmbedSpec(
                title="Trade History",
                description=f"{description}\n\nNo trades yet.",
                color=COLOR_INDIGO,
                timestamp=True,
            )

        lines = [_format_transaction(tx) for tx in d.transactions]
        fields = [EmbedField(
            name=f"Last {len(lines)} Trades",
            value="\n".join(lines),
            inline=False,
        )]

        return EmbedSpec(
            title="Trade History",
            description=description,
            color=COLOR_INDIGO,
            fields=fields,
            timestamp=True,
        )

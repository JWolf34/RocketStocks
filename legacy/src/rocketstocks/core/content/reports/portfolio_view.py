"""PortfolioView content class — /trade portfolio embed."""
import logging

from rocketstocks.core.content.models import (
    COLOR_BLUE,
    COLOR_GREEN,
    COLOR_RED,
    EmbedField,
    EmbedSpec,
    PortfolioViewData,
)

logger = logging.getLogger(__name__)

_MAX_POSITIONS_SHOWN = 10


def _gain_loss_str(value: float, pct: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}${value:,.2f} ({sign}{pct:.2f}%)"


class PortfolioView:
    """Embed showing a user's paper trading portfolio."""

    def __init__(self, data: PortfolioViewData):
        self.data = data

    def build(self) -> EmbedSpec:
        d = self.data
        gl_sign = "+" if d.total_gain_loss >= 0 else ""
        color = COLOR_GREEN if d.total_gain_loss >= 0 else COLOR_RED

        description = (
            f"**{d.user_name}'s Portfolio**\n"
            f"Total Value: **${d.total_value:,.2f}**  "
            f"({gl_sign}${d.total_gain_loss:,.2f} / {gl_sign}{d.total_gain_loss_pct:.2f}%)\n"
            f"Cash: **${d.cash:,.2f}**"
        )

        fields = []

        if d.positions:
            shown = d.positions[:_MAX_POSITIONS_SHOWN]
            lines = []
            for pos in shown:
                gl = _gain_loss_str(pos.gain_loss, pos.gain_loss_pct)
                lines.append(
                    f"**{pos.ticker}** — {pos.shares:,} shares @ ${pos.current_price:,.2f}  "
                    f"(${pos.market_value:,.2f})  {gl}"
                )
            if len(d.positions) > _MAX_POSITIONS_SHOWN:
                lines.append(f"… +{len(d.positions) - _MAX_POSITIONS_SHOWN} more")
            fields.append(EmbedField(
                name=f"Positions ({len(d.positions)})",
                value="\n".join(lines),
                inline=False,
            ))
        else:
            fields.append(EmbedField(
                name="Positions",
                value="No open positions.",
                inline=False,
            ))

        if d.pending_orders:
            order_lines = []
            for order in d.pending_orders[:5]:
                order_lines.append(
                    f"#{order['id']} — {order['side']} {order['shares']:,}x **{order['ticker']}** "
                    f"@ ${order['quoted_price']:,.2f}"
                )
            if len(d.pending_orders) > 5:
                order_lines.append(f"… +{len(d.pending_orders) - 5} more")
            fields.append(EmbedField(
                name=f"Pending Orders ({len(d.pending_orders)})",
                value="\n".join(order_lines),
                inline=False,
            ))

        return EmbedSpec(
            title="Paper Trading Portfolio",
            description=description,
            color=color,
            fields=fields,
            timestamp=True,
        )

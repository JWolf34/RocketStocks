"""MoversCard — top daily movers/losers embed for /data movers and /data losers."""
import logging

from rocketstocks.core.content.models import COLOR_GOLD, COLOR_RED, EmbedField, EmbedSpec, MoverData

logger = logging.getLogger(__name__)


class MoversCard:
    """Builds a top movers or top losers embed."""

    def __init__(self, data: MoverData):
        self.data = data

    def build(self) -> EmbedSpec:
        is_losers = self.data.direction == 'losers'
        title = "Top 10 Daily Losers" if is_losers else "Top 10 Daily Movers"
        color = COLOR_RED if is_losers else COLOR_GOLD

        if not self.data.screeners:
            return EmbedSpec(
                title=title,
                description="No mover data available.",
                color=color,
            )

        fields = []
        for mover in self.data.screeners[:10]:
            ticker = mover.get('symbol', 'N/A')
            last_price = mover.get('lastPrice', 'N/A')
            change_pct = mover.get('percentChange', 'N/A')
            volume = mover.get('totalVolume', 'N/A')

            change_pct_str = f"{change_pct:+.2f}%" if isinstance(change_pct, (int, float)) else str(change_pct)
            volume_str = f"{volume:,}" if isinstance(volume, (int, float)) else str(volume)

            fields.append(EmbedField(
                name=f"{ticker}  {change_pct_str}",
                value=f"**Price:** ${last_price}  |  **Volume:** {volume_str}",
                inline=False,
            ))

        return EmbedSpec(
            title=title,
            description="",
            color=color,
            fields=fields,
        )

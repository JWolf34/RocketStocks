"""Leaderboard content class — /trade leaderboard embed."""
import logging

from rocketstocks.core.content.models import (
    COLOR_BLUE,
    COLOR_GOLD,
    EmbedField,
    EmbedSpec,
    LeaderboardViewData,
)

logger = logging.getLogger(__name__)

_MAX_ENTRIES_SHOWN = 15


def _rank_medal(rank: int) -> str:
    if rank == 1:
        return "🥇"
    if rank == 2:
        return "🥈"
    if rank == 3:
        return "🥉"
    return f"**#{rank}**"


def _gl_str(value: float, pct: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}${value:,.2f} ({sign}{pct:.2f}%)"


class Leaderboard:
    """Embed showing all guild members ranked by portfolio value."""

    def __init__(self, data: LeaderboardViewData):
        self.data = data

    def build(self) -> EmbedSpec:
        d = self.data

        if not d.entries:
            return EmbedSpec(
                title="Paper Trading Leaderboard",
                description=f"**{d.guild_name}**\n\nNo portfolios yet. Use `/trade buy` to get started!",
                color=COLOR_BLUE,
                timestamp=True,
            )

        shown = d.entries[:_MAX_ENTRIES_SHOWN]
        lines = []
        for i, entry in enumerate(shown, start=1):
            gl = _gl_str(entry.total_gain_loss, entry.total_gain_loss_pct)
            pos_label = f"{entry.position_count} position{'s' if entry.position_count != 1 else ''}"
            lines.append(
                f"{_rank_medal(i)} **{entry.user_name}** — ${entry.total_value:,.2f}  {gl}  _{pos_label}_"
            )

        if len(d.entries) > _MAX_ENTRIES_SHOWN:
            lines.append(f"… +{len(d.entries) - _MAX_ENTRIES_SHOWN} more")

        fields = [EmbedField(
            name=f"Rankings ({len(d.entries)} traders)",
            value="\n".join(lines),
            inline=False,
        )]

        return EmbedSpec(
            title="Paper Trading Leaderboard",
            description=f"**{d.guild_name}** — starting capital $10,000",
            color=COLOR_GOLD,
            fields=fields,
            timestamp=True,
        )

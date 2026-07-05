"""WeeklyRoundup content class — Sunday paper trading roundup embed."""
import logging

from rocketstocks.core.content.models import (
    COLOR_GOLD,
    EmbedField,
    EmbedSpec,
    WeeklyRoundupData,
)

logger = logging.getLogger(__name__)

_MAX_LEADERBOARD_SHOWN = 10
_MAX_EMBED_CHARS = 6000


def _rank_medal(rank: int) -> str:
    if rank == 1:
        return "🥇"
    if rank == 2:
        return "🥈"
    if rank == 3:
        return "🥉"
    return f"**#{rank}**"


def _leaderboard_field(entries: list) -> EmbedField:
    """Build the leaderboard rankings field."""
    shown = entries[:_MAX_LEADERBOARD_SHOWN]
    lines = []
    for i, entry in enumerate(shown, start=1):
        sign = "+" if entry.total_gain_loss >= 0 else ""
        lines.append(
            f"{_rank_medal(i)} **{entry.user_name}** — "
            f"${entry.total_value:,.0f} ({sign}{entry.total_gain_loss_pct:.1f}%)"
        )
    if len(entries) > _MAX_LEADERBOARD_SHOWN:
        lines.append(f"… +{len(entries) - _MAX_LEADERBOARD_SHOWN} more")
    return EmbedField(
        name=f"Leaderboard ({len(entries)} traders)",
        value="\n".join(lines) if lines else "No data",
        inline=False,
    )


def _awards_field(awards: list) -> EmbedField:
    """Build all 15 awards into a single compact field."""
    lines = []
    for award in awards:
        if award.winner_name:
            detail = f" ({award.detail})" if award.detail else ""
            lines.append(f"**{award.award_name}** — {award.winner_name}{detail}")
        else:
            lines.append(f"**{award.award_name}** — _No winner this week_")
    return EmbedField(
        name="Weekly Awards",
        value="\n".join(lines) if lines else "No awards",
        inline=False,
    )


def _server_stats_field(stats: dict) -> EmbedField:
    """Build the server-wide stats field."""
    lines = []
    if stats.get('total_trades') is not None:
        lines.append(f"Total trades: **{stats['total_trades']:,}**")
    if stats.get('active_traders') is not None:
        lines.append(f"Active traders: **{stats['active_traders']:,}**")
    if stats.get('most_traded_ticker'):
        lines.append(f"Most traded: **{stats['most_traded_ticker']}**")
    if stats.get('total_volume') is not None:
        lines.append(f"Volume traded: **${stats['total_volume']:,.0f}**")
    return EmbedField(
        name="Server Stats",
        value="\n".join(lines) if lines else "No data",
        inline=False,
    )


class WeeklyRoundup:
    """Sunday weekly paper trading roundup.

    Builds one or two EmbedSpec objects. Call build() for the primary embed
    (leaderboard + server stats). If the combined character count would exceed
    Discord's 6000-char limit, awards are split into a second embed returned by
    build_awards_embed().
    """

    def __init__(self, data: WeeklyRoundupData):
        self.data = data

    def _primary_fields(self) -> list:
        d = self.data
        fields = []
        if d.leaderboard:
            fields.append(_leaderboard_field(d.leaderboard))
        fields.append(_server_stats_field(d.server_stats))
        return fields

    def _awards_fields(self) -> list:
        return [_awards_field(self.data.awards)]

    def _estimate_chars(self, fields: list) -> int:
        total = len(self.data.guild_name) + len(self.data.week_label)
        for f in fields:
            total += len(f.name) + len(f.value)
        return total

    def build(self) -> EmbedSpec:
        """Return the primary embed (leaderboard + server stats, and awards if they fit)."""
        d = self.data
        primary = self._primary_fields()
        awards = self._awards_fields()
        combined = primary + awards
        if self._estimate_chars(combined) <= _MAX_EMBED_CHARS:
            fields = combined
        else:
            fields = primary
        return EmbedSpec(
            title=f"Weekly Paper Trading Roundup — {d.week_label}",
            description=f"**{d.guild_name}** — Week in review",
            color=COLOR_GOLD,
            fields=fields,
            timestamp=True,
        )

    def needs_split(self) -> bool:
        """Return True if awards must be sent as a separate embed."""
        primary = self._primary_fields()
        awards = self._awards_fields()
        return self._estimate_chars(primary + awards) > _MAX_EMBED_CHARS

    def build_awards_embed(self) -> EmbedSpec:
        """Return the overflow awards embed (only used when needs_split() is True)."""
        d = self.data
        return EmbedSpec(
            title=f"Weekly Awards — {d.week_label}",
            description=f"**{d.guild_name}**",
            color=COLOR_GOLD,
            fields=self._awards_fields(),
            timestamp=True,
        )

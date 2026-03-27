"""PerformanceView content class — /trade performance embed."""
import logging

from rocketstocks.core.content.models import (
    COLOR_BLUE,
    COLOR_GREEN,
    COLOR_RED,
    EmbedField,
    EmbedSpec,
    PerformanceViewData,
)

logger = logging.getLogger(__name__)

_MAX_ROWS_SHOWN = 10


def _gl_str(value: float, pct: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}${value:,.2f} ({sign}{pct:.2f}%)"


class PerformanceView:
    """Embed showing a user's portfolio value history over the requested time window."""

    def __init__(self, data: PerformanceViewData):
        self.data = data

    def build(self) -> EmbedSpec:
        d = self.data
        gl_sign = "+" if d.total_gain_loss >= 0 else ""
        color = COLOR_GREEN if d.total_gain_loss > 0 else (COLOR_RED if d.total_gain_loss < 0 else COLOR_BLUE)

        description = (
            f"**{d.user_name}'s Performance** — last {d.days} day{'s' if d.days != 1 else ''}\n"
            f"Current Value: **${d.current_value:,.2f}**  "
            f"({gl_sign}${d.total_gain_loss:,.2f} / {gl_sign}{d.total_gain_loss_pct:.2f}% vs starting capital)"
        )

        fields = []
        if d.snapshots:
            # Show up to _MAX_ROWS_SHOWN most-recent snapshots in chronological order
            shown = d.snapshots[-_MAX_ROWS_SHOWN:]
            lines = []
            for snap in shown:
                date_str = snap['snapshot_date'].strftime("%m/%d") if hasattr(snap['snapshot_date'], 'strftime') else str(snap['snapshot_date'])
                value = snap['portfolio_value']
                lines.append(f"`{date_str}` — **${value:,.2f}**")
            fields.append(EmbedField(
                name=f"Daily Snapshots ({len(d.snapshots)} recorded)",
                value="\n".join(lines),
                inline=False,
            ))
        else:
            fields.append(EmbedField(
                name="Daily Snapshots",
                value="No snapshots recorded yet for this period.",
                inline=False,
            ))

        return EmbedSpec(
            title="Portfolio Performance",
            description=description,
            color=color,
            fields=fields,
            timestamp=True,
        )

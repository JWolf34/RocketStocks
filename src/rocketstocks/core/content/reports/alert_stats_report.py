"""Embed builder for /alert stats — predictive accuracy dashboard."""
import logging

from rocketstocks.core.content.models import (
    AlertStatsData,
    COLOR_INDIGO,
    EmbedField,
    EmbedSpec,
)

logger = logging.getLogger(__name__)


def _rate_str(rate: float | None) -> str:
    return f"{rate:.1f}%" if rate is not None else "n/a"


def _count_str(d: dict) -> str:
    total = d.get('total', 0)
    confirmed = d.get('confirmed', 0)
    expired = d.get('expired', 0)
    pending = d.get('pending', 0)
    return f"{total} total · {confirmed} confirmed · {expired} expired · {pending} pending"


def _outcome_str(agg: dict, key: str) -> str:
    entry = agg.get(key, {})
    mean = entry.get('mean')
    pos_rate = entry.get('positive_rate')
    count = entry.get('count', 0)
    if count == 0:
        return "no data"
    parts = []
    if mean is not None:
        parts.append(f"avg {mean:+.2f}%")
    if pos_rate is not None:
        parts.append(f"{pos_rate:.0f}% positive")
    parts.append(f"n={count}")
    return " · ".join(parts)


class AlertStats:
    """Predictive accuracy dashboard built from existing popularity_surges and market_signals data."""

    def __init__(self, data: AlertStatsData):
        self.data = data

    def build(self) -> EmbedSpec:
        logger.debug("Building AlertStats embed...")

        fields: list[EmbedField] = []

        # --- Alert volume ---
        if self.data.alert_counts:
            count_lines = [
                f"• **{atype.replace('_', ' ').title()}**: {cnt}"
                for atype, cnt in sorted(self.data.alert_counts.items())
            ]
            fields.append(EmbedField(
                name="Alerts Fired",
                value="\n".join(count_lines) or "None",
                inline=False,
            ))

        # --- Popularity surge confidence ---
        sc = self.data.surge_confidence
        if sc.get('total', 0) > 0:
            fields.append(EmbedField(
                name="Popularity Surge Confirmation Rate",
                value=(
                    f"**{_rate_str(sc.get('rate'))}** of settled surges confirmed\n"
                    f"{_count_str(sc)}"
                ),
                inline=False,
            ))

        # --- Market signal confidence ---
        mc = self.data.signal_confidence
        if mc.get('total', 0) > 0:
            fields.append(EmbedField(
                name="Market Signal Confirmation Rate",
                value=(
                    f"**{_rate_str(mc.get('rate'))}** of settled signals confirmed\n"
                    f"{_count_str(mc)}"
                ),
                inline=False,
            ))

        # --- Price outcome after alert ---
        agg = self.data.price_outcomes.get('aggregate', {})
        if agg:
            outcome_lines = []
            for key, label in [('pct_1d', 'T+1 day'), ('pct_4d', 'T+4 days')]:
                val = _outcome_str(agg, key)
                outcome_lines.append(f"• **{label}**: {val}")
            if outcome_lines:
                fields.append(EmbedField(
                    name="Price Outcome After Alert",
                    value="\n".join(outcome_lines),
                    inline=False,
                ))

        if not fields:
            return EmbedSpec(
                title=f"Alert Stats — {self.data.period_label}",
                description="No alert data found for this period.",
                color=COLOR_INDIGO,
                timestamp=True,
            )

        return EmbedSpec(
            title=f"Alert Stats — {self.data.period_label}",
            description=(
                "Confirmation rates are computed from settled (confirmed/expired) signals only. "
                "Pending signals are excluded from the rate denominator."
            ),
            color=COLOR_INDIGO,
            fields=fields,
            footer="RocketStocks · alert-stats",
            timestamp=True,
        )

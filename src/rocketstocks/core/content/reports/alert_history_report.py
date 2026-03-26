"""Embed builder for /alert history — recent alerts for a ticker with outcomes."""
import logging

from rocketstocks.core.content.models import (
    AlertHistoryData,
    COLOR_INDIGO,
    EmbedField,
    EmbedSpec,
)

logger = logging.getLogger(__name__)

_ALERT_LABELS = {
    'POPULARITY_SURGE':      '🔥 Popularity Surge',
    'MOMENTUM_CONFIRMATION': '⚡ Momentum Confirmed',
    'WATCHLIST_ALERT':       '👁 Watchlist Mover',
    'EARNINGS_ALERT':        '📊 Earnings Mover',
    'MARKET_MOVER':          '📈 Market Mover',
    'VOLUME_ACCUMULATION':   '📊 Volume Accumulation',
    'BREAKOUT':              '🚀 Breakout',
}


def _format_alert_entry(alert: dict) -> str:
    alert_type = alert.get('alert_type', 'UNKNOWN')
    label = _ALERT_LABELS.get(alert_type, alert_type.replace('_', ' ').title())
    date = alert.get('date')
    date_str = str(date) if date else '?'

    parts = [f"**{label}** — {date_str}"]

    # Price outcome if available
    pct_1d = alert.get('pct_1d')
    pct_4d = alert.get('pct_4d')
    if pct_1d is not None or pct_4d is not None:
        outcome_parts = []
        if pct_1d is not None:
            outcome_parts.append(f"T+1d: {pct_1d:+.2f}%")
        if pct_4d is not None:
            outcome_parts.append(f"T+4d: {pct_4d:+.2f}%")
        parts.append("  " + " · ".join(outcome_parts))

    # Key alert data
    alert_data = alert.get('alert_data') or {}
    details = []
    if alert_type == 'POPULARITY_SURGE':
        rank = alert_data.get('current_rank')
        ratio = alert_data.get('mention_ratio')
        if rank:
            details.append(f"rank #{rank}")
        if ratio:
            details.append(f"mentions {ratio:.1f}x")
    elif alert_type == 'MOMENTUM_CONFIRMATION':
        pct = alert_data.get('price_change_since_flag')
        if pct is not None:
            details.append(f"{pct:+.1f}% since flag")
    else:
        pct = alert_data.get('pct_change')
        if pct is not None:
            details.append(f"{pct:+.1f}%")

    if details:
        parts.append("  " + " · ".join(details))

    return "\n".join(parts)


class AlertHistory:
    """Recent alerts for a single ticker, with price outcome data where available."""

    def __init__(self, data: AlertHistoryData):
        self.data = data

    def build(self) -> EmbedSpec:
        logger.debug(f"Building AlertHistory embed for {self.data.ticker}...")

        if not self.data.alerts:
            return EmbedSpec(
                title=f"Alert History — {self.data.ticker}",
                description="No alerts found for this ticker.",
                color=COLOR_INDIGO,
                timestamp=True,
            )

        entries = [_format_alert_entry(a) for a in self.data.alerts]
        # Discord embed field values are limited to 1024 chars; chunk if needed
        fields: list[EmbedField] = []
        chunk: list[str] = []
        running = 0
        for entry in entries:
            cost = len(entry) + (1 if chunk else 0)
            if running + cost > 950:
                fields.append(EmbedField(name="\u200b", value="\n\n".join(chunk), inline=False))
                chunk = [entry]
                running = len(entry)
            else:
                chunk.append(entry)
                running += cost
        if chunk:
            fields.append(EmbedField(name="\u200b", value="\n\n".join(chunk), inline=False))

        total = self.data.count
        shown = len(self.data.alerts)
        footer_note = f"Showing {shown} of {total} alerts" if total > shown else f"{total} alert(s)"

        return EmbedSpec(
            title=f"Alert History — {self.data.ticker}",
            description=f"Recent alerts with price outcome at T+1d and T+4d where data is available.",
            color=COLOR_INDIGO,
            fields=fields,
            footer=f"RocketStocks · {footer_note}",
            timestamp=True,
        )

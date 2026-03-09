import logging
from collections import defaultdict

from rocketstocks.core.content.models import (
    AlertSummaryData,
    COLOR_INDIGO,
    EmbedField,
    EmbedSpec,
)

logger = logging.getLogger(__name__)

_ALERT_TYPE_LABELS = {
    'MARKET_MOVER':           'Market Movers',
    'MARKET_ALERT':           'Market Alerts',
    'WATCHLIST_ALERT':        'Watchlist Alerts',
    'EARNINGS_ALERT':         'Earnings Alerts',
    'POPULARITY_SURGE':       'Popularity Surges',
    'MOMENTUM_CONFIRMATION':  'Momentum Confirmations',
}
_ALERT_TYPE_ORDER = [
    'MARKET_MOVER', 'MARKET_ALERT', 'WATCHLIST_ALERT',
    'EARNINGS_ALERT', 'POPULARITY_SURGE', 'MOMENTUM_CONFIRMATION',
]

EMBED_CHAR_BUDGET = 5500


def _format_line(ticker: str, alert_data: dict, alert_type: str) -> str:
    if alert_type in ('MARKET_MOVER', 'MARKET_ALERT'):
        pct = alert_data.get('pct_change')
        score = alert_data.get('composite_score')
        signal = alert_data.get('dominant_signal', '')
        pct_str = f"{pct:+.1f}%" if pct is not None else 'n/a'
        score_str = f"score: {score:.2f}" if score is not None else ''
        parts = [f"• {ticker}", pct_str]
        if score_str:
            parts.append(score_str)
        if signal:
            parts.append(signal)
        return '  '.join(parts)

    if alert_type in ('WATCHLIST_ALERT', 'EARNINGS_ALERT'):
        pct = alert_data.get('pct_change')
        zscore = alert_data.get('zscore')
        pct_str = f"{pct:+.1f}%" if pct is not None else 'n/a'
        z_str = f"z: {zscore:.1f}" if zscore is not None else ''
        parts = [f"• {ticker}", pct_str]
        if z_str:
            parts.append(z_str)
        return '  '.join(parts)

    if alert_type == 'POPULARITY_SURGE':
        rank = alert_data.get('current_rank')
        rank_change = alert_data.get('rank_change')
        ratio = alert_data.get('mention_ratio')
        rank_str = f"rank ▲{rank_change}" if rank_change is not None else ''
        ratio_str = f"mentions ×{ratio:.1f}" if ratio is not None else ''
        parts = [f"• {ticker}"]
        if rank_str:
            parts.append(rank_str)
        if ratio_str:
            parts.append(ratio_str)
        return '  '.join(parts)

    if alert_type == 'MOMENTUM_CONFIRMATION':
        pct = alert_data.get('price_change_since_flag')
        pct_str = f"{pct:+.1f}% since flag" if pct is not None else 'n/a'
        return f"• {ticker}  {pct_str}"

    # Fallback for unknown types
    return f"• {ticker}"


def _build_field_value(lines: list[str], limit: int = 1000) -> str:
    """Join lines up to limit chars; append '… +N more' if truncated."""
    output = []
    running = 0
    for i, line in enumerate(lines):
        addition = len(line) + (1 if output else 0)  # +1 for newline separator
        if running + addition > limit:
            remaining = len(lines) - i
            output.append(f"… +{remaining} more")
            break
        output.append(line)
        running += addition
    return "\n".join(output)


class AlertSummary:
    """Digest of all alerts since a given datetime, grouped by alert type."""

    def __init__(self, data: AlertSummaryData):
        self.data = data

    def build(self) -> EmbedSpec:
        logger.debug("Building AlertSummary embed...")
        title = f"Alert Summary — {self.data.label}"

        if not self.data.alerts:
            return EmbedSpec(
                title=title,
                description="No alerts found for this period.",
                color=COLOR_INDIGO,
                timestamp=True,
            )

        # Group by alert_type
        groups: dict[str, list] = defaultdict(list)
        for alert in self.data.alerts:
            groups[alert['alert_type']].append(alert)

        fields = []
        running_total = len(title)

        for alert_type in _ALERT_TYPE_ORDER:
            group = groups.get(alert_type)
            if not group:
                continue

            label = _ALERT_TYPE_LABELS.get(alert_type, alert_type)
            lines = [
                _format_line(a['ticker'], a.get('alert_data') or {}, alert_type)
                for a in group
            ]
            field_value = _build_field_value(lines)
            field_name = f"{label} ({len(group)})"

            cost = len(field_name) + len(field_value)
            if running_total + cost > EMBED_CHAR_BUDGET:
                fields.append(EmbedField(
                    name="⚠️ Truncated",
                    value="Too many alerts to display. Narrow the time range.",
                    inline=False,
                ))
                break
            running_total += cost
            fields.append(EmbedField(name=field_name, value=field_value, inline=False))

        return EmbedSpec(
            title=title,
            description="",
            color=COLOR_INDIGO,
            fields=fields,
            timestamp=True,
        )

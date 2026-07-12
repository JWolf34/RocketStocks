"""Alert → Embed, from the alert row alone (ADR-0024: no serve, no market-data
reads, no post-time enrichment). Minimal by design — field-level layout is
deferred to the post-Phase-2 alert-presentation spec (ADR-0026); the payload
numbers are shown raw (ADR-0026 transparency).

A render exception falls back to the generic embed: an alert can be
render-ugly, never render-poisoned (spec pin #5a).
"""

import logging

import discord

from rs_persistence import PendingAlert

logger = logging.getLogger(__name__)


def render(pending: PendingAlert) -> discord.Embed:
    try:
        return _render_full(pending)
    except Exception:
        logger.exception(
            "render failed for alert id=%d — posting the generic fallback embed", pending.id
        )
        return _render_generic(pending)


def _render_full(pending: PendingAlert) -> discord.Embed:
    alert = pending.alert
    embed = discord.Embed(
        title=f"{alert.symbol} · {alert.alert_type}",
        timestamp=alert.triggered_at,
    )
    for key, value in alert.payload.items():
        embed.add_field(name=str(key), value=str(value))
    embed.set_footer(text=f"alert #{pending.id}")
    return embed


def _render_generic(pending: PendingAlert) -> discord.Embed:
    # Deliberately dumb: no per-field iteration, payload as one text blob —
    # nothing here depends on the payload's shape.
    alert = pending.alert
    embed = discord.Embed(
        title=f"{alert.symbol} · {alert.alert_type}",
        description=str(alert.payload),
        timestamp=alert.triggered_at,
    )
    embed.set_footer(text=f"alert #{pending.id}")
    return embed

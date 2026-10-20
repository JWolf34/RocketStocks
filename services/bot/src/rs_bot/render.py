"""Alert -> Embed, from the alert row plus the current base rate. The bot never
calls a vendor: every fact was written into the payload at alert time.

An attention-surge payload is rendered typed; any other alert type, a payload
that does not parse, or any render exception falls back to the generic embed:
an alert can be render-ugly, never render-poisoned.

Discord caps an embed at 25 fields, 256-char names, 1024-char values and 6000
characters in all. Fields are clipped and, if the total is still over, dropped
from the end, so the same alert always renders the same embed.
"""

import logging

import discord

from rs_domain import (
    AttentionSurgePayload,
    BaseRate,
    BaseRateInput,
    FactUnavailable,
)
from rs_persistence import PendingAlert

logger = logging.getLogger(__name__)

ATTENTION_SURGE = "attention_surge"

MAX_FIELDS = 25
MAX_NAME = 256
MAX_VALUE = 1024
MAX_DESCRIPTION = 4096
MAX_FOOTER = 2048
MAX_TOTAL = 6000
ELLIPSIS = "..."


def render(
    pending: PendingAlert, base_rate: BaseRate | None = None, *, base_rate_failed: bool = False
) -> discord.Embed:
    """`base_rate_failed` says the read errored, so "none measured yet" is not claimed."""
    try:
        return _render_full(pending, base_rate, base_rate_failed)
    except Exception:
        logger.exception(
            "render failed for alert id=%d - posting the generic fallback embed", pending.id
        )
        return _render_generic(pending)


def _render_full(
    pending: PendingAlert, base_rate: BaseRate | None, base_rate_failed: bool
) -> discord.Embed:
    alert = pending.alert
    if alert.alert_type != ATTENTION_SURGE:
        return _render_generic(pending)
    payload = AttentionSurgePayload.model_validate(alert.payload)
    # Most important first: the fit drops from the end.
    fields = [
        ("Why it fired", _rule_text(payload)),
        ("Price", _price_text(payload)),
        ("Volume", _volume_text(payload)),
        ("Base rate", _base_rate_text(base_rate, base_rate_failed)),
        ("Next earnings", _earnings_text(payload)),
        ("Benchmark", _benchmark_text(payload)),
        ("Recent 8-Ks", _filings_text(payload)),
    ]
    parts = [f"alert #{pending.id}", f"rule v{payload.rule_version}"]
    if alert.alert_price is not None:
        parts.append(f"alert price ${alert.alert_price:,.2f}")
    if alert.quote_time is not None:
        parts.append(f"quote {alert.quote_time:%Y-%m-%d %H:%M} UTC")
    embed = discord.Embed(title=f"{alert.symbol} · attention surge", timestamp=alert.triggered_at)
    embed.set_footer(text=_clip(" | ".join(parts), MAX_FOOTER))
    for name, value in _fit(embed, fields):
        embed.add_field(name=name, value=value, inline=False)
    return embed


def _render_generic(pending: PendingAlert) -> discord.Embed:
    # Deliberately dumb: no per-field iteration, payload as one text blob -
    # nothing here depends on the payload's shape.
    alert = pending.alert
    embed = discord.Embed(
        title=_clip(f"{alert.symbol} · {alert.alert_type}", MAX_NAME),
        description=_clip(str(alert.payload), MAX_DESCRIPTION),
        timestamp=alert.triggered_at,
    )
    embed.set_footer(text=f"alert #{pending.id}")
    return embed


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - len(ELLIPSIS)] + ELLIPSIS


def _fit(embed: discord.Embed, fields: list[tuple[str, str]]) -> list[tuple[str, str]]:
    fitted = [(_clip(n, MAX_NAME), _clip(v, MAX_VALUE)) for n, v in fields[:MAX_FIELDS]]
    used = len(embed.title or "") + len(embed.description or "") + len(embed.footer.text or "")
    while fitted and used + sum(len(n) + len(v) for n, v in fitted) > MAX_TOTAL:
        fitted.pop()
    return fitted


def _unavailable(fact: FactUnavailable) -> str:
    return f"unavailable: {fact.reason}"


def _rule_text(payload: AttentionSurgePayload) -> str:
    lines = []
    for rule in payload.fired:
        if rule == "mention_surge":
            lines.append(
                f"Mention surge: {payload.mentions} mentions vs {payload.mentions_24h_ago} 24h ago "
                f"({payload.mention_ratio or 0:.1f}x; rule needs {payload.mention_multiplier:g}x "
                f"from at least {payload.mention_min_prior})"
            )
        else:
            lines.append(
                f"Rank jump: #{payload.rank} from #{payload.rank_24h_ago} 24h ago "
                f"(gained {payload.rank_gain})"
            )
    lines.append(f"Polled {payload.polled_at:%Y-%m-%d %H:%M} UTC, rank #{payload.rank}")
    return "\n".join(lines)


def _price_text(payload: AttentionSurgePayload) -> str:
    fact = payload.facts.price_move
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    return (
        f"${fact.last_price:,.2f} ({fact.change_pct:+.2f}% vs previous close "
        f"${fact.previous_close:,.2f}), quote {fact.quote_time:%Y-%m-%d %H:%M} UTC"
    )


def _volume_text(payload: AttentionSurgePayload) -> str:
    fact = payload.facts.volume
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    text = (
        f"{fact.volume:,} vs {fact.average_volume:,.0f} average over the last {fact.sessions} "
        f"sessions ({fact.ratio:.1f}x)"
    )
    return text + (" - partial day, the final multiple will be higher" if fact.intraday else "")


def _earnings_text(payload: AttentionSurgePayload) -> str:
    fact = payload.facts.next_earnings
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    text = f"{fact.date:%Y-%m-%d}"
    if fact.time_of_day:
        text += f" ({fact.time_of_day.replace('_', ' ')})"
    return text + (" - estimated" if fact.estimated else "")


def _benchmark_text(payload: AttentionSurgePayload) -> str:
    fact = payload.facts.benchmark
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    return f"{fact.symbol} ${fact.price:,.2f} at {fact.quote_time:%Y-%m-%d %H:%M} UTC"


def _filings_text(payload: AttentionSurgePayload) -> str:
    fact = payload.facts.recent_filings
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    if not fact.filings:
        return f"none in the last {fact.window_days} days"
    lines = [
        f"[{f.form} {f.filing_date:%Y-%m-%d}"
        + (f" items {', '.join(f.items)}" if f.items else "")
        + f"]({f.url})"
        for f in fact.filings
    ]
    shown: list[str] = []
    for index, line in enumerate(lines):
        # Reserve room for the "+N more" tail so the tail is never the part cut off.
        tail = f"+{len(lines) - index - 1} more"
        if shown and len("\n".join([*shown, line, tail])) > MAX_VALUE:
            return "\n".join([*shown, f"+{len(lines) - index} more"])
        shown.append(line)
    return "\n".join(shown)


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.0f}%"


def _excess(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:+.2f}%"


def _base_rate_text(base_rate: BaseRate | None, failed: bool) -> str:
    if failed:
        return "unavailable: the base rate could not be read"
    if base_rate is None:
        return "No base rate has been measured for this rule version yet."
    label = (
        "measured on past alerts"
        if base_rate.input is BaseRateInput.LOGGED
        else "reconstructed from history"
    )
    unpriced = f", {base_rate.n_unpriced} unpriced" if base_rate.n_unpriced else ""
    horizons = (
        (1, base_rate.hit_rate_1d, base_rate.median_excess_1d),
        (5, base_rate.hit_rate_5d, base_rate.median_excess_5d),
        (10, base_rate.hit_rate_10d, base_rate.median_excess_10d),
    )
    rows = [
        f"{days}d: hit rate {_pct(hit)}, median excess vs SPY {_excess(median)}"
        for days, hit, median in horizons
    ]
    header = f"{label} (input: {base_rate.input.value}), N={base_rate.n}{unpriced}"
    return "\n".join([header, *rows, base_rate.survivorship_caveat])

"""Alert -> Embed, from the alert row plus the current track record. The bot never
calls a vendor: every fact was written into the payload at alert time.

A popularity-surge payload is rendered typed; any other Trigger, a payload
that does not parse, or any render exception falls back to the generic embed:
an alert can be render-ugly, never render-poisoned.

Discord caps an embed at 25 fields, 256-char names, 1024-char values and 6000
characters in all. Fields are clipped and, if the total is still over, dropped
from the end, so the same alert always renders the same embed.
"""

import logging
import re
from collections.abc import Sequence

import discord

from rs_domain import (
    FactUnavailable,
    PopularitySurgePayload,
    TrackRecord,
)
from rs_persistence import PendingAlert

logger = logging.getLogger(__name__)

POPULARITY_SURGE = "popularity_surge"

MAX_FIELDS = 25
MAX_NAME = 256
MAX_VALUE = 1024
MAX_DESCRIPTION = 4096
MAX_FOOTER = 2048
MAX_TOTAL = 6000
ELLIPSIS = "..."

# Every embed opens its footer with the row id; delivery finds the bot's own message for a row in
# channel history by it, because history reads do not reliably carry a send's nonce.
_FOOTER_ID = re.compile(r"alert #(\d+)(?: \||\Z)")


def footer_marker(row_id: int) -> str:
    return f"alert #{row_id}"


def marked_row_id(embed: discord.Embed) -> int | None:
    """The row id an embed's footer names, or None when it carries none."""
    match = _FOOTER_ID.match(embed.footer.text or "")
    return None if match is None else int(match.group(1))


def render(
    pending: PendingAlert, track_record: Sequence[TrackRecord] = (), *, track_record_failed: bool = False
) -> discord.Embed:
    """`track_record` is the Trigger version's rows, one per horizon. `track_record_failed`
    says the read errored, so "none measured yet" is not claimed."""
    try:
        return _render_full(pending, track_record, track_record_failed)
    except Exception:
        logger.exception(
            "render failed for alert id=%d - posting the generic fallback embed", pending.id
        )
        return render_generic(pending)


def _render_full(
    pending: PendingAlert, track_record: Sequence[TrackRecord], track_record_failed: bool
) -> discord.Embed:
    alert = pending.alert
    if alert.trigger != POPULARITY_SURGE:
        return render_generic(pending)
    payload = PopularitySurgePayload.model_validate(alert.payload)
    # Most important first: the fit drops from the end.
    fields = [
        ("Why it fired", _rule_text(payload)),
        ("Price", _price_text(payload)),
        ("Volume", _volume_text(payload)),
        ("Track record", _track_record_text(track_record, track_record_failed)),
        ("Next earnings", _earnings_text(payload)),
        ("Benchmark", _benchmark_text(payload)),
    ]
    parts = [footer_marker(pending.id), f"rule v{payload.rule_version}"]
    if alert.alert_price is not None:
        parts.append(f"alert price ${alert.alert_price:,.2f}")
    if alert.quote_time is not None:
        parts.append(f"quote {alert.quote_time:%Y-%m-%d %H:%M} UTC")
    embed = discord.Embed(title=f"{alert.symbol} · Popularity surge", timestamp=alert.triggered_at)
    embed.set_footer(text=_clip(" | ".join(parts), MAX_FOOTER))
    for name, value in _fit(embed, fields):
        embed.add_field(name=name, value=value, inline=False)
    return embed


def render_generic(pending: PendingAlert) -> discord.Embed:
    # Deliberately dumb: no per-field iteration, payload as one text blob -
    # nothing here depends on the payload's shape.
    alert = pending.alert
    embed = discord.Embed(
        title=_clip(f"{alert.symbol} · {alert.trigger}", MAX_NAME),
        description=_clip(str(alert.payload), MAX_DESCRIPTION),
        timestamp=alert.triggered_at,
    )
    embed.set_footer(text=footer_marker(pending.id))
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


def _rule_text(payload: PopularitySurgePayload) -> str:
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


def _price_text(payload: PopularitySurgePayload) -> str:
    fact = payload.facts.price_move
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    return (
        f"${fact.last_price:,.2f} ({fact.change_pct:+.2f}% vs previous close "
        f"${fact.previous_close:,.2f}), quote {fact.quote_time:%Y-%m-%d %H:%M} UTC"
    )


def _volume_text(payload: PopularitySurgePayload) -> str:
    fact = payload.facts.volume
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    text = (
        f"{fact.volume:,} vs {fact.average_volume:,.0f} average over the last {fact.sessions} "
        f"sessions ({fact.ratio:.1f}x)"
    )
    return text + (" - partial day, the final multiple will be higher" if fact.intraday else "")


def _earnings_text(payload: PopularitySurgePayload) -> str:
    fact = payload.facts.next_earnings
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    text = f"{fact.date:%Y-%m-%d}"
    if fact.time_of_day:
        text += f" ({fact.time_of_day.replace('_', ' ')})"
    return text + (" - estimated" if fact.estimated else "")


def _benchmark_text(payload: PopularitySurgePayload) -> str:
    fact = payload.facts.benchmark
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    return f"{fact.symbol} ${fact.price:,.2f} at {fact.quote_time:%Y-%m-%d %H:%M} UTC"


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.0f}%"


def _excess(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:+.2f}%"


def _multiple(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}x"


def _horizon_text(record: TrackRecord) -> str:
    if record.too_few_cases:
        return f"{record.horizon}d: too few cases ({record.outcomes} Outcomes)"
    unpriced = f", {record.unpriced} could not be priced" if record.unpriced else ""
    return (
        f"{record.horizon}d: {record.outcomes} Outcomes{unpriced}; "
        f"{_pct(record.share_at_2x)} moved 2x typical or more "
        f"(same stocks on ordinary days: {_pct(record.ordinary_share)}), "
        f"median {_multiple(record.median_multiple)} typical, "
        f"median excess vs SPY {_excess(record.median_excess)}"
    )


def _track_record_text(track_record: Sequence[TrackRecord], failed: bool) -> str:
    if failed:
        return "unavailable: the track record could not be read"
    if not track_record:
        return "No track record has been measured for this rule version yet."
    first = track_record[0]
    header = f"{first.cases} cases, {first.live_cases} live (the rest replayed from history)"
    return "\n".join([header, *(_horizon_text(record) for record in track_record)])

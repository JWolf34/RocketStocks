"""Alert -> Embed, from the alert row plus the current Track record. The bot never
calls a vendor: every fact was written into the payload at alert time.

A popularity-surge Alert is a templated talking point, the reasons, the price, the Track
record and the next Catalyst; a reply in its thread (`reply_to` set) is a short follow-up
saying what escalated. Every time is Discord timestamp markup, so each reader sees their own
zone; a date without a time is shown as a date. Any other Trigger, a payload that does not
parse, or any render exception falls back to the generic embed: an alert can be render-ugly,
never render-poisoned.

Discord caps an embed at 25 fields, 256-char names, 1024-char values and 6000
characters in all. Fields are clipped and, if the total is still over, dropped
from the end, so the same alert always renders the same embed.
"""

import logging
import re
from collections.abc import Sequence
from datetime import datetime

import discord

from rs_domain import (
    TOO_FEW_CASES_BELOW,
    EscalationPayload,
    FactUnavailable,
    PopularitySurgePayload,
    TrackRecord,
)
from rs_persistence import PendingAlert

logger = logging.getLogger(__name__)

POPULARITY_SURGE = "popularity_surge"
TRIGGER_TITLE = "Popularity surge"

_RULE_NAME = {"mention_surge": "Mention surge", "rank_jump": "Rank jump"}
_TIME_OF_DAY = {"pre_market": "before the open", "after_hours": "after the close"}

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
    if alert.reply_to is not None:
        return _render_reply(pending, EscalationPayload.model_validate(alert.payload))
    payload = PopularitySurgePayload.model_validate(alert.payload)
    # Most important first: the fit drops from the end.
    fields = [
        ("Why it fired", _rule_text(payload)),
        ("Price", _price_text(payload)),
        ("Track record", _track_record_text(track_record, track_record_failed)),
        ("Next Catalyst", _earnings_text(payload)),
    ]
    embed = discord.Embed(
        title=f"{alert.symbol} · {TRIGGER_TITLE}",
        description=_clip(_talking_point(alert.symbol, payload), MAX_DESCRIPTION),
        timestamp=alert.triggered_at,
    )
    embed.set_footer(text=_clip(f"{footer_marker(pending.id)} | {POPULARITY_SURGE} v{payload.rule_version}", MAX_FOOTER))
    for name, value in _fit(embed, fields):
        embed.add_field(name=name, value=value, inline=False)
    return embed


def _render_reply(pending: PendingAlert, payload: EscalationPayload) -> discord.Embed:
    """A short follow-up in the Alert's thread: what escalated and when."""
    alert = pending.alert
    lines = []
    if payload.new_rules:
        names = " and ".join(_RULE_NAME[rule].lower() for rule in payload.new_rules)
        lines.append(f"The {names} rule also fired now; the Alert did not carry it.")
    if payload.strength_after is not None:
        lines.append(
            f"{alert.symbol} is more Unusual than at the Alert: percentile "
            f"{payload.strength_after * 100:.0f} of this Trigger's hits over the last 30 days"
            + (
                "."
                if payload.strength_before is None
                else f", up from {payload.strength_before * 100:.0f}."
            )
        )
    if not lines:
        lines.append("It escalated; the details were not stored.")
    lines.append(f"As of {_when(payload.polled_at)}")
    embed = discord.Embed(
        title=f"{alert.symbol} · escalated",
        description=_clip("\n".join(lines), MAX_DESCRIPTION),
        timestamp=alert.triggered_at,
    )
    embed.set_footer(text=f"{footer_marker(pending.id)} | reply to #{alert.reply_to}")
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


def _when(moment: datetime) -> str:
    """A moment as Discord timestamp markup, so each reader sees their own zone."""
    unix = int(moment.timestamp())
    return f"<t:{unix}:f> (<t:{unix}:R>)"


def _talking_point(symbol: str, payload: PopularitySurgePayload) -> str:
    """One templated line on what is Unusual now. It describes; it never advises."""
    parts = []
    prior = payload.mentions_24h_ago
    if "mention_surge" in payload.fired and prior:
        ratio = payload.mention_ratio or payload.mentions / prior
        parts.append(f"mentions are {ratio:.1f}x ({prior} to {payload.mentions})")
    if "rank_jump" in payload.fired and payload.rank_24h_ago:
        parts.append(f"its rank moved from #{payload.rank_24h_ago} to #{payload.rank}")
    if not parts:
        return f"{symbol} is Unusual on Popularity: rank #{payload.rank} with {payload.mentions} mentions."
    return f"{symbol} is being discussed far more than a day ago: {' and '.join(parts)}."


def _rule_text(payload: PopularitySurgePayload) -> str:
    lines = [f"Trigger: {TRIGGER_TITLE} v{payload.rule_version}"]
    for rule in payload.fired:
        if rule == "mention_surge":
            lines.append(
                f"- {_RULE_NAME[rule]}: {payload.mentions} mentions vs {payload.mentions_24h_ago} a day ago "
                f"({payload.mention_ratio or 0:.1f}x; the rule needs {payload.mention_multiplier:g}x "
                f"from at least {payload.mention_min_prior})"
            )
        else:
            lines.append(
                f"- {_RULE_NAME[rule]}: #{payload.rank_24h_ago} to #{payload.rank} (gained {payload.rank_gain})"
            )
    lines.append(f"Popularity as of {_when(payload.polled_at)}")
    return "\n".join(lines)


def _price_text(payload: PopularitySurgePayload) -> str:
    fact = payload.facts.price_move
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    return (
        f"${fact.last_price:,.2f} ({fact.change_pct:+.2f}% vs previous close ${fact.previous_close:,.2f})\n"
        f"As of {_when(fact.quote_time)}"
    )


def _earnings_text(payload: PopularitySurgePayload) -> str:
    fact = payload.facts.next_earnings
    if isinstance(fact, FactUnavailable):
        return _unavailable(fact)
    # A date has no time zone: shown as a date, never as timestamp markup.
    text = f"Earnings on {fact.date:%b} {fact.date.day}, {fact.date.year}"
    if fact.time_of_day in _TIME_OF_DAY:
        text += f" ({_TIME_OF_DAY[fact.time_of_day]})"
    return text + (", estimated" if fact.estimated else "")


def _measures(record: TrackRecord, *, with_unpriced: bool = False) -> list[tuple[str, str]]:
    """One horizon's cells by row; "n/a" where nothing backs the number."""

    def cell(value: float | None, scale: float, fmt: str, unit: str) -> str:
        return "n/a" if value is None else f"{format(value * scale, fmt)}{unit}"

    return [
        ("vs SPY", cell(record.median_excess, 100, "+.1f", "%")),
        ("2x+", cell(record.share_at_2x, 100, ".0f", "%")),
        ("ordinary", cell(record.ordinary_share, 100, ".0f", "%")),
        ("x typical", cell(record.median_multiple, 1, ".1f", "x")),
        ("Outcomes", str(record.outcomes)),
        *([("unpriced", str(record.unpriced))] if with_unpriced else []),
    ]


def _table(records: Sequence[TrackRecord]) -> str:
    """Horizons as columns, the measures as rows, in a code block so it lines up on a phone."""
    # The unpriced row appears for every horizon once any has one.
    with_unpriced = any(record.unpriced for record in records)
    columns = [_measures(record, with_unpriced=with_unpriced) for record in records]
    rows = [(name, [column[i][1] for column in columns]) for i, (name, _) in enumerate(columns[0])]
    label_width = max(len(name) for name, _ in rows)
    cell_width = max(len(cell) for _, cells in rows for cell in cells) + 2
    header = " " * label_width + "".join(f"{record.horizon}d".rjust(cell_width) for record in records)
    body = [name.ljust(label_width) + "".join(cell.rjust(cell_width) for cell in cells) for name, cells in rows]
    return "```\n" + "\n".join([header, *body]) + "\n```"


def _track_record_text(track_record: Sequence[TrackRecord], failed: bool) -> str:
    if failed:
        return "unavailable: the Track record could not be read"
    if not track_record:
        return "Not measured yet for this Trigger version."
    first = track_record[0]
    enough = [r for r in track_record if not r.too_few_cases]
    lines = [f"{first.cases} cases, {first.live_cases} live (the rest replayed from history)"]
    if enough:
        lines.append(_table(enough))
        lines.append(
            "vs SPY: median excess return. 2x+: share of cases that moved 2x the stock's typical "
            "move or more, against the same stocks' ordinary share. x typical: median multiple."
        )
        if any(cell == "n/a" for r in enough for _, cell in _measures(r)):
            lines.append("n/a: not measurable (too little price history or missing bars).")
    lines.extend(
        f"{r.horizon}d: too few cases ({r.outcomes} Outcomes, {TOO_FEW_CASES_BELOW} needed)"
        for r in track_record
        if r.too_few_cases
    )
    return "\n".join(lines)

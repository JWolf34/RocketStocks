"""Find the zone v1 wrote its naive timestamps in, from live overlap.

While v1 and v2 both poll ApeWisdom, the same listing lands in both tables. For
each candidate zone, convert every v1 timestamp to UTC and ask whether v2 holds
the same (ticker, rank, mentions, mentions_24h_ago) in that half-hour bucket or
an adjacent one. The right zone matches nearly everything; a wrong one matches
almost nothing.
"""

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from rs_backfill.dump import V1Row
from rs_backfill.zones import (
    BUCKET,
    CANDIDATE_ZONES,
    floor_bucket,
    is_local_zone,
    naive_to_utc,
    resolve_zone,
)

DEFAULT_THRESHOLD = 0.99
# One full poll is ~1000 tickers; fewer rows than that cannot tell zones apart.
DEFAULT_MIN_OVERLAP = 1000

# Largest |offset| any naive timestamp could carry; bounds the rows worth converting.
_MAX_OFFSET = timedelta(hours=14)

SPRING_FORWARD_DAY = date(2026, 3, 8)
FALL_BACK_DAY = date(2025, 11, 2)

LiveKey = tuple[str, datetime]
LiveValue = tuple[int, int, int | None]


@dataclass
class LiveIndex:
    """v2 `live` rows reduced to what matching needs."""

    rows: dict[LiveKey, LiveValue] = field(default_factory=dict)
    boundaries: set[datetime] = field(default_factory=set)

    def add(
        self, polled_at: datetime, ticker: str, rank: int, mentions: int, mentions_24h_ago: int | None
    ) -> None:
        self.rows[(ticker, polled_at)] = (rank, mentions, mentions_24h_ago)
        self.boundaries.add(polled_at)


@dataclass(frozen=True)
class Candidate:
    zone: str
    overlap_rows: int
    matched_rows: int
    overlap_start: datetime | None
    overlap_end: datetime | None

    @property
    def rate(self) -> float:
        return self.matched_rows / self.overlap_rows if self.overlap_rows else 0.0


@dataclass(frozen=True)
class DstCrossCheck:
    """What the dump says about the two DST days named in the spec."""

    spring_covered: bool
    spring_forward_rows: int  # distinct v1 timestamps in 02:00-03:00 on 2026-03-08
    fall_covered: bool
    fall_back_timestamps: frozenset[datetime]  # distinct v1 timestamps on 2025-11-02

    @property
    def looks_local(self) -> bool | None:
        """True when the nonexistent spring-forward hour is empty (a local zone's
        signature), False when it holds rows, None when the dump does not span it."""
        if not self.spring_covered:
            return None
        return self.spring_forward_rows == 0

    def repeated_hour_lost(self, zone: str) -> list[datetime] | None:
        """UTC boundaries of the fall-back hour's second pass that hold no v1 row
        under `zone`: v1's (datetime, rank) key dropped them. None when the dump
        does not span the day or the zone has no repeated hour."""
        if not self.fall_covered or not is_local_zone(zone):
            return None
        tz = resolve_zone(zone)
        converted = {naive_to_utc(t, tz) for t in self.fall_back_timestamps}
        second_pass_start = datetime(2025, 11, 2, 1, 0, tzinfo=tz, fold=1).astimezone(UTC)
        boundaries = [second_pass_start + BUCKET * i for i in range(2)]
        return [b for b in boundaries if b not in converted]


@dataclass(frozen=True)
class OffsetReport:
    candidates: list[Candidate]
    winner: Candidate | None
    reason: str
    dst: DstCrossCheck
    threshold: float
    min_overlap: int


def verify_offset(
    rows: Iterable[V1Row],
    live: LiveIndex,
    *,
    threshold: float = DEFAULT_THRESHOLD,
    min_overlap: int = DEFAULT_MIN_OVERLAP,
) -> OffsetReport:
    zones = {name: resolve_zone(name) for name in CANDIDATE_ZONES}
    overlap = dict.fromkeys(zones, 0)
    matched = dict.fromkeys(zones, 0)
    first: dict[str, datetime] = {}
    last: dict[str, datetime] = {}

    spring_timestamps: set[datetime] = set()
    fall_timestamps: set[datetime] = set()
    if live.boundaries:
        window_start = min(live.boundaries).replace(tzinfo=None) - _MAX_OFFSET
        window_end = max(live.boundaries).replace(tzinfo=None) + _MAX_OFFSET
    else:
        window_start = window_end = datetime.min

    for row in rows:
        day = row.at.date()
        if day == SPRING_FORWARD_DAY:
            spring_timestamps.add(row.at)
        elif day == FALL_BACK_DAY:
            fall_timestamps.add(row.at)
        if row.ticker is None or row.rank is None or row.mentions is None:
            continue
        if not window_start <= row.at <= window_end:
            continue
        ticker = row.ticker.strip().upper()
        wanted = (row.rank, row.mentions, row.mentions_24h_ago)
        for name, tz in zones.items():
            instant = naive_to_utc(row.at, tz)
            if instant is None:
                continue
            bucket = floor_bucket(instant)
            near = (bucket - BUCKET, bucket, bucket + BUCKET)
            if not any(b in live.boundaries for b in near):
                continue
            overlap[name] += 1
            first[name] = min(first.get(name, bucket), bucket)
            last[name] = max(last.get(name, bucket), bucket)
            if any(live.rows.get((ticker, b)) == wanted for b in near):
                matched[name] += 1

    candidates = [
        Candidate(name, overlap[name], matched[name], first.get(name), last.get(name)) for name in zones
    ]
    dst = DstCrossCheck(
        spring_covered=_spans(spring_timestamps, 2, 3),
        spring_forward_rows=sum(1 for t in spring_timestamps if 2 <= t.hour < 3),
        fall_covered=_spans(fall_timestamps, 1, 2),
        fall_back_timestamps=frozenset(fall_timestamps),
    )
    winner, reason = _decide(candidates, dst, threshold, min_overlap)
    return OffsetReport(candidates, winner, reason, dst, threshold, min_overlap)


def _spans(timestamps: set[datetime], hour_from: int, hour_to: int) -> bool:
    """The dump has rows both before and after the [hour_from, hour_to) hour."""
    return any(t.hour < hour_from for t in timestamps) and any(t.hour >= hour_to for t in timestamps)


def _decide(
    candidates: list[Candidate], dst: DstCrossCheck, threshold: float, min_overlap: int
) -> tuple[Candidate | None, str]:
    passing = [c for c in candidates if c.overlap_rows >= min_overlap and c.rate >= threshold]
    if not passing:
        best = max(candidates, key=lambda c: (c.rate, c.overlap_rows))
        if best.overlap_rows < min_overlap:
            return None, (
                f"overlap too small: best candidate {best.zone} has {best.overlap_rows} overlapping"
                f" rows, need at least {min_overlap}"
            )
        return None, (
            f"no candidate reached {threshold:.0%}: best is {best.zone} at {best.rate:.2%}"
        )

    evidence = dst.looks_local
    if len(passing) > 1:
        # E.g. America/Chicago and -05:00 agree for a summer-only overlap; the
        # spring-forward hour tells a local zone from a fixed offset.
        if evidence is None:
            zones = ", ".join(c.zone for c in passing)
            return None, (
                f"ambiguous: {zones} all pass and the dump does not span the 2026-03-08"
                " spring-forward hour to tell a local zone from a fixed offset"
            )
        passing = [c for c in passing if is_local_zone(c.zone) == evidence]
        if len(passing) != 1:
            return None, "ambiguous: the DST evidence leaves no single candidate"
    winner = passing[0]
    if evidence is not None and is_local_zone(winner.zone) != evidence:
        kind = "has rows" if not evidence else "is empty"
        return None, (
            f"{winner.zone} matches but contradicts the DST evidence: the 2026-03-08"
            f" spring-forward hour {kind}"
        )
    return winner, f"{winner.zone} matches {winner.rate:.2%} of {winner.overlap_rows} overlapping rows"


def render_report(report: OffsetReport) -> str:
    lines = [f"{'zone':<18}{'overlap':>10}{'matched':>10}{'rate':>9}"]
    for c in report.candidates:
        lines.append(f"{c.zone:<18}{c.overlap_rows:>10}{c.matched_rows:>10}{c.rate:>9.2%}")
    dst = report.dst
    lines.append("")
    lines.append("DST cross-check")
    if dst.spring_covered:
        verdict = "local zone" if dst.looks_local else "UTC or a fixed offset"
        lines.append(
            f"  2026-03-08 spring-forward hour (02:00-03:00): {dst.spring_forward_rows} v1 timestamps"
            f" -> consistent with {verdict}"
        )
    else:
        lines.append("  2026-03-08 spring-forward hour: the dump does not span it")
    if report.winner is None:
        lines.append("  2025-11-02 repeated hour: no winner to evaluate")
    else:
        lost = dst.repeated_hour_lost(report.winner.zone)
        if lost is None:
            lines.append("  2025-11-02 repeated hour: not applicable or the dump does not span it")
        else:
            stamps = ", ".join(t.strftime("%H:%MZ") for t in lost) or "none"
            lines.append(
                f"  2025-11-02 repeated hour under {report.winner.zone}: second-pass boundaries"
                f" with no v1 row (lost to v1's primary key): {stamps}"
            )
    lines.append("")
    lines.append(("VERIFIED: " if report.winner else "NOT VERIFIED: ") + report.reason)
    return "\n".join(lines)

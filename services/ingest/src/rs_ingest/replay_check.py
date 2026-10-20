"""Semantic alert-decision comparison (S007, ADR-0023).

A semantic alert decision is the full rs_domain.Alert — (symbol, alert_type,
triggered_at, payload) — everything the detector decided, nothing the DB added
(id/created_at/posted_at are delivery bookkeeping, excluded by construction).

The diff is a strict multiset comparison over a canonical serialization:
sorted-keys JSON of the model's JSON-mode dump, so payload key order (which
JSONB does not preserve) and container ordering can never manufacture a
difference. Full-model equality means a field mismatch simply lands one entry
in each bucket — there is no third "changed" bucket, and no tolerance path.
"""

import json
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from rs_domain import Alert, BarClosed
from rs_persistence import AlertRepository, build_engine, build_session_factory


def decision_line(alert: Alert) -> str:
    """Canonical one-line JSON for a decision — the multiset identity, the
    export format, and the diff display are all this same serialization."""
    return json.dumps(alert.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class ReplayDiff:
    only_in_live: tuple[Alert, ...]
    only_in_replay: tuple[Alert, ...]

    @property
    def identical(self) -> bool:
        return not self.only_in_live and not self.only_in_replay


def compare(live: Iterable[Alert], replayed: Iterable[Alert]) -> ReplayDiff:
    """Strict multiset diff of two sides' semantic decisions."""
    live_counts = Counter(decision_line(a) for a in live)
    replay_counts = Counter(decision_line(a) for a in replayed)

    def surplus(counts: Counter[str], other: Counter[str]) -> tuple[Alert, ...]:
        extra = counts - other
        return tuple(
            Alert.model_validate_json(line) for line in sorted(extra.elements())
        )

    return ReplayDiff(
        only_in_live=surplus(live_counts, replay_counts),
        only_in_replay=surplus(replay_counts, live_counts),
    )


@dataclass(frozen=True)
class DecisionScope:
    """Which live alert rows a fixture's replay may be compared against: the
    fixture's own symbols over the UTC-day span covering its bar buckets.
    Derived from the fixture, never operator-typed — a scoping typo can't
    silently diff against the wrong day."""

    symbols: frozenset[str]
    start: datetime  # inclusive, UTC midnight of the earliest bucket's day
    end: datetime  # exclusive, UTC midnight after the latest bucket's day


def fixture_scope(events: Sequence[BarClosed]) -> DecisionScope:
    buckets = [event.bar.bucket for event in events]
    first, last = min(buckets), max(buckets)
    start = datetime(first.year, first.month, first.day, tzinfo=UTC)
    end = datetime(last.year, last.month, last.day, tzinfo=UTC) + timedelta(days=1)
    return DecisionScope(
        symbols=frozenset(event.bar.symbol for event in events), start=start, end=end
    )


def dump_decisions(decisions: Iterable[Alert], path: Path | str) -> None:
    """Write decisions as canonical JSONL — the golden-expectations format,
    produced by the same serialization the comparator diffs on."""
    Path(path).write_text(
        "".join(decision_line(alert) + "\n" for alert in decisions), encoding="utf-8"
    )


def load_decisions(path: Path | str) -> list[Alert]:
    text = Path(path).read_text(encoding="utf-8")
    return [Alert.model_validate_json(line) for line in text.splitlines() if line]


async def fetch_decisions(url: str, scope: DecisionScope | None = None) -> list[Alert]:
    """A database's alert rows as semantic decisions, id order.

    One projection, both sides: the live-DB baseline read (scoped to the
    fixture) and the replay-DB read (unscoped — the throwaway DB holds nothing
    else) go through the same AlertRepository.list_decisions, so a projection
    bug can't blind one path silently."""
    engine = build_engine(url)
    try:
        async with build_session_factory(engine)() as session:
            repo = AlertRepository(session)
            if scope is None:
                return await repo.list_decisions()
            return await repo.list_decisions(
                symbols=scope.symbols, start=scope.start, end=scope.end
            )
    finally:
        await engine.dispose()

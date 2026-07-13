"""Replay a recorded session fixture through the real pipeline (ADR-0023).

    # prove the ADR-0008 error boundary on real session data (S003 criterion 30):
    python -m rs_ingest.tools.replay --fixture data/session-fixture-2026-07-06.jsonl --plant-raiser

    # assert replay == live: identical semantic alert decisions (S007):
    python -m rs_ingest.tools.replay --fixture data/session-fixture-2026-07-09.jsonl \\
        --live-db postgresql+psycopg://postgres:t@localhost:5498/rocketstocks

Feeds a recorded BarClosed stream (RecordedEventSource) through Pipeline exactly
as the live daemon does, into a throwaway database the tool creates, migrates,
and drops — so it never touches the acceptance run's rows. Point --admin-db at
any Postgres you can create a database on; the acceptance 5498 *server* is fine —
the replay lands in a separate, auto-dropped database, never the acceptance one.

--plant-raiser adds an always-raising detector alongside the real threshold
detector: the boundary must swallow every raise, so bars still persist and real
alerts still land while detector_errors climbs. Exit 0 / "BOUNDARY HELD" iff every
replayed bar persisted despite a raise on each and no alert write was lost.

Assertion mode (S007): the replay's alert rows are projected to semantic
decisions (rs_ingest.replay_check) and diffed strictly against a baseline —
either --live-db (the live run's rows, auto-filtered to the fixture's own
day/symbols) or --expected (a decisions JSONL previously written by
--export-decisions). Exit 0 iff the multisets are identical, 1 on any
difference. The threshold must match the live run's (printed in the header;
the structural fix is Phase 2's config-version stamp). A nonzero live
alerts_dropped legitimately shows up as "only in replay" — reconcile against
the live run's ERROR logs.
"""

import argparse
import asyncio
import logging
import sys
import uuid
from collections.abc import Sequence
from pathlib import Path

from alembic import command
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import text

from rs_domain import Alert, Bar, Detector
from rs_ingest.detector import ThresholdDetector
from rs_ingest.pipeline import Pipeline
from rs_ingest.replay_check import (
    compare,
    decision_line,
    dump_decisions,
    fetch_decisions,
    fixture_scope,
    load_decisions,
)
from rs_ingest.sources.recording import RecordedEventSource, load_events
from rs_persistence import build_engine, build_session_factory, migration_config

logger = logging.getLogger(__name__)

DEFAULT_ADMIN_DB = "postgresql+psycopg://postgres:t@localhost:5499/postgres"


class _AlwaysRaises:
    """Planted canary: raises on every decision, so the ADR-0008 boundary is
    exercised on every bar of the replay (S003 criterion 30)."""

    def decide(self, window: Sequence[Bar]) -> Alert | None:
        raise RuntimeError("planted boundary probe")


def boundary_held(counters: dict[str, int], n_events: int) -> bool:
    """The boundary held iff every replayed bar persisted despite a detector
    raising on each, and no alert write was lost — and something was actually
    replayed (an empty fixture proves nothing)."""
    return (
        n_events > 0
        and counters["bars_persisted"] == n_events
        and counters["detector_errors"] == n_events
        and counters["alerts_dropped"] == 0
    )


async def replay_fixture(
    fixture: Path, detectors: Sequence[Detector], admin_db: str
) -> tuple[dict[str, int], list[Alert]]:
    """Create a throwaway DB, migrate it, replay the fixture through the pipeline,
    then drop the DB. Returns the pipeline counters and the replay's semantic
    decisions (the alert rows it wrote, read back through the shared projection
    before the drop)."""
    db = f"rs_replay_{uuid.uuid4().hex[:10]}"
    admin = create_sync_engine(admin_db, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{db}"'))
    url = admin_db.rsplit("/", 1)[0] + f"/{db}"
    try:
        command.upgrade(migration_config(url), "head")
        engine = build_engine(url)
        try:
            pipeline = Pipeline(build_session_factory(engine), detectors)
            await pipeline.run(RecordedEventSource(fixture))
        finally:
            await engine.dispose()
        decisions = await fetch_decisions(url)
        return pipeline.counters, decisions
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE "{db}" WITH (FORCE)'))
        admin.dispose()


def comparison_report(
    fixture: Path,
    threshold: int,
    baseline_label: str,
    baseline: Sequence[Alert],
    replayed: Sequence[Alert],
) -> tuple[str, int]:
    """Render the strict multiset comparison and its exit code: 0 iff identical,
    1 on any difference. The threshold rides in the header — the operator's
    proof the replay ran the live run's config (S007; Phase 2's config-version
    stamp is the structural fix)."""
    diff = compare(baseline, replayed)
    verdict = "IDENTICAL" if diff.identical else (
        f"DIFFERENT ({len(diff.only_in_live)} only in live, "
        f"{len(diff.only_in_replay)} only in replay)"
    )
    lines = [
        f"semantic decision check (S007): fixture={fixture} threshold={threshold} baseline={baseline_label}",
        f"baseline={len(baseline)} decisions  replay={len(replayed)} decisions  -> {verdict}",
    ]
    for label, bucket in (("only in live", diff.only_in_live), ("only in replay", diff.only_in_replay)):
        if bucket:
            lines.append(f"{label}:")
            lines.extend(f"  {decision_line(alert)}" for alert in bucket)
    return "\n".join(lines), 0 if diff.identical else 1


async def _run(
    fixture: Path,
    threshold: int,
    plant_raiser: bool,
    admin_db: str,
    live_db: str | None = None,
    expected: Path | None = None,
    export_decisions: Path | None = None,
) -> int:
    events = load_events(fixture)
    n_events = len(events)
    if n_events == 0:
        # Same stance as boundary_held: an empty fixture proves nothing —
        # refuse loudly rather than replay nothing and report success.
        print(f"empty fixture — nothing to replay or assert: {fixture}")
        return 1
    detectors: list[Detector] = [ThresholdDetector(threshold)]
    if plant_raiser:
        detectors.append(_AlwaysRaises())
    counters, decisions = await replay_fixture(fixture, detectors, admin_db)
    logger.info("replayed %d events -> %s", n_events, counters)

    if export_decisions is not None:
        dump_decisions(decisions, export_decisions)
        logger.info("exported %d decisions -> %s", len(decisions), export_decisions)

    if plant_raiser:
        held = boundary_held(counters, n_events)
        print(
            f"error-boundary proof (criterion 30): fixture={n_events} bars  "
            f"bars_persisted={counters['bars_persisted']}  detector_errors={counters['detector_errors']}  "
            f"alerts_emitted={counters['alerts_emitted']}  alerts_dropped={counters['alerts_dropped']}  "
            f"-> {'BOUNDARY HELD' if held else 'FAILED'}"
        )
        return 0 if held else 1

    if live_db is not None:
        baseline = await fetch_decisions(live_db, fixture_scope(events))
        label = "live-db"
    elif expected is not None:
        baseline = load_decisions(expected)
        label = "expected"
    else:
        return 0

    report, code = comparison_report(fixture, threshold, label, baseline, decisions)
    print(report)
    return code


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="replay a recorded fixture through the pipeline")
    parser.add_argument("--fixture", required=True, type=Path, help="recorded JSONL fixture (RS_INGEST_FIXTURE_PATH output)")
    parser.add_argument("--threshold", type=int, default=500_000, help="volume threshold for the real detector (must match the live run's — printed in the result header)")
    parser.add_argument("--admin-db", default=DEFAULT_ADMIN_DB, help="admin DSN to create the throwaway replay DB on")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plant-raiser", action="store_true", help="add an always-raising detector to prove the ADR-0008 boundary")
    mode.add_argument("--live-db", help="baseline DSN: assert replay decisions == the live run's alert rows (auto-filtered to the fixture's day/symbols)")
    mode.add_argument("--expected", type=Path, help="baseline file: assert replay decisions == this decisions JSONL (written by --export-decisions)")
    parser.add_argument("--export-decisions", type=Path, help="write the replay's semantic decisions as canonical JSONL (the golden-expectations format)")
    args = parser.parse_args()
    if not args.fixture.exists():
        parser.error(f"fixture not found: {args.fixture}")
    if args.expected is not None and not args.expected.exists():
        parser.error(f"expected-decisions file not found: {args.expected}")
    return asyncio.run(
        _run(
            args.fixture,
            args.threshold,
            args.plant_raiser,
            args.admin_db,
            live_db=args.live_db,
            expected=args.expected,
            export_decisions=args.export_decisions,
        )
    )


if __name__ == "__main__":
    sys.exit(main())

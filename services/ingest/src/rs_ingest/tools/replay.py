"""Replay a recorded session fixture through the real pipeline (ADR-0023).

    # prove the ADR-0008 error boundary on real session data (S003 criterion 30):
    python -m rs_ingest.tools.replay --fixture data/session-fixture-2026-07-06.jsonl --plant-raiser

Feeds a recorded BarClosed stream (RecordedEventSource) through Pipeline exactly
as the live daemon does, into a throwaway database the tool creates, migrates,
and drops — so it never touches the acceptance run's rows. Point --admin-db at
any Postgres you can create a database on; the acceptance 5498 *server* is fine —
the replay lands in a separate, auto-dropped database, never the acceptance one.

--plant-raiser adds an always-raising detector alongside the real threshold
detector: the boundary must swallow every raise, so bars still persist and real
alerts still land while detector_errors climbs. Exit 0 / "BOUNDARY HELD" iff every
replayed bar persisted despite a raise on each and no alert write was lost.
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


async def replay_fixture(fixture: Path, detectors: Sequence[Detector], admin_db: str) -> dict[str, int]:
    """Create a throwaway DB, migrate it, replay the fixture through the pipeline,
    then drop the DB. Returns the pipeline counters."""
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
            return pipeline.counters
        finally:
            await engine.dispose()
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE "{db}" WITH (FORCE)'))
        admin.dispose()


async def _run(fixture: Path, threshold: int, plant_raiser: bool, admin_db: str) -> int:
    n_events = len(load_events(fixture))
    detectors: list[Detector] = [ThresholdDetector(threshold)]
    if plant_raiser:
        detectors.append(_AlwaysRaises())
    counters = await replay_fixture(fixture, detectors, admin_db)
    logger.info("replayed %d events -> %s", n_events, counters)

    if not plant_raiser:
        return 0
    held = boundary_held(counters, n_events)
    print(
        f"error-boundary proof (criterion 30): fixture={n_events} bars  "
        f"bars_persisted={counters['bars_persisted']}  detector_errors={counters['detector_errors']}  "
        f"alerts_emitted={counters['alerts_emitted']}  alerts_dropped={counters['alerts_dropped']}  "
        f"-> {'BOUNDARY HELD' if held else 'FAILED'}"
    )
    return 0 if held else 1


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="replay a recorded fixture through the pipeline")
    parser.add_argument("--fixture", required=True, type=Path, help="recorded JSONL fixture (RS_INGEST_FIXTURE_PATH output)")
    parser.add_argument("--threshold", type=int, default=500_000, help="volume threshold for the real detector")
    parser.add_argument("--plant-raiser", action="store_true", help="add an always-raising detector to prove the ADR-0008 boundary")
    parser.add_argument("--admin-db", default=DEFAULT_ADMIN_DB, help="admin DSN to create the throwaway replay DB on")
    args = parser.parse_args()
    if not args.fixture.exists():
        parser.error(f"fixture not found: {args.fixture}")
    return asyncio.run(_run(args.fixture, args.threshold, args.plant_raiser, args.admin_db))


if __name__ == "__main__":
    sys.exit(main())

"""Golden replay fixture (S007, ADR-0023): the recorded 2026-07-09 session
excerpt replayed through the REAL pipeline into a real database must reproduce
the committed semantic alert decisions, exactly.

The expectations file is hand-derived from the fixture bars against the
threshold (an independent source of truth, not a blessed export): at 500,000,
the 13:30 bar (1,681,435) and 13:35 bar (705,817) fire; 13:40 (461,490) and
13:45 (474,041) do not. If detection semantics change deliberately, re-derive
the expectations in review — never re-bless mechanically (ADR-0023's rot
hazard).

First instance of the roadmap's "golden fixture + semantic assertions" pattern;
every detection slice after this ships one.
"""

from pathlib import Path

from rs_ingest.detector import ThresholdDetector
from rs_ingest.pipeline import Pipeline
from rs_ingest.replay_check import compare, fetch_decisions, load_decisions
from rs_ingest.sources.recording import RecordedEventSource

FIXTURES = Path(__file__).parent / "fixtures"
SESSION_FIXTURE = FIXTURES / "session-excerpt-2026-07-09.jsonl"
EXPECTED_DECISIONS = FIXTURES / "expected-decisions-2026-07-09.jsonl"
THRESHOLD = 500_000  # the live run's config — pinned, not defaulted (S007 Q5)


async def test_replay_reproduces_the_committed_semantic_decisions(
    migrated_db_url, session_factory
):
    pipeline = Pipeline(session_factory, [ThresholdDetector(THRESHOLD)])
    await pipeline.run(RecordedEventSource(SESSION_FIXTURE))

    replayed = await fetch_decisions(migrated_db_url)
    diff = compare(load_decisions(EXPECTED_DECISIONS), replayed)

    assert diff.identical, (
        f"replay diverged from the golden decisions: "
        f"only_in_live={diff.only_in_live} only_in_replay={diff.only_in_replay}"
    )
    assert pipeline.counters["alerts_dropped"] == 0

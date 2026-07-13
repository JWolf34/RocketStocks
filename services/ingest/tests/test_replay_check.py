"""Comparator core (S007): semantic alert decisions diffed as multisets.

A semantic decision is the full rs_domain.Alert — everything the detector
decided, nothing the DB added. The diff is strict: identical means the two
multisets match exactly; there is no tolerance path (a comparator that excuses
differences is ADR-0023's rot hazard with extra steps).
"""

from datetime import UTC, datetime

from rs_domain import Alert, Bar, BarClosed
from rs_ingest.replay_check import (
    DecisionScope,
    compare,
    dump_decisions,
    fetch_decisions,
    fixture_scope,
    load_decisions,
)
from rs_persistence import AlertRepository


def _alert(bucket_minute: int = 30, volume: int = 1_000_000, **overrides) -> Alert:
    fields = {
        "symbol": "SPY",
        "alert_type": "volume_threshold",
        "triggered_at": datetime(2026, 7, 9, 13, bucket_minute, tzinfo=UTC),
        "payload": {"volume": volume, "threshold": 500_000},
    }
    fields.update(overrides)
    return Alert(**fields)


def test_identical_multisets_have_no_diff():
    live = [_alert(30), _alert(35)]
    replayed = [_alert(35), _alert(30)]  # order must not matter
    diff = compare(live, replayed)
    assert diff.identical
    assert diff.only_in_live == ()
    assert diff.only_in_replay == ()


def test_payload_key_order_does_not_matter():
    # JSONB does not preserve key order — the live-DB read may hand back the
    # payload keys in a different order than a freshly decided Alert.
    live = [_alert(payload={"volume": 1_000_000, "threshold": 500_000})]
    replayed = [_alert(payload={"threshold": 500_000, "volume": 1_000_000})]
    assert compare(live, replayed).identical


def test_field_mismatch_lands_one_entry_per_side():
    live = [_alert(volume=1_000_000)]
    replayed = [_alert(volume=999_999)]
    diff = compare(live, replayed)
    assert not diff.identical
    assert diff.only_in_live == (live[0],)
    assert diff.only_in_replay == (replayed[0],)


def test_duplicate_decisions_are_counted_not_collapsed():
    # Multiset semantics: live fired twice, replay once — one copy is missing.
    live = [_alert(), _alert()]
    replayed = [_alert()]
    diff = compare(live, replayed)
    assert not diff.identical
    assert diff.only_in_live == (_alert(),)
    assert diff.only_in_replay == ()


def test_alert_only_in_replay():
    # The live-drop shape (Tier-2 alerts_dropped): replay re-derives a decision
    # the live run never committed.
    live = []
    replayed = [_alert()]
    diff = compare(live, replayed)
    assert not diff.identical
    assert diff.only_in_live == ()
    assert diff.only_in_replay == (_alert(),)


def test_empty_both_sides_is_identical():
    assert compare([], []).identical


def test_scope_derived_from_fixture_events():
    # The live-DB filter comes FROM the fixture (never operator-typed): the
    # events' symbols, and the UTC-day span covering every bar bucket.
    events = [
        BarClosed(
            event_time=datetime(2026, 7, 9, 13, 35, tzinfo=UTC),
            sequence=0,
            bar=Bar(
                symbol="SPY",
                bucket=datetime(2026, 7, 9, 13, 30, tzinfo=UTC),
                open=747.16,
                high=747.45,
                low=745.68,
                close=747.34,
                volume=1_681_435,
            ),
        )
    ]
    scope = fixture_scope(events)
    assert scope.symbols == frozenset({"SPY"})
    assert scope.start == datetime(2026, 7, 9, tzinfo=UTC)
    assert scope.end == datetime(2026, 7, 10, tzinfo=UTC)


def test_decisions_file_round_trip(tmp_path):
    path = tmp_path / "decisions.jsonl"
    decisions = [_alert(30), _alert(35, volume=705_817)]
    dump_decisions(decisions, path)
    assert load_decisions(path) == decisions


class TestFetchDecisions:
    """Row -> Alert projection against a real database (the one projection both
    the live-DB baseline and the replay-DB read share)."""

    async def _insert(self, session_factory, alerts):
        async with session_factory() as session:
            repo = AlertRepository(session)
            for alert in alerts:
                await repo.insert(alert)
            await session.commit()

    async def test_scope_filters_to_fixture_symbols_and_day_span(
        self, migrated_db_url, session_factory
    ):
        in_scope = _alert(30)
        wrong_symbol = _alert(30, symbol="QQQ")
        wrong_day = _alert(30, triggered_at=datetime(2026, 7, 10, 13, 30, tzinfo=UTC))
        await self._insert(session_factory, [in_scope, wrong_symbol, wrong_day])

        scope = DecisionScope(
            symbols=frozenset({"SPY"}),
            start=datetime(2026, 7, 9, tzinfo=UTC),
            end=datetime(2026, 7, 10, tzinfo=UTC),
        )
        assert await fetch_decisions(migrated_db_url, scope) == [in_scope]

    async def test_no_scope_returns_every_decision(self, migrated_db_url, session_factory):
        decisions = [_alert(30), _alert(35, volume=705_817)]
        await self._insert(session_factory, decisions)
        assert await fetch_decisions(migrated_db_url) == decisions

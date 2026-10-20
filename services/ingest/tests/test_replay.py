"""replay tool: the pass/fail logic. The DB orchestration (create/migrate/
replay/drop) is exercised end-to-end against a synthetic fixture; here we pin the
boundary-held invariant (S003 criterion 30) and the assertion mode's report +
exit semantics (S007): exit 0 iff the semantic-decision multisets are
identical, 1 on any difference — strict, no tolerance path."""

from datetime import UTC, datetime
from pathlib import Path

from rs_domain import Alert
from rs_ingest.tools.replay import _run, boundary_held, comparison_report


def _counters(bars, errors, emitted=0, dropped=0):
    return {
        "bars_persisted": bars,
        "alerts_emitted": emitted,
        "detector_errors": errors,
        "alerts_dropped": dropped,
    }


def test_held_when_every_bar_persisted_despite_a_raise_each():
    # 78 bars replayed, a raise counted on each, capture never stopped, alerts landed
    assert boundary_held(_counters(bars=78, errors=78, emitted=6), 78)


def test_not_held_if_a_bar_was_lost():
    # capture stopped somewhere — a raise took down more than itself
    assert not boundary_held(_counters(bars=77, errors=78), 78)


def test_not_held_if_a_raise_went_uncounted():
    # a decision raised but wasn't caught + counted by the boundary
    assert not boundary_held(_counters(bars=78, errors=77), 78)


def test_not_held_if_an_alert_write_was_dropped():
    assert not boundary_held(_counters(bars=78, errors=78, dropped=1), 78)


def test_empty_fixture_proves_nothing():
    # a vacuous "all zero" is not a proof — the boundary was never exercised
    assert not boundary_held(_counters(bars=0, errors=0), 0)


def _decision(minute: int = 30, volume: int = 1_681_435) -> Alert:
    return Alert(
        symbol="SPY",
        alert_type="volume_threshold",
        triggered_at=datetime(2026, 7, 9, 13, minute, tzinfo=UTC),
        payload={"volume": volume, "threshold": 500_000},
    )


class TestComparisonReport:
    def test_identical_exits_zero_with_header(self):
        decisions = [_decision(30), _decision(35, volume=705_817)]
        text, code = comparison_report(
            Path("data/f.jsonl"), 500_000, "live-db", decisions, decisions
        )
        assert code == 0
        assert "data/f.jsonl" in text
        assert "500000" in text  # the threshold the replay ran — Q5's config pin
        assert "baseline=2" in text and "replay=2" in text
        assert "IDENTICAL" in text

    def test_any_difference_exits_one_and_classifies_each_side(self):
        live = [_decision(30)]
        replayed = [_decision(30, volume=999_999)]
        text, code = comparison_report(Path("f.jsonl"), 500_000, "expected", live, replayed)
        assert code == 1
        assert "DIFFERENT" in text
        assert "only in live" in text and '"volume":1681435' in text
        assert "only in replay" in text and '"volume":999999' in text

    def test_baseline_label_names_what_was_compared_against(self):
        text, _ = comparison_report(Path("f.jsonl"), 500_000, "live-db", [], [])
        assert "live-db" in text


async def test_empty_fixture_cannot_be_asserted(tmp_path):
    # Same stance as boundary_held: an empty fixture proves nothing. The
    # assertion mode refuses it loudly instead of crashing in fixture_scope.
    empty = tmp_path / "empty.jsonl"
    empty.write_text("")
    code = await _run(empty, 500_000, plant_raiser=False, admin_db="unused", live_db="unused")
    assert code == 1

"""replay tool (S003 criterion 30): the boundary-held invariant. The DB
orchestration (create/migrate/replay/drop) is exercised end-to-end against a real
fixture; here we pin the pass/fail logic that decides whether the boundary held."""

from rs_ingest.tools.replay import boundary_held


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

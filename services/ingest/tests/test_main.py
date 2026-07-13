"""Entry-point metrics wiring (S006): the scrape-time view of the session
aggregator's counters. The aggregator exists only once a session opens, so
the view serves zeros until then — the metric names are present from process
start, and rate() sees a continuous series."""

from types import SimpleNamespace

from rs_ingest.__main__ import LiveAggregatorCounters
from rs_ingest.aggregator import COUNTER_NAMES


def test_serves_all_zero_counters_before_the_session_starts():
    view = LiveAggregatorCounters(SimpleNamespace(aggregator=None))
    assert dict(view) == dict.fromkeys(COUNTER_NAMES, 0)


def test_reads_through_to_the_live_aggregator_at_scrape_time():
    live = SimpleNamespace(aggregator=None)
    view = LiveAggregatorCounters(live)
    live.aggregator = SimpleNamespace(counters={"bars_built": 7, "ticks_late_dropped": 1})
    assert dict(view) == {"bars_built": 7, "ticks_late_dropped": 1}
    live.aggregator.counters["bars_built"] += 1
    assert view["bars_built"] == 8

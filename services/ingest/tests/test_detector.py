"""ThresholdDetector: level-trigger on the newest bar's volume."""

from datetime import UTC, datetime

from rs_domain import Bar
from rs_ingest.detector import ALERT_TYPE, ThresholdDetector

BUCKET = datetime(2026, 7, 6, 14, 30, tzinfo=UTC)


def bar(volume: int, *, symbol: str = "SPY", bucket: datetime = BUCKET) -> Bar:
    return Bar(symbol=symbol, bucket=bucket, open=100.0, high=101.0, low=99.0, close=100.5, volume=volume)


def test_empty_window_is_silent():
    assert ThresholdDetector(500_000).decide([]) is None


def test_below_threshold_is_silent():
    assert ThresholdDetector(500_000).decide([bar(499_999)]) is None


def test_at_threshold_fires():
    # >= is a level trigger: exactly at the threshold must fire.
    assert ThresholdDetector(500_000).decide([bar(500_000)]) is not None


def test_above_threshold_fires_with_semantic_payload():
    alert = ThresholdDetector(500_000).decide([bar(750_000)])
    assert alert is not None
    assert alert.symbol == "SPY"
    assert alert.alert_type == ALERT_TYPE
    assert alert.triggered_at == BUCKET  # the bar's open instant
    assert alert.payload == {"volume": 750_000, "threshold": 500_000}  # facts, no prose


def test_decides_on_newest_bar_only():
    older = bar(900_000, bucket=datetime(2026, 7, 6, 14, 25, tzinfo=UTC))
    newest = bar(100_000, bucket=BUCKET)
    # An earlier bar over the line does not fire; only window[-1] is judged.
    assert ThresholdDetector(500_000).decide([older, newest]) is None

    newest_hot = bar(600_000, bucket=BUCKET)
    alert = ThresholdDetector(500_000).decide([bar(100_000), newest_hot])
    assert alert is not None and alert.payload["volume"] == 600_000

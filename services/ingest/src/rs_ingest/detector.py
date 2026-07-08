"""Threshold volume detector (S003 B6, grilling #5).

Level-trigger: fires on every bar whose volume is at or above the threshold —
no cooldown or dedup in the skeleton (dedup/cooldown semantics are the Phase-2
design session). The default threshold (~p80 of 2026-07-06's measured bars)
yields roughly 6-10 fires a day, enough to exercise the alert path end-to-end
in the B8 acceptance run. It reads only the newest bar today; the full window
is in the signature so day-cumulative detectors slot in behind the same port.

`triggered_at` is the bar's bucket (its UTC open instant) — the conventional
label for a bar and, unlike the close instant, independent of the bar width.
"""

from collections.abc import Sequence

from rs_domain import Alert, Bar

ALERT_TYPE = "volume_threshold"


class ThresholdDetector:
    """rs_domain.Detector: an Alert whenever the newest bar's volume >= threshold."""

    def __init__(self, threshold: int) -> None:
        self._threshold = threshold

    def decide(self, window: Sequence[Bar]) -> Alert | None:
        if not window:
            return None
        bar = window[-1]
        if bar.volume < self._threshold:
            return None
        return Alert(
            symbol=bar.symbol,
            alert_type=ALERT_TYPE,
            triggered_at=bar.bucket,
            payload={"volume": bar.volume, "threshold": self._threshold},
        )

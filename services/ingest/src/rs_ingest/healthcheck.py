"""Container healthcheck: `python -m rs_ingest.healthcheck`.

Exits 0 when the daemon's loop heartbeat (read from its own metrics endpoint)
is fresh, 1 when it is stale, missing or unreachable.
"""

import os
import sys
import urllib.request

from prometheus_client.parser import text_string_to_metric_families

from rs_domain import Clock, SystemClock
from rs_ingest.metrics import LOOP_TICK_METRIC

DEFAULT_PORT = 9090
DEFAULT_MAX_AGE_SECONDS = 120.0


def heartbeat_age_seconds(metrics_text: str, clock: Clock) -> float | None:
    """Seconds since the loop's last tick, or None when no tick was exposed."""
    for family in text_string_to_metric_families(metrics_text):
        for sample in family.samples:
            if sample.name == LOOP_TICK_METRIC and sample.value > 0:
                return clock.now().timestamp() - sample.value
    return None


def main() -> int:
    port = int(os.environ.get("RS_INGEST_METRICS_PORT", DEFAULT_PORT))
    max_age = float(os.environ.get("RS_INGEST_HEALTH_MAX_AGE_SECONDS", DEFAULT_MAX_AGE_SECONDS))
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
            body = response.read().decode()
    except OSError as exc:
        print(f"unhealthy: metrics endpoint unreachable: {exc}", file=sys.stderr)
        return 1
    age = heartbeat_age_seconds(body, SystemClock())
    if age is None:
        print("unhealthy: no loop heartbeat exposed yet", file=sys.stderr)
        return 1
    if age > max_age:
        print(f"unhealthy: loop heartbeat is {age:.0f}s old (max {max_age:.0f}s)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

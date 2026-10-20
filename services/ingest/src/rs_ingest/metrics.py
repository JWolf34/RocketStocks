"""Native prometheus_client metrics for the daemon. Names are bare (the scrape
job labels distinguish services); the label is `job_name` because `job` is
the scrape target's own label."""

from collections.abc import Iterable

from prometheus_client import Counter, Gauge
from prometheus_client.registry import Collector

OUTCOMES = ("success", "failure", "auth_error", "skipped_token_dead", "skipped_overlap")

LOOP_TICK_METRIC = "daemon_loop_last_tick_timestamp_seconds"


class DaemonMetrics:
    def __init__(self) -> None:
        # registry=None: the metrics server registers these on its own registry.
        self.loop_last_tick = Gauge(
            LOOP_TICK_METRIC,
            "Unix time of the daemon loop's last tick; stale means the loop is hung",
            registry=None,
        )
        self.job_last_success = Gauge(
            "cron_job_last_success_timestamp_seconds",
            "Unix time of the job's last successful run",
            ["job_name"],
            registry=None,
        )
        self.job_runs = Counter(
            "cron_job_runs",
            "Scheduled job slots by outcome",
            ["job_name", "outcome"],
            registry=None,
        )
        self.token_dead = Gauge(
            "schwab_token_dead",
            "1 while the Schwab refresh token is dead and Schwab jobs are paused",
            registry=None,
        )

    def register_job(self, name: str) -> None:
        """Create the job's counter series up front so rate() sees them from start."""
        for outcome in OUTCOMES:
            self.job_runs.labels(name, outcome)

    def collectors(self) -> Iterable[Collector]:
        return (self.loop_last_tick, self.job_last_success, self.job_runs, self.token_dead)

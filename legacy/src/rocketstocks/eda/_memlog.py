"""Lightweight peak-RSS telemetry for EDA pipeline stages.

Uses stdlib ``resource`` — no additional dependency.
"""
import resource


def log_memory(stage: str) -> None:
    """Print current peak RSS to stdout.

    On Linux, ``ru_maxrss`` is in kilobytes.  Call after each major stage
    (detect, bar_counts fetch, every 50 tickers, post-engine) to confirm
    that RSS plateaus in the streaming loop rather than climbing.

    Args:
        stage: Short label printed alongside the RSS reading.
    """
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(f"  [mem] {stage}: peak RSS {rss_kb / 1024:.0f} MB")

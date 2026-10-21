"""Counter vocabulary for the bot. In-memory dict, logged on every
sweep that did work and at shutdown — the bot never exits daily, so
shutdown-only logging would be blind."""

COUNTER_NAMES = (
    "discord_posts_total",
    "post_failures_total",
    "posts_abandoned_total",
    "posts_stale_total",
    "posts_reconciled_total",
    "sweeps_total",
    "notify_wakes_total",
    "listen_reconnects_total",
)


def new_counters() -> dict[str, int]:
    """One shared dict per daemon: the sweep worker and the LISTEN loop each
    increment their own names in it, so one log line shows the whole bot."""
    return dict.fromkeys(COUNTER_NAMES, 0)

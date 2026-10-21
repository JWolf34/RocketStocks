"""ADR-0022 depth check: how far back does Schwab price history actually reach
per frequency? (throwaway — measured facts live in docs/reference/schwab-facts.md)

Requests each frequency with a start far beyond the expected window and reports
the earliest candle returned. Working figures under test: ~7 days @ 1-min,
~9 months @ 5-min, years @ daily.

    .venv/bin/python scripts/spikes/history_depth_check.py
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import build_client, utc_now


def probe(label, fetch):
    resp = fetch()
    resp.raise_for_status()
    candles = resp.json().get("candles", [])
    if not candles:
        print(f"{label:12s} -> 0 candles returned")
        return
    first = datetime.fromtimestamp(candles[0]["datetime"] / 1000, tz=timezone.utc)
    last = datetime.fromtimestamp(candles[-1]["datetime"] / 1000, tz=timezone.utc)
    depth = (utc_now() - first).days
    print(f"{label:12s} -> {len(candles):>7,} candles · earliest {first:%Y-%m-%d} · latest {last:%Y-%m-%d} · depth ≈ {depth} days")


def main() -> int:
    client = build_client()
    now = utc_now()
    probe("1-min", lambda: client.get_price_history_every_minute(
        "SPY", start_datetime=now - timedelta(days=30), end_datetime=now))
    probe("5-min", lambda: client.get_price_history_every_five_minutes(
        "SPY", start_datetime=now - timedelta(days=500), end_datetime=now))
    probe("daily", lambda: client.get_price_history_every_day(
        "SPY", start_datetime=now - timedelta(days=365 * 30), end_datetime=now))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""ADR-0025 check: are Schwab REST daily candles raw or adjusted? (throwaway)

get_price_history has NO adjustment parameter (verified by introspection), so the
basis is fixed — this determines WHICH basis empirically:
  1. NVDA around its 10:1 split (2024-06-10): a ~10x discontinuity → raw;
     a smooth series → split-adjusted.
  2. AAPL close on 2020-01-02 (raw $300.35): ≈$75.09 → split-adjusted only;
     ≈$73.5 → split+dividend adjusted; ≈$300.35 → fully raw.

    .venv/bin/python scripts/spikes/adjust_check.py
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import build_client


def daily(client, symbol: str, start: str, end: str) -> list[dict]:
    resp = client.get_price_history_every_day(
        symbol,
        start_datetime=datetime.fromisoformat(start).replace(tzinfo=timezone.utc),
        end_datetime=datetime.fromisoformat(end).replace(tzinfo=timezone.utc),
    )
    resp.raise_for_status()
    return resp.json()["candles"]


def main() -> int:
    client = build_client()

    print("NVDA daily closes around the 2024-06-10 10:1 split:")
    for c in daily(client, "NVDA", "2024-06-04", "2024-06-13"):
        day = datetime.fromtimestamp(c["datetime"] / 1000, tz=timezone.utc).date()
        print(f"  {day}  close={c['close']:>10.2f}  volume={c['volume']:>12,}")

    print("\nAAPL close on 2020-01-02 (raw was $300.35):")
    for c in daily(client, "AAPL", "2020-01-02", "2020-01-03"):
        day = datetime.fromtimestamp(c["datetime"] / 1000, tz=timezone.utc).date()
        print(f"  {day}  close={c['close']:.4f}")
    print("\n  ≈75.09 → split-adjusted only · ≈73.5 → split+div adjusted · ≈300.35 → raw")
    return 0


if __name__ == "__main__":
    sys.exit(main())

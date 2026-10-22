"""Post-close rate-limit probe for ADR-0020 (throwaway).

Ramps SPY quote-request pace (1/s → 2/s → 4/s, ~120 requests max) until the
first 429, logging every response's status + full headers (incl. Retry-After)
to data/spike-recordings/burst-probe-<date>.jsonl. Refuses to run during RTH
so rate-limit fallout can never touch the live ingest daemon's market-hours calls.

    .venv/bin/python scripts/spikes/burst_probe.py
"""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import SPIKE_DATA_DIR, build_client, today_tag, utc_now

PHASES = [(1.0, 30), (0.5, 30), (0.25, 60)]  # (seconds between requests, count)


def in_rth() -> bool:
    from rs_sources import ExchangeCalendar

    cal = ExchangeCalendar()
    today = utc_now().date()
    return cal.is_session(today) and cal.session_open(today) <= utc_now() < cal.session_close(today)


def main() -> int:
    if in_rth():
        sys.exit("market is open — run the burst probe AFTER the close (protects the live ingest daemon)")
    client = build_client()
    out = SPIKE_DATA_DIR / f"burst-probe-{today_tag()}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    sent = 0
    with out.open("a", encoding="utf-8") as fh:
        for pace_s, count in PHASES:
            print(f"phase: {1 / pace_s:g} req/s × {count}")
            for _ in range(count):
                resp = client.get_quote("SPY")
                sent += 1
                fh.write(
                    json.dumps(
                        {"ts": utc_now().isoformat(), "n": sent, "status": resp.status_code, "headers": dict(resp.headers)}
                    )
                    + "\n"
                )
                fh.flush()
                if resp.status_code == 429:
                    print(f"429 after {sent} requests — Retry-After: {resp.headers.get('Retry-After')!r} → {out}")
                    return 0
                time.sleep(pace_s)
    print(f"no 429 after {sent} requests (finding: limit not reached at ≤4 req/s) → {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

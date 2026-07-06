"""Phase-0 chain-snapshot spike: pull full option chains across the session;
SPY every minute (finest cadence Phase-2 can ever analyze — downsample later,
never the reverse), the other 9 liquid tickers every 5 minutes. Each raw
response is gzipped locally.

Full-day data stays under data/chain-snapshots/ (never committed — hundreds of
MB); chain_diff.py later emits committed samples + findings. Rate-limit
response headers and any 429s are logged (ADR-0020 budget measurement input).

    .venv/bin/python scripts/spikes/chain_spike.py            # live, during RTH
    .venv/bin/python scripts/spikes/chain_spike.py --dry-run  # one synthetic write cycle
"""

import argparse
import gzip
import json
import sys
import time
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import REPO_ROOT, build_client, today_tag, todays_session_bounds, utc_now

TICKERS = ["SPY", "QQQ", "AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "META", "AMD", "GOOGL"]
FAST_TICKER = "SPY"  # 1-min cadence; the rest every SLOW_EVERY ticks
SLOW_TICKERS = [t for t in TICKERS if t != FAST_TICKER]
TICK_S = 60
SLOW_EVERY = 5
# Unbounded index-ETF chains 502 server-side (response ceiling, probed 2026-07-06:
# SPY unfiltered → 502 in 8.5s; all strikes ≤45d → 200 in 1.6s). Bound expiries,
# keep every strike — near-term flow incl. wings is the Phase-2 target.
BOUNDED_TICKERS = {"SPY", "QQQ"}
CHAIN_WINDOW_DAYS = 45
SNAP_ROOT = REPO_ROOT / "data" / "chain-snapshots"
RATELIMIT_HEADER_PREFIXES = ("x-ratelimit", "retry-after", "schwab")


def write_snapshot(ticker: str, day_tag: str, stamp: str, payload: dict) -> Path:
    out = SNAP_ROOT / day_tag / ticker / f"{stamp}.json.gz"
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh)
    return out


def pull_cycle(client, tickers: list[str], day_tag: str, header_log: Path) -> None:
    stamp = utc_now().strftime("%H%M")
    for ticker in tickers:
        kwargs = {}
        if ticker in BOUNDED_TICKERS:
            today = utc_now().date()
            kwargs = {"from_date": today, "to_date": today + timedelta(days=CHAIN_WINDOW_DAYS)}
        try:
            resp = client.get_option_chain(ticker, **kwargs)
        except Exception as exc:
            print(f"[{stamp}] {ticker}: request failed: {type(exc).__name__}: {exc}")
            continue
        interesting = {k: v for k, v in resp.headers.items() if k.lower().startswith(RATELIMIT_HEADER_PREFIXES)}
        with header_log.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"ts": utc_now().isoformat(), "ticker": ticker, "status": resp.status_code, "headers": interesting}) + "\n")
        if resp.status_code == 429:
            print(f"[{stamp}] {ticker}: RATE LIMITED (429) — headers: {interesting}")
            continue
        if resp.status_code != 200:
            print(f"[{stamp}] {ticker}: HTTP {resp.status_code}")
            continue
        out = write_snapshot(ticker, day_tag, stamp, resp.json())
        print(f"[{stamp}] {ticker}: {out.stat().st_size / 1024:.0f} KB")


def run_live() -> int:
    _, session_close = todays_session_bounds()
    day_tag = today_tag()
    header_log = SNAP_ROOT / day_tag / "ratelimit-headers.jsonl"
    header_log.parent.mkdir(parents=True, exist_ok=True)
    client = build_client()
    print(
        f"snapshotting {FAST_TICKER} every {TICK_S}s, {len(SLOW_TICKERS)} others every "
        f"{TICK_S * SLOW_EVERY}s, until {session_close:%H:%M:%S} UTC"
    )

    tick = 0
    while utc_now() < session_close:
        cycle_start = time.monotonic()
        batch = [FAST_TICKER] + (SLOW_TICKERS if tick % SLOW_EVERY == 0 else [])
        pull_cycle(client, batch, day_tag, header_log)
        tick += 1
        elapsed = time.monotonic() - cycle_start
        time.sleep(max(0.0, TICK_S - elapsed))
    print(f"session over — snapshots under {SNAP_ROOT / day_tag}")
    return 0


def run_dry() -> int:
    payload = {
        "symbol": "SPY",
        "callExpDateMap": {"2026-07-10:4": {"560.0": [{"symbol": "SPY 260710C560", "totalVolume": 100, "openInterest": 5000, "bid": 3.1, "ask": 3.2}]}},
        "putExpDateMap": {},
    }
    out = write_snapshot("SPY", today_tag(), "dry0", payload)
    print(f"dry-run snapshot written: {out}")
    out.unlink()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return run_dry() if args.dry_run else run_live()


if __name__ == "__main__":
    sys.exit(main())

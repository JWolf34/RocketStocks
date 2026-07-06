"""Phase-0 streaming spike: record one full session of SPY level_one_equity
plus level_one_option for ~20 near-ATM SPY contracts (nearest expiry, picked
once at startup — Schwab allows one streamer connection per user, so both
services share it).

Every message schwab-py delivers is appended (with a UTC receive timestamp)
to data/spike-recordings/spy-level1-<date>.jsonl and
spy-options-level1-<date>.jsonl (gitignored — raw recordings stay local).
Disconnects are survived with backoff and logged; reconnects re-subscribe
both services. Runs until session close + 2 minutes.

    .venv/bin/python scripts/spikes/stream_spike.py            # live, during RTH
    .venv/bin/python scripts/spikes/stream_spike.py --dry-run  # writer/summary path, no network
"""

import argparse
import asyncio
import json
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import SPIKE_DATA_DIR, build_client, today_tag, todays_session_bounds, utc_now

GRACE = timedelta(minutes=2)
SYMBOL = "SPY"


class Recorder:
    def __init__(self, out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = out_path.open("a", encoding="utf-8")
        self.path = out_path
        self.count = 0
        self.first_ts = None
        self.last_ts = None
        self.largest_gap_s = 0.0

    def record(self, message: dict) -> None:
        now = utc_now()
        if self.last_ts is not None:
            self.largest_gap_s = max(self.largest_gap_s, (now - self.last_ts).total_seconds())
        self.first_ts = self.first_ts or now
        self.last_ts = now
        self.count += 1
        self._fh.write(json.dumps({"recv_ts": now.isoformat(), "msg": message}) + "\n")
        self._fh.flush()

    def summary(self, reconnects: int) -> str:
        span = (self.last_ts - self.first_ts) if self.count else timedelta(0)
        return (
            f"messages={self.count} span={span} largest_gap={self.largest_gap_s:.1f}s "
            f"reconnects={reconnects} file={self.path}"
        )

    def close(self) -> None:
        self._fh.close()


async def pick_option_contracts(client) -> list[str]:
    """~20 near-ATM SPY contracts for the nearest expiry; static all day."""
    today = utc_now().date()
    resp = await client.get_option_chain(
        SYMBOL, strike_count=10, from_date=today, to_date=today + timedelta(days=4)
    )
    resp.raise_for_status()
    payload = resp.json()
    symbols: list[str] = []
    for side in ("callExpDateMap", "putExpDateMap"):
        exp_map = payload.get(side) or {}
        if not exp_map:
            continue
        nearest = min(exp_map)  # "YYYY-MM-DD:dte" keys sort chronologically
        for contract_list in exp_map[nearest].values():
            symbols += [c["symbol"] for c in contract_list]
    return symbols


async def run_live(recorder: Recorder, options_recorder: Recorder) -> int:
    from schwab.streaming import StreamClient

    _, session_close = todays_session_bounds()
    stop_at = session_close + GRACE
    print(f"recording {SYMBOL} until {stop_at:%H:%M:%S} UTC → {recorder.path}")

    client = build_client(asyncio=True)
    try:
        contracts = await pick_option_contracts(client)
        print(f"options leg: {len(contracts)} contracts → {options_recorder.path}")
    except Exception as exc:  # options leg is secondary — never kill the equity recording
        contracts = []
        print(f"options leg DISABLED (contract pick failed): {type(exc).__name__}: {exc}")
    total_reconnects = 0
    consecutive_failures = 0  # resets once a connection survives 60s

    while utc_now() < stop_at:
        connected_at = None
        try:
            stream = StreamClient(client)
            await stream.login()
            stream.add_level_one_equity_handler(recorder.record)
            await stream.level_one_equity_subs([SYMBOL])
            if contracts:
                stream.add_level_one_option_handler(options_recorder.record)
                await stream.level_one_option_subs(contracts)
            connected_at = utc_now()
            print(f"[{connected_at:%H:%M:%S}] connected, subscribed {SYMBOL} + {len(contracts)} option contracts")
            while utc_now() < stop_at:
                await asyncio.wait_for(stream.handle_message(), timeout=90)
        except TimeoutError:
            print(f"[{utc_now():%H:%M:%S}] 90s without any message — reconnecting")
        except Exception as exc:
            print(f"[{utc_now():%H:%M:%S}] stream dropped: {type(exc).__name__}: {exc}")
        else:
            break  # reached stop_at with the connection healthy

        was_stable = connected_at is not None and (utc_now() - connected_at).total_seconds() >= 60
        consecutive_failures = 0 if was_stable else consecutive_failures + 1
        total_reconnects += 1
        if utc_now() >= stop_at:
            break
        backoff = min(30, 2**consecutive_failures)
        print(f"[{utc_now():%H:%M:%S}] reconnect #{total_reconnects} in {backoff}s")
        await asyncio.sleep(backoff)

    print("session over:", recorder.summary(total_reconnects))
    print("options leg:", options_recorder.summary(total_reconnects))
    return 0


def run_dry(recorder: Recorder, options_recorder: Recorder) -> int:
    for i in range(20):
        recorder.record(
            {
                "service": "LEVELONE_EQUITIES",
                "timestamp": 1751812200000 + i * 1000,
                "content": [{"key": SYMBOL, "LAST_PRICE": 560.0 + i * 0.05, "TOTAL_VOLUME": 1000 * (i + 1)}],
            }
        )
    for i in range(5):
        options_recorder.record(
            {
                "service": "LEVELONE_OPTIONS",
                "timestamp": 1751812200000 + i * 1000,
                "content": [{"key": "SPY   260707C00560000", "LAST_PRICE": 3.1 + i * 0.01, "TOTAL_VOLUME": 10 * (i + 1)}],
            }
        )
    print("dry-run:", recorder.summary(0))
    print("dry-run options:", options_recorder.summary(0))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    if args.dry_run and args.out is None:
        parser.error("--dry-run requires --out; refusing to append synthetic ticks to the real day-file")
    out = args.out or SPIKE_DATA_DIR / f"spy-level1-{today_tag()}.jsonl"
    options_out = (
        args.out.with_suffix(".options.jsonl")
        if args.out
        else SPIKE_DATA_DIR / f"spy-options-level1-{today_tag()}.jsonl"
    )
    recorder = Recorder(out)
    options_recorder = Recorder(options_out)
    try:
        if args.dry_run:
            return run_dry(recorder, options_recorder)
        return asyncio.run(run_live(recorder, options_recorder))
    finally:
        recorder.close()
        options_recorder.close()


if __name__ == "__main__":
    sys.exit(main())

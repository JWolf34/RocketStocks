"""Phase-0 streaming spike: record one full session of SPY level_one_equity.

Every message schwab-py delivers is appended (with a UTC receive timestamp)
to data/spike-recordings/spy-level1-<date>.jsonl (gitignored — raw recordings
stay local). Disconnects are survived with backoff and logged. Runs until
session close + 2 minutes.

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


async def run_live(recorder: Recorder) -> int:
    from schwab.streaming import StreamClient

    _, session_close = todays_session_bounds()
    stop_at = session_close + GRACE
    print(f"recording {SYMBOL} until {stop_at:%H:%M:%S} UTC → {recorder.path}")

    client = build_client(asyncio=True)
    reconnects = -1  # first connect isn't a reconnect

    while utc_now() < stop_at:
        reconnects += 1
        if reconnects:
            backoff = min(30, 2**reconnects)
            print(f"[{utc_now():%H:%M:%S}] reconnect #{reconnects} in {backoff}s")
            await asyncio.sleep(backoff)
        try:
            stream = StreamClient(client)
            await stream.login()
            stream.add_level_one_equity_handler(recorder.record)
            await stream.level_one_equity_subs([SYMBOL])
            print(f"[{utc_now():%H:%M:%S}] connected, subscribed {SYMBOL}")
            while utc_now() < stop_at:
                await asyncio.wait_for(stream.handle_message(), timeout=90)
        except TimeoutError:
            print(f"[{utc_now():%H:%M:%S}] 90s without any message — reconnecting")
        except Exception as exc:
            print(f"[{utc_now():%H:%M:%S}] stream dropped: {type(exc).__name__}: {exc}")

    print("session over:", recorder.summary(max(reconnects, 0)))
    return 0


def run_dry(recorder: Recorder) -> int:
    for i in range(20):
        recorder.record(
            {
                "service": "LEVELONE_EQUITIES",
                "timestamp": 1751812200000 + i * 1000,
                "content": [{"key": SYMBOL, "LAST_PRICE": 560.0 + i * 0.05, "TOTAL_VOLUME": 1000 * (i + 1)}],
            }
        )
    print("dry-run:", recorder.summary(0))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    out = args.out or SPIKE_DATA_DIR / f"spy-level1-{today_tag()}.jsonl"
    recorder = Recorder(out)
    try:
        if args.dry_run:
            return run_dry(recorder)
        return asyncio.run(run_live(recorder))
    finally:
        recorder.close()


if __name__ == "__main__":
    sys.exit(main())

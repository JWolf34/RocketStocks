"""Post-session raw-vs-adjusted check: 5-minute candles built from the recorded
stream vs Schwab REST price history for the same day.

    .venv/bin/python scripts/spikes/stream_compare.py                       # after the session
    .venv/bin/python scripts/spikes/stream_compare.py --jsonl path --dry-run  # candle-builder only, no REST
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import FINDINGS_DIR, SPIKE_DATA_DIR, build_client, today_tag

BUCKET_MS = 5 * 60 * 1000


def _extract_ticks(jsonl_path: Path):
    """Yield (epoch_ms, last_price, total_volume) from recorded messages.

    Handles both schwab-py's relabeled field names and raw numeric keys
    (LAST_PRICE = field 3, TOTAL_VOLUME = field 8 in LEVELONE_EQUITIES).
    Schwab streams only changed fields, so either value may be None —
    volume-only updates must still flow through or their deltas smear
    into later candles.
    """
    with jsonl_path.open(encoding="utf-8") as fh:
        for line in fh:
            entry = json.loads(line)
            msg = entry["msg"]
            ts = msg.get("timestamp")
            for item in msg.get("content", []):
                price = item.get("LAST_PRICE", item.get("3"))
                volume = item.get("TOTAL_VOLUME", item.get("8"))
                if ts is None or (price is None and volume is None):
                    continue
                yield (
                    ts,
                    None if price is None else float(price),
                    None if volume is None else float(volume),
                )


def build_candles(jsonl_path: Path) -> dict[int, dict]:
    """5-min OHLC + per-bucket volume (from cumulative TOTAL_VOLUME deltas).

    Volume-only updates contribute to their own bucket without touching OHLC;
    a bucket that got volume but never a price (unlikely for SPY in RTH) rolls
    its volume forward into the next bucket that has a candle.
    """
    candles: dict[int, dict] = {}
    volumes: dict[int, float] = {}
    prev_cumulative = None
    for ts, price, cumulative in _extract_ticks(jsonl_path):
        bucket = ts - ts % BUCKET_MS
        if price is not None:
            candle = candles.setdefault(
                bucket, {"open": price, "high": price, "low": price, "close": price, "volume": 0.0}
            )
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
        if cumulative is not None:
            if prev_cumulative is not None:
                volumes[bucket] = volumes.get(bucket, 0.0) + max(0.0, cumulative - prev_cumulative)
            prev_cumulative = cumulative
    pending = 0.0
    for bucket in sorted(set(candles) | set(volumes)):
        pending += volumes.get(bucket, 0.0)
        if bucket in candles:
            candles[bucket]["volume"] = pending
            pending = 0.0
    return candles


def fetch_rest_candles(day_tag: str) -> dict[int, dict]:
    client = build_client()
    day = datetime.strptime(day_tag, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    resp = client.get_price_history_every_five_minutes(
        "SPY",
        start_datetime=day.replace(hour=0),
        end_datetime=day.replace(hour=23, minute=59),
        need_extended_hours_data=False,
    )
    resp.raise_for_status()
    return {
        c["datetime"]: {"open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"], "volume": c["volume"]}
        for c in resp.json()["candles"]
    }


def compare(stream: dict[int, dict], rest: dict[int, dict], out_md: Path) -> None:
    rows, max_dev = [], 0.0
    for bucket in sorted(set(stream) | set(rest)):
        s, r = stream.get(bucket), rest.get(bucket)
        t = datetime.fromtimestamp(bucket / 1000, tz=timezone.utc).strftime("%H:%M")
        if s is None or r is None:
            rows.append(f"| {t} | {'—' if s is None else 'stream-only'} | {'—' if r is None else 'rest-only'} | | |")
            continue
        devs = {f: abs(s[f] - r[f]) for f in ("open", "high", "low", "close")}
        worst = max(devs, key=devs.get)
        max_dev = max(max_dev, devs[worst])
        vol_pct = 100 * (s["volume"] - r["volume"]) / r["volume"] if r["volume"] else float("nan")
        rows.append(f"| {t} | {s['close']:.2f} | {r['close']:.2f} | {devs[worst]:.4f} ({worst}) | {vol_pct:+.1f}% |")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(
        "# Stream-built vs REST 5-min candles — SPY\n\n"
        f"Stream bars: {len(stream)} · REST bars: {len(rest)} · max price deviation: {max_dev:.4f}\n\n"
        "| bar (UTC) | stream close | REST close | worst field dev | volume Δ |\n|---|---|---|---|---|\n"
        + "\n".join(rows)
        + "\n\nNotes: stream candles are last-trade-price aggregations from LEVELONE_EQUITIES; "
        "REST candles are Schwab's official 5-min history (RTH only). Divergence here sizes the "
        "tolerance for S003's acceptance check.\n"
    )
    print(f"stream={len(stream)} rest={len(rest)} max_price_dev={max_dev:.4f} → {out_md}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jsonl", type=Path, default=None)
    parser.add_argument("--date", default=today_tag())
    parser.add_argument("--dry-run", action="store_true", help="build candles only; skip REST")
    args = parser.parse_args()

    jsonl = args.jsonl or SPIKE_DATA_DIR / f"spy-level1-{args.date}.jsonl"
    if not jsonl.exists():
        sys.exit(f"no recording at {jsonl}")
    stream_candles = build_candles(jsonl)
    if args.dry_run:
        for bucket, candle in sorted(stream_candles.items()):
            print(datetime.fromtimestamp(bucket / 1000, tz=timezone.utc).strftime("%H:%M"), candle)
        return 0
    rest_candles = fetch_rest_candles(args.date)
    compare(stream_candles, rest_candles, FINDINGS_DIR / "streaming" / f"comparison-{args.date}.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())

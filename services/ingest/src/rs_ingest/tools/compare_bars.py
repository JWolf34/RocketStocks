"""Post-session validation (S003 B7): the 5-min bars ingest persisted for a UTC
day vs Schwab's official REST candles for the same day.

    python -m rs_ingest.tools.compare_bars --date 2026-07-06 [--symbol SPY]

OHLC prices are gated — the worst per-bucket field deviation, maxed over the day,
must be <= PRICE_TOLERANCE (exit 0), else the run fails (exit 1). Volume and
bucket coverage (stream-only / rest-only) are reported only: the stream's
per-bucket volume is a cumulative-TOTAL_VOLUME delta and legitimately diverges
from REST's official candle volume, and no-trade buckets are absent from both
sides by design (grilling #3). --date is explicit (no ambient 'today'): bars are
keyed by UTC day and the tool must stay reproducible (ADR-0023).
"""

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

from rs_ingest.schwab_client import TokenMissingError, build_schwab_client
from rs_ingest.settings import IngestSettings
from rs_persistence import BarRepository, SchwabTokenStore, build_engine, build_session_factory

logger = logging.getLogger(__name__)

# The Phase-0 stream-vs-REST spike measured a 0.17 max OHLC deviation on
# 2026-07-06 (see docs/foundation/research/vendor-stack.md, Measured Schwab facts); 0.25
# gates with headroom above that observed noise floor.
PRICE_TOLERANCE = 0.25
PRICE_FIELDS = ("open", "high", "low", "close")


class Ohlcv(NamedTuple):
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class BucketDiff:
    bucket: datetime
    stream: Ohlcv | None
    rest: Ohlcv | None
    worst_field: str | None  # None when the bucket is on only one side
    price_dev: float | None
    volume_pct: float | None  # (stream - rest)/rest * 100; None if unmatched or rest vol 0

    @property
    def matched(self) -> bool:
        return self.stream is not None and self.rest is not None


@dataclass(frozen=True)
class Comparison:
    diffs: list[BucketDiff]

    @property
    def matched(self) -> list[BucketDiff]:
        return [d for d in self.diffs if d.matched]

    @property
    def stream_only(self) -> list[datetime]:
        return [d.bucket for d in self.diffs if d.stream is not None and d.rest is None]

    @property
    def rest_only(self) -> list[datetime]:
        return [d.bucket for d in self.diffs if d.rest is not None and d.stream is None]

    @property
    def max_price_dev(self) -> float:
        return max((d.price_dev for d in self.matched), default=0.0)

    def within_tolerance(self, tol: float = PRICE_TOLERANCE) -> bool:
        # Gate on price (the plan's acceptance criterion); coverage gaps are
        # reported for the operator to judge, not folded into the pass/fail —
        # EXCEPT that zero overlap while either side has bars is a misalignment
        # failure, never a vacuous pass on max_dev == 0.0 (a validation tool must
        # not report success when it compared nothing against existing data).
        if not self.matched:
            return not self.stream_only and not self.rest_only
        return self.max_price_dev <= tol


def compare(stream: dict[datetime, Ohlcv], rest: dict[datetime, Ohlcv]) -> Comparison:
    """Align two bar sets by UTC bucket; per matched bucket, the worst OHLC field
    deviation and the volume % delta. A bucket on only one side is carried as
    unmatched (worst_field / price_dev None)."""
    diffs: list[BucketDiff] = []
    for bucket in sorted(set(stream) | set(rest)):
        s, r = stream.get(bucket), rest.get(bucket)
        if s is None or r is None:
            diffs.append(BucketDiff(bucket, s, r, None, None, None))
            continue
        field_devs = {f: abs(getattr(s, f) - getattr(r, f)) for f in PRICE_FIELDS}
        worst = max(field_devs, key=field_devs.__getitem__)
        vol_pct = 100.0 * (s.volume - r.volume) / r.volume if r.volume else None
        diffs.append(BucketDiff(bucket, s, r, worst, field_devs[worst], vol_pct))
    return Comparison(diffs)


def format_report(result: Comparison, symbol: str, day: date) -> str:
    verdict = "PASS" if result.within_tolerance() else "FAIL"
    lines = [
        f"{symbol} {day} — stream-built vs REST 5-min bars",
        f"  matched {len(result.matched)}  stream-only {len(result.stream_only)}  rest-only {len(result.rest_only)}",
        f"  max price deviation {result.max_price_dev:.4f}  (tolerance {PRICE_TOLERANCE})  -> {verdict}",
    ]
    for d in sorted(result.matched, key=lambda d: d.price_dev, reverse=True)[:5]:
        vol = "n/a" if d.volume_pct is None else f"{d.volume_pct:+.1f}%"
        lines.append(f"    {d.bucket:%H:%M}  dev {d.price_dev:.4f} ({d.worst_field})  vol {vol}")
    for bucket in result.rest_only:
        lines.append(f"  rest-only (ingest has no bar): {bucket:%H:%M}")
    for bucket in result.stream_only:
        lines.append(f"  stream-only (no REST candle): {bucket:%H:%M}")
    return "\n".join(lines)


async def fetch_stream_bars(session_factory, symbol: str, day: date) -> dict[datetime, Ohlcv]:
    async with session_factory() as session:
        bars = await BarRepository(session).list_for_day(symbol, day)
    return {b.bucket: Ohlcv(b.open, b.high, b.low, b.close, float(b.volume)) for b in bars}


async def fetch_rest_candles(client, symbol: str, day: date) -> dict[datetime, Ohlcv]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    resp = await client.get_price_history_every_five_minutes(
        symbol,
        start_datetime=start,
        end_datetime=end - timedelta(minutes=1),
        need_extended_hours_data=False,
    )
    resp.raise_for_status()
    # Schwab honors periodType=day/period=1 loosely and can return adjacent sessions
    # (observed 2026-07-09: a 7/9 request yielded 7/8 + 7/9 candles). Keep only the
    # requested UTC day so coverage and the price gate compare the right session — the
    # same half-open [start, start+1d) window fetch_stream_bars reads from the DB.
    return {
        ts: Ohlcv(c["open"], c["high"], c["low"], c["close"], float(c["volume"]))
        for c in resp.json()["candles"]
        if start <= (ts := datetime.fromtimestamp(c["datetime"] / 1000, tz=timezone.utc)) < end
    }


async def _run(settings: IngestSettings, symbol: str, day: date) -> int:
    engine = build_engine(settings.rs_db_url)
    try:
        session_factory = build_session_factory(engine)
        try:
            client = await build_schwab_client(settings, SchwabTokenStore(session_factory))
        except TokenMissingError as exc:
            logger.error("%s", exc)
            return 1
        stream = await fetch_stream_bars(session_factory, symbol, day)
        rest = await fetch_rest_candles(client, symbol, day)
    finally:
        await engine.dispose()
    result = compare(stream, rest)
    print(format_report(result, symbol, day))
    return 0 if result.within_tolerance() else 1


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="stream-built vs REST 5-min bar comparison")
    parser.add_argument("--date", required=True, help="UTC calendar day, YYYY-MM-DD")
    parser.add_argument("--symbol", default="SPY")
    args = parser.parse_args()
    try:
        day = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        parser.error("--date must be YYYY-MM-DD")
    return asyncio.run(_run(IngestSettings(), args.symbol, day))


if __name__ == "__main__":
    sys.exit(main())

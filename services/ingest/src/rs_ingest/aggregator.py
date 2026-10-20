"""Pure 5-minute bar aggregation from Schwab LEVELONE_EQUITIES messages.

Implements the S003 bucketing rule (spec OQ#3 + pinned decisions 1-3):
trade-event-time bucketing keyed on TRADE_TIME_MILLIS floored to 5 minutes,
bar volume as the delta of cumulative TOTAL_VOLUME, RTH-only bar building
with a two-layer filter (the cumulative-volume tracker sees every message,
including pre-RTH), and no-baseline -> no-bar. The no-baseline rule also
enforces discard-to-boundary on mid-session starts: the partial bucket in
progress at startup can never have a cumvol observation from before its
start, so it is dropped by construction.

No I/O and no ambient time: close decisions are driven by tick event times
and by `flush(now)`, where `now` comes from the caller's injected Clock.

A bucket whose volume moved but that saw no LAST_PRICE tick (price never
changed) produces no bar row — matching Schwab REST's omission of no-trade
candles; its volume accrues to the baseline so neighbouring bars stay exact.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from rs_domain import Bar

logger = logging.getLogger(__name__)

BUCKET_MS = 5 * 60 * 1000

# Raw numeric field ids for LEVELONE_EQUITIES, accepted alongside schwab-py's
# relabeled names (the adapter normally delivers relabeled dicts).
_FIELD_ALIASES = {"LAST_PRICE": "3", "TOTAL_VOLUME": "8", "TRADE_TIME_MILLIS": "35"}

COUNTER_NAMES = (
    "bars_built",
    "bars_built_degraded_total",
    "buckets_no_trade",
    "buckets_gap_no_baseline",
    "ticks_late_dropped",
    "ticks_out_of_rth",
    "items_other_key",
)


@dataclass(frozen=True)
class Tick:
    """One normalized content item. `attribution_ms` orders and buckets it:
    TRADE_TIME_MILLIS when present, else the message timestamp."""

    attribution_ms: int
    price: float | None
    cumulative_volume: int | None


def _get(item: dict, field: str) -> Any:
    value = item.get(field)
    return value if value is not None else item.get(_FIELD_ALIASES[field])


class BarAggregator:
    """Single-symbol, single-session bar builder (one instance per run_session)."""

    def __init__(
        self,
        symbol: str,
        session_open: datetime,
        session_close: datetime,
        grace: timedelta = timedelta(seconds=30),
    ) -> None:
        self.symbol = symbol
        self._open_ms = int(session_open.timestamp() * 1000)
        self._close_ms = int(session_close.timestamp() * 1000)
        self._grace_ms = int(grace.total_seconds() * 1000)
        self.counters: dict[str, int] = {name: 0 for name in COUNTER_NAMES}

        # Cumulative-volume tracker (two-layer rule: fed by every message).
        # A bucket's baseline must come from an observation attributed strictly
        # BEFORE its start — that is what drops partial startup buckets.
        self._latest_cumvol: int | None = None
        self._cumvol_bucket_ms: int | None = None  # bucket of the latest cumvol attribution
        self._cumvol_baselines: dict[int, int] = {}  # bucket start -> cumvol just before it
        self._bucket_start_ms: int | None = None  # open bucket, None if none
        self._bucket_baseline: int | None = None  # cumvol baseline of the open bucket
        self._ohlc: dict[str, float] | None = None  # None while open bucket has no trade tick
        self._open_attr_ms = 0  # attribution of the earliest print (owns `open`)
        self._close_attr_ms = 0  # attribution of the latest print (owns `close`)
        self._resolved_until_ms: int | None = None  # buckets ending <= this are settled
        self._degraded = False  # set by note_disconnect, cleared at next close

    # -- ingress ------------------------------------------------------------

    def normalize(self, msg: dict) -> list[Tick]:
        """Extract this symbol's ticks from one raw stream message. Tolerates
        any field subset (Schwab sends only changed fields after the snapshot);
        items with neither price nor volume are irrelevant quote-only updates."""
        ticks = []
        msg_ts = msg.get("timestamp")
        for item in msg.get("content", []):
            if item.get("key") != self.symbol:
                self.counters["items_other_key"] += 1
                continue
            price = _get(item, "LAST_PRICE")
            volume = _get(item, "TOTAL_VOLUME")
            trade_ms = _get(item, "TRADE_TIME_MILLIS")
            if price is None and volume is None:
                continue
            attribution = trade_ms if trade_ms is not None else msg_ts
            if attribution is None:
                continue
            ticks.append(
                Tick(
                    attribution_ms=int(attribution),
                    price=None if price is None else float(price),
                    cumulative_volume=None if volume is None else int(volume),
                )
            )
        return ticks

    def process_message(self, msg: dict) -> list[Bar]:
        bars = []
        for tick in self.normalize(msg):
            bars.extend(self.process_tick(tick))
        return bars

    def process_tick(self, tick: Tick) -> list[Bar]:
        bars = []
        # Boundary crossing closes the open bucket before anything else.
        if self._bucket_start_ms is not None and tick.attribution_ms >= self._bucket_start_ms + BUCKET_MS:
            bars.extend(self._close_open_bucket())

        # Cumulative volume is tracked from every message, wherever it falls
        # (pre-RTH observations are exactly what make the first bar buildable).
        # Crossing into a new cumvol bucket snapshots the value last seen
        # before that boundary — the only legal baseline for that bucket.
        if tick.cumulative_volume is not None:
            bucket = tick.attribution_ms - tick.attribution_ms % BUCKET_MS
            if self._cumvol_bucket_ms is None or bucket > self._cumvol_bucket_ms:
                if self._latest_cumvol is not None:
                    self._cumvol_baselines[bucket] = self._latest_cumvol
                self._cumvol_bucket_ms = bucket
            if self._latest_cumvol is None or tick.cumulative_volume > self._latest_cumvol:
                self._latest_cumvol = tick.cumulative_volume

        if tick.price is None:
            return bars
        if not (self._open_ms <= tick.attribution_ms < self._close_ms):
            self.counters["ticks_out_of_rth"] += 1
            return bars

        bucket_start = tick.attribution_ms - tick.attribution_ms % BUCKET_MS
        if (self._resolved_until_ms is not None and bucket_start < self._resolved_until_ms) or (
            self._bucket_start_ms is not None and bucket_start < self._bucket_start_ms
        ):
            self.counters["ticks_late_dropped"] += 1  # never reopens/pollutes an earlier bucket
            return bars

        if self._bucket_start_ms is None:
            self._open_bucket(bucket_start)
        if self._ohlc is None:
            self._ohlc = {"open": tick.price, "high": tick.price, "low": tick.price, "close": tick.price}
            self._open_attr_ms = self._close_attr_ms = tick.attribution_ms
        else:
            # open/close follow attribution order, not arrival order, so an
            # out-of-order print inside the open bucket can't corrupt them.
            self._ohlc["high"] = max(self._ohlc["high"], tick.price)
            self._ohlc["low"] = min(self._ohlc["low"], tick.price)
            if tick.attribution_ms < self._open_attr_ms:
                self._ohlc["open"] = tick.price
                self._open_attr_ms = tick.attribution_ms
            if tick.attribution_ms >= self._close_attr_ms:
                self._ohlc["close"] = tick.price
                self._close_attr_ms = tick.attribution_ms
        return bars

    # -- clock-driven closes (watchdog + session close) ----------------------

    def flush(self, now: datetime) -> list[Bar]:
        """Close the open bucket once wall clock passes its end (or the session
        close) plus grace — a quiet or stalled feed cannot hold a bar open.
        Call this periodically with the injected Clock's now()."""
        if self._bucket_start_ms is None:
            return []
        deadline = min(self._bucket_start_ms + BUCKET_MS, self._close_ms) + self._grace_ms
        if int(now.timestamp() * 1000) >= deadline:
            return self._close_open_bucket()
        return []

    def note_disconnect(self) -> None:
        """Mark the next BUILT bar as disconnect-scarred (its cumvol delta is
        still exact; only OHLC may have missed prints). The scar deliberately
        survives gap/no-trade closes: buckets inside an outage produce no bars,
        and the first bar built after reconnect is the potentially clipped one.
        Logged + counted, not schema'd."""
        self._degraded = True

    # -- internals ------------------------------------------------------------

    def _open_bucket(self, bucket_start: int) -> None:
        expected = self._resolved_until_ms
        if expected is not None and bucket_start > expected:
            skipped = (bucket_start - expected) // BUCKET_MS
            self.counters["buckets_no_trade"] += skipped
            logger.info("%s: %d no-trade bucket(s) before %s", self.symbol, skipped, _fmt(bucket_start))
        self._bucket_start_ms = bucket_start
        self._bucket_baseline = self._baseline_for(bucket_start)
        self._ohlc = None

    def _baseline_for(self, bucket_start: int) -> int | None:
        """Cumvol last observed strictly before `bucket_start`, or None (=> gap)."""
        if bucket_start in self._cumvol_baselines:
            return self._cumvol_baselines[bucket_start]
        if self._cumvol_bucket_ms is not None and self._cumvol_bucket_ms < bucket_start:
            return self._latest_cumvol  # every observation so far predates this bucket
        return None

    def _close_open_bucket(self) -> list[Bar]:
        start = self._bucket_start_ms
        assert start is not None
        end = start + BUCKET_MS
        ohlc, baseline = self._ohlc, self._bucket_baseline
        self._bucket_start_ms = self._bucket_baseline = self._ohlc = None
        self._resolved_until_ms = end

        # Buckets only open on price ticks, so an open bucket always has OHLC;
        # no-trade buckets never open and are counted in _open_bucket instead.
        assert ohlc is not None
        if baseline is None:
            self.counters["buckets_gap_no_baseline"] += 1
            logger.warning("%s bucket %s: no cumvol baseline, gap (no bar)", self.symbol, _fmt(start))
            return []

        latest = self._latest_cumvol if self._latest_cumvol is not None else baseline
        volume = max(0, latest - baseline)
        if self._degraded:
            self.counters["bars_built_degraded_total"] += 1
            logger.warning("%s bucket %s: built across a stream gap (OHLC may clip)", self.symbol, _fmt(start))
            self._degraded = False
        self.counters["bars_built"] += 1
        bar = Bar(
            symbol=self.symbol,
            bucket=datetime.fromtimestamp(start / 1000, tz=UTC),
            volume=volume,
            **ohlc,
        )
        logger.info("%s bar %s: close=%.2f volume=%d", self.symbol, _fmt(start), bar.close, volume)
        return [bar]


def _fmt(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC).strftime("%H:%M")

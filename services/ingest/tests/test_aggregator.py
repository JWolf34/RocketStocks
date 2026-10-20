"""BarAggregator unit tests over synthetic tick sequences (invented numbers;
no vendor recordings are committed)."""

from datetime import datetime, timedelta, timezone

from rs_ingest.aggregator import BUCKET_MS, BarAggregator

SESSION_OPEN = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
SESSION_CLOSE = datetime(2026, 7, 6, 20, 0, tzinfo=timezone.utc)
OPEN_MS = int(SESSION_OPEN.timestamp() * 1000)
CLOSE_MS = int(SESSION_CLOSE.timestamp() * 1000)


def make_aggregator(**kwargs) -> BarAggregator:
    return BarAggregator("SPY", SESSION_OPEN, SESSION_CLOSE, **kwargs)


def item(key="SPY", price=None, vol=None, tt=None) -> dict:
    out = {"key": key}
    if price is not None:
        out["LAST_PRICE"] = price
    if vol is not None:
        out["TOTAL_VOLUME"] = vol
    if tt is not None:
        out["TRADE_TIME_MILLIS"] = tt
    return out


def msg(ts_ms, *items) -> dict:
    return {"service": "LEVELONE_EQUITIES", "timestamp": ts_ms, "content": list(items)}


def feed(agg, *msgs):
    bars = []
    for m in msgs:
        bars.extend(agg.process_message(m))
    return bars


def test_snapshot_then_changed_field_deltas_builds_bar():
    agg = make_aggregator()
    bars = feed(
        agg,
        # pre-open full snapshot establishes the cumvol baseline
        msg(OPEN_MS - 60_000, item(price=749.0, vol=10_000_000, tt=OPEN_MS - 61_000)),
        # RTH ticks, arbitrary field subsets
        msg(OPEN_MS + 1_000, item(price=750.0, vol=10_050_000, tt=OPEN_MS + 900)),
        msg(OPEN_MS + 60_000, item(price=751.5, tt=OPEN_MS + 59_000)),
        msg(OPEN_MS + 120_000, item(price=749.5, vol=10_200_000, tt=OPEN_MS + 119_000)),
        # boundary tick closes the first bucket
        msg(OPEN_MS + BUCKET_MS + 1_000, item(price=750.2, vol=10_260_000, tt=OPEN_MS + BUCKET_MS + 500)),
    )
    assert len(bars) == 1
    bar = bars[0]
    assert bar.bucket == SESSION_OPEN
    assert (bar.open, bar.high, bar.low, bar.close) == (750.0, 751.5, 749.5, 749.5)
    assert bar.volume == 200_000  # 10.2M - 10.0M, the boundary tick's volume excluded


def test_volume_only_messages_flow_into_bar_volume():
    agg = make_aggregator()
    bars = feed(
        agg,
        msg(OPEN_MS - 1_000, item(vol=5_000_000)),  # baseline, no trade time -> msg ts
        msg(OPEN_MS + 1_000, item(price=750.0, vol=5_010_000, tt=OPEN_MS + 900)),
        msg(OPEN_MS + 2_000, item(vol=5_025_000, tt=OPEN_MS + 1_900)),  # volume-only trade update
        msg(OPEN_MS + BUCKET_MS, item(price=750.1, vol=5_030_000, tt=OPEN_MS + BUCKET_MS)),
    )
    assert len(bars) == 1
    assert bars[0].volume == 25_000


def test_mid_bucket_start_discards_to_boundary():
    agg = make_aggregator()
    mid = OPEN_MS + BUCKET_MS + 120_000  # 2 min into the second bucket
    bars = feed(
        agg,
        msg(mid, item(price=750.0, vol=8_000_000, tt=mid)),  # first observation, mid-bucket
        msg(mid + 30_000, item(price=750.5, vol=8_050_000, tt=mid + 30_000)),
        # crossing into bucket 3 must NOT emit the partial bucket-2 bar
        msg(OPEN_MS + 2 * BUCKET_MS + 1_000, item(price=750.2, vol=8_060_000, tt=OPEN_MS + 2 * BUCKET_MS + 500)),
        # ... and bucket 3 closes with an exact volume delta
        msg(OPEN_MS + 3 * BUCKET_MS, item(price=750.4, vol=8_100_000, tt=OPEN_MS + 3 * BUCKET_MS)),
    )
    assert agg.counters["buckets_gap_no_baseline"] == 1
    assert [b.bucket for b in bars] == [SESSION_OPEN + timedelta(minutes=10)]
    # baseline = last cumvol before bucket-3 start (8_050_000); the closing
    # tick's 8_100_000 update lands after the close
    assert bars[0].volume == 8_060_000 - 8_050_000
    assert bars[0].open == 750.2


def test_premarket_ticks_warm_baseline_but_never_build_bars():
    agg = make_aggregator()
    bars = feed(
        agg,
        msg(OPEN_MS - 300_000, item(price=748.0, vol=9_000_000, tt=OPEN_MS - 301_000)),
        msg(OPEN_MS - 200_000, item(price=748.5, vol=9_100_000, tt=OPEN_MS - 201_000)),
        msg(OPEN_MS + 1_000, item(price=750.0, vol=9_150_000, tt=OPEN_MS + 500)),
        msg(OPEN_MS + BUCKET_MS, item(price=750.3, vol=9_200_000, tt=OPEN_MS + BUCKET_MS)),
    )
    assert agg.counters["ticks_out_of_rth"] == 2  # premarket prices dropped from OHLC
    assert len(bars) == 1
    assert bars[0].open == 750.0  # premarket prints not in the bar
    assert bars[0].volume == 9_150_000 - 9_100_000  # premarket cumvol IS the baseline


def test_late_tick_for_closed_bucket_is_dropped_never_reopened():
    agg = make_aggregator()
    bars = feed(
        agg,
        msg(OPEN_MS - 1_000, item(vol=1_000_000)),
        msg(OPEN_MS + 1_000, item(price=750.0, vol=1_010_000, tt=OPEN_MS + 900)),
        msg(OPEN_MS + BUCKET_MS + 1_000, item(price=751.0, vol=1_020_000, tt=OPEN_MS + BUCKET_MS + 900)),
    )
    assert len(bars) == 1
    late = feed(agg, msg(OPEN_MS + BUCKET_MS + 2_000, item(price=740.0, tt=OPEN_MS + 60_000)))
    assert late == []
    assert agg.counters["ticks_late_dropped"] == 1


def test_out_of_order_tick_before_first_close_cannot_pollute_open_bar():
    agg = make_aggregator()
    b2 = OPEN_MS + BUCKET_MS
    feed(
        agg,
        msg(b2 - 60_000, item(vol=1_000_000)),  # baseline for bucket 2
        msg(b2 + 1_000, item(price=750.0, vol=1_010_000, tt=b2 + 900)),  # opens bucket 2
        # out-of-order print from bucket 1, arriving before any close has happened
        msg(b2 + 2_000, item(price=740.0, tt=OPEN_MS + 30_000)),
    )
    bars = feed(agg, msg(b2 + BUCKET_MS, item(price=750.2, vol=1_020_000, tt=b2 + BUCKET_MS)))
    assert agg.counters["ticks_late_dropped"] == 1
    assert bars[0].low == 750.0  # the 740.0 print never touched the bar


def test_intra_bucket_out_of_order_print_keeps_open_close_by_trade_time():
    agg = make_aggregator()
    bars = feed(
        agg,
        msg(OPEN_MS - 1_000, item(vol=1_000_000)),
        msg(OPEN_MS + 10_000, item(price=750.0, vol=1_010_000, tt=OPEN_MS + 9_000)),
        # straggler: arrives later but its trade time precedes the first print
        msg(OPEN_MS + 11_000, item(price=749.0, tt=OPEN_MS + 4_000)),
        msg(OPEN_MS + 20_000, item(price=750.5, tt=OPEN_MS + 19_000)),
        msg(OPEN_MS + BUCKET_MS, item(price=751.0, vol=1_020_000, tt=OPEN_MS + BUCKET_MS)),
    )
    assert len(bars) == 1
    bar = bars[0]
    assert bar.open == 749.0  # earliest by trade time, despite arriving second
    assert bar.close == 750.5  # latest by trade time
    assert (bar.high, bar.low) == (750.5, 749.0)


def test_watchdog_flush_respects_grace():
    agg = make_aggregator()
    feed(
        agg,
        msg(OPEN_MS - 1_000, item(vol=1_000_000)),
        msg(OPEN_MS + 1_000, item(price=750.0, vol=1_010_000, tt=OPEN_MS + 900)),
    )
    end = SESSION_OPEN + timedelta(milliseconds=BUCKET_MS)
    assert agg.flush(end + timedelta(seconds=29)) == []  # inside grace: stays open
    bars = agg.flush(end + timedelta(seconds=30))
    assert len(bars) == 1
    assert bars[0].close == 750.0


def test_final_bar_closes_at_session_close_plus_grace():
    agg = make_aggregator()
    last_start = CLOSE_MS - BUCKET_MS
    feed(
        agg,
        msg(last_start - 1_000, item(vol=2_000_000)),
        msg(last_start + 1_000, item(price=752.0, vol=2_050_000, tt=last_start + 900)),
    )
    bars = agg.flush(SESSION_CLOSE + timedelta(seconds=30))
    assert len(bars) == 1
    assert bars[0].bucket == SESSION_CLOSE - timedelta(milliseconds=BUCKET_MS)
    assert bars[0].volume == 50_000


def test_zero_trade_bucket_yields_no_bar_and_exact_neighbours():
    agg = make_aggregator()
    b2 = OPEN_MS + BUCKET_MS
    b3 = OPEN_MS + 2 * BUCKET_MS
    bars = feed(
        agg,
        msg(OPEN_MS - 1_000, item(vol=1_000_000)),
        msg(OPEN_MS + 1_000, item(price=750.0, vol=1_010_000, tt=OPEN_MS + 900)),
        # bucket 2: volume moves (price never changes -> volume-only updates)
        msg(b2 + 1_000, item(vol=1_020_000, tt=b2 + 900)),
        # bucket 3 trade closes bucket 2 (no bar) and opens bucket 3
        msg(b3 + 1_000, item(price=750.5, vol=1_030_000, tt=b3 + 900)),
        msg(b3 + BUCKET_MS, item(price=750.6, vol=1_040_000, tt=b3 + BUCKET_MS)),
    )
    assert agg.counters["buckets_no_trade"] == 1
    assert [b.bucket for b in bars] == [SESSION_OPEN, SESSION_OPEN + timedelta(minutes=10)]
    assert bars[0].volume == 10_000
    assert bars[1].volume == 10_000  # bucket-2 volume stayed in the baseline, not smeared here


def test_skipped_buckets_counted_as_no_trade():
    agg = make_aggregator()
    b3 = OPEN_MS + 2 * BUCKET_MS
    feed(
        agg,
        msg(OPEN_MS - 1_000, item(vol=1_000_000)),
        msg(OPEN_MS + 1_000, item(price=750.0, vol=1_010_000, tt=OPEN_MS + 900)),
        msg(b3 + 1_000, item(price=750.5, vol=1_020_000, tt=b3 + 900)),  # nothing at all in bucket 2
    )
    assert agg.counters["buckets_no_trade"] == 1


def test_disconnect_scarred_bar_is_built_and_counted():
    agg = make_aggregator()
    feed(
        agg,
        msg(OPEN_MS - 1_000, item(vol=1_000_000)),
        msg(OPEN_MS + 1_000, item(price=750.0, vol=1_010_000, tt=OPEN_MS + 900)),
    )
    agg.note_disconnect()
    bars = feed(
        agg,
        msg(OPEN_MS + 240_000, item(price=750.4, vol=1_100_000, tt=OPEN_MS + 239_000)),
        msg(OPEN_MS + BUCKET_MS, item(price=750.5, vol=1_110_000, tt=OPEN_MS + BUCKET_MS)),
    )
    assert len(bars) == 1
    assert bars[0].volume == 100_000  # cumvol delta exact across the gap
    assert agg.counters["bars_built_degraded_total"] == 1


def test_raw_numeric_field_ids_accepted():
    agg = make_aggregator()
    bars = feed(
        agg,
        msg(OPEN_MS - 1_000, {"key": "SPY", "8": 1_000_000}),
        msg(OPEN_MS + 1_000, {"key": "SPY", "3": 750.0, "8": 1_010_000, "35": OPEN_MS + 900}),
        msg(OPEN_MS + BUCKET_MS, {"key": "SPY", "3": 750.1, "8": 1_020_000, "35": OPEN_MS + BUCKET_MS}),
    )
    assert len(bars) == 1
    assert bars[0].volume == 10_000


def test_other_symbols_ignored():
    agg = make_aggregator()
    bars = feed(
        agg,
        msg(OPEN_MS - 1_000, item(vol=1_000_000)),
        msg(OPEN_MS + 1_000, item(key="QQQ", price=500.0, vol=9_999_999, tt=OPEN_MS + 900)),
        msg(OPEN_MS + 2_000, item(price=750.0, vol=1_010_000, tt=OPEN_MS + 1_900)),
        msg(OPEN_MS + BUCKET_MS, item(price=750.1, vol=1_020_000, tt=OPEN_MS + BUCKET_MS)),
    )
    assert agg.counters["items_other_key"] == 1
    assert len(bars) == 1
    assert bars[0].open == 750.0
    assert bars[0].volume == 10_000


def test_synthetic_multi_bucket_stream():
    """A hand-built, clearly synthetic tick stream (invented prices, not market
    data) through several buckets: a mid-bucket start (that bucket is a gap),
    two full buckets with hand-derived OHLCV, and a partial tail (no close
    trigger, so no bar)."""
    # (offset into the bucket in ms, price, cumulative day volume)
    script = {
        3: [(120_000, 750.0, 8_000_000), (150_000, 750.5, 8_050_000)],  # partial start
        4: [
            (1_000, 750.2, 8_060_000),
            (60_000, 751.0, 8_100_000),
            (120_000, 749.8, 8_130_000),
            (250_000, 750.6, 8_170_000),
        ],
        5: [
            (500, 750.7, 8_180_000),
            (100_000, 752.0, 8_230_000),
            (200_000, 750.4, 8_260_000),
            (299_000, 751.2, 8_300_000),
        ],
        6: [(1_000, 751.3, 8_310_000), (30_000, 751.1, 8_330_000)],  # partial tail
    }
    agg = make_aggregator()
    bars = []
    for bucket, ticks in script.items():
        for offset, price, cumvol in ticks:
            t = OPEN_MS + bucket * BUCKET_MS + offset
            bars.extend(agg.process_message(msg(t, item(price=price, vol=cumvol, tt=t))))

    # Mid-bucket start: bucket 3 is a gap, not a partial bar.
    assert agg.counters["buckets_gap_no_baseline"] == 1
    assert agg.counters["ticks_late_dropped"] == 0

    assert [b.bucket for b in bars] == [
        SESSION_OPEN + timedelta(minutes=20),
        SESSION_OPEN + timedelta(minutes=25),
    ]
    b4, b5 = bars
    assert (b4.open, b4.high, b4.low, b4.close) == (750.2, 751.0, 749.8, 750.6)
    assert b4.volume == 8_170_000 - 8_050_000
    assert (b5.open, b5.high, b5.low, b5.close) == (750.7, 752.0, 750.4, 751.2)
    assert b5.volume == 8_300_000 - 8_170_000

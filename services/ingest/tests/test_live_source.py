"""LiveEventSource: single-session lifecycle, reconnect policy, watchdog flush.

The fake stream runs a script of async steps — deliver a message, advance the
fake clock, raise — so every network behavior the adapter must survive is
reproducible without a socket. Session bounds match the aggregator tests:
2026-07-06 13:30–20:00 UTC.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from authlib.integrations.base_client import OAuthError

from rs_ingest.sources.live import LiveEventSource, ReconnectPolicy

OPEN = datetime(2026, 7, 6, 13, 30, tzinfo=UTC)
CLOSE = datetime(2026, 7, 6, 20, 0, tzinfo=UTC)
GRACE = timedelta(seconds=30)
AFTER_STOP = CLOSE + GRACE + timedelta(seconds=1)


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def tick_msg(at: datetime, price: float | None = None, volume: int | None = None) -> dict:
    item: dict = {"key": "SPY", "TRADE_TIME_MILLIS": ms(at)}
    if price is not None:
        item["LAST_PRICE"] = price
    if volume is not None:
        item["TOTAL_VOLUME"] = volume
    return {"timestamp": ms(at), "content": [item]}


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance_to(self, at: datetime) -> None:
        self._now = at


class FakeCalendar:
    def __init__(self, session: bool = True) -> None:
        self._session = session

    def is_session(self, day):
        return self._session

    def session_open(self, day):
        return OPEN

    def session_close(self, day):
        return CLOSE


class FakeStream:
    """Each script step is an async callable(handler); an exhausted script
    hangs like a quiet feed (the message timeout is the way out)."""

    def __init__(self, *steps) -> None:
        self._steps = list(steps)
        self._handler = None
        self.subscribed = None

    async def login(self) -> None:
        pass

    def add_level_one_equity_handler(self, handler) -> None:
        self._handler = handler

    async def level_one_equity_subs(self, symbols) -> None:
        self.subscribed = symbols

    async def handle_message(self) -> None:
        if not self._steps:
            await asyncio.Event().wait()
        await self._steps.pop(0)(self._handler)


def deliver(message: dict):
    async def step(handler):
        handler(message)

    return step


def advance(clock: FakeClock, to: datetime):
    async def step(handler):
        clock.advance_to(to)

    return step


def crash(exc: Exception):
    async def step(handler):
        raise exc

    return step


def advance_then_crash(clock: FakeClock, to: datetime, exc: Exception):
    async def step(handler):
        clock.advance_to(to)
        raise exc

    return step


class HangingLoginStream(FakeStream):
    """Connects but its login never returns — the connect-timeout path."""

    async def login(self) -> None:
        await asyncio.Event().wait()


class StreamFactory:
    def __init__(self, *streams: FakeStream) -> None:
        self._streams = list(streams)
        self.created: list[FakeStream] = []

    def __call__(self) -> FakeStream:
        stream = self._streams.pop(0)
        self.created.append(stream)
        return stream


def make_source(factory, clock, calendar=None, **kwargs) -> LiveEventSource:
    return LiveEventSource(
        symbol="SPY",
        stream_factory=factory,
        calendar=calendar or FakeCalendar(),
        clock=clock,
        grace=GRACE,
        watchdog_interval=0.01,
        **kwargs,
    )


async def collect(source: LiveEventSource) -> list:
    return [event async for event in source.events()]


# -- reconnect policy (pure) --------------------------------------------------


def test_backoff_doubles_and_caps():
    policy = ReconnectPolicy()
    assert [policy.next_backoff(None) for _ in range(6)] == [2, 4, 8, 16, 30, 30]


def test_backoff_resets_after_stable_connection():
    policy = ReconnectPolicy()
    policy.next_backoff(None)
    policy.next_backoff(None)
    assert policy.next_backoff(120.0) == 1.0  # survived >= 60s → counter reset
    assert policy.total_reconnects == 3


def test_backoff_survives_a_very_long_outage():
    # float(2**1024) overflows; the cap must hold well past that many failures.
    policy = ReconnectPolicy()
    policy.consecutive_failures = 1023
    assert [policy.next_backoff(None) for _ in range(3)] == [30, 30, 30]


def test_short_lived_connection_is_not_stable():
    policy = ReconnectPolicy()
    policy.next_backoff(None)
    assert policy.next_backoff(5.0) == 4.0  # 5s lifetime keeps escalating


# -- session lifecycle (pinned #11) -------------------------------------------


async def test_non_session_day_yields_nothing_without_connecting():
    factory = StreamFactory()  # any construction attempt would IndexError
    source = make_source(factory, FakeClock(OPEN), calendar=FakeCalendar(session=False))
    assert await collect(source) == []
    assert factory.created == []


async def test_started_after_close_yields_nothing_without_connecting():
    factory = StreamFactory()
    source = make_source(factory, FakeClock(AFTER_STOP))
    assert await collect(source) == []
    assert factory.created == []


async def test_bars_stream_as_ordered_barclosed_events():
    clock = FakeClock(OPEN - timedelta(minutes=1))
    stream = FakeStream(
        deliver(tick_msg(OPEN - timedelta(minutes=1), volume=1_000_000)),  # premarket baseline
        deliver(tick_msg(OPEN + timedelta(seconds=10), price=100.0, volume=1_000_100)),
        deliver(tick_msg(OPEN + timedelta(minutes=1), price=101.0, volume=1_050_000)),
        deliver(tick_msg(OPEN + timedelta(minutes=5, seconds=1), price=100.5, volume=1_080_000)),
        advance(clock, AFTER_STOP),
    )
    factory = StreamFactory(stream)
    source = make_source(factory, clock)
    events = await collect(source)

    assert stream.subscribed == ["SPY"]
    assert [(e.event_time, e.sequence) for e in events] == [
        (OPEN + timedelta(minutes=5), 0),
        (OPEN + timedelta(minutes=10), 0),
    ]
    first, second = (e.bar for e in events)
    assert (first.open, first.high, first.low, first.close) == (100.0, 101.0, 100.0, 101.0)
    assert first.volume == 50_000  # cumvol at close boundary − premarket baseline
    assert second.close == 100.5
    assert second.volume == 30_000  # flushed by the watchdog at session end


async def test_watchdog_flushes_stalled_bucket_mid_session():
    clock = FakeClock(OPEN - timedelta(minutes=1))
    flush_at = OPEN + timedelta(minutes=5) + GRACE + timedelta(seconds=1)
    stream = FakeStream(
        deliver(tick_msg(OPEN - timedelta(minutes=1), volume=1_000_000)),
        deliver(tick_msg(OPEN + timedelta(seconds=10), price=100.0, volume=1_040_000)),
        advance(clock, flush_at),  # then the script hangs: feed went quiet
    )
    source = make_source(StreamFactory(stream), clock)

    events = []
    async for event in source.events():
        events.append((event, clock.now()))
        break  # close the generator mid-session — we only need the flush proof

    (event, seen_at), = events
    assert seen_at < CLOSE  # yielded by the watchdog, not by session end
    assert event.bar.bucket == OPEN
    assert event.bar.volume == 40_000


async def test_stream_error_reconnects_and_scars_next_bar():
    clock = FakeClock(OPEN - timedelta(minutes=1))
    first = FakeStream(
        deliver(tick_msg(OPEN - timedelta(minutes=1), volume=1_000_000)),
        deliver(tick_msg(OPEN + timedelta(seconds=10), price=100.0, volume=1_000_500)),
        crash(RuntimeError("socket closed")),
    )
    second = FakeStream(
        deliver(tick_msg(OPEN + timedelta(minutes=1), price=101.0, volume=1_040_000)),
        deliver(tick_msg(OPEN + timedelta(minutes=5, seconds=5), price=102.0, volume=1_100_000)),
        advance(clock, AFTER_STOP),
    )
    factory = StreamFactory(first, second)
    policy = ReconnectPolicy(cap_seconds=0)  # no real sleeping in tests
    source = make_source(factory, clock, policy=policy)
    events = await collect(source)

    assert len(factory.created) == 2
    assert policy.total_reconnects == 1
    assert [e.bar.volume for e in events] == [40_000, 60_000]
    assert source.aggregator.counters["bars_built_degraded_total"] == 1


async def test_message_silence_triggers_reconnect():
    clock = FakeClock(OPEN)
    first = FakeStream()  # empty script: hangs immediately
    second = FakeStream(advance(clock, AFTER_STOP))
    factory = StreamFactory(first, second)
    policy = ReconnectPolicy(cap_seconds=0)
    source = make_source(factory, clock, policy=policy, message_timeout=0.05)

    assert await collect(source) == []
    assert len(factory.created) == 2
    assert policy.total_reconnects == 1


async def test_oauth_failure_is_fatal_not_reconnectable():
    clock = FakeClock(OPEN)
    factory = StreamFactory(FakeStream(crash(OAuthError(error="invalid_grant"))))
    source = make_source(factory, clock)

    with pytest.raises(OAuthError):
        await collect(source)
    assert len(factory.created) == 1  # no second connection attempt


async def test_stalled_connect_reconnects():
    # A login that never returns must reconnect, not park the whole session.
    clock = FakeClock(OPEN)
    first = HangingLoginStream()
    second = FakeStream(advance(clock, AFTER_STOP))
    factory = StreamFactory(first, second)
    policy = ReconnectPolicy(cap_seconds=0)
    source = make_source(factory, clock, policy=policy, connect_timeout=0.05)

    assert await collect(source) == []
    assert len(factory.created) == 2
    assert policy.total_reconnects == 1


async def test_drop_at_session_end_is_not_counted_as_reconnect():
    # A drop whose fallout lands past stop_at is a session-end teardown, not a
    # reconnect — the counter must not tick.
    clock = FakeClock(OPEN)
    stream = FakeStream(advance_then_crash(clock, AFTER_STOP, RuntimeError("drop at close")))
    factory = StreamFactory(stream)
    policy = ReconnectPolicy()
    source = make_source(factory, clock, policy=policy)

    assert await collect(source) == []
    assert len(factory.created) == 1
    assert policy.total_reconnects == 0

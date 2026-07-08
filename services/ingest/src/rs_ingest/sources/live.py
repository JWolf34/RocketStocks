"""Live EventSource: schwab-py stream → BarAggregator → BarClosed (S003 B5).

Wraps the streamer behind the rs_domain EventSource port for exactly one
market session (spec pinned #11): a non-session day yields nothing; started
pre-open it connects immediately (premarket ticks warm the cumvol baseline,
making the 09:30 bar buildable); after session close + grace the final bar
is flushed and the iterator ends. An outer multi-session loop can wrap
`events()` later without redesign.

Reconnect policy ported from scripts/spikes/stream_spike.py:97-125: no
message for MESSAGE_TIMEOUT_SECONDS or any stream error tears the connection
down and reconnects with exponential backoff (capped, reset once a
connection proves stable); the aggregator is scarred via note_disconnect()
so the next built bar is counted degraded. An OAuth failure is fatal, not
reconnectable (pinned #9): the refresh token has a 7-day hard expiry and
retrying cannot fix it — log loudly, crash, re-login is the remedy.
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Protocol

# schwab-py drives its OAuth through authlib and does not wrap its errors, so
# a refresh failure (expired 7-day refresh token) surfaces as authlib's
# OAuthError — every token-refresh subclass derives from it. authlib is a
# pinned direct dependency for this reason; if a future schwab-py moves off it,
# this import fails loudly at startup rather than silently mismatching.
from authlib.integrations.base_client import OAuthError

from rs_domain import Bar, BarClosed, Clock, TradingCalendar
from rs_ingest.aggregator import BUCKET_MS, BarAggregator

logger = logging.getLogger(__name__)

BUCKET = timedelta(milliseconds=BUCKET_MS)  # bar width; single source is the aggregator
MESSAGE_TIMEOUT_SECONDS = 90.0  # spike-measured: quiet RTH gaps stay well under this
CONNECT_TIMEOUT_SECONDS = 30.0  # a login/subscribe that stalls past this reconnects
WATCHDOG_INTERVAL_SECONDS = 1.0

LOGIN_REMEDY = "run: python -m rs_ingest.tools.login"


class EquityStream(Protocol):
    """The slice of schwab-py's StreamClient this adapter touches."""

    async def login(self) -> None: ...

    def add_level_one_equity_handler(self, handler: Callable[[dict], None]) -> None: ...

    async def level_one_equity_subs(self, symbols: list[str]) -> None: ...

    async def handle_message(self) -> None: ...


class ReconnectPolicy:
    """Exponential backoff capped at `cap_seconds`, counter reset once a
    connection survives `stability_seconds` (stream_spike.py behavior)."""

    def __init__(self, cap_seconds: float = 30.0, stability_seconds: float = 60.0) -> None:
        self.cap_seconds = cap_seconds
        self.stability_seconds = stability_seconds
        self.consecutive_failures = 0
        self.total_reconnects = 0

    def next_backoff(self, connection_lifetime_seconds: float | None) -> float:
        """Seconds to wait before reconnecting; the argument is how long the
        just-failed connection lived (None if it never came up)."""
        was_stable = (
            connection_lifetime_seconds is not None
            and connection_lifetime_seconds >= self.stability_seconds
        )
        self.consecutive_failures = 0 if was_stable else self.consecutive_failures + 1
        self.total_reconnects += 1
        return min(self.cap_seconds, float(2**self.consecutive_failures))


class LiveEventSource:
    """rs_domain.ports.EventSource implementation for one live session."""

    def __init__(
        self,
        symbol: str,
        stream_factory: Callable[[], EquityStream],
        calendar: TradingCalendar,
        clock: Clock,
        *,
        grace: timedelta = timedelta(seconds=30),
        policy: ReconnectPolicy | None = None,
        message_timeout: float = MESSAGE_TIMEOUT_SECONDS,
        connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
        watchdog_interval: float = WATCHDOG_INTERVAL_SECONDS,
    ) -> None:
        self._symbol = symbol
        self._stream_factory = stream_factory
        self._calendar = calendar
        self._clock = clock
        self._grace = grace
        self.policy = policy or ReconnectPolicy()
        self._message_timeout = message_timeout
        self._connect_timeout = connect_timeout
        self._watchdog_interval = watchdog_interval
        self.aggregator: BarAggregator | None = None  # set once a session starts

    async def events(self) -> AsyncIterator[BarClosed]:
        now = self._clock.now()
        # Session key from the UTC date. This is exact only for a pre-open
        # (ET-morning) start, where the UTC and ET dates agree — the documented
        # S003 operating procedure (spec pinned #11/#13). The by-v2-end outer
        # loop must resolve the session day itself for post-open/overnight
        # starts; it cannot rely on now.date() the way this single session does.
        day = now.date()
        if not self._calendar.is_session(day):
            logger.info("%s is not a trading session — nothing to do", day)
            return
        session_open = self._calendar.session_open(day)
        session_close = self._calendar.session_close(day)
        stop_at = session_close + self._grace
        if now >= stop_at:
            logger.info("session %s already closed — nothing to do", day)
            return

        aggregator = BarAggregator(self._symbol, session_open, session_close, grace=self._grace)
        self.aggregator = aggregator
        bars: asyncio.Queue[Bar] = asyncio.Queue()

        def on_message(msg: dict) -> None:
            for bar in aggregator.process_message(msg):
                bars.put_nowait(bar)

        receiver = asyncio.create_task(self._receive_until(stop_at, on_message, aggregator))
        logger.info("session %s: streaming %s until %s UTC", day, self._symbol, stop_at.strftime("%H:%M:%S"))
        try:
            while True:
                # Yield every boundary-closed bar (already in event_time order),
                # THEN run the watchdog flush: a flushed bar closes the latest
                # open bucket, so it can never precede a still-queued earlier one.
                while not bars.empty():
                    yield self._to_event(bars.get_nowait())
                now = self._clock.now()
                for bar in aggregator.flush(now):
                    yield self._to_event(bar)
                if now >= stop_at:
                    break
                if receiver.done() and (exc := receiver.exception()) is not None:
                    raise exc  # receiver failures crash loud (spec failure policy)
                await asyncio.sleep(self._watchdog_interval)
        finally:
            receiver.cancel()
            with suppress(asyncio.CancelledError):
                await receiver
        while not bars.empty():  # bars the receiver queued during its final teardown
            yield self._to_event(bars.get_nowait())
        logger.info("session %s over: %s", day, aggregator.counters)

    def _to_event(self, bar: Bar) -> BarClosed:
        # Single symbol => one BarClosed per event_time by construction, so
        # sequence is 0; the durable-log derivation (spec OQ#3) is the
        # multi-symbol rule.
        return BarClosed(event_time=bar.bucket + BUCKET, sequence=0, bar=bar)

    async def _receive_until(
        self,
        stop_at: datetime,
        on_message: Callable[[dict], None],
        aggregator: BarAggregator,
    ) -> None:
        while self._clock.now() < stop_at:
            connected_at: datetime | None = None
            try:
                stream = self._stream_factory()
                # A stalled connect/auth must reconnect too, not just a stalled
                # message loop — so login and subscribe are timeout-bounded.
                await asyncio.wait_for(stream.login(), timeout=self._connect_timeout)
                stream.add_level_one_equity_handler(on_message)
                await asyncio.wait_for(
                    stream.level_one_equity_subs([self._symbol]), timeout=self._connect_timeout
                )
                connected_at = self._clock.now()
                logger.info("connected, subscribed %s", self._symbol)
                while self._clock.now() < stop_at:
                    await asyncio.wait_for(stream.handle_message(), timeout=self._message_timeout)
                return  # reached stop_at with the connection healthy
            except TimeoutError:
                logger.warning("stream connect/message stalled — reconnecting")
            except OAuthError:
                logger.error("Schwab OAuth failed — refresh token likely expired (7-day limit); %s", LOGIN_REMEDY)
                raise
            except Exception as exc:
                logger.warning("stream dropped: %s: %s", type(exc).__name__, exc)
            if connected_at is not None:
                aggregator.note_disconnect()
            if self._clock.now() >= stop_at:
                return  # session-end teardown, not a reconnect — don't count it
            lifetime = None if connected_at is None else (self._clock.now() - connected_at).total_seconds()
            backoff = self.policy.next_backoff(lifetime)
            logger.info("reconnect #%d in %.0fs", self.policy.total_reconnects, backoff)
            await asyncio.sleep(backoff)

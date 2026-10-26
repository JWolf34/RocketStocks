"""The LISTEN half of alert delivery: a dedicated psycopg AsyncConnection in
autocommit holds LISTEN and turns every NOTIFY into a wake of the sweep worker.
The payload (alert id) is logged, never consumed as the work item - the row
is the truth, the sweep is the consumer.

Cover for a dead LISTEN is structural: a dropped connection reconnects with capped
backoff, and EVERY (re)connect sets the wake event, so "sweep on reconnect"
is not a special case - anything committed while the LISTEN was dead is
picked up by the recovery sweep the moment the connection heals.

Shutdown is by task cancellation: CancelledError is a BaseException, so the
reconnect handler below can never swallow it.
"""

import asyncio
import logging

import psycopg

from rs_persistence import NOTIFY_CHANNEL

logger = logging.getLogger(__name__)


class ListenLoop:
    def __init__(
        self,
        conninfo: str,
        wake: asyncio.Event,
        *,
        counters: dict[str, int],
        channel: str = NOTIFY_CHANNEL,
        initial_backoff: float = 1.0,
        max_backoff: float = 30.0,
    ) -> None:
        self._conninfo = conninfo
        self._wake = wake
        self._counters = counters
        self._channel = channel
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff

    async def run(self) -> None:
        backoff = self._initial_backoff
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(
                    self._conninfo, autocommit=True
                ) as conn:
                    await conn.execute(f'LISTEN "{self._channel}"')
                    backoff = self._initial_backoff
                    logger.info("LISTEN established on %s", self._channel)
                    self._wake.set()  # recovery sweep - (re)connect always sweeps
                    async for notify in conn.notifies():
                        self._counters["notify_wakes_total"] += 1
                        logger.info("notify wake: channel=%s payload=%s", notify.channel, notify.payload)
                        self._wake.set()
            except Exception:
                self._counters["listen_reconnects_total"] += 1
                logger.warning(
                    "LISTEN connection lost - reconnecting in %.1fs (reconnects=%d)",
                    backoff,
                    self._counters["listen_reconnects_total"],
                    exc_info=True,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._max_backoff)

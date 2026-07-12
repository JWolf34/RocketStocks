"""The single posting path (spec pins #3/#4): one worker waits on (wake event
OR the sweep interval), then drains every unposted alert in id order. NOTIFY
never posts — the LISTEN loop only sets the wake event, so the correctness
mechanism (cursor sweep) runs on every post and cannot rot unnoticed
(Finding 3 is the cautionary live demo).

Per row: render → send → mark_posted → commit. The per-row commit bounds the
crash-dupe window to one alert: delivery is at-least-once, a crash inside the
send→commit gap re-posts ≤1 alert on restart (pin #4). A send failure is
ERROR-logged with the full payload, counted, and skipped — retried on every
sweep, uncapped (pin #5b; escalation machinery waits for the ADR-0016 rail).

Strict id-order posting is a normal-operation property, not a failure-mode
guarantee: a skipped poison row posts after its younger siblings once it
heals.
"""

import asyncio
import logging
from contextlib import suppress
from typing import Protocol

import discord
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_persistence import AlertRepository

from rs_bot.counters import new_counters
from rs_bot.render import render

logger = logging.getLogger(__name__)


class EmbedSender(Protocol):
    """The send seam: tests fake this; the daemon backs it with the resolved
    dev channel. Returns the Discord message id (logged for G5 auditing)."""

    async def send(self, embed: discord.Embed) -> int: ...


class SweepWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        sender: EmbedSender,
        *,
        sweep_seconds: float,
        counters: dict[str, int] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._sender = sender
        self._sweep_seconds = sweep_seconds
        self.counters = counters if counters is not None else new_counters()
        # Set by the LISTEN loop on NOTIFY and on every (re)connect — so
        # "sweep on reconnect" is not a special case (spec Interface).
        self.wake = asyncio.Event()
        self._stopping = asyncio.Event()

    def request_stop(self) -> None:
        """Graceful shutdown: finish the in-flight post, then return. Rows not
        yet reached stay unposted — the next start's initial sweep reconciles
        them (the at-least-once contract, not a special shutdown path)."""
        self._stopping.set()
        self.wake.set()

    async def run(self) -> None:
        await self.sweep()  # unconditional initial sweep (spec lifecycle)
        while not self._stopping.is_set():
            with suppress(TimeoutError):
                await asyncio.wait_for(self.wake.wait(), timeout=self._sweep_seconds)
            self.wake.clear()
            if self._stopping.is_set():
                break
            await self.sweep()

    async def sweep(self) -> None:
        """One drain pass over the cursor: every posted_at IS NULL row, id
        order, no LIMIT (pin #10 — the sweep drains fully)."""
        self.counters["sweeps_total"] += 1
        async with self._session_factory() as session:
            repo = AlertRepository(session)
            pending = await repo.list_unposted()
            for item in pending:
                if self._stopping.is_set():
                    break
                embed = render(item)
                try:
                    message_id = await self._sender.send(embed)
                except Exception:
                    self.counters["post_failures_total"] += 1
                    logger.error(
                        "send failed — skipped, retried next sweep: alert id=%d %s %s "
                        "triggered_at=%s payload=%s",
                        item.id,
                        item.alert.alert_type,
                        item.alert.symbol,
                        item.alert.triggered_at,
                        item.alert.payload,
                        exc_info=True,
                    )
                    continue
                posted_at = await repo.mark_posted(item.id)
                await session.commit()  # per-row commit: dupe window ≤ 1 alert
                self.counters["discord_posts_total"] += 1
                if posted_at is None:
                    logger.warning(
                        "alert id=%d was already marked posted — possible duplicate post "
                        "(crash inside a previous send→commit gap)",
                        item.id,
                    )
                else:
                    # DB-derived lag (pin #9): posted_at − triggered_at, both
                    # server-side stamps — no client clock in rs_bot.
                    logger.info(
                        "posted alert id=%d message_id=%d lag=%.1fs",
                        item.id,
                        message_id,
                        (posted_at - item.alert.triggered_at).total_seconds(),
                    )
        if pending:
            logger.info("counters: %s", self.counters)

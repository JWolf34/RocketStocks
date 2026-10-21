"""The single posting path: one worker waits on (wake event
OR the sweep interval), then drains every unposted alert in id order. NOTIFY
never posts — the LISTEN loop only sets the wake event, so the correctness
mechanism (cursor sweep) runs on every post and cannot rot unnoticed.

Per row: read track record → render → send → mark_posted → commit. The per-row
commit bounds the crash-dupe window to one alert: delivery is at-least-once, a
crash inside the send→commit gap re-posts ≤1 alert on restart. A send
failure is ERROR-logged with the full payload, counted in `post_attempts` and
skipped; at `max_attempts` the alert is failed-to-post: it leaves the sweep
(`posted_at` stays NULL) and ops is told once.

Strict id-order posting is a normal-operation property, not a failure-mode
guarantee: a skipped poison row posts after its younger siblings once it
heals.
"""

import asyncio
import logging
from contextlib import suppress
from typing import Protocol

import aiohttp
import discord
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_bot.counters import new_counters
from rs_bot.render import POPULARITY_SURGE, render, render_generic
from rs_domain import TrackRecord
from rs_observability import HealthTracker, Source, notify_ops, unit_of_work
from rs_persistence import AlertRepository, PendingAlert
from rs_query import current_track_record

logger = logging.getLogger(__name__)


# Discord's answer about one embed it will not take (a malformed or oversized body).
_REJECTED_EMBED_STATUS = 400


def _discord_health(exc: Exception) -> bool | None:
    """What a failed send says about Discord: True when it answered (reachable), False when it
    is failing (a 5xx, a 429, a refused permission, a dropped connection), None when it says
    nothing (a bug in rendering or sending one Alert), so one bad Alert never flaps Discord."""
    if isinstance(exc, discord.HTTPException):
        return exc.status == _REJECTED_EMBED_STATUS
    if isinstance(exc, aiohttp.ClientError | OSError | TimeoutError | discord.ConnectionClosed):
        return False
    return None


class EmbedSender(Protocol):
    """The send seam: tests fake this; the daemon backs it with the resolved
    alert channel. Returns the Discord message id (logged for auditing)."""

    async def send(self, embed: discord.Embed) -> int: ...


class SweepWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        sender: EmbedSender,
        *,
        sweep_seconds: float,
        health: HealthTracker,
        max_attempts: int = 5,
        retry_gap_seconds: float | None = None,
        counters: dict[str, int] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._sender = sender
        self._health = health
        self._sweep_seconds = sweep_seconds
        self._max_attempts = max_attempts
        self._retry_gap = sweep_seconds if retry_gap_seconds is None else retry_gap_seconds
        self.counters = counters if counters is not None else new_counters()
        # Set by the LISTEN loop on NOTIFY and on every (re)connect — so
        # "sweep on reconnect" is not a special case.
        # Monotonic retry gate per alert id (in memory: a restart retries at once).
        self._retry_not_before: dict[int, float] = {}
        self.wake = asyncio.Event()
        self._stopping = asyncio.Event()

    def request_stop(self) -> None:
        """Graceful shutdown: finish the in-flight post, then return. Rows not
        yet reached stay unposted — the next start's initial sweep reconciles
        them (the at-least-once contract, not a special shutdown path)."""
        self._stopping.set()
        self.wake.set()

    async def run(self) -> None:
        await self.sweep()  # unconditional initial sweep
        while not self._stopping.is_set():
            with suppress(TimeoutError):
                await asyncio.wait_for(self.wake.wait(), timeout=self._sweep_seconds)
            self.wake.clear()
            if self._stopping.is_set():
                break
            await self.sweep()

    async def _track_record(
        self,
        session: AsyncSession,
        item: PendingAlert,
        cache: dict[tuple[str, str], tuple[TrackRecord | None, bool]],
    ) -> tuple[TrackRecord | None, bool]:
        """(track record, read failed), read once per rule version per sweep."""
        alert = item.alert
        if alert.alert_type != POPULARITY_SURGE or alert.rule_version is None:
            return None, False
        key = (alert.alert_type, alert.rule_version)
        if key not in cache:
            try:
                cache[key] = (await current_track_record(session, *key), False)
            except Exception:
                logger.exception("track record read failed for %s v%s", *key)
                await session.rollback()
                cache[key] = (None, True)
        return cache[key]

    async def sweep(self) -> None:
        """One drain pass over the cursor: every posted_at IS NULL row, id
        order, no LIMIT - the sweep drains fully."""
        self.counters["sweeps_total"] += 1
        async with self._session_factory() as session:
            repo = AlertRepository(session)
            pending = await repo.list_unposted(self._max_attempts)
            loop = asyncio.get_running_loop()
            track_records: dict[tuple[str, str], tuple[TrackRecord | None, bool]] = {}
            for item in pending:
                if self._stopping.is_set():
                    break
                # One posting attempt = one unit of work: discord.py's
                # retry internals log inside it and group under the alert.
                # alert_id is the cross-service join key (ingest's insert log
                # carries the same field).
                with unit_of_work(alert_id=item.id):
                    if self._retry_not_before.get(item.id, 0.0) > loop.time():
                        continue  # failed moments ago; a NOTIFY burst must not burn the cap
                    track_record, track_record_failed = await self._track_record(session, item, track_records)
                    if item.post_attempts + 1 >= self._max_attempts:
                        # Last try: the plain embed cannot be rejected for its shape.
                        embed = render_generic(item)
                    else:
                        embed = render(item, track_record, track_record_failed=track_record_failed)
                    try:
                        message_id = await self._sender.send(embed)
                    except Exception as exc:
                        reachable = _discord_health(exc)
                        if reachable:
                            await self._health.report_ok(Source.DISCORD)
                        elif reachable is False:
                            await self._health.report_failed(Source.DISCORD, type(exc).__name__)
                        self.counters["post_failures_total"] += 1
                        attempts = await repo.increment_post_attempts(item.id)
                        await session.commit()
                        self._retry_not_before[item.id] = loop.time() + self._retry_gap
                        logger.error(
                            "send failed (attempt %s of %d): alert id=%d %s %s "
                            "triggered_at=%s payload=%s",
                            attempts,
                            self._max_attempts,
                            item.id,
                            item.alert.alert_type,
                            item.alert.symbol,
                            item.alert.triggered_at,
                            item.alert.payload,
                            exc_info=True,
                        )
                        if attempts is not None and attempts >= self._max_attempts:
                            self.counters["posts_abandoned_total"] += 1
                            await notify_ops(
                                f"Alert #{item.id} {item.alert.symbol} {item.alert.alert_type} "
                                f"failed to post after {attempts} attempts and will not be "
                                "retried. It is still in the alerts table (posted_at NULL).",
                                key=f"alert-post-failed:{item.id}",
                            )
                        continue
                    await self._health.report_ok(Source.DISCORD)
                    posted_at = await repo.mark_posted(item.id)
                    await session.commit()  # per-row commit: dupe window ≤ 1 alert
                    self.counters["discord_posts_total"] += 1
                    if posted_at is None:
                        # message_id identifies the duplicate for the audit.
                        logger.warning(
                            "alert id=%d was already marked posted — possible duplicate post "
                            "message_id=%d (crash inside a previous send→commit gap)",
                            item.id,
                            message_id,
                        )
                    else:
                        # DB-derived lag: posted_at − triggered_at, both
                        # server-side stamps — no client clock in rs_bot.
                        logger.info(
                            "posted alert id=%d message_id=%d lag=%.1fs",
                            item.id,
                            message_id,
                            (posted_at - item.alert.triggered_at).total_seconds(),
                        )
        if pending:
            logger.info("counters: %s", self.counters)

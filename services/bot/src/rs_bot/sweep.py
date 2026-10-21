"""The single posting path: one worker waits on (wake event
OR the sweep interval), then drains every undelivered Alert and reply in id order.
NOTIFY never posts - the LISTEN loop only sets the wake event, so the correctness
mechanism (cursor sweep) runs on every post and cannot rot unnoticed.

Per row: read track record, render, commit a sending mark, send with the row id as the
nonce, record the message id, commit. A crash inside the send-to-record gap leaves the
row marked sending, so the next sweep looks the bot's own message for that row up in the
channel's (an Alert's) or the thread's (a reply's) recent history before it sends
anything: found means record it, not found means send, and a failed read means do not
post and send an Ops ping. Delivery therefore does not post twice.

A row older than `STALE_AFTER` at send time, measured from its hit time, is recorded as
stale and not posted. A send failure is ERROR-logged with the full payload, counted in
`post_attempts` and skipped (the row stays marked sending, so the retry checks history
first); at `max_attempts` the row is failed-to-post: it leaves the sweep and the operator
is told once, unless Discord itself is already reported failing.

Strict id-order posting is a normal-operation property, not a failure-mode
guarantee: a skipped poison row posts after its younger siblings once it
heals.
"""

import asyncio
import logging
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Protocol

import aiohttp
import discord
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_bot.counters import new_counters
from rs_bot.render import POPULARITY_SURGE, render, render_generic
from rs_domain import (
    Alert,
    Clock,
    Delivery,
    FactUnavailable,
    PopularitySurgePayload,
    SurgeFacts,
    TrackRecord,
)
from rs_observability import HealthTracker, Source, notify_ops, unit_of_work
from rs_persistence import AlertDelivery, AlertRepository, PendingAlert
from rs_query import current_track_record

logger = logging.getLogger(__name__)

# After an outage, an Alert or reply older than this is recorded but not posted.
STALE_AFTER = timedelta(hours=1)

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


class AlertSender(Protocol):
    """The Discord seam: tests fake this; the daemon backs it with the resolved alert
    channel. Sends return the Discord message id; a `find` returns the bot's own message
    for a row sent at or after `since`, None when there is none, and raises when the
    history cannot be read."""

    async def send_alert(self, embed: discord.Embed, *, nonce: int) -> int: ...

    async def send_reply(self, thread_id: int, embed: discord.Embed, *, nonce: int) -> int: ...

    async def open_thread(self, message_id: int, name: str) -> int: ...

    async def find_alert(self, row_id: int, since: datetime) -> int | None: ...

    async def find_reply(self, thread_id: int, row_id: int, since: datetime) -> int | None: ...


def _missing_facts(alert: Alert) -> list[str]:
    """The supporting facts an Alert's payload says were unavailable, with their reasons."""
    if alert.trigger != POPULARITY_SURGE:
        return []
    try:
        facts = PopularitySurgePayload.model_validate(alert.payload).facts
    except ValidationError:
        return []
    missing = []
    for name in SurgeFacts.model_fields:
        fact = getattr(facts, name)
        if isinstance(fact, FactUnavailable):
            missing.append(f"{name.replace('_', ' ')} ({fact.reason})")
    return missing


class SweepWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        sender: AlertSender,
        *,
        clock: Clock,
        sweep_seconds: float,
        health: HealthTracker,
        max_attempts: int = 5,
        retry_gap_seconds: float | None = None,
        stale_after: timedelta = STALE_AFTER,
        counters: dict[str, int] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._sender = sender
        self._clock = clock
        self._health = health
        self._sweep_seconds = sweep_seconds
        self._max_attempts = max_attempts
        self._retry_gap = sweep_seconds if retry_gap_seconds is None else retry_gap_seconds
        self._stale_after = stale_after
        self.counters = counters if counters is not None else new_counters()
        # Set by the LISTEN loop on NOTIFY and on every (re)connect — so
        # "sweep on reconnect" is not a special case.
        # Monotonic retry gate per alert id (in memory: a restart retries at once).
        self._retry_not_before: dict[int, float] = {}
        # Rows whose unreadable history already pinged the operator (one ping per row).
        self._unreadable_pinged: set[int] = set()
        self.wake = asyncio.Event()
        self._stopping = asyncio.Event()

    def request_stop(self) -> None:
        """Graceful shutdown: finish the in-flight post, then return. Rows not
        yet reached stay undelivered - the next start's initial sweep reconciles
        them (a row marked sending is checked against history, not re-sent blindly)."""
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
        if alert.trigger != POPULARITY_SURGE or alert.rule_version is None:
            return None, False
        key = (alert.trigger, alert.rule_version)
        if key not in cache:
            try:
                cache[key] = (await current_track_record(session, *key), False)
            except Exception:
                logger.exception("track record read failed for %s v%s", *key)
                await session.rollback()
                cache[key] = (None, True)
        return cache[key]

    async def _report_discord(self, exc: Exception) -> None:
        reachable = _discord_health(exc)
        if reachable:
            await self._health.report_ok(Source.DISCORD)
        elif reachable is False:
            await self._health.report_failed(Source.DISCORD, type(exc).__name__)

    async def sweep(self) -> None:
        """One drain pass over the cursor: every row still pending or sending, id
        order, no LIMIT - the sweep drains fully."""
        self.counters["sweeps_total"] += 1
        async with self._session_factory() as session:
            repo = AlertRepository(session)
            pending = await repo.list_undelivered(self._max_attempts)
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
                    await self._deliver(session, repo, item, track_records, loop)
            await self._thread_stragglers(session, repo)
        if pending:
            logger.info("counters: %s", self.counters)

    async def _deliver(
        self,
        session: AsyncSession,
        repo: AlertRepository,
        item: PendingAlert,
        track_records: dict[tuple[str, str], tuple[TrackRecord | None, bool]],
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        alert = item.alert
        # A reply goes where its Alert stands now, which may have changed earlier this sweep.
        parent = None if alert.reply_to is None else await repo.delivery_of(alert.reply_to)
        if item.delivery is Delivery.SENDING:
            try:
                found = await self._find(item, parent)
            except Exception as exc:
                await self._history_unreadable(item, exc, loop)
                return
            if found is not None:
                logger.warning(
                    "alert id=%d was sent before the last stop and never recorded: "
                    "recording message_id=%d instead of sending again",
                    item.id,
                    found,
                )
                self.counters["posts_reconciled_total"] += 1
                await self._record_posted(session, repo, item, found)
                return

        if self._clock.now() - alert.triggered_at > self._stale_after:
            await self._record_stale(session, repo, item, "older than the stale limit")
            return

        if alert.reply_to is not None:
            if parent is None or parent.delivery is Delivery.STALE:
                await self._record_stale(session, repo, item, "its Alert was not posted")
                return
            if parent.delivery is not Delivery.POSTED:
                return  # waits for its Alert, which posts first in id order
            if parent.message_id is None:
                await self._record_stale(session, repo, item, "its Alert has no stored message")
                return

        track_record, track_record_failed = await self._track_record(session, item, track_records)
        if item.post_attempts + 1 >= self._max_attempts:
            # Last try: the plain embed cannot be rejected for its shape.
            embed = render_generic(item)
        else:
            embed = render(item, track_record, track_record_failed=track_record_failed)
        try:
            message_id = await self._post(session, repo, item, embed, parent)
        except Exception as exc:
            await self._send_failed(session, repo, item, exc, loop)
            return
        if message_id is None:
            return  # settled meanwhile; nothing was sent
        await self._health.report_ok(Source.DISCORD)
        await self._record_posted(session, repo, item, message_id)

    async def _post(
        self,
        session: AsyncSession,
        repo: AlertRepository,
        item: PendingAlert,
        embed: discord.Embed,
        parent: AlertDelivery | None,
    ) -> int | None:
        """Mark the row sending, commit the mark, then send; returns the Discord message id."""
        alert = item.alert
        thread_id = None
        if alert.reply_to is not None:
            assert parent is not None and parent.message_id is not None  # checked by the caller
            thread_id = parent.thread_id
            if thread_id is None:
                thread_id = await self._sender.open_thread(
                    parent.message_id, _thread_name(alert.symbol, alert.reply_to)
                )
                await repo.set_thread(alert.reply_to, thread_id)
                await session.commit()
        if not await repo.mark_sending(item.id):
            return None
        await session.commit()  # the evidence a crash between send and record leaves
        if thread_id is None:
            return await self._sender.send_alert(embed, nonce=item.id)
        return await self._sender.send_reply(thread_id, embed, nonce=item.id)

    async def _find(self, item: PendingAlert, parent: AlertDelivery | None) -> int | None:
        """The bot's own message for a row marked sending, or None when it was never sent."""
        assert item.sending_at is not None  # a row is marked sending with its stamp
        if item.alert.reply_to is None:
            return await self._sender.find_alert(item.id, item.sending_at)
        if parent is None or parent.thread_id is None:
            return None  # a reply is only ever sent once its Alert has a thread
        return await self._sender.find_reply(parent.thread_id, item.id, item.sending_at)

    async def _record_posted(
        self, session: AsyncSession, repo: AlertRepository, item: PendingAlert, message_id: int
    ) -> None:
        posted_at = await repo.mark_posted(item.id, message_id)
        await session.commit()  # per-row commit: a crash after this leaves nothing to reconcile
        self.counters["discord_posts_total"] += 1
        if posted_at is None:
            logger.warning(
                "alert id=%d was already settled when message_id=%d was recorded", item.id, message_id
            )
            return
        # DB-derived lag: posted_at - triggered_at, a database stamp against the hit time.
        logger.info(
            "posted alert id=%d message_id=%d lag=%.1fs",
            item.id,
            message_id,
            (posted_at - item.alert.triggered_at).total_seconds(),
        )
        if item.alert.reply_to is not None:
            return
        await self._open_thread(session, repo, item.id, message_id, item.alert.symbol)
        await self._ping_missing_facts(item)

    async def _open_thread(
        self,
        session: AsyncSession,
        repo: AlertRepository,
        alert_id: int,
        message_id: int,
        symbol: str,
    ) -> None:
        """Start the Alert's thread from its message and store it. A failure is logged and
        the next sweep tries again; the Alert is already posted either way."""
        try:
            thread_id = await self._sender.open_thread(message_id, _thread_name(symbol, alert_id))
        except Exception as exc:
            await self._report_discord(exc)
            self._retry_not_before[alert_id] = asyncio.get_running_loop().time() + self._retry_gap
            logger.error(
                "could not start the thread for alert id=%d message_id=%d", alert_id, message_id, exc_info=True
            )
            return
        await repo.set_thread(alert_id, thread_id)
        await session.commit()

    async def _thread_stragglers(self, session: AsyncSession, repo: AlertRepository) -> None:
        """Posted Alerts whose thread could not be started, within the stale limit."""
        for alert_id, message_id, symbol in await repo.list_unthreaded(
            self._clock.now() - self._stale_after
        ):
            if self._stopping.is_set():
                break
            with unit_of_work(alert_id=alert_id):
                if self._retry_not_before.get(alert_id, 0.0) > asyncio.get_running_loop().time():
                    continue  # failed moments ago
                await self._open_thread(session, repo, alert_id, message_id, symbol)

    async def _ping_missing_facts(self, item: PendingAlert) -> None:
        missing = _missing_facts(item.alert)
        if missing:
            await notify_ops(
                f"Alert #{item.id} {item.alert.symbol} {item.alert.trigger} posted with facts "
                f"unavailable: {', '.join(missing)}.",
                key=f"missing-facts:{item.id}",
            )

    async def _record_stale(
        self, session: AsyncSession, repo: AlertRepository, item: PendingAlert, why: str
    ) -> None:
        await repo.mark_stale(item.id)
        await session.commit()
        self.counters["posts_stale_total"] += 1
        logger.warning(
            "alert id=%d %s %s recorded as stale, not posted (%s): triggered_at=%s",
            item.id,
            item.alert.trigger,
            item.alert.symbol,
            why,
            item.alert.triggered_at,
        )

    async def _history_unreadable(
        self, item: PendingAlert, exc: Exception, loop: asyncio.AbstractEventLoop
    ) -> None:
        """Without the history there is no telling whether the row posted, so it is not sent."""
        await self._report_discord(exc)
        self._retry_not_before[item.id] = loop.time() + self._retry_gap
        logger.error(
            "could not read history to check alert id=%d %s - not posting it", item.id, item.alert.symbol, exc_info=True
        )
        # One cause, one ping: Discord failing is already the Ops ping.
        if not self._health.is_failing(Source.DISCORD) and item.id not in self._unreadable_pinged:
            self._unreadable_pinged.add(item.id)
            await notify_ops(
                f"Alert #{item.id} {item.alert.symbol} {item.alert.trigger} may or may not have "
                f"posted before the last stop, and the channel history could not be read "
                f"({type(exc).__name__}). It will not be posted until it can be checked. See the bot logs.",
                key=f"history-unreadable:{item.id}",
            )

    async def _send_failed(
        self,
        session: AsyncSession,
        repo: AlertRepository,
        item: PendingAlert,
        exc: Exception,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        await self._report_discord(exc)
        self.counters["post_failures_total"] += 1
        attempts = await repo.increment_post_attempts(item.id)
        await session.commit()
        self._retry_not_before[item.id] = loop.time() + self._retry_gap
        logger.error(
            "send failed (attempt %s of %d): alert id=%d %s %s triggered_at=%s payload=%s",
            attempts,
            self._max_attempts,
            item.id,
            item.alert.trigger,
            item.alert.symbol,
            item.alert.triggered_at,
            item.alert.payload,
            exc_info=True,
        )
        if attempts is None or attempts < self._max_attempts:
            return
        self.counters["posts_abandoned_total"] += 1
        # One cause, one ping: when Discord is failing its own ping already says so, and
        # every queued Alert would otherwise add one of its own.
        if not self._health.is_failing(Source.DISCORD):
            await notify_ops(
                f"Alert #{item.id} {item.alert.symbol} {item.alert.trigger} "
                f"failed to post after {attempts} attempts and will not be "
                "retried. It is still in the alerts table, not posted.",
                key=f"alert-post-failed:{item.id}",
            )


def _thread_name(symbol: str, alert_id: int) -> str:
    return f"{symbol} alert #{alert_id}"

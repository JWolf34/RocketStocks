"""Bus-1 fan-out: the ordered synchronous stage downstream of the EventSource.

For each BarClosed, in order (ADR-0005 Bus-1; grilling #4/#5):

  1. Persist the bar — its own transaction, crash-loud. The bar log is the
     durable source of truth; if it cannot be written the daemon stops rather
     than silently dropping capture (no retry/buffer machinery in the skeleton).
  2. Append the bar to the session window (all of today's RTH bars, newest
     last). Appended only after the commit, so every bar a detector sees is
     already in the log — an alert's input bar always exists durably.
  3. Run each detector over the window behind the ADR-0008 error boundary, then
     write the emitted alerts (alert row + outbox row + NOTIFY) in a *second*
     transaction.

Two transactions per bar (grilling #4): the bar commit and the alert commit are
independent, so an alert-write failure can never roll back a persisted bar, and
the detector only ever runs on a bar that is already durable.

Boundary scope — the boundary wraps `decide()`, not the alert write. A detector's
own faults surface *inside* decide(): Alert is a validated pydantic model, so a
malformed alert raises at construction, which the boundary catches and counts. By
the time an Alert object exists it is well-formed, so the alert write can only
fail on a genuine persistence problem, which crashes loud exactly like the bar
write. A misbehaving detection *rule* never stops capture; a broken *database*
stops everything, at the bar log or the alert log alike.

Session-scoped window: one Pipeline per run_session (pinned #11, one process =
one session), so the window starts empty and needs no cross-day clearing here.
The by-v2-end multi-session loop (ADR-0018 warm-up) owns that lifecycle.
"""

import logging
from collections.abc import Sequence

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Alert, Bar, BarClosed, Detector, EventSource
from rs_persistence import NOTIFY_CHANNEL, AlertRepository, BarRepository

logger = logging.getLogger(__name__)


class Pipeline:
    """Ordered persister -> detector fan-out for one live session."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        detectors: Sequence[Detector],
        *,
        notify_channel: str = NOTIFY_CHANNEL,
    ) -> None:
        self._session_factory = session_factory
        self._detectors = tuple(detectors)
        self._notify_channel = notify_channel
        self._window: list[Bar] = []
        self.counters = {"bars_persisted": 0, "alerts_emitted": 0, "detector_errors": 0}

    async def run(self, source: EventSource) -> None:
        """Drive the fan-out over an EventSource until it is exhausted."""
        async for event in source.events():
            await self.handle(event)

    async def handle(self, event: BarClosed) -> None:
        await self._persist(event.bar)
        self._window.append(event.bar)  # mirrors the log: append only post-commit
        self.counters["bars_persisted"] += 1
        await self._detect(event.bar)

    async def _persist(self, bar: Bar) -> None:
        # Transaction 1 — the durable bar log, alone. Crash-loud: an exception
        # here propagates out of run() and stops the daemon (grilling #4).
        async with self._session_factory() as session:
            await BarRepository(session).insert(bar)
            await session.commit()

    async def _detect(self, bar: Bar) -> None:
        alerts = self._decide(bar)
        if not alerts:
            return
        # Transaction 2 — every alert this bar produced, each one's outbox row,
        # and each one's NOTIFY, atomically (grilling #6). Separate from the bar
        # commit, and reached only for a bar that is already durable.
        async with self._session_factory() as session:
            repo = AlertRepository(session, self._notify_channel)
            for alert in alerts:
                await repo.insert(alert)
            await session.commit()
        self.counters["alerts_emitted"] += len(alerts)
        for alert in alerts:
            logger.info("alert %s %s payload=%s", alert.alert_type, alert.symbol, alert.payload)

    def _decide(self, bar: Bar) -> list[Alert]:
        # Detectors get an immutable snapshot (the port types it Sequence[Bar]): a
        # misbehaving rule cannot mutate the shared window and corrupt what later
        # bars or other detectors see — a fault the ADR-0008 boundary can't catch.
        window = tuple(self._window)
        alerts: list[Alert] = []
        for detector in self._detectors:
            try:
                decision = detector.decide(window)
            except Exception:
                # ADR-0008 error boundary: a detector fault is logged + counted;
                # the next detector and the next bar still run. Capture never
                # stops because a detection rule misbehaved.
                self.counters["detector_errors"] += 1
                logger.exception(
                    "detector %s raised on %s bucket %s — skipped (ADR-0008); capture continues",
                    type(detector).__name__,
                    bar.symbol,
                    bar.bucket,
                )
                continue
            if decision is not None:
                alerts.append(decision)
        return alerts

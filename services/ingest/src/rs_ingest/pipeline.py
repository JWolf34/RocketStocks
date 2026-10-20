"""Bus-1 fan-out: the ordered synchronous stage downstream of the EventSource.

For each BarClosed, in order (ADR-0005 Bus-1; grilling #4/#5):

  1. Persist the bar - its own transaction, crash-loud. The bar log is the
     durable source of truth; if it cannot be written the daemon stops rather
     than silently dropping capture (no retry/buffer machinery in the skeleton).
  2. Append the bar to the session window (all of today's RTH bars, newest
     last). Appended only after the commit, so every bar a detector sees is
     already in the log - an alert's input bar always exists durably.
  3. Run detectors over the window and write any emitted alerts (alert row +
     NOTIFY) in a *second* transaction - the whole reaction stage
     behind the ADR-0008 error boundary (scope below).

Two transactions per bar (grilling #4): the bar commit and the alert commit are
independent, so an alert-write failure can never roll back a persisted bar, and
the detector only ever runs on a bar that is already durable.

Boundary scope - the boundary covers the whole reaction stage: both `decide()`
and the alert write. This is criticality tiering. The bar log is Tier-1 - a live
tick stream is unrepeatable, so its capture must never be stopped by anything
downstream. Alerts are Tier-2 - a deterministic, replayable projection of the log
(ADR-0023), so a lost alert is re-derivable where a lost bar is gone. A Tier-2
failure (a detector that raises, or an alert write that fails) is therefore
tolerated: counted and logged with the full alert content - never a silent drop -
and capture continues. This does not mask a real outage: a systemic database
failure still fails fast, because the *next* bar's persist is crash-loud within
one bar interval. Crash-loud is a reliability strategy only with a supervisor to
restart to a known-good state, and the skeleton has none (pinned #11), so a
tolerable Tier-2 fault must not be fatal. v2 makes the drop recoverable (a
dead-letter) and moves alerting out-of-band as a bar-log consumer, at
which point its failures cannot touch capture at all.

Session-scoped window: one Pipeline per run_session (pinned #11, one process =
one session), so the window starts empty and needs no cross-day clearing here.
The by-v2-end multi-session loop (ADR-0018 warm-up) owns that lifecycle.
"""

import logging
from collections.abc import Sequence

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Alert, Bar, BarClosed, Detector, EventSource
from rs_observability import unit_of_work
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
        self.counters = {
            "bars_persisted": 0,
            "alerts_emitted": 0,
            "detector_errors": 0,
            "alerts_dropped": 0,
        }

    async def run(self, source: EventSource) -> None:
        """Drive the fan-out over an EventSource until it is exhausted."""
        async for event in source.events():
            await self.handle(event)

    async def handle(self, event: BarClosed) -> None:
        # One event dispatch = one unit of work (S006): bar close → persist →
        # detect → alert write → NOTIFY commit share a correlation_id, with the
        # domain keys as first-class fields on every record.
        with unit_of_work(symbol=event.bar.symbol, bucket=event.bar.bucket.isoformat()):
            await self._persist(event.bar)
            self._window.append(event.bar)  # mirrors the log: append only post-commit
            self.counters["bars_persisted"] += 1
            await self._detect(event.bar)

    async def _persist(self, bar: Bar) -> None:
        # Transaction 1 - the durable bar log, alone. Crash-loud: an exception
        # here propagates out of run() and stops the daemon (grilling #4).
        async with self._session_factory() as session:
            await BarRepository(session).insert(bar)
            await session.commit()

    async def _detect(self, bar: Bar) -> None:
        alerts = self._decide(bar)
        if not alerts:
            return
        try:
            # Transaction 2 - every alert this bar produced and each one's NOTIFY,
            # atomically (grilling #6). Separate from the bar commit, and reached
            # only for a bar that is already durable.
            async with self._session_factory() as session:
                repo = AlertRepository(session, self._notify_channel)
                results = [(await repo.insert(alert), alert) for alert in alerts]
                # None = deduplicated; streaming alerts carry no dedup_key, so none are.
                inserted = [(alert_id, alert) for alert_id, alert in results if alert_id is not None]
                await session.commit()
        except Exception:
            # Tier-2 reaction path: a failed alert write is tolerated so it can
            # never stop Tier-1 capture. Never silent - count it and log each
            # dropped alert's full content so the loss is auditable (and, in v2,
            # recoverable via a dead-letter). A systemic outage still fails fast
            # at the next bar's crash-loud persist.
            self.counters["alerts_dropped"] += len(alerts)
            for alert in alerts:
                logger.error(
                    "alert write failed - DROPPED, capture continues: %s %s triggered_at=%s payload=%s",
                    alert.alert_type,
                    alert.symbol,
                    alert.triggered_at,
                    alert.payload,
                )
        else:
            self.counters["alerts_emitted"] += len(inserted)
            for alert_id, alert in inserted:
                # alert_id is THE cross-service join key (S006 pin #3): this
                # insert log and the bot's post log both carry it as a field.
                logger.info(
                    "alert %s %s payload=%s",
                    alert.alert_type,
                    alert.symbol,
                    alert.payload,
                    extra={"alert_id": alert_id},
                )

    def _decide(self, bar: Bar) -> list[Alert]:
        # Detectors get an immutable snapshot (the port types it Sequence[Bar]): a
        # misbehaving rule cannot mutate the shared window and corrupt what later
        # bars or other detectors see - a fault the ADR-0008 boundary can't catch.
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
                    "detector %s raised on %s bucket %s - skipped (ADR-0008); capture continues",
                    type(detector).__name__,
                    bar.symbol,
                    bar.bucket,
                )
                continue
            if decision is not None:
                alerts.append(decision)
        return alerts

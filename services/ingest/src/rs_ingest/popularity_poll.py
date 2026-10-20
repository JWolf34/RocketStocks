"""The popularity poll: store one ApeWisdom snapshot per half-hour boundary,
judge it with the attention-surge rule, and write each new alert with its facts.

Every step is idempotent on the boundary: snapshots are first-write-wins, the
rule judges the *stored* rows (so a rerun sees what the first run saw), and an
alert's `dedup_key` makes its insert a no-op the second time. A rerun after a
crash therefore finishes the work and never duplicates it.
"""

import asyncio
import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Protocol

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_analysis.attention_surge import (
    COOLDOWN,
    SubRule,
    SurgeEvent,
    SurgeInputs,
    evaluate_latest,
)
from rs_domain import (
    Alert,
    AttentionSurgePayload,
    Clock,
    PopularityOrigin,
    PopularitySnapshot,
    SurgeFacts,
)
from rs_ingest.alert_writer import AlertWriter
from rs_ingest.surge_facts import SurgeFactsGatherer
from rs_persistence import AlertRepository, PopularityRepository
from rs_sources import ApeWisdomError, ApeWisdomRow

logger = logging.getLogger(__name__)

ALERT_TYPE = "attention_surge"
POLL_MINUTES = 30
POLL_INTERVAL = timedelta(minutes=POLL_MINUTES)
# Alerts being written at once; each already fans its facts out in parallel.
WRITE_CONCURRENCY = 4


class PopularitySource(Protocol):
    async def fetch_all(self) -> list[ApeWisdomRow]: ...


# A fresh gatherer per run: its dead-token memory must not outlive the run.
GathererFactory = Callable[[], SurgeFactsGatherer]


def poll_boundary(moment: datetime) -> datetime:
    """The UTC :00/:30 boundary at or before `moment`."""
    moment = moment.astimezone(UTC)
    return moment.replace(minute=moment.minute - moment.minute % POLL_MINUTES, second=0, microsecond=0)


def dedup_key(event: SurgeEvent) -> str:
    return f"{ALERT_TYPE}:{event.rule_version}:{event.ticker}:{event.polled_at:%Y%m%dT%H%MZ}"


class PopularityPoll:
    def __init__(
        self,
        *,
        source: PopularitySource,
        session_factory: async_sessionmaker[AsyncSession],
        clock: Clock,
        writer: AlertWriter,
        gatherer: GathererFactory,
    ) -> None:
        self._source = source
        self._session_factory = session_factory
        self._clock = clock
        self._writer = writer
        self._new_gatherer = gatherer

    async def run(self) -> None:
        fetched_at = self._clock.now()
        polled_at = poll_boundary(fetched_at)
        rows = await self._fetch()
        inserted = await self._store(rows, polled_at, fetched_at)
        events = await self._detect(polled_at)
        logger.info(
            "popularity poll %s: %d rows fetched, %d stored, %d new surge event(s)",
            polled_at.isoformat(), len(rows), inserted, len(events),
        )
        await self._write_alerts(events, polled_at)

    async def _fetch(self) -> list[ApeWisdomRow]:
        """The listing; a failed or empty fetch raises so the daemon logs and counts it."""
        rows = await self._source.fetch_all()
        if not rows:
            raise ApeWisdomError("ApeWisdom returned no rows")
        return rows

    async def _store(self, rows: Sequence[ApeWisdomRow], polled_at: datetime, fetched_at: datetime) -> int:
        snapshots = [
            PopularitySnapshot(
                polled_at=polled_at,
                ticker=row.ticker,
                rank=row.rank,
                mentions=row.mentions,
                upvotes=row.upvotes,
                rank_24h_ago=row.rank_24h_ago,
                mentions_24h_ago=row.mentions_24h_ago,
                fetched_at=fetched_at,
                origin=PopularityOrigin.LIVE,
            )
            for row in rows
        ]
        async with self._session_factory() as session:
            inserted = await PopularityRepository(session).insert_many(snapshots)
            await session.commit()
        return inserted

    async def _detect(self, polled_at: datetime) -> list[SurgeEvent]:
        """Events with no alert yet for this boundary and the one before it, judged
        against the stored snapshots and the alert log's 24h cooldown. The earlier
        boundary catches up an alert whose write failed or whose run was cut short;
        it goes first so the cooldown sees it when judging this one."""
        earlier = polled_at - POLL_INTERVAL
        async with self._session_factory() as session:
            snapshots = await PopularityRepository(session).list_between(earlier, polled_at + POLL_INTERVAL)
            alerts = await AlertRepository(session).list_decisions(
                start=earlier - COOLDOWN, end=polled_at + POLL_INTERVAL
            )
        surge_alerts = [a for a in alerts if a.alert_type == ALERT_TYPE]
        written = {a.dedup_key for a in surge_alerts}
        logged = [(a.triggered_at, event) for a in surge_alerts if (event := _prior_event(a))]

        events: list[SurgeEvent] = []
        for boundary in (earlier, polled_at):
            # Imported history is never caught up: only a live poll can have missed its alert.
            judged = [
                s for s in snapshots
                if s.polled_at == boundary and (boundary == polled_at or s.origin == PopularityOrigin.LIVE)
            ]
            # events found for the earlier boundary count as prior for this one
            prior = [event for at, event in logged if at < boundary] + events
            found = (evaluate_latest(snapshot, prior) for snapshot in judged)
            fresh = [event for event in found if event is not None and dedup_key(event) not in written]
            events.extend(fresh)
        return events

    async def _write_alerts(self, events: Sequence[SurgeEvent], current: datetime) -> None:
        gatherer = self._new_gatherer()
        gate = asyncio.Semaphore(WRITE_CONCURRENCY)

        async def one(event: SurgeEvent) -> None:
            async with gate:
                await self._write_alert(event, gatherer, current)

        results = await asyncio.gather(*(one(event) for event in events), return_exceptions=True)
        failures = [r for r in results if isinstance(r, Exception)]
        if failures:
            raise ExceptionGroup(f"{len(failures)} of {len(events)} surge alert(s) not stored", failures)

    async def _write_alert(self, event: SurgeEvent, gatherer: SurgeFactsGatherer, current: datetime) -> None:
        gathered = await gatherer.gather(event.ticker, self._clock.now())
        payload = _payload(event, gathered.facts)
        alert_id = await self._writer.write(
            Alert(
                symbol=event.ticker,
                alert_type=ALERT_TYPE,
                triggered_at=event.polled_at,
                payload=payload.model_dump(mode="json"),
                rule_version=event.rule_version,
                alert_price=gathered.alert_price,
                quote_time=gathered.quote_time,
                dedup_key=dedup_key(event),
            )
        )
        if alert_id is None:
            logger.info("surge alert %s was already stored", dedup_key(event))
        else:
            late = f" late for boundary {event.polled_at.isoformat()}" if event.polled_at < current else ""
            logger.info("surge alert %d stored%s for %s (%s)", alert_id, late, event.ticker, [r.value for r in event.fired])


def _payload(event: SurgeEvent, facts: SurgeFacts) -> AttentionSurgePayload:
    inputs = event.inputs
    return AttentionSurgePayload(
        rule_version=event.rule_version,
        polled_at=event.polled_at,
        fired=tuple(rule.value for rule in event.fired),
        rank=inputs.rank,
        mentions=inputs.mentions,
        rank_24h_ago=inputs.rank_24h_ago,
        mentions_24h_ago=inputs.mentions_24h_ago,
        rank_gain=inputs.rank_gain,
        mention_ratio=inputs.mention_ratio,
        mention_multiplier=inputs.mention_multiplier,
        mention_min_prior=inputs.mention_min_prior,
        facts=facts,
    )


def _prior_event(alert: Alert) -> SurgeEvent | None:
    """Rebuild the logged event the cooldown reads. An alert whose payload no
    longer parses is skipped loudly rather than stalling every later poll."""
    try:
        payload = AttentionSurgePayload.model_validate(alert.payload)
    except ValidationError:
        logger.exception("cannot read the payload of prior %s alert %s; ignoring it for the cooldown",
                         alert.alert_type, alert.dedup_key)
        return None
    return SurgeEvent(
        ticker=alert.symbol,
        polled_at=payload.polled_at,
        fired=tuple(SubRule(rule) for rule in payload.fired),
        inputs=SurgeInputs(
            rank=payload.rank,
            mentions=payload.mentions,
            rank_24h_ago=payload.rank_24h_ago,
            mentions_24h_ago=payload.mentions_24h_ago,
            rank_gain=payload.rank_gain,
            mention_ratio=payload.mention_ratio,
            mention_multiplier=payload.mention_multiplier,
            mention_min_prior=payload.mention_min_prior,
        ),
        rule_version=payload.rule_version,
    )

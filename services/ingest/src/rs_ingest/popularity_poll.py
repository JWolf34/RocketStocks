"""The popularity poll: store one ApeWisdom snapshot per half-hour boundary,
judge it with the popularity-surge rule, and store every hit as a candidate
with the selector's verdict; a posted one is stored with its Alert and facts.

Every step is idempotent on the boundary: snapshots are first-write-wins, the
rule judges the *stored* rows (so a rerun sees what the first run saw), and a
hit that already has a candidate is skipped. A candidate and its Alert commit
in one transaction, so a rerun after a crash finishes the work and never
duplicates it.
"""

import asyncio
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_analysis.popularity_surge import SurgeEvent, evaluate_latest
from rs_domain import (
    Alert,
    Candidate,
    Clock,
    PopularityOrigin,
    PopularitySnapshot,
    PopularitySurgePayload,
    SurgeFacts,
    Verdict,
    eastern_day,
)
from rs_ingest.alert_writer import AlertWriter
from rs_ingest.selector import DayState, select
from rs_ingest.surge_facts import SurgeFactsGatherer
from rs_persistence import AlertRepository, CandidateRepository, PopularityRepository
from rs_sources import ApeWisdomError, ApeWisdomRow

logger = logging.getLogger(__name__)

TRIGGER = "popularity_surge"
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


@dataclass(frozen=True)
class Decision:
    """A hit and the selector's verdict on it."""

    event: SurgeEvent
    verdict: Verdict
    decided_at: datetime


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
        decisions = await self._decide(await self._detect(polled_at))
        logger.info(
            "popularity poll %s: %d rows fetched, %d stored, %d new surge hit(s), %d posted",
            polled_at.isoformat(), len(rows), inserted, len(decisions),
            sum(d.verdict is Verdict.POSTED for d in decisions),
        )
        await self._write(decisions, polled_at)

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
        """Hits at this boundary and the one before it that no candidate records yet,
        judged against the stored snapshots, oldest first. The earlier boundary
        catches up a hit whose write failed or whose run was cut short."""
        earlier = polled_at - POLL_INTERVAL
        async with self._session_factory() as session:
            snapshots = await PopularityRepository(session).list_between(earlier, polled_at + POLL_INTERVAL)
            decided = await CandidateRepository(session).list_window(TRIGGER, earlier, polled_at + POLL_INTERVAL)
        known = {(c.rule_version, c.symbol, c.hit_at) for c in decided}

        # Imported history is never caught up: only a live poll can have missed its hit.
        judged = [
            s for s in snapshots
            if s.polled_at == polled_at or (s.polled_at == earlier and s.origin == PopularityOrigin.LIVE)
        ]
        hits = (evaluate_latest(snapshot) for snapshot in judged)
        found = [e for e in hits if e is not None and (e.rule_version, e.ticker, e.polled_at) not in known]
        return sorted(found, key=lambda e: (e.polled_at, e.ticker))

    async def _decide(self, events: Sequence[SurgeEvent]) -> list[Decision]:
        """The selector's verdict for each hit, in order, each seeing the Alerts the
        hits before it would produce."""
        symbols = {event.ticker for event in events}
        alerted: set[tuple[date, str]] = set()
        async with self._session_factory() as session:
            repo = AlertRepository(session)
            for day in {eastern_day(event.polled_at) for event in events}:
                alerted |= {(day, symbol) for symbol in await repo.alerted_symbols(day, symbols)}

        decisions = []
        for event in events:
            slot = (eastern_day(event.polled_at), event.ticker)
            verdict = select(event, DayState(alerted=slot in alerted))
            if verdict is Verdict.POSTED:
                alerted.add(slot)
            decisions.append(Decision(event=event, verdict=verdict, decided_at=self._clock.now()))
        return decisions

    async def _write(self, decisions: Sequence[Decision], current: datetime) -> None:
        """Store every decision. A stock's hits go in order and stop at its first
        failure: a later hit was decided on the assumption that the earlier one
        is stored, so it waits for the next run to be decided again."""
        gatherer = self._new_gatherer()
        gate = asyncio.Semaphore(WRITE_CONCURRENCY)
        by_symbol: dict[str, list[Decision]] = {}
        for decision in decisions:
            by_symbol.setdefault(decision.event.ticker, []).append(decision)

        async def one_stock(chain: list[Decision]) -> None:
            async with gate:
                for decision in chain:
                    await self._write_one(decision, gatherer, current)

        results = await asyncio.gather(*(one_stock(chain) for chain in by_symbol.values()), return_exceptions=True)
        failures = [r for r in results if isinstance(r, Exception)]
        if failures:
            raise ExceptionGroup(f"{len(failures)} of {len(by_symbol)} stock(s) had a hit not stored", failures)

    async def _write_one(self, decision: Decision, gatherer: SurgeFactsGatherer, current: datetime) -> None:
        event = decision.event
        candidate = Candidate(
            trigger=TRIGGER,
            rule_version=event.rule_version,
            symbol=event.ticker,
            hit_at=event.polled_at,
            inputs=_inputs(event),
            verdict=decision.verdict,
            decided_at=decision.decided_at,
        )
        alert = None
        if decision.verdict is Verdict.POSTED:
            gathered = await gatherer.gather(event.ticker, self._clock.now())
            alert = Alert(
                symbol=event.ticker,
                trigger=TRIGGER,
                triggered_at=event.polled_at,
                payload=_payload(event, gathered.facts).model_dump(mode="json"),
                rule_version=event.rule_version,
                alert_price=gathered.alert_price,
                quote_time=gathered.quote_time,
            )
        stored = await self._writer.write(candidate, alert)
        key = f"{event.ticker} at {event.polled_at.isoformat()}"
        if stored is None:
            logger.info("surge hit %s was already stored", key)
        elif stored.alert_id is None:
            logger.info("surge hit %s stored as candidate %d (%s)", key, stored.candidate_id, decision.verdict.value)
        else:
            late = f" late for boundary {event.polled_at.isoformat()}" if event.polled_at < current else ""
            logger.info(
                "surge alert %d stored%s for %s (%s)",
                stored.alert_id, late, event.ticker, [r.value for r in event.fired],
            )


def _inputs(event: SurgeEvent) -> dict[str, Any]:
    """What the rule decided on: the sub-rules that fired and the numbers they saw."""
    return {"fired": [rule.value for rule in event.fired], **event.inputs.model_dump(mode="json")}


def _payload(event: SurgeEvent, facts: SurgeFacts) -> PopularitySurgePayload:
    inputs = event.inputs
    return PopularitySurgePayload(
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

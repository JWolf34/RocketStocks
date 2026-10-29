"""Runs a plan: the survey (chain sizes, bar depth) while the clock waits for each timed sample.

Every chain, in the survey and in the timed samples, goes down the same ladder: fetch, fetch
again once if the answer was a gateway error or a timeout, then fetch the three expiry ranges
(the last open-ended), each tried twice. A sample that is due goes before the next call of
anything else, between the parts of a ladder included.

Every call waits on the limiter, builds its client from the stored token (a survey reuses one
for a few minutes, a sample always rebuilds), and ends as one record: a failed request, a
timeout, a dead or unreadable token are recorded and the run goes on to the next call. Records
are the progress: a restart skips every call that has one, and a sample slot more than MAX_LATE
past is recorded as missed, never fetched.
"""

import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import date, datetime, timedelta
from functools import partial
from typing import Any, Protocol

import httpx
from sqlalchemy.exc import SQLAlchemyError

from rs_domain import EASTERN, Clock, eastern_day
from rs_ingest.tools.sweep_probe.payloads import (
    candle_span,
    expiry_count,
    iter_contracts,
    parse_json_object,
)
from rs_ingest.tools.sweep_probe.planning import (
    SurveyTask,
    bars_key,
    chain_base,
    choose_small_tickers,
    survey_tasks,
    timed_base,
    unfinished,
)
from rs_ingest.tools.sweep_probe.schedule import MAX_LATE
from rs_ingest.tools.sweep_probe.sizes import PARTS, needs_ladder
from rs_ingest.tools.sweep_probe.store import (
    CallRecord,
    Kind,
    Outcome,
    Plan,
    ProbeDir,
    error_text,
)
from rs_sources.schwab import SchwabAuthError, TokenMissingError, translate_auth_errors

logger = logging.getLogger(__name__)

HISTORY_START = datetime(1990, 1, 1, tzinfo=EASTERN)
# A survey reuses its client for this long; a token ingest refreshed or a re-login is then picked up.
CLIENT_MAX_AGE = timedelta(minutes=5)
WAIT_CHUNK_SECONDS = 60.0
# A survey task that found no usable token is tried again this long after the survey's last task.
SURVEY_RETRY_AFTER = timedelta(minutes=10)
# Days to expiry that end the first two ranges of the ladder; the third is open-ended.
LADDER_DAYS = (30, 180)
ATTEMPTS = 2


class Limiter(Protocol):
    async def acquire(self) -> None: ...


def expiry_ranges(today: date) -> tuple[tuple[date, date | None], ...]:
    """The ladder: expiries within 30 days, 31 to 180 days, and everything after."""
    near, mid = LADDER_DAYS
    return (
        (today, today + timedelta(days=near)),
        (today + timedelta(days=near + 1), today + timedelta(days=mid)),
        (today + timedelta(days=mid + 1), None),
    )


class ProbeRunner:
    def __init__(
        self,
        *,
        plan: Plan,
        out: ProbeDir,
        make_client: Callable[[], Awaitable[Any]],
        limiter: Limiter,
        clock: Clock,
        sleep: Callable[[float], Awaitable[None]],
        timer: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._plan = plan
        self._out = out
        self._make_client = make_client
        self._limiter = limiter
        self._clock = clock
        self._sleep = sleep
        self._timer = timer
        self._client: Any | None = None
        self._client_built: datetime | None = None
        self._rebuild_client = False
        self._handled: set[datetime] = set()
        self._in_sample = False

    async def run(self) -> None:
        samples = self._plan.samples
        survey = deque(survey_tasks(self._plan))
        deferred: list[SurveyTask] = []  # tasks that found no usable token, tried again later
        retry_at: datetime | None = None
        logger.info(
            "probe starting",
            extra={"survey_tasks": len(survey), "samples": len(samples), "first_sample": samples[0].isoformat()},
        )
        while True:
            await self._serve_due_samples()
            if survey:
                task = survey.popleft()
                await self._survey_task(task)
                if unfinished(self._out, task):
                    deferred.append(task)
                continue
            upcoming = next((t for t in samples if t not in self._handled), None)
            if upcoming is None:
                break
            now = self._clock.now()
            if deferred:
                retry_at = retry_at or now + SURVEY_RETRY_AFTER
                if now >= retry_at:
                    survey.extend(deferred)
                    deferred.clear()
                    retry_at = None
                    continue
                upcoming = min(upcoming, retry_at)
            await self._wait_until(upcoming)
        logger.info("probe complete", extra={"records": len(self._out.records)})

    async def _wait_until(self, target: datetime) -> None:
        logger.info("waiting for next sample", extra={"target": target.isoformat()})
        while (remaining := (target - self._clock.now()).total_seconds()) > 0:
            await self._sleep(min(remaining, WAIT_CHUNK_SECONDS))

    async def _serve_due_samples(self) -> None:
        """Take every sample that is due; called before each call so no survey work delays one."""
        if self._in_sample:
            return
        while (
            due := next((t for t in self._plan.samples if t not in self._handled and t <= self._clock.now()), None)
        ) is not None:
            self._in_sample = True
            try:
                await self._sample(due)
            finally:
                self._in_sample = False
            self._handled.add(due)

    # ---- survey

    async def _survey_task(self, task: SurveyTask) -> None:
        if task.kind == Kind.BARS:
            symbol = task.spellings[0]
            if self._out.terminal(bars_key(symbol)) is None:
                await self._part(bars_key(symbol), Kind.BARS, symbol, partial(self._history, symbol=symbol), None)
            return
        for spelling in task.spellings:
            records = await self._ladder(Kind.CHAIN, chain_base(spelling), spelling)
            if records[0].status != 400:
                return

    async def _ladder(self, kind: Kind, base: str, symbol: str, target: datetime | None = None) -> list[CallRecord]:
        """A chain's full request, and when that failed twice its three expiry ranges."""
        key = f"{base}/full"
        full = self._out.terminal(key) or await self._part(
            key, kind, symbol, partial(self._chain, symbol=symbol), target, variant="full"
        )
        records = [full]
        if not needs_ladder(full):
            return records
        today = eastern_day(self._clock.now())
        for number, (start, end) in enumerate(expiry_ranges(today), start=1):
            key = f"{base}/split{number}"
            part = self._out.terminal(key) or await self._part(
                key,
                kind,
                symbol,
                partial(self._chain, symbol=symbol, start=start, end=end),
                target,
                variant=f"split{number}",
                from_date=start.isoformat(),
                to_date=None if end is None else end.isoformat(),
            )
            records.append(part)
            if part.outcome == Outcome.UNAVAILABLE:
                break
        assert len(records) <= PARTS + 1
        return records

    # ---- timed samples

    async def _sample(self, target: datetime) -> None:
        if self._plan.small_tickers is None:
            small = choose_small_tickers(self._out, self._plan)
            self._plan = self._plan.with_small(small)
            self._out.save_plan(self._plan)
            logger.info("small tickers chosen", extra={"small_tickers": list(small)})
        self._rebuild_client = True
        for symbol in self._plan.timed_tickers:
            await self._ladder(Kind.TIMED, timed_base(target, symbol), symbol, target)

    # ---- requests

    @staticmethod
    async def _chain(client: Any, *, symbol: str, start: date | None = None, end: date | None = None) -> Any:
        return await client.get_option_chain(
            symbol, contract_type=client.Options.ContractType.ALL, from_date=start, to_date=end
        )

    async def _history(self, client: Any, *, symbol: str) -> Any:
        return await client.get_price_history_every_day(
            symbol,
            start_datetime=HISTORY_START,
            end_datetime=self._clock.now(),
            need_extended_hours_data=False,
        )

    # ---- one call

    async def _current_client(self) -> Any:
        now = self._clock.now()
        fresh = self._client_built is not None and now - self._client_built <= CLIENT_MAX_AGE
        if self._rebuild_client or self._client is None or not fresh:
            self._client = None
            self._client = await self._make_client()
            self._client_built = now
            self._rebuild_client = False
        return self._client

    async def _part(
        self,
        key: str,
        kind: Kind,
        symbol: str,
        request: Callable[[Any], Awaitable[Any]],
        target: datetime | None,
        **fields: Any,
    ) -> CallRecord:
        """One call of a ladder or survey: tried again once when its answer is worth another try.

        A timed call whose sample slot has passed by MAX_LATE is recorded as missed instead.
        """
        record: CallRecord | None = None
        made = 0
        first_failure: str | None = None
        for _ in range(ATTEMPTS):
            await self._serve_due_samples()
            now = self._clock.now()
            if target is not None:
                fields |= {"target": target.isoformat(), "late_s": round((now - target).total_seconds(), 1)}
                if now - target > MAX_LATE:
                    record = record or CallRecord(key, kind, symbol, Outcome.MISSED, at=now.isoformat(), **fields)
                    break
            record = await self._attempt(key, kind, symbol, request, fields)
            made += 1
            if not needs_ladder(record):
                break
            first_failure = first_failure or record.label
        assert record is not None
        if made == ATTEMPTS and first_failure is not None:
            record = replace(record, attempts=made, first_failure=first_failure)
        self._append(record)
        return record

    async def _attempt(
        self, key: str, kind: Kind, symbol: str, request: Callable[[Any], Awaitable[Any]], fields: dict[str, Any]
    ) -> CallRecord:
        def record(outcome: Outcome, **more: Any) -> CallRecord:
            return CallRecord(key, kind, symbol, outcome, at=self._clock.now().isoformat(), **fields, **more)

        try:
            client = await self._current_client()
        except (TokenMissingError, SchwabAuthError, ValueError, SQLAlchemyError, OSError) as exc:
            # No token, a dead one, or the database could not be read: the next slot tries again.
            return record(Outcome.UNAVAILABLE, detail=error_text(exc))
        except Exception as exc:
            return record(Outcome.ERROR, detail=error_text(exc))

        await self._limiter.acquire()
        started = self._timer()
        try:
            with translate_auth_errors():
                response = await request(client)
        except SchwabAuthError as exc:
            self._client = None
            return record(Outcome.UNAVAILABLE, latency_s=self._elapsed(started), detail=error_text(exc))
        except (TimeoutError, httpx.TimeoutException):
            return record(Outcome.TIMEOUT, latency_s=self._elapsed(started))
        except Exception as exc:
            return record(Outcome.ERROR, latency_s=self._elapsed(started), detail=error_text(exc))

        latency = self._elapsed(started)
        if response.status_code == 401:
            self._client = None  # the next call rebuilds from whatever the row holds by then
        content: bytes = response.content
        raw = self._out.write_raw(key, content) if content else None
        return record(
            Outcome.HTTP,
            status=response.status_code,
            bytes=len(content),
            latency_s=latency,
            raw=raw,
            **self._summarise(kind, response.status_code, content),
        )

    def _elapsed(self, started: float) -> float:
        return round(self._timer() - started, 3)

    @staticmethod
    def _summarise(kind: Kind, status: int, content: bytes) -> dict[str, Any]:
        if status != 200:
            return {}
        payload = parse_json_object(content)
        if payload is None:
            return {"detail": "200 body is not a JSON object"}
        if kind == Kind.BARS:
            candles, earliest, latest, zeros = candle_span(payload)
            return {
                "candles": candles,
                "earliest": None if earliest is None else earliest.isoformat(),
                "latest": None if latest is None else latest.isoformat(),
                "epoch_zero": zeros,
            }
        truncated = payload.get("isChainTruncated")
        chain_status = payload.get("status")
        return {
            "contracts": sum(1 for _ in iter_contracts(payload)),
            "expiries": expiry_count(payload),
            "truncated": truncated if isinstance(truncated, bool) else None,
            "chain_status": chain_status if isinstance(chain_status, str) else None,
        }

    def _append(self, record: CallRecord) -> None:
        self._out.append(record)
        logger.info(
            "call",
            extra={
                "call_key": record.key,
                "outcome": record.outcome,
                "status": record.status,
                "attempts": record.attempts,
                "contracts": record.contracts,
                "bytes": record.bytes,
                "latency_s": record.latency_s,
                "late_s": record.late_s,
                "detail": record.detail,
            },
        )


__all__ = ["ProbeRunner", "expiry_ranges"]

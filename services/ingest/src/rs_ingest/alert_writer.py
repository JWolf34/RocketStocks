"""Durable write of a candidate and the Alert it produced, in one transaction:
bounded retry on transient database errors, then an Ops ping carrying the
content, then the failure is raised to the caller.

A lost Alert is never swallowed or merely counted: the content reaches ops so
it can be posted by hand, and the exception reaches the job boundary's log.
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Alert, Candidate
from rs_observability import notify_ops
from rs_persistence import AlertRepository, CandidateRepository

logger = logging.getLogger(__name__)

# Connection loss, server restart, lock or statement timeout: worth waiting out.
# Anything else (integrity, data, programming errors) fails the same way again.
_TRANSIENT = (OperationalError, InterfaceError, TimeoutError)


class AlertWriteError(RuntimeError):
    """A candidate (and its Alert) could not be stored; it was reported to ops."""


@dataclass(frozen=True)
class Stored:
    """What one write stored: the candidate, and the Alert it produced (None for
    a candidate that produces none)."""

    candidate_id: int
    alert_id: int | None


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 6
    first_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 15.0

    def backoff(self, failed_attempts: int) -> float:
        return min(self.first_backoff_seconds * 2 ** (failed_attempts - 1), self.max_backoff_seconds)


class AlertWriter:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        policy: RetryPolicy | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._session_factory = session_factory
        self._policy = policy or RetryPolicy()
        self._sleep = sleep

    async def write(self, candidate: Candidate, alert: Alert | None = None) -> Stored | None:
        """Store the candidate with its Alert, if any, in one transaction (with the
        Alert's NOTIFY). Returns what was stored, or None when that Trigger version
        already decided this hit: nothing is written then. Raises AlertWriteError
        after reporting a failure to ops."""
        for attempt in range(1, self._policy.attempts + 1):
            try:
                return await self._insert(candidate, alert)
            except _TRANSIENT as exc:
                if attempt == self._policy.attempts:
                    await self._give_up(candidate, alert, exc, f"after {attempt} attempts")
                logger.warning(
                    "%s insert for %s failed (attempt %d/%d): %s",
                    _what(alert), candidate.symbol, attempt, self._policy.attempts, type(exc).__name__,
                )
                await self._sleep(self._policy.backoff(attempt))
            except SQLAlchemyError as exc:
                await self._give_up(candidate, alert, exc, "with a non-retryable error")
        raise AssertionError("unreachable: the last attempt always gives up")

    async def _insert(self, candidate: Candidate, alert: Alert | None) -> Stored | None:
        async with self._session_factory() as session:
            candidates = CandidateRepository(session)
            # Checked first: a decided hit's Alert already holds its stock's day.
            if await candidates.is_decided(candidate):
                return None
            alert_id = None if alert is None else await AlertRepository(session).insert(alert)
            candidate_id = await candidates.insert(candidate.model_copy(update={"alert_id": alert_id}))
            if candidate_id is None:  # decided by a concurrent writer since the check
                await session.rollback()  # takes the Alert and its NOTIFY with it
                return None
            await session.commit()
            return Stored(candidate_id=candidate_id, alert_id=alert_id)

    async def _give_up(
        self, candidate: Candidate, alert: Alert | None, exc: Exception, how: str
    ) -> None:
        what = _what(alert)
        stored = candidate if alert is None else alert
        logger.error("%s insert for %s failed %s", what.lower(), candidate.symbol, how, exc_info=exc)
        await notify_ops(
            f"{what} NOT stored: {candidate.trigger} for {candidate.symbol} failed {how} "
            f"({type(exc).__name__}). {what} content: "
            f"{json.dumps(stored.model_dump(mode='json'), separators=(',', ':'))}",
            key=(
                f"{what.lower()}-write-failed:{candidate.trigger}:{candidate.rule_version}:"
                f"{candidate.symbol}:{candidate.hit_at:%Y%m%dT%H%MZ}"
            ),
        )
        raise AlertWriteError(
            f"{candidate.trigger} {what.lower()} for {candidate.symbol} not stored {how}"
        ) from exc


def _what(alert: Alert | None) -> str:
    return "Candidate" if alert is None else "Alert"

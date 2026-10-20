"""Durable alert insert: bounded retry on transient database errors, then an
ops notice carrying the alert, then the failure is raised to the caller.

A lost alert is never swallowed or merely counted: the content reaches ops so
it can be posted by hand, and the exception reaches the job boundary's log.
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Alert
from rs_observability import notify_ops
from rs_persistence import AlertRepository

logger = logging.getLogger(__name__)

# Connection loss, server restart, lock or statement timeout: worth waiting out.
# Anything else (integrity, data, programming errors) fails the same way again.
_TRANSIENT = (OperationalError, InterfaceError, TimeoutError)


class AlertWriteError(RuntimeError):
    """An alert could not be stored; it was reported to ops."""


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

    async def write(self, alert: Alert) -> int | None:
        """Store the alert; returns its id, or None when its `dedup_key` already
        existed. Raises AlertWriteError after reporting a failure to ops."""
        for attempt in range(1, self._policy.attempts + 1):
            try:
                return await self._insert(alert)
            except _TRANSIENT as exc:
                if attempt == self._policy.attempts:
                    await self._give_up(alert, exc, f"after {attempt} attempts")
                logger.warning(
                    "alert insert for %s failed (attempt %d/%d): %s",
                    alert.symbol, attempt, self._policy.attempts, type(exc).__name__,
                )
                await self._sleep(self._policy.backoff(attempt))
            except SQLAlchemyError as exc:
                await self._give_up(alert, exc, "with a non-retryable error")
        raise AssertionError("unreachable: the last attempt always gives up")

    async def _insert(self, alert: Alert) -> int | None:
        async with self._session_factory() as session:
            alert_id = await AlertRepository(session).insert(alert)
            await session.commit()
            return alert_id

    async def _give_up(self, alert: Alert, exc: Exception, how: str) -> None:
        logger.error("alert insert for %s failed %s", alert.symbol, how, exc_info=exc)
        await notify_ops(
            f"Alert NOT stored: {alert.alert_type} for {alert.symbol} failed {how} "
            f"({type(exc).__name__}). Alert content: {json.dumps(alert.model_dump(mode='json'), separators=(',', ':'))}",
            key=f"alert-write-failed:{alert.dedup_key or alert.symbol}",
        )
        raise AlertWriteError(f"{alert.alert_type} alert for {alert.symbol} not stored {how}") from exc

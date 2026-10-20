"""DMs John before the Schwab refresh token expires.

Reads only the token's issue time (the schwab_token_issue view), never the token.
Each threshold is sent once per token issue; a re-login is a new issue.
"""

import asyncio
import logging
from datetime import timedelta
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Clock
from rs_persistence import TokenNoticeRepository
from rs_query import schwab_token_issued_at

logger = logging.getLogger(__name__)

TOKEN_LIFETIME = timedelta(days=7)
THRESHOLD_HOURS = (24, 6)


class DirectMessenger(Protocol):
    """The send seam: tests fake it; the daemon backs it with the admin's DM channel."""

    async def send_dm(self, text: str) -> None: ...


class TokenAgeNotifier:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        messenger: DirectMessenger,
        clock: Clock,
    ) -> None:
        self._session_factory = session_factory
        self._messenger = messenger
        self._clock = clock

    async def check(self) -> int | None:
        """DM if a threshold is newly crossed; returns the threshold (hours) that was sent."""
        async with self._session_factory() as session:
            issued_at = await schwab_token_issued_at(session)
            if issued_at is None:
                return None
            expires_at = issued_at + TOKEN_LIFETIME
            remaining = expires_at - self._clock.now()
            # An expired token is the ingest ops notice's job, not a countdown.
            if remaining <= timedelta(0):
                return None
            due = [h for h in THRESHOLD_HOURS if remaining <= timedelta(hours=h)]
            if not due:
                return None
            tightest = min(due)
            repo = TokenNoticeRepository(session)
            if tightest in await repo.sent_thresholds(issued_at):
                return None
            await self._messenger.send_dm(
                f"The Schwab token expires <t:{int(expires_at.timestamp())}:R>. "
                "Run /setup schwab to sign in again before then."
            )
            # A larger threshold skipped while the bot was down is covered by the tighter notice.
            for hours in due:
                await repo.record(issued_at, hours)
            await session.commit()
            return tightest

    async def run(self, interval_seconds: float = 600.0) -> None:
        while True:
            try:
                await self.check()
            except Exception:
                logger.exception("Schwab token age check failed")
            await asyncio.sleep(interval_seconds)

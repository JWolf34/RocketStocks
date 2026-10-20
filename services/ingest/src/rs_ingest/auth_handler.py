"""Serves `/setup schwab`: the only place a sign-in is exchanged for a token.

The bot opens a schwab_auth_request; this loop gives it a sign-in URL, then trades
the callback URL John pasted for a token and writes it, so ingest stays the one
token holder. NOTIFY wakes the loop and a poll covers a missed one. The OAuth
context lives in memory, so a request open across an ingest restart fails with a
message to start again.

Neither the callback URL nor a token is ever logged or put in a stored error.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any, Protocol

import psycopg
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import AuthRequestState, Clock, SchwabAuthRequest
from rs_persistence import (
    AUTH_NOTIFY_CHANNEL,
    AUTH_REQUEST_WINDOW,
    AuthRequestRepository,
    SchwabTokenStore,
)
from rs_sources.schwab import CallbackRejected, ExchangeFailed

logger = logging.getLogger(__name__)

LOST_CONTEXT = "Ingest restarted before the sign-in finished. Run /setup schwab again."
UNEXPECTED = "The sign-in could not be completed. Run /setup schwab again."


class OAuthFlow(Protocol):
    def begin(self) -> tuple[str, Any]: ...

    async def exchange(self, context: Any, received_url: str) -> dict[str, Any]: ...


class AuthRequestHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        token_store: SchwabTokenStore,
        oauth: OAuthFlow,
        clock: Clock,
        on_renewed: Callable[[], Awaitable[None]],
        *,
        conninfo: str | None = None,
        poll_seconds: float = 15.0,
    ) -> None:
        self._session_factory = session_factory
        self._token_store = token_store
        self._oauth = oauth
        self._clock = clock
        self._on_renewed = on_renewed
        self._conninfo = conninfo
        self._poll_seconds = poll_seconds
        self._contexts: dict[int, Any] = {}
        self._wake = asyncio.Event()

    async def run(self) -> None:
        """Handle requests until cancelled."""
        listener = None
        if self._conninfo is not None:
            listener = asyncio.create_task(self._listen(self._conninfo), name="schwab-auth-listen")
        try:
            while True:
                try:
                    await self.process_once()
                except Exception:
                    logger.exception("schwab auth request pass failed")
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), timeout=self._poll_seconds)
                self._wake.clear()
        finally:
            if listener is not None:
                listener.cancel()
                with suppress(asyncio.CancelledError):
                    await listener

    async def process_once(self) -> None:
        async with self._session_factory() as session:
            repo = AuthRequestRepository(session)
            expired = await repo.expire_older_than(self._clock.now() - AUTH_REQUEST_WINDOW)
            await session.commit()
            if expired:
                logger.info("expired %d stale schwab auth request(s)", expired)
            for request in await repo.list_in_state(AuthRequestState.REQUESTED):
                await self._provide_url(repo, request)
                await session.commit()
            for request in await repo.list_in_state(AuthRequestState.CALLBACK_RECEIVED):
                await self._exchange(session, repo, request)
            # A request that expired or failed elsewhere leaves its context behind.
            waiting = {r.id for r in await repo.list_in_state(AuthRequestState.URL_READY)}
            self._contexts = {i: c for i, c in self._contexts.items() if i in waiting}

    async def _provide_url(self, repo: AuthRequestRepository, request: SchwabAuthRequest) -> None:
        auth_url, context = self._oauth.begin()
        if await repo.set_auth_url(request.id, auth_url):
            self._contexts[request.id] = context
            logger.info("schwab auth request %d: sign-in URL ready", request.id)

    async def _exchange(
        self, session: AsyncSession, repo: AuthRequestRepository, request: SchwabAuthRequest
    ) -> None:
        context = self._contexts.pop(request.id, None)
        if context is None:
            await self._fail(session, repo, request.id, LOST_CONTEXT)
            return
        assert request.callback_url is not None
        try:
            token = await self._oauth.exchange(context, request.callback_url)
            await self._token_store.save(token)
        except (CallbackRejected, ExchangeFailed) as exc:
            await self._fail(session, repo, request.id, str(exc))
            return
        except Exception as exc:
            # The type only: the text can carry the callback URL.
            logger.error("schwab auth request %d failed (%s)", request.id, type(exc).__name__)
            await self._fail(session, repo, request.id, UNEXPECTED)
            return
        await repo.complete(request.id)
        await session.commit()
        logger.info("schwab auth request %d done: token stored", request.id)
        try:
            await self._on_renewed()
        except Exception:
            logger.exception("token stored but the Schwab client could not be rebuilt")

    async def _fail(
        self, session: AsyncSession, repo: AuthRequestRepository, request_id: int, error: str
    ) -> None:
        await repo.fail(request_id, error)
        await session.commit()
        logger.warning("schwab auth request %d failed: %s", request_id, error)

    async def _listen(self, conninfo: str) -> None:
        backoff = 1.0
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as conn:
                    await conn.execute(f'LISTEN "{AUTH_NOTIFY_CHANNEL}"')
                    backoff = 1.0
                    self._wake.set()  # anything that arrived while disconnected
                    async for _ in conn.notifies():
                        self._wake.set()
            except Exception:
                logger.warning("schwab auth LISTEN lost - reconnecting in %.0fs", backoff, exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

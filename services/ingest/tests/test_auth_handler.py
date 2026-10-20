"""AuthRequestHandler: the ingest half of `/setup schwab`.

The OAuth exchange is faked at the OAuthFlow seam (it is covered against schwab-py
in rs_sources); requests, the token row and the gate are real.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from rs_domain import AuthRequestState
from rs_ingest.auth_handler import LOST_CONTEXT, UNEXPECTED, AuthRequestHandler
from rs_ingest.daemon import TokenGate
from rs_ingest.metrics import DaemonMetrics
from rs_persistence import AuthRequestRepository, SchwabTokenStore
from rs_sources.schwab import CallbackRejected

BOT_USER = 4242
CALLBACK = "https://127.0.0.1:8182/?code=SECRETCODE&session=SECRETSESSION"
TOKEN = {"creation_timestamp": 1760443200, "token": {"refresh_token": "SECRETREFRESH"}}
AUTH_URL = "https://api.schwabapi.com/v1/oauth/authorize?client_id=k"


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.current = now

    def now(self) -> datetime:
        return self.current


class FakeOAuth:
    def __init__(self) -> None:
        self.begun = 0
        self.received: list[str] = []
        self.error: Exception | None = None

    def begin(self) -> tuple[str, Any]:
        self.begun += 1
        return AUTH_URL, f"context-{self.begun}"

    async def exchange(self, context: Any, received_url: str) -> dict[str, Any]:
        self.received.append(received_url)
        if self.error is not None:
            raise self.error
        return TOKEN


class Rig:
    def __init__(self, session_factory, clock: FakeClock) -> None:
        self.session_factory = session_factory
        self.clock = clock
        self.oauth = FakeOAuth()
        self.store = SchwabTokenStore(session_factory)
        self.renewals = 0
        self.handler = self.build()

    def build(self) -> AuthRequestHandler:
        return AuthRequestHandler(
            self.session_factory, self.store, self.oauth, self.clock, self._renewed
        )

    async def _renewed(self) -> None:
        self.renewals += 1

    async def request(self) -> int:
        async with self.session_factory() as s:
            request = await AuthRequestRepository(s).create(BOT_USER)
            await s.commit()
        return request.id

    async def paste(self, request_id: int, url: str = CALLBACK) -> None:
        async with self.session_factory() as s:
            assert await AuthRequestRepository(s).set_callback_url(request_id, url)
            await s.commit()

    async def get(self, request_id: int):
        async with self.session_factory() as s:
            request = await AuthRequestRepository(s).get(request_id)
        assert request is not None
        return request


@pytest.fixture
def rig(session_factory) -> Rig:
    return Rig(session_factory, FakeClock(datetime.now(UTC)))


async def test_happy_path_hands_out_the_url_then_stores_the_token(rig):
    request_id = await rig.request()

    await rig.handler.process_once()
    waiting = await rig.get(request_id)
    assert (waiting.state, waiting.auth_url) == (AuthRequestState.URL_READY, AUTH_URL)

    await rig.paste(request_id)
    await rig.handler.process_once()

    done = await rig.get(request_id)
    assert done.state is AuthRequestState.DONE
    assert done.callback_url is None
    assert rig.oauth.received == [CALLBACK]
    assert await rig.store.load() == TOKEN
    assert rig.renewals == 1


async def test_success_clears_a_dead_token_gate(rig, notices):
    gate = TokenGate(rig.store, rig._renewed, rig.clock, DaemonMetrics())
    await gate.trip("refresh token rejected")
    assert gate.dead

    async def token_renewed() -> None:
        await gate.renewed()

    handler = AuthRequestHandler(rig.session_factory, rig.store, rig.oauth, rig.clock, token_renewed)
    request_id = await rig.request()
    await handler.process_once()
    await rig.paste(request_id)
    await handler.process_once()

    assert not gate.dead
    assert rig.renewals == 1  # the gate rebuilt the Schwab client from the new token


async def test_a_rejected_callback_fails_the_request_and_stores_nothing(rig):
    rig.oauth.error = CallbackRejected("Schwab did not accept that callback URL.")
    request_id = await rig.request()
    await rig.handler.process_once()
    await rig.paste(request_id, "https://127.0.0.1:8182/?code=BAD")

    await rig.handler.process_once()

    failed = await rig.get(request_id)
    assert failed.state is AuthRequestState.FAILED
    assert failed.error == "Schwab did not accept that callback URL."
    assert failed.callback_url is None
    assert await rig.store.load() is None
    assert rig.renewals == 0


async def test_an_unexpected_error_is_reported_without_its_text(rig, caplog):
    rig.oauth.error = RuntimeError(f"boom while exchanging {CALLBACK}")
    request_id = await rig.request()
    await rig.handler.process_once()
    await rig.paste(request_id)

    with caplog.at_level(logging.DEBUG):
        await rig.handler.process_once()

    failed = await rig.get(request_id)
    assert (failed.state, failed.error) == (AuthRequestState.FAILED, UNEXPECTED)
    assert "SECRETCODE" not in caplog.text


async def test_a_request_left_open_expires_and_gets_no_url(rig):
    request_id = await rig.request()
    rig.clock.current += timedelta(minutes=11)

    await rig.handler.process_once()

    expired = await rig.get(request_id)
    assert (expired.state, expired.auth_url) == (AuthRequestState.EXPIRED, None)
    assert rig.oauth.begun == 0


async def test_a_request_pasted_after_an_ingest_restart_fails_with_a_clear_message(rig):
    request_id = await rig.request()
    await rig.handler.process_once()
    await rig.paste(request_id)

    restarted = rig.build()  # the OAuth context lived in memory
    await restarted.process_once()

    failed = await rig.get(request_id)
    assert (failed.state, failed.error) == (AuthRequestState.FAILED, LOST_CONTEXT)
    assert rig.oauth.received == []


async def test_nothing_secret_is_logged_on_the_happy_path(rig, caplog):
    with caplog.at_level(logging.DEBUG):
        request_id = await rig.request()
        await rig.handler.process_once()
        await rig.paste(request_id)
        await rig.handler.process_once()

    for secret in ("SECRETCODE", "SECRETSESSION", "SECRETREFRESH", CALLBACK):
        assert secret not in caplog.text


async def test_notify_wakes_the_loop_without_waiting_for_the_poll(session_factory, migrated_db_url):
    rig = Rig(session_factory, FakeClock(datetime.now(UTC)))
    handler = AuthRequestHandler(
        session_factory,
        rig.store,
        rig.oauth,
        rig.clock,
        rig._renewed,
        conninfo=migrated_db_url.replace("+psycopg", "", 1),
        poll_seconds=3600,
    )
    task = asyncio.create_task(handler.run())
    try:
        await asyncio.sleep(0.5)  # first pass done, LISTEN established
        request_id = await rig.request()
        for _ in range(50):
            if (await rig.get(request_id)).state is AuthRequestState.URL_READY:
                break
            await asyncio.sleep(0.1)
        assert (await rig.get(request_id)).state is AuthRequestState.URL_READY
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

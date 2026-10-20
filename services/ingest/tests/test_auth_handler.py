"""AuthRequestHandler: the ingest half of `/setup schwab`.

The OAuth exchange is faked at the OAuthFlow seam (it is covered against schwab-py
in rs_sources); requests, the token row and the gate are real.
"""

import asyncio
import logging
import traceback
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text

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
        self.exchange_result: dict[str, Any] = TOKEN
        self.begin_error: Exception | None = None
        self.during_exchange: Callable[[], Awaitable[None]] | None = None

    def begin(self) -> tuple[str, Any]:
        if self.begin_error is not None:
            raise self.begin_error
        self.begun += 1
        return AUTH_URL, f"context-{self.begun}"

    async def exchange(self, context: Any, received_url: str) -> dict[str, Any]:
        self.received.append(received_url)
        if self.during_exchange is not None:
            await self.during_exchange()
        if self.error is not None:
            raise self.error
        return self.exchange_result


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


# Short on purpose: SQLAlchemy truncates long parameter reprs in error text.
POISON_TOKEN = {"r": "S3CR3T"}


@pytest.fixture
async def failing_token_writes(session_factory):
    """Every write to the token row fails in the database, with the token bound."""
    async with session_factory() as s:
        await s.execute(text(
            "CREATE FUNCTION reject_token() RETURNS trigger LANGUAGE plpgsql AS"
            " $$ BEGIN RAISE EXCEPTION 'token writes are down'; END $$"
        ))
        await s.execute(text(
            "CREATE TRIGGER reject_token BEFORE INSERT ON schwab_tokens"
            " FOR EACH ROW EXECUTE FUNCTION reject_token()"
        ))
        await s.commit()
    yield
    async with session_factory() as s:
        await s.execute(text("DROP TRIGGER reject_token ON schwab_tokens"))
        await s.execute(text("DROP FUNCTION reject_token()"))
        await s.commit()


def _assert_no_secret_in_records(caplog) -> None:
    assert "S3CR3T" not in caplog.text
    for record in caplog.records:
        assert "S3CR3T" not in record.getMessage()
        if record.exc_info:
            assert "S3CR3T" not in "".join(traceback.format_exception(*record.exc_info))


async def test_a_failed_token_write_during_sign_in_logs_no_token_value(rig, failing_token_writes, caplog):
    rig.oauth.exchange_result = POISON_TOKEN
    request_id = await rig.request()
    await rig.handler.process_once()
    await rig.paste(request_id)

    with caplog.at_level(logging.DEBUG):
        await rig.handler.process_once()

    assert (await rig.get(request_id)).state is AuthRequestState.FAILED
    _assert_no_secret_in_records(caplog)


async def test_a_failed_token_refresh_write_logs_no_token_value(rig, failing_token_writes, caplog):
    with caplog.at_level(logging.DEBUG):
        rig.store.schedule_save(POISON_TOKEN)
        await rig.store.drain()

    assert any(r.levelno >= logging.ERROR for r in caplog.records)
    _assert_no_secret_in_records(caplog)


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


async def test_a_sign_in_url_failure_fails_that_request_and_not_the_pass(rig):
    broken = await rig.request()
    rig.oauth.begin_error = RuntimeError("bad api key")
    await rig.handler.process_once()
    assert (await rig.get(broken)).state is AuthRequestState.FAILED
    assert (await rig.get(broken)).error == UNEXPECTED

    rig.oauth.begin_error = None
    healthy = await rig.request()
    await rig.handler.process_once()
    assert (await rig.get(healthy)).state is AuthRequestState.URL_READY


async def test_a_callback_pasted_during_another_exchange_keeps_its_context(rig):
    first, second = await rig.request(), await rig.request()
    await rig.handler.process_once()
    await rig.paste(first)

    async def paste_second_midway() -> None:
        await rig.paste(second)

    rig.oauth.during_exchange = paste_second_midway
    await rig.handler.process_once()
    rig.oauth.during_exchange = None
    await rig.handler.process_once()

    assert (await rig.get(first)).state is AuthRequestState.DONE
    assert (await rig.get(second)).state is AuthRequestState.DONE

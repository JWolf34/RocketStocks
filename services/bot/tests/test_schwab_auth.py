"""`/setup schwab`: the bot half of the handshake.

Ingest is faked by a stand-in that answers the shared schwab_auth_request rows each
time the flow sleeps between polls; the rows and the state machine are real.
"""

import logging
from types import SimpleNamespace

import discord
import pytest
from discord import app_commands

from rs_bot.commands import register_commands
from rs_bot.pings import PingRoles
from rs_bot.schwab_auth import (
    DENIED,
    SchwabAuthFlow,
    is_authorized,
    looks_like_callback,
)
from rs_domain import AuthRequestState
from rs_persistence import AuthRequestRepository

ADMIN = 111
OWNER = 222
STRANGER = 333
AUTH_URL = "https://api.schwabapi.com/v1/oauth/authorize?client_id=k"
CALLBACK = "https://127.0.0.1:8182/?code=SECRETCODE&session=SECRETSESSION"


class FakeIngest:
    """Answers requests the way the real handler does, one step per poll sleep."""

    def __init__(self, session_factory, *, answer: bool = True, outcome: str = "done") -> None:
        self._session_factory = session_factory
        self.answer = answer
        self.outcome = outcome

    async def __call__(self, _seconds: float) -> None:
        if not self.answer:
            return
        async with self._session_factory() as s:
            repo = AuthRequestRepository(s)
            for request in await repo.list_in_state(AuthRequestState.REQUESTED):
                await repo.set_auth_url(request.id, AUTH_URL)
            for request in await repo.list_in_state(AuthRequestState.CALLBACK_RECEIVED):
                if self.outcome == "done":
                    await repo.complete(request.id)
                else:
                    await repo.fail(request.id, "Schwab did not accept that callback URL.")
            await s.commit()


def _flow(session_factory, ingest: FakeIngest) -> SchwabAuthFlow:
    return SchwabAuthFlow(
        session_factory,
        poll_seconds=1.0,
        url_wait_seconds=5.0,
        outcome_wait_seconds=5.0,
        sleep=ingest,
    )


async def _state(session_factory, request_id: int):
    async with session_factory() as s:
        request = await AuthRequestRepository(s).get(request_id)
    assert request is not None
    return request


# --- who may run it ---------------------------------------------------------


@pytest.mark.parametrize(
    ("user", "owner", "admin", "allowed"),
    [
        (OWNER, OWNER, None, True),
        (ADMIN, OWNER, ADMIN, True),
        (STRANGER, OWNER, ADMIN, False),
        (STRANGER, OWNER, None, False),
        (STRANGER, None, None, False),
    ],
)
def test_guild_owner_or_configured_admin_only(user, owner, admin, allowed):
    assert is_authorized(user, owner, admin) is allowed


def _interaction(user_id: int, owner_id: int = OWNER):
    sent: list[dict] = []

    async def send_message(content, **kwargs):
        sent.append({"content": content, **kwargs})

    async def defer(**kwargs):
        pass

    async def followup(content, **kwargs):
        sent.append({"content": content, **kwargs})

    interaction = SimpleNamespace(
        user=SimpleNamespace(id=user_id),
        guild=SimpleNamespace(owner_id=owner_id),
        response=SimpleNamespace(send_message=send_message, defer=defer),
        followup=SimpleNamespace(send=followup),
    )
    return interaction, sent


def _command(session_factory, ingest: FakeIngest, admin_user_id: int | None):
    tree = app_commands.CommandTree(discord.Client(intents=discord.Intents.default()))
    register_commands(
        tree,
        flow=_flow(session_factory, ingest),
        pings=PingRoles(session_factory),
        admin_user_id=admin_user_id,
    )
    return tree.get_command("setup").get_command("schwab")


async def test_a_non_admin_is_denied_ephemerally_and_no_request_is_opened(session_factory):
    command = _command(session_factory, FakeIngest(session_factory), ADMIN)
    interaction, sent = _interaction(STRANGER)

    await command.callback(interaction)

    assert sent == [{"content": DENIED, "ephemeral": True}]
    async with session_factory() as s:
        assert await AuthRequestRepository(s).list_in_state(AuthRequestState.REQUESTED) == []


async def test_the_admin_gets_the_sign_in_url_in_an_ephemeral_reply(session_factory):
    command = _command(session_factory, FakeIngest(session_factory), ADMIN)
    interaction, sent = _interaction(ADMIN)

    await command.callback(interaction)

    assert len(sent) == 1
    assert AUTH_URL in sent[0]["content"]
    assert sent[0]["ephemeral"] is True


async def test_the_reply_says_so_when_ingest_does_not_answer(session_factory):
    command = _command(session_factory, FakeIngest(session_factory, answer=False), ADMIN)
    interaction, sent = _interaction(ADMIN)

    await command.callback(interaction)

    assert "did not answer" in sent[0]["content"]
    assert AUTH_URL not in sent[0]["content"]


# --- the flow ---------------------------------------------------------------


async def test_happy_path_reports_success(session_factory):
    flow = _flow(session_factory, FakeIngest(session_factory))

    sign_in = await flow.start(ADMIN)
    assert sign_in.auth_url == AUTH_URL

    message = await flow.submit(sign_in.request_id, ADMIN, CALLBACK)

    assert "signed in again" in message
    assert (await _state(session_factory, sign_in.request_id)).state is AuthRequestState.DONE


async def test_a_failed_exchange_shows_the_reason(session_factory):
    flow = _flow(session_factory, FakeIngest(session_factory, outcome="failed"))
    sign_in = await flow.start(ADMIN)

    message = await flow.submit(sign_in.request_id, ADMIN, CALLBACK)

    assert message == "Sign-in failed: Schwab did not accept that callback URL."


async def test_text_that_is_not_a_callback_url_is_refused_and_keeps_the_request_usable(session_factory):
    flow = _flow(session_factory, FakeIngest(session_factory))
    sign_in = await flow.start(ADMIN)

    message = await flow.submit(sign_in.request_id, ADMIN, "not a url")

    assert "does not look like" in message
    assert (await _state(session_factory, sign_in.request_id)).state is AuthRequestState.URL_READY
    assert "signed in again" in await flow.submit(sign_in.request_id, ADMIN, CALLBACK)


async def test_an_expired_request_cannot_take_a_callback(session_factory):
    flow = _flow(session_factory, FakeIngest(session_factory))
    sign_in = await flow.start(ADMIN)
    async with session_factory() as s:
        await AuthRequestRepository(s).fail(sign_in.request_id, "expired")
        await s.commit()

    message = await flow.submit(sign_in.request_id, ADMIN, CALLBACK)

    assert "expired or was already used" in message


async def test_someone_elses_request_cannot_take_a_callback(session_factory):
    flow = _flow(session_factory, FakeIngest(session_factory))
    sign_in = await flow.start(ADMIN)

    message = await flow.submit(sign_in.request_id, STRANGER, CALLBACK)

    assert "not yours" in message
    assert (await _state(session_factory, sign_in.request_id)).state is AuthRequestState.URL_READY


async def test_an_ingest_that_never_answers_ends_the_wait(session_factory):
    ingest = FakeIngest(session_factory)
    flow = _flow(session_factory, ingest)
    sign_in = await flow.start(ADMIN)
    ingest.answer = False

    message = await flow.submit(sign_in.request_id, ADMIN, CALLBACK)

    assert "has not answered" in message


async def test_the_callback_url_is_never_logged(session_factory, caplog):
    flow = _flow(session_factory, FakeIngest(session_factory))
    with caplog.at_level(logging.DEBUG):
        sign_in = await flow.start(ADMIN)
        message = await flow.submit(sign_in.request_id, ADMIN, CALLBACK)

    for secret in ("SECRETCODE", "SECRETSESSION", CALLBACK):
        assert secret not in caplog.text
        assert secret not in message


@pytest.mark.parametrize(
    ("text", "ok"),
    [
        (CALLBACK, True),
        ("  https://127.0.0.1:8182/?code=abc  ", True),
        ("https://127.0.0.1:8182/", False),
        ("code=abc", False),
        ("ftp://host/?code=abc", False),
    ],
)
def test_callback_shape_check(text, ok):
    assert looks_like_callback(text) is ok

"""SchwabSession: one client, rebuilt from the store when the token is renewed."""

from typing import Any

import pytest

from rs_ingest.schwab_session import SchwabSession
from rs_sources.schwab import TokenMissingError


class Builder:
    """Hand-written stand-in for build_schwab_client: hands out numbered
    clients, or raises TokenMissingError while no token is 'stored'."""

    def __init__(self) -> None:
        self.token_stored = False
        self.built: list[tuple[str, str]] = []

    async def __call__(self, api_key: str, api_secret: str, token_store: Any) -> str:
        if not self.token_stored:
            raise TokenMissingError("no Schwab token in the database")
        self.built.append((api_key, api_secret))
        return f"client-{len(self.built)}"


@pytest.fixture
def builder() -> Builder:
    return Builder()


@pytest.fixture
def session(builder: Builder) -> SchwabSession:
    return SchwabSession("key", "secret", token_store=object(), build=builder)  # type: ignore[arg-type]


async def test_load_reports_false_and_client_raises_without_a_token(session):
    assert await session.load() is False
    with pytest.raises(TokenMissingError):
        _ = session.client


async def test_load_builds_the_client_from_the_credentials(session, builder):
    builder.token_stored = True

    assert await session.load() is True

    assert session.client == "client-1"
    assert builder.built == [("key", "secret")]


async def test_reload_replaces_the_client(session, builder):
    builder.token_stored = True
    await session.load()

    await session.reload()

    assert session.client == "client-2"


async def test_reload_without_a_stored_token_is_an_error(session):
    with pytest.raises(TokenMissingError):
        await session.reload()

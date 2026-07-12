"""Serve test fixtures: the real ASGI app over a real migrated TimescaleDB.

The DB fixture (gating semantics, throwaway database) is the shared
rs_persistence.testing one. Tests hit the app through httpx's ASGI transport —
the same middleware/router/serialization path a real consumer exercises.
Shared literals travel as fixtures (api_key/auth_headers/bars_url), the
sanctioned cross-file channel under pytest's importlib mode.
"""

# ruff: noqa: F811 — fixture params named after the imported migrated_db_url
# fixture are how pytest injects it, not redefinitions.

import httpx
import pytest
from sqlalchemy import text

from rs_persistence import (
    ServiceClientRepository,
    build_engine,
    build_session_factory,
)
from rs_persistence.testing import migrated_db_url  # noqa: F401
from rs_serve.app import create_app
from rs_serve.auth import hash_api_key
from rs_serve.settings import ServeSettings

API_KEY = "test-key-not-secret"


@pytest.fixture
def api_key() -> str:
    """The one provisioned key (see session_factory)."""
    return API_KEY


@pytest.fixture
def auth_headers(api_key) -> dict[str, str]:
    return {"X-API-Key": api_key}


@pytest.fixture
def bars_url() -> str:
    """A valid bars range request (the 2026-07-09 RTH session window)."""
    return "/v1/bars/SPY?start=2026-07-09T13:30:00Z&end=2026-07-09T20:00:00Z"


@pytest.fixture
async def session_factory(migrated_db_url):
    # The shared session-scoped DB is truncated per test for isolation; one
    # known API key is provisioned so tests can authenticate.
    engine = build_engine(migrated_db_url)
    factory = build_session_factory(engine)
    async with factory() as s:
        await s.execute(text("TRUNCATE service_clients RESTART IDENTITY"))
        await s.execute(text("TRUNCATE stream_bars_5m"))
        await ServiceClientRepository(s).provision(name="test", api_key_hash=hash_api_key(API_KEY))
        await s.commit()
    yield factory
    await engine.dispose()


@pytest.fixture
async def app(migrated_db_url, session_factory):
    app = create_app(ServeSettings(rs_db_url=migrated_db_url))
    yield app
    await app.state.engine.dispose()


@pytest.fixture
async def client(app):
    # raise_app_exceptions=False: Starlette's ServerErrorMiddleware re-raises
    # after responding, and the 500-handler tests assert on the response.
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://serve") as c:
        yield c

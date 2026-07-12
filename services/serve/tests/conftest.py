"""Serve test fixtures: the real ASGI app over a real migrated TimescaleDB.

DB gating semantics are identical to libs/persistence/tests/conftest.py
(RS_TEST_DB_URL, default 5499 container; explicit-but-unreachable FAILS,
absent default SKIPs). Tests hit the app through httpx's ASGI transport —
the same middleware/router/serialization path a real consumer exercises.
"""

import hashlib
import os
import uuid

import httpx
import pytest
from alembic import command
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import text

from rs_persistence import (
    ServiceClientRepository,
    build_engine,
    build_session_factory,
    migration_config,
)
from rs_serve.app import create_app
from rs_serve.settings import ServeSettings

DEFAULT_URL = "postgresql+psycopg://postgres:t@localhost:5499/postgres"

API_KEY = "test-key-not-secret"
API_KEY_HASH = hashlib.sha256(API_KEY.encode()).hexdigest()


def _reachable(url: str) -> bool:
    engine = create_sync_engine(url, connect_args={"connect_timeout": 2})
    try:
        with engine.connect():
            return True
    except Exception:
        return False
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def migrated_db_url():
    explicit_url = os.environ.get("RS_TEST_DB_URL")
    admin_url = explicit_url or DEFAULT_URL
    if not _reachable(admin_url):
        if explicit_url:
            pytest.fail(f"RS_TEST_DB_URL is set but unreachable: {explicit_url}")
        pytest.skip(f"no test database at {DEFAULT_URL} (start the container or set RS_TEST_DB_URL)")

    db_name = f"rs_test_{uuid.uuid4().hex[:12]}"
    admin_engine = create_sync_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{db_name}"'))

    test_url = admin_url.rsplit("/", 1)[0] + f"/{db_name}"
    try:
        command.upgrade(migration_config(test_url), "head")
        yield test_url
    finally:
        with admin_engine.connect() as conn:
            conn.execute(text(f'DROP DATABASE "{db_name}" WITH (FORCE)'))
        admin_engine.dispose()


@pytest.fixture
async def session_factory(migrated_db_url):
    # The shared session-scoped DB is truncated per test for isolation; one
    # known API key is provisioned so tests can authenticate.
    engine = build_engine(migrated_db_url)
    factory = build_session_factory(engine)
    async with factory() as s:
        await s.execute(text("TRUNCATE service_clients RESTART IDENTITY"))
        await s.execute(text("TRUNCATE stream_bars_5m"))
        await ServiceClientRepository(s).provision(name="test", api_key_hash=API_KEY_HASH)
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

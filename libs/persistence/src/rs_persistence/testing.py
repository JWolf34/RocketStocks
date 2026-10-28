"""Shared pytest fixtures for DB-backed tests - the "5499 pattern", extracted
once a second workspace member (rs_bot) needed a verbatim copy.

Target resolution: RS_TEST_DB_URL, defaulting to the ephemeral
container (docker run -d --rm -p 5499:5432 -e POSTGRES_PASSWORD=t
timescale/timescaledb:2.30.2-pg16).

Gating semantics: with no RS_TEST_DB_URL set and the default container absent,
tests SKIP (visible, local-dev friendly). When RS_TEST_DB_URL is explicitly
set - as CI does - an unreachable database FAILS the suite, so a port/env
drift can never turn the DB acceptance tests into silent green skips.

Each test session creates a throwaway database, migrates it to head, and
drops it afterward - reruns never see stale state.

Usage, from a member's tests/conftest.py (a plain import - pytest discovers
fixtures by name in the conftest namespace; `pytest_plugins` would be rejected
in a non-rootdir conftest):

    from rs_persistence.testing import migrated_db_url, session_factory  # noqa: F401

Test-support only: pytest is a dev dependency, so this module must never be
imported by runtime code.
"""

import os
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
from alembic import command
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_persistence import build_engine, build_session_factory, migration_config

DEFAULT_URL = "postgresql+psycopg://postgres:t@localhost:5499/postgres"


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
def migrated_db_url() -> Iterator[str]:
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
async def session_factory(migrated_db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Async session factory over the migrated DB. migrated_db_url is
    session-scoped and shared; this truncates so each test starts from empty
    tables with a reset identity."""
    engine = build_engine(migrated_db_url)
    factory = build_session_factory(engine)
    async with factory() as s:
        await s.execute(
            text(
                "TRUNCATE alerts, candidate, popularity_snapshot, daily_bar,"
                " bar_request, unpriced_symbol, outcome, track_record, schwab_auth_request,"
                " schwab_tokens, schwab_token_notice, trigger_role, universe_listing, universe_snapshot"
                " RESTART IDENTITY"
            )
        )
        await s.commit()
    yield factory
    await engine.dispose()

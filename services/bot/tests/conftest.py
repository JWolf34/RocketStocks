"""DB-gated fixtures — the 5499 conftest pattern (see libs/persistence/tests/
conftest.py, the pattern's origin, for the gating semantics: default container
absent → visible SKIP; explicit RS_TEST_DB_URL unreachable → FAIL)."""

import os
import uuid

import pytest
from alembic import command
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import text

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
    # migrated_db_url is session-scoped and shared; truncate so each test starts
    # from an empty alerts/outbox with a reset identity.
    engine = build_engine(migrated_db_url)
    factory = build_session_factory(engine)
    async with factory() as s:
        await s.execute(text("TRUNCATE alerts, outbox RESTART IDENTITY"))
        await s.commit()
    yield factory
    await engine.dispose()

"""Shared DB-gated pytest fixture for every member's DB tests (S005 extraction
— the serve conftest had copied this verbatim from libs/persistence, and
gating semantics must not drift between members).

Imports pytest/alembic at module import: only test conftests import this
module, and both are dev dependencies of the workspace, never of runtime code.

Target resolution: RS_TEST_DB_URL, defaulting to the spec's ephemeral
container (docker run -d --rm -p 5499:5432 -e POSTGRES_PASSWORD=t
timescale/timescaledb:latest-pg16).

Gating semantics: with no RS_TEST_DB_URL set and the default container absent,
tests SKIP (visible, local-dev friendly). When RS_TEST_DB_URL is explicitly
set — as CI does — an unreachable database FAILS the suite, so a port/env
drift can never turn DB acceptance tests into silent green skips.

Each test session creates a throwaway database, migrates it to head, and
drops it afterward — reruns never see stale state.

Usage (in a member's tests/conftest.py):

    from rs_persistence.testing import migrated_db_url  # noqa: F401
"""

import os
import uuid

import pytest
from alembic import command
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import text

from rs_persistence import migration_config

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

"""DB-gated fixtures. Tests skip (visibly) unless a TimescaleDB is reachable.

Target resolution: RS_TEST_DB_URL, defaulting to the spec's ephemeral
container (docker run -d --rm -p 5499:5432 -e POSTGRES_PASSWORD=t
timescale/timescaledb:latest-pg16). Each test session creates a throwaway
database, migrates it to head, and drops it afterward — reruns never see
stale state.
"""

import os
import uuid

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import text

from rs_persistence import alembic_config_path

DEFAULT_URL = "postgresql+psycopg://postgres:t@localhost:5499/postgres"


def _admin_url() -> str:
    return os.environ.get("RS_TEST_DB_URL", DEFAULT_URL)


def _reachable(url: str) -> bool:
    try:
        engine = create_sync_engine(url, connect_args={"connect_timeout": 2})
        with engine.connect():
            return True
    except Exception:
        return False
    finally:
        try:
            engine.dispose()
        except UnboundLocalError:
            pass


@pytest.fixture(scope="session")
def migrated_db_url():
    admin_url = _admin_url()
    if not _reachable(admin_url):
        pytest.skip(f"no test database reachable at {admin_url} (set RS_TEST_DB_URL or start the container)")

    db_name = f"rs_test_{uuid.uuid4().hex[:12]}"
    admin_engine = create_sync_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{db_name}"'))

    test_url = admin_url.rsplit("/", 1)[0] + f"/{db_name}"
    config = Config(str(alembic_config_path()))
    os.environ["RS_DB_URL"] = test_url
    try:
        command.upgrade(config, "head")
        yield test_url
    finally:
        os.environ.pop("RS_DB_URL", None)
        with admin_engine.connect() as conn:
            conn.execute(text(f'DROP DATABASE "{db_name}" WITH (FORCE)'))
        admin_engine.dispose()

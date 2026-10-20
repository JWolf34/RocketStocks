"""DB-gated fixtures - shared via rs_persistence.testing (the 5499 pattern;
see that module for target resolution and gating semantics)."""

import uuid
from collections.abc import Iterator

import psycopg
import pytest
from sqlalchemy.engine import make_url

from rs_migrate.roles import SERVICES, apply_roles, role_name
from rs_persistence.testing import migrated_db_url, session_factory  # noqa: F401


@pytest.fixture
def research_url(migrated_db_url: str) -> Iterator[str]:  # noqa: F811
    """DSN of the real `rs_research` role, granted by rs_migrate. Roles are
    cluster-global and the test cluster is shared, so the names are throwaway."""
    prefix = f"rs_t{uuid.uuid4().hex[:8]}_"
    passwords = {service: f"pw-{service}-{uuid.uuid4().hex}" for service in SERVICES}
    apply_roles(migrated_db_url, passwords, prefix=prefix)
    yield (
        make_url(migrated_db_url)
        .set(username=role_name("research", prefix), password=passwords["research"])
        .render_as_string(False)
    )
    conninfo = make_url(migrated_db_url).set(drivername="postgresql").render_as_string(False)
    with psycopg.connect(conninfo, autocommit=True) as conn:
        for service in SERVICES:
            conn.execute(f'DROP OWNED BY "{role_name(service, prefix)}"')
            conn.execute(f'DROP ROLE "{role_name(service, prefix)}"')

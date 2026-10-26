"""DB-gated fixtures - shared via rs_persistence.testing (the 5499 pattern;
see that module for target resolution and gating semantics)."""

from collections.abc import Iterator

import pytest

from rs_observability.testing import captured_ops_pings
from rs_persistence.testing import migrated_db_url, session_factory  # noqa: F401


@pytest.fixture
def notices() -> Iterator[list[str]]:
    """Ops pings the code under test sent (de-dup off so every send shows)."""
    with captured_ops_pings() as sent:
        yield sent

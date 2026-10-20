import logging

import pytest
import structlog


@pytest.fixture(autouse=True)
def _restore_logging():
    """main() calls configure_logging, which installs a root handler bound to the
    test's captured stdout; left in place it breaks the next test's logging."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)
    structlog.reset_defaults()

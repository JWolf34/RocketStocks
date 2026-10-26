"""DB-gated fixtures - shared via rs_persistence.testing (the 5499 pattern;
see that module for target resolution and gating semantics)."""

from rs_persistence.testing import migrated_db_url, session_factory  # noqa: F401

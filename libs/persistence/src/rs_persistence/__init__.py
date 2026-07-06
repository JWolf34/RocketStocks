"""RocketStocks v2 persistence: repositories and TimescaleDB access."""

from pathlib import Path

from rs_persistence.engine import create_engine, create_session_factory
from rs_persistence.models import AlertRow, Base, OutboxRow, StreamBar5m
from rs_persistence.repository import BarRepository


def alembic_config_path() -> Path:
    """Path to this member's alembic.ini (used by rs_migrate and tests)."""
    return Path(__file__).resolve().parents[2] / "alembic.ini"


__all__ = [
    "AlertRow",
    "Base",
    "BarRepository",
    "OutboxRow",
    "StreamBar5m",
    "alembic_config_path",
    "create_engine",
    "create_session_factory",
]

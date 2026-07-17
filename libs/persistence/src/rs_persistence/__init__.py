"""RocketStocks v2 persistence: repositories and TimescaleDB access."""

from pathlib import Path

from alembic.config import Config

from rs_persistence.engine import build_engine, build_session_factory
from rs_persistence.models import (
    AlertRow,
    Base,
    OutboxRow,
    SchwabTokenRow,
    ServiceClientRow,
    StreamBar5m,
    chain_snapshot_table,
)
from rs_persistence.repository import (
    NOTIFY_CHANNEL,
    OUTBOX_EVENT_TYPE,
    AlertRepository,
    BarRepository,
    ChainSnapshotRepository,
    PendingAlert,
    ServiceClientRepository,
)
from rs_persistence.token_store import SchwabTokenStore


def migration_config(db_url: str) -> Config:
    """Alembic config targeting the in-package migrations.

    Built programmatically (no alembic.ini) so it works identically for
    editable installs, wheels, and Docker images — the migrations ship
    inside the package.
    """
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).resolve().parent / "migrations"))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


__all__ = [
    "NOTIFY_CHANNEL",
    "OUTBOX_EVENT_TYPE",
    "AlertRepository",
    "AlertRow",
    "Base",
    "BarRepository",
    "ChainSnapshotRepository",
    "OutboxRow",
    "PendingAlert",
    "SchwabTokenRow",
    "SchwabTokenStore",
    "ServiceClientRepository",
    "ServiceClientRow",
    "StreamBar5m",
    "build_engine",
    "build_session_factory",
    "chain_snapshot_table",
    "migration_config",
]

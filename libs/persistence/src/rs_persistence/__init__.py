"""RocketStocks v2 persistence: repositories and TimescaleDB access."""

from pathlib import Path

from alembic.config import Config

from rs_persistence.alerts import NOTIFY_CHANNEL, AlertRepository, PendingAlert
from rs_persistence.auth_requests import (
    AUTH_NOTIFY_CHANNEL,
    AUTH_REQUEST_WINDOW,
    AuthRequestRepository,
)
from rs_persistence.chains import ChainSnapshotRepository
from rs_persistence.daily_bars import DailyBarRepository
from rs_persistence.engine import build_engine, build_session_factory, psycopg_conninfo
from rs_persistence.import_verification import (
    ImportVerification,
    ImportVerificationRepository,
)
from rs_persistence.models import (
    AlertRow,
    Base,
    DailyBarRow,
    ImportVerificationRow,
    PopularitySnapshotRow,
    SchwabAuthRequestRow,
    SchwabTokenNoticeRow,
    SchwabTokenRow,
    TrackRecordRow,
    UnpricedSymbolRow,
    chain_snapshot_table,
)
from rs_persistence.popularity import PopularityRepository
from rs_persistence.token_notices import TokenNoticeRepository
from rs_persistence.token_store import SchwabTokenStore
from rs_persistence.track_records import TrackRecordRepository


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
    "AUTH_NOTIFY_CHANNEL",
    "AUTH_REQUEST_WINDOW",
    "NOTIFY_CHANNEL",
    "AlertRepository",
    "AlertRow",
    "AuthRequestRepository",
    "Base",
    "TrackRecordRepository",
    "TrackRecordRow",
    "ChainSnapshotRepository",
    "DailyBarRepository",
    "DailyBarRow",
    "ImportVerification",
    "ImportVerificationRepository",
    "ImportVerificationRow",
    "PendingAlert",
    "PopularityRepository",
    "PopularitySnapshotRow",
    "SchwabAuthRequestRow",
    "SchwabTokenNoticeRow",
    "SchwabTokenRow",
    "SchwabTokenStore",
    "TokenNoticeRepository",
    "UnpricedSymbolRow",
    "build_engine",
    "build_session_factory",
    "psycopg_conninfo",
    "chain_snapshot_table",
    "migration_config",
]

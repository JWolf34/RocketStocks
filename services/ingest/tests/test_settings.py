import pytest
from pydantic import ValidationError

from rs_ingest.settings import IngestSettings

REQUIRED = {
    "RS_DB_URL": "postgresql+psycopg://u:p@localhost:5498/rs",
    "SCHWAB_API_KEY": "key",
    "SCHWAB_API_SECRET": "hunter2",
}


def _set_required(monkeypatch):
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)


def test_defaults(monkeypatch):
    _set_required(monkeypatch)
    settings = IngestSettings(_env_file=None)
    assert settings.symbol == "SPY"
    assert settings.volume_threshold == 500_000
    assert settings.notify_channel == "alerts"
    assert settings.watchdog_grace_seconds == 30.0
    assert settings.rs_db_url == REQUIRED["RS_DB_URL"]


def test_secret_not_leaked_in_repr(monkeypatch):
    _set_required(monkeypatch)
    settings = IngestSettings(_env_file=None)
    assert "hunter2" not in repr(settings)
    assert settings.schwab_api_secret.get_secret_value() == "hunter2"


def test_env_overrides(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("RS_INGEST_SYMBOL", "QQQ")
    monkeypatch.setenv("RS_INGEST_VOLUME_THRESHOLD", "250000")
    settings = IngestSettings(_env_file=None)
    assert settings.symbol == "QQQ"
    assert settings.volume_threshold == 250_000


def test_missing_required_fails(monkeypatch):
    for name in REQUIRED:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValidationError):
        IngestSettings(_env_file=None)

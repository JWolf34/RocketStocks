from pathlib import Path

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
    assert settings.watchdog_grace_seconds == 30.0
    assert settings.fixture_path is None
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


def test_fixture_path_resolves_from_env(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("RS_INGEST_FIXTURE_PATH", "data/spike-recordings/session.jsonl")
    settings = IngestSettings(_env_file=None)
    assert settings.fixture_path == Path("data/spike-recordings/session.jsonl")


def test_missing_required_fails(monkeypatch):
    for name in REQUIRED:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValidationError):
        IngestSettings(_env_file=None)


def test_metrics_port_default_and_override(monkeypatch):
    _set_required(monkeypatch)
    assert IngestSettings(_env_file=None).metrics_port == 9090
    monkeypatch.setenv("RS_INGEST_METRICS_PORT", "9190")
    assert IngestSettings(_env_file=None).metrics_port == 9190

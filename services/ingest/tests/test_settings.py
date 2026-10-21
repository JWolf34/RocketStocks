import pytest
from pydantic import ValidationError

from rs_ingest.settings import IngestSettings

REQUIRED = {
    "RS_DB_URL": "postgresql+psycopg://u:p@localhost:5498/rs",
    "SCHWAB_API_KEY": "key",
    "SCHWAB_API_SECRET": "hunter2",
    "RS_EDGAR_USER_AGENT": "RocketStocks test@example.com",
}


def _set_required(monkeypatch):
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)


def test_defaults(monkeypatch):
    _set_required(monkeypatch)
    settings = IngestSettings(_env_file=None)
    assert settings.rs_db_url == REQUIRED["RS_DB_URL"]


def test_supervision_defaults_and_env_overrides(monkeypatch):
    _set_required(monkeypatch)
    settings = IngestSettings(_env_file=None)
    assert settings.token_poll_seconds == 60.0
    assert settings.schwab_calls_per_minute == 100
    assert settings.shutdown_grace_seconds == 30.0

    monkeypatch.setenv("RS_INGEST_SHUTDOWN_GRACE_SECONDS", "10")
    settings = IngestSettings(_env_file=None)
    assert settings.shutdown_grace_seconds == 10.0


def test_secret_not_leaked_in_repr(monkeypatch):
    _set_required(monkeypatch)
    settings = IngestSettings(_env_file=None)
    assert "hunter2" not in repr(settings)
    assert settings.schwab_api_secret.get_secret_value() == "hunter2"


def test_missing_required_fails(monkeypatch):
    for name in REQUIRED:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValidationError):
        IngestSettings(_env_file=None)


def test_edgar_identity_is_required(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.delenv("RS_EDGAR_USER_AGENT")
    with pytest.raises(ValidationError, match="rs_edgar_user_agent"):
        IngestSettings(_env_file=None)


def test_edgar_identity_cannot_be_blank(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("RS_EDGAR_USER_AGENT", "")
    with pytest.raises(ValidationError, match="rs_edgar_user_agent"):
        IngestSettings(_env_file=None)


def test_edgar_identity_is_read_from_env(monkeypatch):
    _set_required(monkeypatch)
    assert IngestSettings(_env_file=None).rs_edgar_user_agent == REQUIRED["RS_EDGAR_USER_AGENT"]


def test_metrics_port_default_and_override(monkeypatch):
    _set_required(monkeypatch)
    assert IngestSettings(_env_file=None).metrics_port == 9090
    monkeypatch.setenv("RS_INGEST_METRICS_PORT", "9190")
    assert IngestSettings(_env_file=None).metrics_port == 9190

import pytest
from pydantic import ValidationError

from rs_bot.settings import BotSettings

REQUIRED = {
    "RS_DB_URL": "postgresql+psycopg://u:p@localhost:5498/rs",
    "RS_BOT_DISCORD_TOKEN": "hunter2",
    "RS_BOT_ALERT_CHANNEL_ID": "123456789012345678",
}


def _set_required(monkeypatch):
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)


def test_defaults(monkeypatch):
    _set_required(monkeypatch)
    settings = BotSettings(_env_file=None)
    assert settings.rs_db_url == REQUIRED["RS_DB_URL"]
    assert settings.alert_channel_id == 123456789012345678
    assert settings.sweep_seconds == 60.0


def test_token_not_leaked_in_repr(monkeypatch):
    _set_required(monkeypatch)
    settings = BotSettings(_env_file=None)
    assert "hunter2" not in repr(settings)
    assert settings.discord_token.get_secret_value() == "hunter2"


def test_v1_discord_token_is_never_read(monkeypatch):
    # Burn-in coexistence guard (spec pin #7): v2 must not fall back to v1's
    # DISCORD_TOKEN — only RS_BOT_DISCORD_TOKEN may supply the credential.
    _set_required(monkeypatch)
    monkeypatch.delenv("RS_BOT_DISCORD_TOKEN")
    monkeypatch.setenv("DISCORD_TOKEN", "v1-secret")
    with pytest.raises(ValidationError):
        BotSettings(_env_file=None)


def test_sweep_seconds_override(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.setenv("RS_BOT_SWEEP_SECONDS", "5")
    settings = BotSettings(_env_file=None)
    assert settings.sweep_seconds == 5.0


def test_missing_required_fails(monkeypatch):
    for name in REQUIRED:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValidationError):
        BotSettings(_env_file=None)


def test_metrics_port_default_and_override(monkeypatch):
    _set_required(monkeypatch)
    assert BotSettings(_env_file=None).metrics_port == 9091
    monkeypatch.setenv("RS_BOT_METRICS_PORT", "9191")
    assert BotSettings(_env_file=None).metrics_port == 9191

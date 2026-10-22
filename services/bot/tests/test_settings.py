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


def test_unprefixed_discord_token_is_never_read(monkeypatch):
    # A stray DISCORD_TOKEN export must not supply the credential:
    # only RS_BOT_DISCORD_TOKEN may.
    _set_required(monkeypatch)
    monkeypatch.delenv("RS_BOT_DISCORD_TOKEN")
    monkeypatch.setenv("DISCORD_TOKEN", "stray-secret")
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


def test_post_max_attempts_default_and_override(monkeypatch):
    _set_required(monkeypatch)
    assert BotSettings(_env_file=None).post_max_attempts == 5
    monkeypatch.setenv("RS_BOT_POST_MAX_ATTEMPTS", "2")
    assert BotSettings(_env_file=None).post_max_attempts == 2


def test_old_dev_channel_variable_is_not_read(monkeypatch):
    _set_required(monkeypatch)
    monkeypatch.delenv("RS_BOT_ALERT_CHANNEL_ID")
    monkeypatch.setenv("RS_DEV_CHANNEL_ID", "1")
    with pytest.raises(ValidationError):
        BotSettings(_env_file=None)


def test_admin_user_id_is_optional_and_blank_means_unset(monkeypatch):
    _set_required(monkeypatch)
    assert BotSettings(_env_file=None).admin_user_id is None
    monkeypatch.setenv("RS_BOT_ADMIN_USER_ID", "")
    assert BotSettings(_env_file=None).admin_user_id is None
    monkeypatch.setenv("RS_BOT_ADMIN_USER_ID", "123456789012345678")
    assert BotSettings(_env_file=None).admin_user_id == 123456789012345678

"""Bot configuration via pydantic-settings (ingest pattern).

RS_DB_URL is the same variable ingest and rs_migrate consume. The Discord
credential is RS_BOT_DISCORD_TOKEN — deliberately not a bare DISCORD_TOKEN, so
a stray shell export can never supply it.
Bot tunables carry an RS_BOT_ prefix so a stray shell export can
never silently reconfigure the daemon.

The .env file is resolved from the working directory — run from the repo root
in dev; a missing required variable is a loud exit-1, never a silent default.
"""

from typing import Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class BotSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    rs_db_url: str
    discord_token: SecretStr = Field(validation_alias="RS_BOT_DISCORD_TOKEN")
    alert_channel_id: int = Field(validation_alias="RS_BOT_ALERT_CHANNEL_ID")
    # Who may run /setup schwab (besides the guild owner) and gets token-expiry DMs;
    # unset, only the guild owner may and the DMs go to the application's owner.
    admin_user_id: int | None = Field(None, validation_alias="RS_BOT_ADMIN_USER_ID")
    # Bounds only the lost-NOTIFY tail - 20% of one bar.
    sweep_seconds: float = Field(60.0, validation_alias="RS_BOT_SWEEP_SECONDS")
    # Failed sends allowed per alert before it is abandoned and ops is told.
    post_max_attempts: int = Field(5, ge=1, validation_alias="RS_BOT_POST_MAX_ATTEMPTS")
    # Prometheus side-port: compose publishes it 127.0.0.1-only.
    metrics_port: int = Field(9091, validation_alias="RS_BOT_METRICS_PORT")
    # The alert NOTIFY channel is intentionally NOT here: it is a fixed wire
    # contract shared with ingest, not a per-deployment tunable —
    # rs_persistence.NOTIFY_CHANNEL is its single source of truth.

    @field_validator("admin_user_id", mode="before")
    @classmethod
    def _blank_is_unset(cls, value: Any) -> Any:
        # Compose passes an unset variable through as "".
        return None if value == "" else value

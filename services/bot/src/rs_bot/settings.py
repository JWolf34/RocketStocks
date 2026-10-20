"""Bot configuration via pydantic-settings (ingest pattern).

RS_DB_URL is the same variable ingest and rs_migrate consume. The Discord
credential is RS_BOT_DISCORD_TOKEN — deliberately not v1's DISCORD_TOKEN, so
v2 can never read v1's credential during burn-in coexistence (spec pin #7).
Bot tunables carry an RS_BOT_ prefix so a stray shell export can
never silently reconfigure the daemon.

The .env file is resolved from the working directory — run from the repo root
in dev; a missing required variable is a loud exit-1, never a silent default.
"""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class BotSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    rs_db_url: str
    discord_token: SecretStr = Field(validation_alias="RS_BOT_DISCORD_TOKEN")
    alert_channel_id: int = Field(validation_alias="RS_BOT_ALERT_CHANNEL_ID")
    # Bounds only the lost-NOTIFY tail — 20% of one bar (spec pin #6).
    sweep_seconds: float = Field(60.0, validation_alias="RS_BOT_SWEEP_SECONDS")
    # Failed sends allowed per alert before it is abandoned and ops is told.
    post_max_attempts: int = Field(5, ge=1, validation_alias="RS_BOT_POST_MAX_ATTEMPTS")
    # Prometheus side-port (ADR-0015): compose publishes it 127.0.0.1-only.
    metrics_port: int = Field(9091, validation_alias="RS_BOT_METRICS_PORT")
    # The Bus-2 NOTIFY channel is intentionally NOT here: it is a pinned wire
    # contract shared with ingest (ADR-0005), not a per-deployment tunable —
    # rs_persistence.NOTIFY_CHANNEL is its single source of truth.

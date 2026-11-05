"""Ingest configuration via pydantic-settings.

RS_DB_URL is the same variable rs_migrate consumes; SCHWAB_API_KEY /
SCHWAB_API_SECRET are the credentials the Schwab login tools read. Ingest's
own tunables carry an RS_INGEST_ prefix so a stray shell export (SYMBOL, ...)
can never silently reconfigure the daemon.

The .env file is resolved from the working directory - run from the repo root
in dev; a missing required variable is a loud
exit-1, never a silent default.
"""

from datetime import time

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

LOGIN_HINT = "run /setup schwab in Discord, or: python -m rs_ingest.tools.login"


class IngestSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    rs_db_url: str
    schwab_api_key: str
    schwab_api_secret: SecretStr
    schwab_callback_url: str = "https://127.0.0.1:8182"
    # EDGAR requires a declared identity (e.g. "RocketStocks you@example.com").
    rs_edgar_user_agent: str = Field(min_length=1)

    # Prometheus side-port: compose publishes it 127.0.0.1-only.
    metrics_port: int = Field(9090, validation_alias="RS_INGEST_METRICS_PORT")
    # Daemon supervision: how often a dead Schwab token is checked for renewal,
    # and the bound on in-flight jobs at SIGTERM (compose stop_grace_period must exceed it).
    token_poll_seconds: float = Field(60.0, gt=0, validation_alias="RS_INGEST_TOKEN_POLL_SECONDS")
    shutdown_grace_seconds: float = Field(
        30.0, ge=0, validation_alias="RS_INGEST_SHUTDOWN_GRACE_SECONDS"
    )
    # Fallback poll for `/setup schwab` requests; NOTIFY normally wakes the handler at once.
    auth_poll_seconds: float = Field(15.0, gt=0, validation_alias="RS_INGEST_AUTH_POLL_SECONDS")
    # Shared by every Schwab job through one limiter; the default sits under Schwab's per-minute cap.
    schwab_calls_per_minute: int = Field(100, ge=1, validation_alias="RS_INGEST_SCHWAB_CALLS_PER_MINUTE")
    # Central clock time the nightly daily bars pass stops at, so it never runs into the morning pass.
    daily_bars_stop_at: time = Field(time(3, 0), validation_alias="RS_INGEST_DAILY_BARS_STOP_AT")
    # The alert NOTIFY channel is intentionally NOT here: it is a fixed wire
    # contract shared with the bot, not a per-deployment tunable -
    # rs_persistence.NOTIFY_CHANNEL is its single source of truth.

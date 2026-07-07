"""Ingest configuration via pydantic-settings.

RS_DB_URL is the same variable rs_migrate consumes; SCHWAB_API_KEY /
SCHWAB_API_SECRET are the same credentials the Phase-0 spikes read. Ingest's
own tunables carry an RS_INGEST_ prefix so a stray shell export (SYMBOL, ...)
can never silently reconfigure the daemon.

The .env file is resolved from the working directory — run from the repo root
in dev (the acceptance runbook does); a missing required variable is a loud
exit-1, never a silent default.
"""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    rs_db_url: str
    schwab_api_key: str
    schwab_api_secret: SecretStr
    schwab_callback_url: str = "https://127.0.0.1:8182"

    symbol: str = Field("SPY", validation_alias="RS_INGEST_SYMBOL")
    volume_threshold: int = Field(500_000, validation_alias="RS_INGEST_VOLUME_THRESHOLD")
    notify_channel: str = Field("alerts", validation_alias="RS_INGEST_NOTIFY_CHANNEL")
    watchdog_grace_seconds: float = Field(30.0, validation_alias="RS_INGEST_WATCHDOG_GRACE_SECONDS")

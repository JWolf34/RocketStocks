"""Ingest configuration via pydantic-settings.

RS_DB_URL is the same variable rs_migrate consumes; SCHWAB_API_KEY /
SCHWAB_API_SECRET are the same credentials the Phase-0 spikes read. Ingest's
own tunables carry an RS_INGEST_ prefix so a stray shell export (SYMBOL, ...)
can never silently reconfigure the daemon.

The .env file is resolved from the working directory — run from the repo root
in dev (the acceptance runbook does); a missing required variable is a loud
exit-1, never a silent default.
"""

from pathlib import Path
from typing import Annotated, Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

LOGIN_HINT = "run: python -m rs_ingest.tools.login"

# The spike's 10 tickers (S008 bootstrap universe; static until S010's
# universe management replaces it).
BOOTSTRAP_UNIVERSE = ["AAPL", "AMD", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "QQQ", "SPY", "TSLA"]


class IngestSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    rs_db_url: str
    schwab_api_key: str
    schwab_api_secret: SecretStr
    schwab_callback_url: str = "https://127.0.0.1:8182"

    # The stream and chain-scan settings below are unused by the daemon until
    # slice 4 reshapes those modules.
    symbol: str = Field("SPY", validation_alias="RS_INGEST_SYMBOL")
    volume_threshold: int = Field(500_000, validation_alias="RS_INGEST_VOLUME_THRESHOLD")
    watchdog_grace_seconds: float = Field(30.0, validation_alias="RS_INGEST_WATCHDOG_GRACE_SECONDS")
    # Prometheus side-port (ADR-0015): compose publishes it 127.0.0.1-only.
    metrics_port: int = Field(9090, validation_alias="RS_INGEST_METRICS_PORT")
    # When set, the session's BarClosed events are recorded to this JSONL fixture
    # (a source tee — orthogonal to persistence). Off by default; the B8 runbook
    # points it at a gitignored data/ path, then a curated excerpt is committed.
    fixture_path: Path | None = Field(None, validation_alias="RS_INGEST_FIXTURE_PATH")
    # Daemon supervision: consecutive failures before one ops notice, how often
    # a dead Schwab token is checked for renewal, and the bound on in-flight
    # jobs at SIGTERM (compose stop_grace_period must exceed it).
    job_failure_threshold: int = Field(3, ge=1, validation_alias="RS_INGEST_JOB_FAILURE_THRESHOLD")
    token_poll_seconds: float = Field(60.0, gt=0, validation_alias="RS_INGEST_TOKEN_POLL_SECONDS")
    shutdown_grace_seconds: float = Field(
        30.0, ge=0, validation_alias="RS_INGEST_SHUTDOWN_GRACE_SECONDS"
    )
    # Shared by every Schwab job through one limiter; the default sits under Schwab's per-minute cap.
    schwab_calls_per_minute: int = Field(100, ge=1, validation_alias="RS_INGEST_SCHWAB_CALLS_PER_MINUTE")
    # The Bus-2 NOTIFY channel is intentionally NOT here: it is a pinned wire
    # contract shared with the bot (ADR-0005), not a per-deployment tunable —
    # rs_persistence.NOTIFY_CHANNEL is its single source of truth.

    # Curated 5-min chain scan (S008). Universe is comma-separated in the env
    # (NoDecode skips pydantic-settings' JSON parsing); empty -> scan disabled.
    chain_scan_universe: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: list(BOOTSTRAP_UNIVERSE),
        validation_alias="RS_INGEST_CHAIN_SCAN_UNIVERSE",
    )
    chain_scan_window: float = Field(0.10, validation_alias="RS_INGEST_CHAIN_SCAN_WINDOW")
    chain_scan_row_cap: int = Field(4000, validation_alias="RS_INGEST_CHAIN_SCAN_ROW_CAP")
    chain_scan_request_timeout_seconds: float = Field(
        30.0, validation_alias="RS_INGEST_CHAIN_SCAN_REQUEST_TIMEOUT_SECONDS"
    )

    @field_validator("chain_scan_universe", mode="before")
    @classmethod
    def _split_universe(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [ticker.strip().upper() for ticker in value.split(",") if ticker.strip()]
        return value

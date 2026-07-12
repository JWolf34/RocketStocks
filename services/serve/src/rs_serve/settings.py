"""Serve configuration via pydantic-settings.

RS_DB_URL is the same variable rs_migrate and rs_ingest consume; serve's own
tunables carry an RS_SERVE_ prefix (same convention as RS_INGEST_ — a stray
shell export can never silently reconfigure the daemon). The .env file is
resolved from the working directory — run from the repo root in dev.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    rs_db_url: str

    # 127.0.0.1 by default: the TLS-terminating reverse proxy is the public
    # face (ADR-0012); binding wider is a deliberate deployment decision (S006).
    host: str = Field("127.0.0.1", validation_alias="RS_SERVE_HOST")
    port: int = Field(8000, validation_alias="RS_SERVE_PORT")

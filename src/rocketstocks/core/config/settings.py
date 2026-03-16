import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.getenv("ENV_FILE", ".env"),
        env_file_encoding="utf-8",
    )

    # Required — app fails at startup if any of these are missing
    discord_token: str
    schwab_api_key: str
    schwab_api_secret: str
    tiingo_api_key: str
    news_api_key: str
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str
    postgres_port: int  # auto-parsed from string

    # Optional — not needed for core functionality
    nasdaq_api_key: str | None = None
    eodhd_api_token: str | None = None
    dolthub_api_token: str | None = None

    # Config (non-secret)
    notification_filter: str = "all"
    tz: str = "America/Chicago"


settings = Settings()

"""Shared plumbing for the Phase-0 Schwab spikes (throwaway code).

Credentials come from the repo-root .env (SCHWAB_API_KEY / SCHWAB_API_SECRET,
never committed); the OAuth token lives at data/schwab-spike-token.json
(gitignored). This file-based token is spike-only and does not prejudge
S003's v2 token-storage decision.
"""

import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
TOKEN_PATH = REPO_ROOT / "data" / "schwab-spike-token.json"
CALLBACK_URL = "https://127.0.0.1:8182"
# Committed findings docs only — raw recordings never go here (they stay in data/).
FIXTURES_DIR = REPO_ROOT / "docs" / "v2" / "specs" / "fixtures"
SPIKE_DATA_DIR = REPO_ROOT / "data" / "spike-recordings"


def load_credentials() -> tuple[str, str]:
    load_dotenv(REPO_ROOT / ".env")
    api_key = os.environ.get("SCHWAB_API_KEY")
    app_secret = os.environ.get("SCHWAB_API_SECRET") or os.environ.get("SCHWAB_SECRET")
    if not api_key or not app_secret:
        sys.exit("SCHWAB_API_KEY / SCHWAB_API_SECRET missing from .env — see scripts/spikes/README.md")
    return api_key, app_secret


def build_client(asyncio: bool = False):
    """schwab-py client from the spike token (browser login on first run)."""
    from schwab import auth

    api_key, app_secret = load_credentials()
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    return auth.easy_client(
        api_key=api_key,
        app_secret=app_secret,
        callback_url=CALLBACK_URL,
        token_path=str(TOKEN_PATH),
        asyncio=asyncio,
    )


def todays_session_bounds() -> tuple[datetime, datetime]:
    """(open, close) UTC instants for today's XNYS session; exits if closed."""
    from rs_sources import PandasMarketCalendar

    cal = PandasMarketCalendar()
    today = datetime.now(timezone.utc).date()
    if not cal.is_session(today):
        sys.exit(f"{today} is not a trading session — nothing to record")
    return cal.session_open(today), cal.session_close(today)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def today_tag(day: date | None = None) -> str:
    return (day or utc_now().date()).isoformat()

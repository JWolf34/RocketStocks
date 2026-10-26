#!/usr/bin/env python
"""Headless Schwab OAuth login for boxes with no browser (WSL, servers).

schwab-py's default `client_from_login_flow` calls `webbrowser.get()` and dies
where there is no runnable browser ("could not locate runnable browser"). This
uses schwab-py's MANUAL flow instead:

  1. it prints an authorization URL,
  2. you open that URL in ANY browser - your Windows host, a phone - log in,
  3. Schwab redirects to the callback (https://127.0.0.1:8182/...). The page
     won't load (nothing is listening), but the browser's ADDRESS BAR now holds
     the full redirect URL with ?code=... - copy that entire URL,
  4. paste it back at the terminal prompt.

The token is written straight into the DB row (`schwab_tokens`, id=1) - the
exact sink `python -m rs_ingest.tools.login` uses, so `ingest` picks it up.

    cd <repo root>            # IngestSettings loads .env from the cwd
    .venv/bin/python scripts/acceptance/schwab_manual_login.py

Credentials + DB URL come from .env (SCHWAB_API_KEY / SCHWAB_API_SECRET /
RS_DB_URL). Run it from the repo root with the compose DB up on :5498.
"""

import asyncio
import json
import tempfile
from pathlib import Path

from schwab.auth import client_from_manual_flow

from rs_ingest.settings import IngestSettings
from rs_persistence import SchwabTokenStore, build_engine, build_session_factory


async def _store(settings: IngestSettings, token: dict) -> None:
    engine = build_engine(settings.rs_db_url)
    try:
        await SchwabTokenStore(build_session_factory(engine)).save(token)
    finally:
        await engine.dispose()


def main() -> int:
    settings = IngestSettings()
    with tempfile.TemporaryDirectory() as tmp:
        token_path = Path(tmp) / "token.json"
        # Interactive: prints the auth URL, then reads the pasted redirect URL
        # from the terminal. No local callback server, no browser launch.
        client_from_manual_flow(
            settings.schwab_api_key,
            settings.schwab_api_secret.get_secret_value(),
            settings.schwab_callback_url,
            str(token_path),
        )
        token = json.loads(token_path.read_text())

    if "token" not in token:
        print("manual flow produced no schwab-py token wrapper - aborting")
        return 1
    asyncio.run(_store(settings, token))
    print("✅ token stored in schwab_tokens (id=1) - ingest can authenticate now")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

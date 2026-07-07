"""Token bootstrap: write the Schwab OAuth token into the DB row (rev-002).

    python -m rs_ingest.tools.login --from-file data/schwab-spike-token.json
    python -m rs_ingest.tools.login          # browser OAuth flow (fallback,
                                             # and the weekly re-auth path
                                             # until ADR-0016 lands)

The browser flow runs schwab-py's login against a temp file, then imports it —
same code path as --from-file, and we stay off schwab-py's token internals.
"""

import argparse
import asyncio
import json
import logging
import sys
import tempfile
from pathlib import Path

from rs_ingest.settings import IngestSettings
from rs_persistence import SchwabTokenStore, build_engine, build_session_factory

logger = logging.getLogger(__name__)


async def _store_token(settings: IngestSettings, token: dict) -> None:
    engine = build_engine(settings.rs_db_url)
    try:
        store = SchwabTokenStore(build_session_factory(engine))
        await store.save(token)
    finally:
        await engine.dispose()


def _token_from_browser_flow(settings: IngestSettings) -> dict:
    from schwab.auth import client_from_login_flow

    with tempfile.TemporaryDirectory() as tmp:
        token_path = Path(tmp) / "token.json"
        client_from_login_flow(
            settings.schwab_api_key,
            settings.schwab_api_secret.get_secret_value(),
            settings.schwab_callback_url,
            str(token_path),
        )
        return json.loads(token_path.read_text())


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-file", type=Path, default=None, help="import an existing schwab-py token file")
    args = parser.parse_args()

    settings = IngestSettings()
    if args.from_file is not None:
        token = json.loads(args.from_file.read_text())
        source = str(args.from_file)
    else:
        token = _token_from_browser_flow(settings)
        source = "browser login flow"

    if "token" not in token:
        print(f"{source}: not a schwab-py token file (missing 'token' wrapper)", file=sys.stderr)
        return 1
    asyncio.run(_store_token(settings, token))
    print(f"token from {source} stored in schwab_tokens (id=1)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""API-key provisioning — the permanent operator mechanism (ADR-0012, S005).

    python -m rs_serve.tools.provision --name cowork    # mint + print a key
    python -m rs_serve.tools.provision --revoke cowork  # revoked_at = now()

The key is printed ONCE and never stored — only its SHA-256 hash lands in
service_clients (rs_serve.auth.hash_api_key is the single hash convention).
Retired only by ADR-0012's OAuth trigger conditions; Phase 3 adds
--scopes/--rps/--burst additively.
"""

import argparse
import asyncio
import secrets
import sys

from sqlalchemy.exc import IntegrityError

from rs_persistence import ServiceClientRepository, build_engine, build_session_factory
from rs_serve.auth import hash_api_key
from rs_serve.settings import ServeSettings


async def _provision(db_url: str, name: str) -> str:
    key = secrets.token_urlsafe(32)
    engine = build_engine(db_url)
    try:
        async with build_session_factory(engine)() as session:
            await ServiceClientRepository(session).provision(name=name, api_key_hash=hash_api_key(key))
            await session.commit()
    finally:
        await engine.dispose()
    return key


async def _revoke(db_url: str, name: str) -> bool:
    engine = build_engine(db_url)
    try:
        async with build_session_factory(engine)() as session:
            revoked = await ServiceClientRepository(session).revoke(name)
            await session.commit()
    finally:
        await engine.dispose()
    return revoked


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--name", help="consumer name to provision a new key for")
    group.add_argument("--revoke", metavar="NAME", help="revoke the named consumer's key")
    args = parser.parse_args(argv)

    settings = ServeSettings()
    if args.revoke is not None:
        if not asyncio.run(_revoke(settings.rs_db_url, args.revoke)):
            print(f"no active client named '{args.revoke}'", file=sys.stderr)
            return 1
        print(f"'{args.revoke}' revoked; its key stops working on the next request")
        return 0

    try:
        key = asyncio.run(_provision(settings.rs_db_url, args.name))
    except IntegrityError:
        print(f"client '{args.name}' already exists (revoke it first to rotate)", file=sys.stderr)
        return 1
    print(f"API key for '{args.name}' — shown once, only the hash is stored:")
    print(key)
    return 0


if __name__ == "__main__":
    sys.exit(main())

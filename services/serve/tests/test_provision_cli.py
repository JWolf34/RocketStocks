"""Provisioning CLI round-trip (S005): provision → the printed key
authenticates over HTTP → revoke → 401. The CLI is the operator seam — tests
drive main(argv) exactly as `python -m rs_serve.tools.provision` would.

main() owns its own event loop (asyncio.run), so async tests call it via
asyncio.to_thread — same as a shell would: a fresh loop per invocation."""

import asyncio
import re

from rs_serve.tools.provision import main


def _printed_key(capsys) -> str:
    # The key is printed once, alone on its stdout line.
    out = capsys.readouterr().out
    match = re.search(r"^([A-Za-z0-9_-]{20,})$", out, flags=re.MULTILINE)
    assert match, f"no key found in CLI output:\n{out}"
    return match.group(1)


async def test_provision_revoke_round_trip(client, capsys, monkeypatch, migrated_db_url, bars_url):
    monkeypatch.setenv("RS_DB_URL", migrated_db_url)

    assert await asyncio.to_thread(main, ["--name", "dev"]) == 0
    key = _printed_key(capsys)

    ok = await client.get(bars_url, headers={"X-API-Key": key})
    assert ok.status_code == 200

    assert await asyncio.to_thread(main, ["--revoke", "dev"]) == 0
    gone = await client.get(bars_url, headers={"X-API-Key": key})
    assert gone.status_code == 401


async def test_duplicate_name_fails_loudly(client, capsys, monkeypatch, migrated_db_url):
    monkeypatch.setenv("RS_DB_URL", migrated_db_url)
    assert await asyncio.to_thread(main, ["--name", "dev"]) == 0
    capsys.readouterr()
    # 'test' is provisioned by the conftest; 'dev' now exists too — repeat must
    # not silently mint a second key row.
    assert await asyncio.to_thread(main, ["--name", "dev"]) == 1


async def test_revoke_unknown_name_fails(client, monkeypatch, migrated_db_url):
    monkeypatch.setenv("RS_DB_URL", migrated_db_url)
    assert await asyncio.to_thread(main, ["--revoke", "ghost"]) == 1

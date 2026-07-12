"""Auth seam (S005/ADR-0012): X-API-Key → SHA-256 → service_clients lookup;
absent, unknown, and revoked keys are indistinguishable (uniform 401)."""

from rs_persistence import ServiceClientRepository


async def test_valid_key_passes(client, bars_url, auth_headers):
    response = await client.get(bars_url, headers=auth_headers)
    assert response.status_code == 200


async def test_absent_key_is_401(client, bars_url):
    response = await client.get(bars_url)
    assert response.status_code == 401


async def test_unknown_key_is_401_with_identical_body(client, bars_url):
    absent = await client.get(bars_url)
    unknown = await client.get(bars_url, headers={"X-API-Key": "wrong"})
    assert unknown.status_code == 401
    # Uniform body: a caller can't distinguish "no such key" from "no key".
    assert unknown.json() == absent.json()


async def test_revoked_key_is_401_on_next_request(client, session_factory, bars_url, auth_headers):
    before = await client.get(bars_url, headers=auth_headers)
    assert before.status_code == 200

    async with session_factory() as s:
        assert await ServiceClientRepository(s).revoke("test") is True
        await s.commit()

    after = await client.get(bars_url, headers=auth_headers)
    assert after.status_code == 401

"""Auth seam (S005/ADR-0012): X-API-Key → SHA-256 → service_clients lookup;
absent, unknown, and revoked keys are indistinguishable (uniform 401)."""

from rs_persistence import ServiceClientRepository

API_KEY = "test-key-not-secret"  # provisioned by the conftest session_factory
BARS_URL = "/v1/bars/SPY?start=2026-07-09T13:30:00Z&end=2026-07-09T20:00:00Z"


async def test_valid_key_passes(client):
    response = await client.get(BARS_URL, headers={"X-API-Key": API_KEY})
    assert response.status_code == 200


async def test_absent_key_is_401(client):
    response = await client.get(BARS_URL)
    assert response.status_code == 401


async def test_unknown_key_is_401_with_identical_body(client):
    absent = await client.get(BARS_URL)
    unknown = await client.get(BARS_URL, headers={"X-API-Key": "wrong"})
    assert unknown.status_code == 401
    # Uniform body: a caller can't distinguish "no such key" from "no key".
    assert unknown.json() == absent.json()


async def test_revoked_key_is_401_on_next_request(client, session_factory):
    before = await client.get(BARS_URL, headers={"X-API-Key": API_KEY})
    assert before.status_code == 200

    async with session_factory() as s:
        assert await ServiceClientRepository(s).revoke("test") is True
        await s.commit()

    after = await client.get(BARS_URL, headers={"X-API-Key": API_KEY})
    assert after.status_code == 401

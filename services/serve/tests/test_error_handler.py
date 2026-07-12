"""Catch-all 500 handler (ADR-0015 FastAPI hook, S005 depth: log + clean JSON;
the SYSTEM_CRITICAL notification joins with ADR-0016 in Phase 2)."""

import httpx

API_KEY = "test-key-not-secret"
HEADERS = {"X-API-Key": API_KEY}


async def test_unhandled_error_returns_clean_json_and_logs_traceback(app, caplog):
    @app.get("/boom")
    async def boom():
        raise RuntimeError("planted failure: secret-internals-string")

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://serve") as client:
        response = await client.get("/boom", headers=HEADERS)

    assert response.status_code == 500
    assert response.json() == {"detail": "internal server error"}
    # The public-posture body never leaks internals...
    assert "secret-internals-string" not in response.text
    # ...but the operator gets the full traceback via logger.exception.
    assert "planted failure: secret-internals-string" in caplog.text
    assert "Traceback" in caplog.text or any(r.exc_info for r in caplog.records)

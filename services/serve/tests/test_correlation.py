"""Correlation middleware (S006): one HTTP request = one unit of work — a
fresh X-Correlation-ID echoed to the caller and present on the request's log
records. Asserted on the rendered JSON stream (rs_observability.testing)."""

import asyncio
import uuid

import httpx
import pytest

from rs_observability.testing import captured_json_stream, json_records


@pytest.fixture
def json_stream():
    with captured_json_stream("serve") as buffer:
        yield buffer


async def test_response_echoes_id_and_request_records_carry_it(
    client, auth_headers, bars_url, json_stream
):
    response = await client.get(bars_url, headers=auth_headers)
    assert response.status_code == 200
    correlation_id = response.headers["x-correlation-id"]
    uuid.UUID(correlation_id)  # opaque, never a domain value
    completed = [r for r in json_records(json_stream) if r["event"] == "request completed"]
    (record,) = completed
    assert record["correlation_id"] == correlation_id
    assert record["method"] == "GET"
    assert record["route"] == "/v1/bars/SPY"
    assert record["status"] == 200
    assert record["service"] == "serve"


async def test_every_request_gets_a_fresh_id(client, auth_headers, bars_url, json_stream):
    first = await client.get(bars_url, headers=auth_headers)
    second = await client.get(bars_url, headers=auth_headers)
    assert first.headers["x-correlation-id"] != second.headers["x-correlation-id"]


async def test_auth_rejections_are_units_of_work_too(client, bars_url, json_stream):
    """Correlation sits OUTSIDE auth: a 401 still gets an id + a record."""
    response = await client.get(bars_url)
    assert response.status_code == 401
    correlation_id = response.headers["x-correlation-id"]
    (record,) = [r for r in json_records(json_stream) if r["event"] == "request completed"]
    assert record["correlation_id"] == correlation_id
    assert record["status"] == 401


async def test_unhandled_error_response_and_traceback_share_the_id(
    app, auth_headers, json_stream
):
    @app.get("/boom")
    async def boom():
        raise RuntimeError("planted failure")

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://serve") as client:
        response = await client.get("/boom", headers=auth_headers)

    assert response.status_code == 500
    correlation_id = response.headers["x-correlation-id"]
    (record,) = [r for r in json_records(json_stream) if "unhandled error" in r["event"]]
    assert record["correlation_id"] == correlation_id
    assert "RuntimeError: planted failure" in record["exception"]


async def test_lifespan_installs_the_asyncio_excepthook(app):
    """The running-loop half of install_excepthooks lives in the lifespan."""
    async with app.router.lifespan_context(app):
        assert asyncio.get_running_loop().get_exception_handler() is not None

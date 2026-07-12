"""/metrics (ADR-0015): Prometheus text exposition behind the same API-key
middleware as the data routes — any known unrevoked key passes (S005; scope
narrowing is Phase 3)."""

API_KEY = "test-key-not-secret"
HEADERS = {"X-API-Key": API_KEY}

BARS_URL = "/v1/bars/SPY?start=2026-07-09T13:30:00Z&end=2026-07-09T20:00:00Z"


async def test_metrics_requires_a_key(client):
    response = await client.get("/metrics")
    assert response.status_code == 401


async def test_metrics_exposition_counts_requests(client):
    def counter_total(text: str) -> float:
        # Sum of all http_requests_total series in the exposition.
        return sum(
            float(line.rsplit(" ", 1)[1])
            for line in text.splitlines()
            if line.startswith("http_requests_total{")
        )

    first = await client.get("/metrics", headers=HEADERS)
    assert first.status_code == 200
    assert first.headers["content-type"].startswith("text/plain")

    await client.get(BARS_URL, headers=HEADERS)
    await client.get(BARS_URL)  # 401s are traffic too — counted, not hidden

    second = await client.get("/metrics", headers=HEADERS)
    assert counter_total(second.text) >= counter_total(first.text) + 3


async def test_bars_requests_labelled_by_route_template_and_status(client):
    await client.get(BARS_URL, headers=HEADERS)
    exposition = (await client.get("/metrics", headers=HEADERS)).text
    # Route template, not the raw path — label cardinality stays bounded.
    assert 'path="/v1/bars/{ticker}"' in exposition
    assert "http_request_duration_seconds" in exposition

"""/metrics (ADR-0015): Prometheus text exposition behind the same API-key
middleware as the data routes — any known unrevoked key passes (S005; scope
narrowing is Phase 3)."""


async def test_metrics_requires_a_key(client):
    response = await client.get("/metrics")
    assert response.status_code == 401


async def test_metrics_exposition_counts_requests(client, bars_url, auth_headers):
    def counter_total(text: str) -> float:
        # Sum of all http_requests_total series in the exposition.
        return sum(
            float(line.rsplit(" ", 1)[1])
            for line in text.splitlines()
            if line.startswith("http_requests_total{")
        )

    first = await client.get("/metrics", headers=auth_headers)
    assert first.status_code == 200
    assert first.headers["content-type"].startswith("text/plain")

    await client.get(bars_url, headers=auth_headers)
    await client.get(bars_url)  # 401s are traffic too — counted, not hidden

    second = await client.get("/metrics", headers=auth_headers)
    assert counter_total(second.text) >= counter_total(first.text) + 3


async def test_unhandled_500s_are_counted(app, client, auth_headers):
    # The catch-all handler lives in Starlette's outermost ServerErrorMiddleware,
    # so the exception passes THROUGH the metrics middleware — it must record
    # the request on the way, not lose it (code-review finding, 2026-07-12).
    @app.get("/boom")
    async def boom():
        raise RuntimeError("planted")

    await client.get("/boom", headers=auth_headers)
    exposition = (await client.get("/metrics", headers=auth_headers)).text
    assert 'status="500"' in exposition


async def test_bars_requests_labelled_by_route_template_and_status(client, bars_url, auth_headers):
    await client.get(bars_url, headers=auth_headers)
    exposition = (await client.get("/metrics", headers=auth_headers)).text
    # Route template, not the raw path — label cardinality stays bounded.
    assert 'path="/v1/bars/{ticker}"' in exposition
    assert "http_request_duration_seconds" in exposition

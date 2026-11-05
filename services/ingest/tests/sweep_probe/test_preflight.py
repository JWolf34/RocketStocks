"""The check before a run: the token loads and Schwab answers one cheap call, or the run stops saying why."""

import asyncio

import httpx
import pytest
from sqlalchemy.exc import OperationalError

from rs_ingest.tools.sweep_probe.preflight import PreflightError, preflight
from rs_sources.schwab import SchwabAuthError, TokenMissingError


async def test_a_token_that_loads_and_a_quote_that_answers_pass(harness, wire):
    await preflight(harness._make_client)

    assert [(c.kind, c.symbol) for c in harness.schwab.calls] == [("quote", "SPY")]


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (TokenMissingError("none"), "no Schwab token in the database"),
        (SchwabAuthError("dead"), "the stored Schwab token is dead"),
        (OperationalError("select", {}, Exception("down")), "could not be read from the database"),
    ],
    ids=["missing", "dead", "database"],
)
async def test_a_token_that_cannot_be_loaded_stops_the_run_with_the_reason(harness, error, reason):
    harness.client_error = error

    with pytest.raises(PreflightError, match=reason):
        await preflight(harness._make_client)

    assert harness.schwab.calls == []


async def test_a_refresh_that_schwab_rejects_while_answering_the_quote_is_a_dead_token(harness):
    from authlib.integrations.base_client import OAuthError

    async def on_quote(symbol):
        raise OAuthError(error="invalid_grant")

    harness.schwab.get_quote = on_quote

    with pytest.raises(PreflightError, match="the stored Schwab token is dead .*invalid_grant"):
        await preflight(harness._make_client)


async def test_a_non_200_quote_stops_the_run_with_the_status(harness, wire):
    harness.schwab.on_quote = lambda symbol: wire.status(503)

    with pytest.raises(PreflightError, match="Schwab answered 503"):
        await preflight(harness._make_client)


@pytest.mark.parametrize("error", [httpx.ReadTimeout("slow"), httpx.ConnectError("refused")], ids=["timeout", "connect"])
async def test_a_quote_that_raises_stops_the_run(harness, error):
    def on_quote(symbol):
        raise error

    harness.schwab.on_quote = on_quote

    with pytest.raises(PreflightError, match="did not answer a quote request in time|quote request failed"):
        await preflight(harness._make_client)


async def test_a_quote_that_never_answers_stops_the_run_at_the_timeout(harness):
    async def hang(symbol):
        await asyncio.sleep(3600)

    harness.schwab.get_quote = hang

    with pytest.raises(PreflightError, match="no answer within"):
        await preflight(harness._make_client, timeout=0.05)

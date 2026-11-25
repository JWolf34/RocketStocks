"""The one sort of a Schwab failure: token, rate limited, per symbol, transient."""

import pytest

from rs_sources.errors import SourceError
from rs_sources.schwab import (
    SchwabAuthError,
    SchwabFailure,
    SchwabResponseError,
    TokenMissingError,
    schwab_failure,
)


@pytest.mark.parametrize("error", [SchwabAuthError("refresh token rejected"), TokenMissingError("no token")])
def test_a_dead_or_missing_token_is_a_token_failure(error):
    assert schwab_failure(error) is SchwabFailure.TOKEN


def test_a_429_is_rate_limited():
    assert schwab_failure(SchwabResponseError("AAA: HTTP 429", 429)) is SchwabFailure.RATE_LIMITED


@pytest.mark.parametrize("status", [400, 404])
def test_a_400_or_404_is_schwabs_answer_about_the_symbol(status):
    assert schwab_failure(SchwabResponseError(f"AAA: HTTP {status}", status)) is SchwabFailure.PER_SYMBOL


@pytest.mark.parametrize(
    "error",
    [
        SchwabResponseError("AAA: HTTP 500", 500),
        SchwabResponseError("AAA: HTTP 401", 401),
        SchwabResponseError("AAA: response is not JSON"),
        # Another source's 429 or 404 is not Schwab's answer.
        SourceError("GET failed", status_code=429),
        SourceError("GET failed", status_code=404),
        TimeoutError(),
        RuntimeError("connection reset"),
    ],
)
def test_anything_else_is_transient(error):
    assert schwab_failure(error) is SchwabFailure.TRANSIENT

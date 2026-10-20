"""SchwabOAuth: the sign-in URL and the callback exchange, without a network call.

A wrong-state callback is rejected before schwab-py contacts Schwab, so the
rejection path runs for real. The happy exchange is covered at the handler seam.
"""

import logging
from urllib.parse import parse_qs, urlparse

import pytest

from rs_sources.schwab import CallbackRejected, SchwabOAuth

CALLBACK = "https://127.0.0.1:8182"


def _oauth() -> SchwabOAuth:
    return SchwabOAuth("the-key", "the-secret", CALLBACK)


def test_begin_returns_a_sign_in_url_for_the_app():
    url, _context = _oauth().begin()

    query = parse_qs(urlparse(url).query)
    assert urlparse(url).hostname == "api.schwabapi.com"
    assert query["client_id"] == ["the-key"]
    assert query["redirect_uri"] == [CALLBACK]


def test_each_sign_in_gets_its_own_state():
    first = _oauth().begin()[1]
    second = _oauth().begin()[1]
    assert first.state != second.state


async def test_a_callback_for_another_sign_in_is_rejected_without_leaking_it(caplog):
    oauth = _oauth()
    _url, context = oauth.begin()
    secret_code = "SECRETCODE123"
    received = f"{CALLBACK}/?code={secret_code}&state=not-the-state"

    with caplog.at_level(logging.DEBUG), pytest.raises(CallbackRejected) as excinfo:
        await oauth.exchange(context, received)

    assert secret_code not in str(excinfo.value)
    assert secret_code not in caplog.text
    assert received not in caplog.text

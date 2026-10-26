"""Which failures are the vendor's answer about one symbol and which say the source is unwell."""

import pytest

from rs_sources.errors import SourceError, is_per_symbol_answer
from rs_sources.schwab import SchwabResponseError


@pytest.mark.parametrize("status", [400, 404])
def test_a_400_or_404_is_the_vendors_answer_about_one_symbol(status):
    assert is_per_symbol_answer(SchwabResponseError("AAA: HTTP", status))
    assert is_per_symbol_answer(SourceError("GET failed", status_code=status))


@pytest.mark.parametrize("status", [401, 403, 429, 500, 503])
def test_other_statuses_say_the_source_is_unwell(status):
    assert not is_per_symbol_answer(SchwabResponseError("AAA: HTTP", status))


def test_a_failure_with_no_status_says_the_source_is_unwell():
    assert not is_per_symbol_answer(SourceError("payload has an unexpected shape"))
    assert not is_per_symbol_answer(SchwabResponseError("AAA: response is not JSON"))
    assert not is_per_symbol_answer(TimeoutError())

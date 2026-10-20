from datetime import UTC, datetime

import pytest

from rs_domain import BaseRate, BaseRateInput
from rs_persistence import BaseRateRepository
from rs_query import BASE_RATE_LOGGED_MIN_N, current_base_rate

T0 = datetime(2026, 7, 6, 3, 0, tzinfo=UTC)


def _rate(input: BaseRateInput, n: int, day: int = 6, **overrides) -> BaseRate:
    fields = {
        "alert_type": "attention_surge",
        "rule_version": "1",
        "computed_at": T0.replace(day=day),
        "input": input,
        "n": n,
        "n_unpriced": 2,
        "hit_rate_1d": 0.48,
        "hit_rate_5d": 0.45,
        "hit_rate_10d": None,
        "median_excess_1d": -0.002,
        "median_excess_5d": -0.006,
        "median_excess_10d": None,
        "survivorship_caveat": "2 unpriced symbols excluded",
    }
    return BaseRate(**(fields | overrides))


async def _seed(session_factory, *rates):
    async with session_factory() as s:
        repo = BaseRateRepository(s)
        for rate in rates:
            await repo.insert(rate)
        await s.commit()


async def _current(session_factory, alert_type="attention_surge", rule_version="1"):
    async with session_factory() as s:
        return await current_base_rate(s, alert_type, rule_version)


def test_threshold_is_thirty():
    assert BASE_RATE_LOGGED_MIN_N == 30


@pytest.mark.parametrize(
    ("logged_n", "expected"),
    [(BASE_RATE_LOGGED_MIN_N - 1, BaseRateInput.RECONSTRUCTED), (BASE_RATE_LOGGED_MIN_N, BaseRateInput.LOGGED)],
)
async def test_logged_row_takes_over_at_the_minimum_n(session_factory, logged_n, expected):
    reconstructed = _rate(BaseRateInput.RECONSTRUCTED, n=212)
    logged = _rate(BaseRateInput.LOGGED, n=logged_n)
    await _seed(session_factory, reconstructed, logged)

    got = await _current(session_factory)

    assert got == {BaseRateInput.RECONSTRUCTED: reconstructed, BaseRateInput.LOGGED: logged}[expected]


async def test_latest_row_of_the_chosen_input_wins(session_factory):
    await _seed(
        session_factory,
        _rate(BaseRateInput.RECONSTRUCTED, n=100, day=1),
        _rate(BaseRateInput.RECONSTRUCTED, n=110, day=8),
        _rate(BaseRateInput.LOGGED, n=40, day=2),
        _rate(BaseRateInput.LOGGED, n=45, day=9),
        _rate(BaseRateInput.LOGGED, n=12, day=5),
    )

    got = await _current(session_factory)

    assert got is not None
    assert (got.input, got.n) == (BaseRateInput.LOGGED, 45)


async def test_a_thin_latest_logged_row_falls_back_even_if_an_older_one_was_thick(session_factory):
    # The latest study run is the truth; a rerun that measured fewer alerts is not hidden by history.
    await _seed(
        session_factory,
        _rate(BaseRateInput.RECONSTRUCTED, n=212),
        _rate(BaseRateInput.LOGGED, n=40, day=2),
        _rate(BaseRateInput.LOGGED, n=10, day=9),
    )

    got = await _current(session_factory)

    assert got is not None
    assert got.input is BaseRateInput.RECONSTRUCTED


async def test_thin_logged_row_alone_is_not_shown(session_factory):
    await _seed(session_factory, _rate(BaseRateInput.LOGGED, n=5))
    assert await _current(session_factory) is None


async def test_thick_logged_row_is_shown_without_a_reconstructed_row(session_factory):
    logged = _rate(BaseRateInput.LOGGED, n=31)
    await _seed(session_factory, logged)
    assert await _current(session_factory) == logged


async def test_scoped_to_alert_type_and_rule_version(session_factory):
    v1 = _rate(BaseRateInput.RECONSTRUCTED, n=212)
    await _seed(
        session_factory,
        v1,
        _rate(BaseRateInput.RECONSTRUCTED, n=99, rule_version="2"),
        _rate(BaseRateInput.RECONSTRUCTED, n=98, alert_type="large_move"),
    )

    assert await _current(session_factory) == v1
    assert await _current(session_factory, rule_version="3") is None
    assert await _current(session_factory, alert_type="nope") is None

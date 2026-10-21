"""HealthTracker: one Ops ping when a subject starts failing, one when it clears."""

import pytest

from rs_observability import HealthTracker, Source, SweepTally, SweepVerdict
from rs_observability.testing import captured_ops_pings


@pytest.fixture
def pings():
    with captured_ops_pings() as sent:
        yield sent


async def test_a_source_pings_once_on_first_failure_and_once_on_recovery(pings):
    health = HealthTracker("ingest")

    await health.report_failed(Source.APEWISDOM, "ReadTimeout")
    await health.report_failed(Source.APEWISDOM, "ReadTimeout")
    await health.report_failed(Source.APEWISDOM, "ConnectError")
    await health.report_ok(Source.APEWISDOM)
    await health.report_ok(Source.APEWISDOM)

    assert pings == [
        "ApeWisdom is failing (ReadTimeout). See the ingest logs.",
        "ApeWisdom recovered.",
    ]


@pytest.mark.parametrize("source", list(Source))
async def test_every_source_has_its_own_pair_of_pings(pings, source):
    health = HealthTracker("ingest")

    await health.report_failed(source, "X")
    await health.report_ok(source)

    assert pings == [f"{source.value} is failing (X). See the ingest logs.", f"{source.value} recovered."]


async def test_an_ok_source_never_pings(pings):
    health = HealthTracker("ingest")

    for _ in range(3):
        await health.report_ok(Source.SCHWAB)

    assert pings == []


async def test_one_source_failing_does_not_hide_another(pings):
    health = HealthTracker("ingest")

    await health.report_failed(Source.NASDAQ, "SourceError")
    await health.report_failed(Source.EDGAR, "SourceError")
    await health.report_ok(Source.NASDAQ)

    assert pings == [
        "Nasdaq is failing (SourceError). See the ingest logs.",
        "EDGAR is failing (SourceError). See the ingest logs.",
        "Nasdaq recovered.",
    ]
    assert health.is_failing(Source.EDGAR)
    assert not health.is_failing(Source.NASDAQ)


async def test_a_source_that_fails_again_after_recovering_pings_again(pings):
    health = HealthTracker("bot")

    await health.report_failed(Source.DISCORD, "A")
    await health.report_ok(Source.DISCORD)
    await health.report_failed(Source.DISCORD, "A")

    assert len(pings) == 3


async def test_a_new_tracker_pings_again_for_a_source_still_failing(pings):
    await HealthTracker("ingest").report_failed(Source.SCHWAB, "ConnectError")
    await HealthTracker("ingest").report_failed(Source.SCHWAB, "ConnectError")  # a restart

    assert len(pings) == 2


def tally(attempted: int, failed: int) -> SweepTally:
    sweep = SweepTally()
    for _ in range(attempted - failed):
        sweep.record_ok()
    for _ in range(failed):
        sweep.record_failure()
    return sweep


@pytest.mark.parametrize(
    ("attempted", "failed", "verdict"),
    [
        (1000, 0, SweepVerdict.HEALTHY),
        (1000, 50, SweepVerdict.HEALTHY),  # exactly 5%
        (1000, 51, SweepVerdict.FAILING),
        (30, 1, SweepVerdict.HEALTHY),  # 3.3%
        (30, 2, SweepVerdict.FAILING),
        (20, 1, SweepVerdict.HEALTHY),  # exactly 5%
        (10, 1, SweepVerdict.INCONCLUSIVE),  # one stock in a small sweep
        (1, 1, SweepVerdict.INCONCLUSIVE),
        (2, 2, SweepVerdict.FAILING),
        (1, 0, SweepVerdict.HEALTHY),
        (0, 0, SweepVerdict.INCONCLUSIVE),
    ],
)
def test_a_sweep_fails_above_about_five_percent_but_never_on_one_stock(attempted, failed, verdict):
    assert tally(attempted, failed).verdict is verdict


def test_a_source_wide_failure_fails_the_sweep_whatever_its_size():
    sweep = SweepTally()
    sweep.record_ok()
    sweep.record_failure(source_wide=True)

    assert sweep.verdict is SweepVerdict.FAILING


async def test_a_failing_sweep_pings_once_and_a_healthy_one_recovers(pings):
    health = HealthTracker("ingest")

    assert await health.report_sweep(Source.SCHWAB, tally(100, 10)) is True
    assert await health.report_sweep(Source.SCHWAB, tally(100, 10)) is True
    assert await health.report_sweep(Source.SCHWAB, tally(100, 0)) is False

    assert pings == [
        "Schwab is failing (10 of 100 requests failed). See the ingest logs.",
        "Schwab recovered.",
    ]


async def test_one_stock_failing_in_a_sweep_pings_nothing_and_changes_nothing(pings):
    health = HealthTracker("ingest")

    assert await health.report_sweep(Source.SCHWAB, tally(5, 1)) is False
    await health.report_failed(Source.SCHWAB, "ConnectError")
    assert await health.report_sweep(Source.SCHWAB, tally(5, 1)) is False  # inconclusive: still failing

    assert pings == ["Schwab is failing (ConnectError). See the ingest logs."]

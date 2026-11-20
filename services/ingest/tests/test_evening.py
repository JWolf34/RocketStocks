"""The evening sequence: stages in order, failures kept, a dead token ending the run."""


import pytest

from rs_ingest.evening import run_evening
from rs_observability import QuietFailure, SourceFailed
from rs_sources.schwab import SchwabAuthError


class Stages:
    """Stage stand-ins that note their order and raise what they are told to."""

    def __init__(self, **errors: Exception) -> None:
        self.order: list[str] = []
        self.errors = errors
        self.judged: list[Exception | None] = []

    def stage(self, name: str):
        async def run() -> None:
            self.order.append(name)
            if name in self.errors:
                raise self.errors[name]

        return name, run

    async def judge(self, error: Exception | None) -> None:
        self.order.append("judge")
        self.judged.append(error)


async def test_each_stage_runs_after_the_one_before_and_the_evening_is_judged_last():
    s = Stages()

    await run_evening([s.stage("chains"), s.stage("bars"), s.stage("outcomes")], s.judge)

    assert s.order == ["chains", "bars", "outcomes", "judge"]
    assert s.judged == [None]


async def test_a_failed_stage_does_not_hold_back_the_stages_after_it_and_is_raised_at_the_end():
    boom = RuntimeError("database went away")
    s = Stages(chains=boom)

    with pytest.raises(RuntimeError, match="database went away"):
        await run_evening([s.stage("chains"), s.stage("bars"), s.stage("outcomes")], s.judge)

    assert s.order == ["chains", "bars", "outcomes", "judge"]
    assert s.judged == [boom]


async def test_a_dead_token_ends_the_evening_at_once_without_judging_it():
    s = Stages(chains=SchwabAuthError("refresh token rejected"))

    with pytest.raises(SchwabAuthError):
        await run_evening([s.stage("chains"), s.stage("bars"), s.stage("outcomes")], s.judge)

    assert s.order == ["chains"]


async def test_the_failure_that_pings_is_raised_over_a_quiet_one_whichever_came_first():
    quiet, loud = QuietFailure("3 of 13000 symbols failed"), RuntimeError("outcomes exited with status 1")
    s = Stages(bars=quiet, outcomes=loud)

    with pytest.raises(RuntimeError, match="status 1"):
        await run_evening([s.stage("chains"), s.stage("bars"), s.stage("outcomes")], s.judge)


async def test_only_quiet_failures_are_raised_quiet():
    s = Stages(bars=QuietFailure("3 of 13000 symbols failed"))

    with pytest.raises(QuietFailure):
        await run_evening([s.stage("chains"), s.stage("bars"), s.stage("outcomes")], s.judge)


async def test_schwab_failing_is_what_the_job_raises_even_when_a_stage_failed():
    s = Stages(bars=RuntimeError("429"))

    async def failing(error: Exception | None) -> None:
        raise SourceFailed("Schwab") from error

    with pytest.raises(SourceFailed):
        await run_evening([s.stage("chains"), s.stage("bars")], failing)

    assert s.order == ["chains", "bars"]

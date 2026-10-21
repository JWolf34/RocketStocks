"""Source health: ok/failed reports in, one Ops ping per transition out.

A subject (a source such as ApeWisdom, or an ingest job) pings once when it
starts failing and once when it clears; repeated reports in the same state say
nothing. State lives in process, so after a restart a still-failing subject
pings once more.

A failure here is a source failing, or more than about 5% of a sweep's
per-stock requests failing. One stock failing (a 404, no data for that symbol)
is never reported as a failure of its source: the caller reports such an answer
as ok, because the source did answer.
"""

import logging
from dataclasses import dataclass
from enum import StrEnum

from rs_observability.ops import notify_ops

logger = logging.getLogger(__name__)

# A sweep is failing above this share of failed requests ("about 5%").
MAX_FAILED_SHARE = 0.05
# ... and never on fewer failures than this: one stock failing must not ping on its own.
MIN_SWEEP_FAILURES = 2


class Source(StrEnum):
    """The outside systems an Ops ping can name; the value is the ping's subject."""

    APEWISDOM = "ApeWisdom"
    SCHWAB = "Schwab"
    NASDAQ = "Nasdaq"
    EDGAR = "EDGAR"
    DISCORD = "Discord"


class QuietFailure(Exception):
    """A job failure that must not raise an Ops ping of its own.

    Raised when the cause was already pinged as a source failure, or is a few
    stocks' (which never ping). The daemon still logs and counts the run as failed.
    """


class SourceFailed(QuietFailure):
    """A source failure that was already reported through a `HealthTracker`."""

    def __init__(self, source: str) -> None:
        super().__init__(f"{source} is failing")
        self.source = source


class SweepVerdict(StrEnum):
    HEALTHY = "healthy"
    FAILING = "failing"
    INCONCLUSIVE = "inconclusive"


@dataclass
class SweepTally:
    """The outcome of every per-stock request in one sweep.

    `source_wide` marks a failure that speaks for the whole source however few
    requests were made, such as being rate limited.
    """

    attempted: int = 0
    failed: int = 0
    source_wide: bool = False

    def record_ok(self) -> None:
        self.attempted += 1

    def record_failure(self, *, source_wide: bool = False) -> None:
        self.attempted += 1
        self.failed += 1
        self.source_wide = self.source_wide or source_wide

    @property
    def verdict(self) -> SweepVerdict:
        if self.source_wide:
            return SweepVerdict.FAILING
        if self.attempted == 0:
            return SweepVerdict.INCONCLUSIVE
        if self.failed / self.attempted <= MAX_FAILED_SHARE:
            return SweepVerdict.HEALTHY
        if self.failed >= MIN_SWEEP_FAILURES:
            return SweepVerdict.FAILING
        # One failure over the share, in a small sweep: could be one stock; say nothing.
        return SweepVerdict.INCONCLUSIVE


class HealthTracker:
    """Per-process failing/ok state per subject, pinging on each transition.

    `service` names whose logs to read ("ingest", "bot"). State is flipped
    before the ping is awaited, so concurrent reports ping once.
    """

    def __init__(self, service: str) -> None:
        self._service = service
        self._failing: set[str] = set()

    def is_failing(self, subject: str) -> bool:
        return subject in self._failing

    async def report_ok(self, subject: str) -> None:
        if subject not in self._failing:
            return
        self._failing.discard(subject)
        logger.info("%s recovered", subject)
        await notify_ops(f"{subject} recovered.")

    async def report_failed(self, subject: str, reason: str) -> None:
        """`reason` goes into the ping: pass an exception type or a count, never exception text
        (it can carry URLs or credentials)."""
        if subject in self._failing:
            return
        self._failing.add(subject)
        logger.warning("%s is failing (%s)", subject, reason)
        await notify_ops(f"{subject} is failing ({reason}). See the {self._service} logs.")

    async def report_sweep(self, subject: str, tally: SweepTally) -> bool:
        """Judge a finished sweep; True when the subject is failing because of it."""
        verdict = tally.verdict
        if verdict is SweepVerdict.FAILING:
            await self.report_failed(subject, f"{tally.failed} of {tally.attempted} requests failed")
            return True
        if verdict is SweepVerdict.HEALTHY:
            await self.report_ok(subject)
        return False

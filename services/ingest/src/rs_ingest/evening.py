"""evening: the evening's passes in order, each starting when the one before finishes.

Chains, then daily bars, then Outcomes and the Track record. A stage that fails (a stock's
bars, a database error, a Tombstone-heavy chain pass) does not hold back the stages after it,
because each of them reads what is stored and skips what is recorded: the failure is kept and
raised once the last stage has run. A dead Schwab token is the exception: it ends the run at
once, the daemon pauses Schwab jobs, and the whole run is owed again once sign-in reopens the
gate, when every stage skips what it had already finished.

The Universe and the earnings snapshot are their own jobs and run beside the sequence.
"""

import logging
from collections.abc import Awaitable, Callable, Sequence
from datetime import date, datetime

from rs_domain import EASTERN
from rs_ingest.universe import EVENING_STARTS_AT
from rs_observability import QuietFailure
from rs_sources.schwab import SchwabAuthError

logger = logging.getLogger(__name__)

Stage = Callable[[], Awaitable[object]]


def evening_start(session: date) -> datetime:
    """The moment `session`'s evening begins."""
    return datetime.combine(session, EVENING_STARTS_AT, tzinfo=EASTERN)


async def run_evening(
    stages: Sequence[tuple[str, Stage]], judge: Callable[[Exception | None], Awaitable[None]]
) -> None:
    """Run `stages` in order, then `judge` the evening's Schwab calls as a whole.

    `judge` raises `SourceFailed` when Schwab is failing (it has pinged Ops already); it gets the
    first stage failure to chain from. What the job raises, most telling first: Schwab failing,
    then a stage failure that must ping, then a quiet stage failure.
    """
    failures: list[Exception] = []
    for name, stage in stages:
        try:
            await stage()
        except SchwabAuthError:
            raise  # the daemon pauses Schwab jobs on this
        except Exception as exc:
            logger.exception("evening stage %s failed; the stages after it still run", name)
            failures.append(exc)
    await judge(failures[0] if failures else None)
    if failures:
        loud = [exc for exc in failures if not isinstance(exc, QuietFailure)]
        raise (loud or failures)[0]

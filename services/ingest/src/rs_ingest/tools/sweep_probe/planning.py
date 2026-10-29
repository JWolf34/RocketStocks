"""What a run measures: the symbols, the survey tasks in order, and the call keys.

The runner executes this plan and the summary reads a directory against it, so what is
"missing" is defined here once.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from rs_domain import TradingCalendar
from rs_ingest.tools.sweep_probe.schedule import first_sample_time, sample_times
from rs_ingest.tools.sweep_probe.sizes import PARTS, ChainSize, ChainState, chain_size
from rs_ingest.tools.sweep_probe.store import (
    CallRecord,
    Kind,
    Plan,
    ProbeDir,
    slug,
    stamp,
)
from rs_ingest.tools.sweep_probe.symbols import Listing, chain_spellings, draw_sample

NAMED_CHAINS = ("SPY", "QQQ", "IWM", "TSLA", "NVDA", "META", "AMD", "AAPL", "BRK/B")
# The chain endpoint is also asked for the other spellings of a class share.
SPELLING_CHAINS = ("BRK.B", "BRKB")
BAR_SYMBOLS = ("SPY", "AAPL", "IBM", "KO", "GE", "BRK.B", "BRK/B")
FIXED_TICKERS = ("AAPL", "NVDA", "AMD")
SMALL_TICKERS = 2
# A chain this small tells little about open interest; the small tickers need at least this many contracts.
MIN_SMALL_CONTRACTS = 20


class Group(StrEnum):
    NAMED = "named"
    SPELLING = "spelling"
    SAMPLE = "sample"
    BARS = "bars"


@dataclass(frozen=True)
class SurveyTask:
    group: Group
    # Spellings tried in order; a later one only after a 400 on the one before.
    spellings: tuple[str, ...]

    @property
    def kind(self) -> Kind:
        return Kind.BARS if self.group == Group.BARS else Kind.CHAIN


def chain_base(symbol: str) -> str:
    """The key prefix of a survey chain's full request and its ranges."""
    return f"chain/{slug(symbol)}"


def timed_base(target: datetime, symbol: str) -> str:
    """The key prefix of one ticker's chain at one sample."""
    return f"timed/{stamp(target)}/{slug(symbol)}"


def bars_key(symbol: str) -> str:
    return f"bars/{slug(symbol)}"


def survey_tasks(plan: Plan) -> list[SurveyTask]:
    tasks = [SurveyTask(Group.NAMED, (symbol,)) for symbol in plan.named_chains]
    tasks += [SurveyTask(Group.BARS, (symbol,)) for symbol in plan.bar_symbols]
    tasks += [SurveyTask(Group.SPELLING, (symbol,)) for symbol in plan.spelling_chains]
    tasks += [SurveyTask(Group.SAMPLE, chain_spellings(listing)) for listing in plan.sample_listings]
    return tasks


def build_plan(
    *,
    now: datetime,
    calendar: TradingCalendar,
    listings: list[Listing],
    seed: int,
    sample_size: int,
    start: datetime | None,
) -> Plan:
    session_day, first = first_sample_time(now, calendar, start)
    drawn = draw_sample(listings, seed, sample_size, frozenset(NAMED_CHAINS) | frozenset(SPELLING_CHAINS))
    return Plan(
        seed=seed,
        session_day=session_day,
        samples=sample_times(session_day, first),
        named_chains=NAMED_CHAINS,
        spelling_chains=SPELLING_CHAINS,
        sample_listings=tuple(drawn),
        bar_symbols=BAR_SYMBOLS,
        fixed_tickers=FIXED_TICKERS,
    )


def ladder_records(out: ProbeDir, base: str) -> list[CallRecord]:
    """The records of one chain so far: its full request, then its ranges."""
    full = out.latest(f"{base}/full")
    if full is None:
        return []
    return [full] + [r for n in range(1, PARTS + 1) if (r := out.latest(f"{base}/split{n}")) is not None]


def task_records(out: ProbeDir, task: SurveyTask) -> list[CallRecord]:
    """The records a task has so far: for a chain, those of the last spelling tried (it stops at the
    first spelling that did not answer 400)."""
    if task.kind == Kind.BARS:
        record = out.latest(bars_key(task.spellings[0]))
        return [] if record is None else [record]
    records: list[CallRecord] = []
    for spelling in task.spellings:
        tried = ladder_records(out, chain_base(spelling))
        if not tried:
            break
        records = tried
        if tried[0].status != 400:
            break
    return records


def task_size(out: ProbeDir, task: SurveyTask) -> ChainSize:
    return chain_size(task_records(out, task), task.spellings[0])


def unfinished(out: ProbeDir, task: SurveyTask) -> bool:
    """Whether a task still has a call to make: none recorded, or one a restart retries."""
    records = task_records(out, task)
    if task.kind == Kind.BARS:
        return not records or not records[0].terminal
    return chain_size(records, task.spellings[0]).state == ChainState.PENDING


def choose_small_tickers(out: ProbeDir, plan: Plan) -> tuple[str, ...]:
    """The sample listings with the fewest contracts (at least MIN_SMALL_CONTRACTS) in the survey,
    ties by symbol; short of enough, the first sample listings in draw order fill the rest."""
    measured: list[tuple[int, str]] = []
    for task in survey_tasks(plan):
        if task.group != Group.SAMPLE:
            continue
        size = task_size(out, task)
        if size.usable and size.contracts >= MIN_SMALL_CONTRACTS:
            measured.append((size.contracts, size.symbol))
    chosen = [symbol for _, symbol in sorted(measured)[:SMALL_TICKERS]]
    for listing in plan.sample_listings:
        if len(chosen) == SMALL_TICKERS:
            break
        if (spelling := chain_spellings(listing)[0]) not in chosen:
            chosen.append(spelling)
    return tuple(chosen)

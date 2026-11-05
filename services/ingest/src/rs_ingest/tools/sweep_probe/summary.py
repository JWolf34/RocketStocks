"""The summary: what a probe directory answers, as counts, times and ratios only.

No prices and no contract rows, so the output can be pasted into an issue. It reads whatever
a run (or a partial run) left and lists what is missing. A chain that answered in expiry
ranges counts with its ranges summed and is flagged.
"""

import gzip
import statistics
import zlib
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from rs_domain import EASTERN
from rs_ingest.tools.sweep_probe.payloads import (
    QUOTE_FIELDS,
    Snapshot,
    parse_json_object,
)
from rs_ingest.tools.sweep_probe.planning import (
    Group,
    SurveyTask,
    ladder_records,
    survey_tasks,
    task_records,
    task_size,
    timed_base,
    unfinished,
)
from rs_ingest.tools.sweep_probe.schedule import evening_sample_time
from rs_ingest.tools.sweep_probe.sizes import ChainSize, ChainState, chain_size
from rs_ingest.tools.sweep_probe.store import (
    PLAN_FILE,
    CallRecord,
    Outcome,
    Plan,
    ProbeDir,
    error_text,
)

FIELD_LABELS = {
    "bid": "bid",
    "ask": "ask",
    "mark": "mark",
    "last": "last",
    "volatility": "implied volatility",
    "quoteTimeInLong": "quote time",
}
# Share of contracts with volume at the reference sample that must agree for a verdict.
VERDICT_SHARE = 0.95
# Open interest counts as settled once this share of its eventual change has happened.
OI_SETTLED_SHARE = 0.9
RANGE_LABELS = {"split1": "<=30d", "split2": "31-180d", "split3": ">180d"}
MAX_LISTED = 20


def build_summary(root: Path) -> str:
    if not (root / PLAN_FILE).is_file():
        return f"no {PLAN_FILE} in {root}: nothing to summarise (the run has not started, or this is not a probe directory)\n"
    out = ProbeDir(root)
    try:
        plan = out.load_plan()
    except (ValueError, KeyError, TypeError) as exc:
        return f"{PLAN_FILE} in {root} is unreadable ({error_text(exc)}): nothing to summarise\n"
    assert plan is not None
    lines: list[str] = []
    lines += _overview(out, plan)
    lines += _chains(out, plan)
    lines += _bars(out, plan)
    lines += _timed(out, plan)
    lines += _missing(out, plan)
    return "\n".join(lines) + "\n"


# ---- formatting helpers


def _et(moment: datetime) -> str:
    return moment.astimezone(EASTERN).strftime("%m-%d %H:%M")


def _when(moment: datetime | None) -> str:
    return "none" if moment is None else f"at {_et(moment)} ET"


def _ratio(part: int, whole: int) -> str:
    return f"{part}/{whole} ({part / whole:.3f})" if whole else f"{part}/0"


def _mb(size: int | None) -> str:
    return "-" if size is None else f"{size / 1_000_000:.2f}MB"


def _count(n: int, one: str, many: str) -> str:
    return f"{n:,} {one if n == 1 else many}"


def _spread(values: list[float], unit: str = "", digits: int = 0) -> str:
    """Min, median, p90, max and mean of the values."""
    if not values:
        return "none"
    ordered = sorted(values)
    p90 = ordered[min(len(ordered) - 1, int(0.9 * len(ordered)))]
    return (
        f"min {ordered[0]:,.{digits}f}{unit}, median {statistics.median(ordered):,.{digits}f}{unit}, "
        f"p90 {p90:,.{digits}f}{unit}, max {ordered[-1]:,.{digits}f}{unit}, "
        f"mean {statistics.fmean(ordered):,.{digits}f}{unit}"
    )


def _result(record: CallRecord) -> str:
    if record.outcome != Outcome.HTTP:
        return str(record.outcome)
    parts = [str(record.status)]
    if record.contracts is not None:
        parts.append(_count(record.contracts, "contract", "contracts"))
    if record.expiries is not None:
        parts.append(_count(record.expiries, "expiry", "expiries"))
    parts.append(_mb(record.bytes))
    if record.latency_s is not None:
        parts.append(f"{record.latency_s:.1f}s")
    if record.truncated is not None:
        parts.append(f"truncated={str(record.truncated).lower()}")
    if record.chain_status not in (None, "SUCCESS"):
        parts.append(f"status={record.chain_status}")
    return " ".join(parts)


def _tally(labels: list[str]) -> str:
    return ", ".join(f"{label}: {n}" for label, n in sorted(Counter(labels).items())) or "none"


# ---- sections


def _overview(out: ProbeDir, plan: Plan) -> list[str]:
    records = out.records
    last = max((r.at for r in records), default=None)
    return [
        "== Probe run ==",
        f"session day {plan.session_day}, first sample {_et(plan.samples[0])} ET, "
        f"{len(plan.samples)} samples, last {_et(plan.samples[-1])} ET",
        f"seed {plan.seed}, {len(plan.sample_listings)} sampled listings, "
        f"timed tickers: {', '.join(plan.timed_tickers) or 'none yet'}",
        f"{len(records)} records written; last at "
        f"{_et(datetime.fromisoformat(last)) + ' ET' if last else 'never'}",
        "",
    ]


def _chains(out: ProbeDir, plan: Plan) -> list[str]:
    lines = ["== Chain sizes and failures (unfiltered, contract type ALL) =="]
    tasks = survey_tasks(plan)
    for group, title in ((Group.NAMED, "named underlyings"), (Group.SPELLING, "other spellings of BRK/B")):
        lines.append(f"-- {title}")
        for task in (t for t in tasks if t.group == group):
            lines += _task_lines(out, task)
    sample = [t for t in tasks if t.group == Group.SAMPLE]
    lines.append(f"-- sampled listings ({len(sample)} drawn)")
    lines += _sample_summary(out, sample)
    lines.append("")
    return lines


def _task_lines(out: ProbeDir, task: SurveyTask) -> list[str]:
    records = task_records(out, task)
    if not records:
        return [f"{task.spellings[0]:<8} not fetched"]
    lines = []
    for record in records:
        if record.variant == "full":
            lines.append(f"{record.symbol:<8} full: {_result(record)}")
        else:
            label = RANGE_LABELS.get(record.variant or "", record.variant)
            lines.append(f"{'':<8}   {label}: {_result(record)}")
    size = task_size(out, task)
    if size.laddered:
        lines.append(f"{'':<8} as a chain: {_size_text(size)}")
    return lines


def _size_text(size: ChainSize) -> str:
    text = f"{size.state}, {_count(size.contracts, 'contract', 'contracts')}, "
    text += f"{_count(size.expiries, 'expiry', 'expiries')}, {_mb(size.bytes)}"
    return text + (", truncated" if size.truncated else "")


def _sample_summary(out: ProbeDir, tasks: list[SurveyTask]) -> list[str]:
    sizes = [task_size(out, task) for task in tasks]
    fetched = [s for s in sizes if s.state != ChainState.PENDING]
    lines = [f"fetched {len(fetched)} of {len(tasks)}; chains: {_tally([str(s.state) for s in fetched])}"]
    usable = [s for s in fetched if s.usable]
    in_parts = sum(1 for s in usable if s.state == ChainState.IN_PARTS)
    if usable:
        lines.append(f"{len(usable)} usable chains ({in_parts} answered in parts, summed and flagged):")
        contracts, expiries, sizes_mb = (
            [float(s.contracts) for s in usable],
            [float(s.expiries) for s in usable],
            [s.bytes / 1_000_000 for s in usable],
        )
        lines.append("  contracts: " + _spread(contracts) + f"; total {sum(contracts):,.0f}")
        lines.append("  expiries: " + _spread(expiries))
        lines.append("  response size: " + _spread(sizes_mb, "MB", 2) + f"; total {sum(sizes_mb):,.2f}MB")
        latencies = [s.latency_s for s in usable if s.latency_s is not None]
        lines.append("  latency (whole-chain request): " + _spread(latencies, "s", 1))
        lines.append(f"  isChainTruncated true on {sum(1 for s in usable if s.truncated)} of {len(usable)} chains")
    statuses = Counter(status for s in fetched for status in s.chain_statuses)
    if statuses:
        lines.append("chain status in the responses: " + _tally(list(statuses.elements())))
    empty = [s.symbol for s in fetched if s.state == ChainState.EMPTY]
    if empty:
        lines.append(f"empty (a 200 with status FAILED or no contracts; left out of the spreads): {_names(empty)}")
    unanswered = [f"{s.symbol} ({s.state})" for s in fetched if s.state in (ChainState.FAILED, ChainState.PARTIAL, ChainState.REJECTED)]
    if unanswered:
        lines.append(f"not answered: {_names(unanswered)}")
    lines += _ladder_lines(out, tasks, sizes)
    for task, size in zip(tasks, sizes, strict=True):
        if len(task.spellings) > 1 and size.state != ChainState.PENDING:
            tried = ", ".join(f"{r.symbol} {r.label}" for r in task_records(out, task) if r.variant == "full")
            lines.append(f"class share spelling - {task.spellings[1]}: {tried}")
    return lines


def _names(names: list[str]) -> str:
    more = f" and {len(names) - MAX_LISTED} more" if len(names) > MAX_LISTED else ""
    return ", ".join(names[:MAX_LISTED]) + more


def _ladder_lines(out: ProbeDir, tasks: list[SurveyTask], sizes: list[ChainSize]) -> list[str]:
    ladders = [(task, size) for task, size in zip(tasks, sizes, strict=True) if size.laddered]
    if not ladders:
        return ["no chain went down the ladder"]
    lines = [f"{len(ladders)} chains went down the ladder (fetched again, then in expiry ranges); range results:"]
    parts = [r for task, _ in ladders for r in task_records(out, task) if r.variant != "full"]
    for variant, label in RANGE_LABELS.items():
        lines.append(f"  {label}: {_tally([r.label for r in parts if r.variant == variant])}")
    answered = [f"{s.symbol} {s.contracts:,} contracts {_mb(s.bytes)} ({s.state})" for _, s in ladders if s.contracts]
    if answered:
        lines.append(f"  sizes when summed: {_names(answered)}")
    return lines


def _bars(out: ProbeDir, plan: Plan) -> list[str]:
    lines = ["== Daily bar depth from 1990-01-01 =="]
    for task in (t for t in survey_tasks(plan) if t.group == Group.BARS):
        records = task_records(out, task)
        if not records:
            lines.append(f"{task.spellings[0]:<8} not fetched")
            continue
        record = records[0]
        detail = _result(record)
        if record.candles is not None:
            detail += f", candles {record.candles:,}, earliest {record.earliest}, latest {record.latest}"
            if record.epoch_zero:
                detail += f", epoch-zero candles {record.epoch_zero}"
        lines.append(f"{record.symbol:<8} {detail}")
    lines.append("")
    return lines


# ---- the close against the morning


@dataclass
class Comparison:
    """One sample's contracts against a reference sample's, over the contracts present in both."""

    common: int = 0
    reference_only: int = 0
    sample_only: int = 0
    volume_positive: int = 0  # common contracts with volume at the reference
    volume_same: int = 0  # ... whose volume is unchanged
    volume_zero: int = 0  # ... whose volume is now zero
    volume_up: int = 0  # all common contracts whose volume rose
    volume_down: int = 0  # all common contracts whose volume fell
    changed: dict[str, int] = field(default_factory=dict)
    oi_changed: int = 0
    underlying_changed: bool | None = None


def compare(reference: Snapshot, sample: Snapshot) -> Comparison:
    """Compare two chains; contracts are matched by OCC symbol and only those in both are counted."""
    before, after = reference.contracts, sample.contracts
    result = Comparison(
        reference_only=len(before.keys() - after.keys()),
        sample_only=len(after.keys() - before.keys()),
        changed=dict.fromkeys(QUOTE_FIELDS, 0),
    )
    for symbol in before.keys() & after.keys():
        old, new = before[symbol], after[symbol]
        result.common += 1
        volume_before, volume_after = old["totalVolume"], new["totalVolume"]
        if isinstance(volume_before, int | float) and isinstance(volume_after, int | float):
            if volume_before > 0:
                result.volume_positive += 1
                result.volume_same += volume_after == volume_before
                result.volume_zero += volume_after == 0
            result.volume_up += volume_after > volume_before
            result.volume_down += volume_after < volume_before
        for name in QUOTE_FIELDS:
            result.changed[name] += old[name] != new[name]
        result.oi_changed += old["openInterest"] != new["openInterest"]
    result.underlying_changed = reference.underlying != sample.underlying
    return result


def volume_verdict(comparison: Comparison) -> str:
    if comparison.volume_positive == 0:
        return "no contract had volume at the reference sample"
    if comparison.volume_same / comparison.volume_positive >= VERDICT_SHARE:
        return "persists"
    if comparison.volume_zero / comparison.volume_positive >= VERDICT_SHARE:
        return "resets to zero"
    return "mixed"


def load_snapshot(out: ProbeDir, base: str) -> Snapshot | None:
    """A sample's chain from its raw responses (all its ranges when it came in parts); None when
    none of them can be read or none carries a contract."""
    payloads = []
    for record in ladder_records(out, base):
        if not record.ok or record.raw is None:
            continue
        try:
            payload = parse_json_object(out.read_raw(record.raw))
        except (OSError, EOFError, gzip.BadGzipFile, zlib.error):
            continue
        if payload is not None:
            payloads.append(payload)
    snapshot = Snapshot.of(*payloads)
    return snapshot if snapshot.contracts else None


@dataclass(frozen=True)
class Reference:
    label: str
    at: datetime
    snapshot: Snapshot
    size: ChainSize


@dataclass(frozen=True)
class Row:
    at: datetime
    size: ChainSize
    comparison: Comparison | None


def _timed(out: ProbeDir, plan: Plan) -> list[str]:
    lines = ["== The close against the morning =="]
    if not plan.timed_tickers:
        return lines + ["no timed tickers chosen yet (the first sample has not been reached)", ""]
    lines.append(
        "Each sample is compared with the first sample after the close and with the 18:05 ET sample, "
        "over contracts present in both (the others are counted apart). "
        f"Volume verdicts need {VERDICT_SHARE:.0%} agreement among contracts that had volume at the reference sample."
    )
    for symbol in plan.timed_tickers:
        lines += _ticker(out, plan, symbol)
    return lines + [""]


def _references(out: ProbeDir, plan: Plan, symbol: str) -> tuple[list[Reference], list[str]]:
    """The first usable sample and the 18:05 sample (when it is another and usable), and why any sample
    before the first usable one was passed over."""
    evening = evening_sample_time(plan.session_day)
    references: list[Reference] = []
    skipped: list[str] = []
    for at in plan.samples:
        if references and at != evening:
            continue  # past the first reference only the 18:05 sample matters
        base = timed_base(at, symbol)
        records = ladder_records(out, base)
        snapshot = load_snapshot(out, base)
        size = chain_size(records, symbol)
        if snapshot is None:
            if not references:
                skipped.append(f"{_et(at)} ET ({_why(records, size)})")
            continue
        references.append(Reference("18:05 sample" if references else "first sample", at, snapshot, size))
        if at == evening:
            break
    return references, skipped


def _why(records: list[CallRecord], size: ChainSize) -> str:
    if not records:
        return "not fetched"
    if size.state == ChainState.PENDING:
        return "no usable token"
    return f"{size.state}, {records[0].label}"


def _ticker(out: ProbeDir, plan: Plan, symbol: str) -> list[str]:
    lines = ["", f"-- {symbol}"]
    references, skipped = _references(out, plan, symbol)
    if not references:
        return lines + [f"no usable sample ({'; '.join(skipped)}): no comparison possible"]
    if skipped:
        lines.append(f"unusable, so not a reference: {'; '.join(skipped)}")
    if len(references) == 1 and references[0].at != evening_sample_time(plan.session_day):
        lines.append("no usable 18:05 sample, so only the first sample is compared with")
    for reference in references:
        lines.append(f"{reference.label} {_et(reference.at)} ET: {_size_text(reference.size)}")
    rows: dict[str, list[Row]] = {reference.label: [] for reference in references}
    for at in plan.samples:
        later = [r for r in references if at > r.at]
        if not later:
            continue
        base = timed_base(at, symbol)
        size = chain_size(ladder_records(out, base), symbol)
        snapshot = load_snapshot(out, base)
        for reference in later:
            rows[reference.label].append(Row(at, size, None if snapshot is None else compare(reference.snapshot, snapshot)))
    for reference in references:
        lines += _table(out, symbol, reference, rows[reference.label])
    return lines


_HEADER = (
    f"{'ET':<12}{'contracts':>11}{'common':>8}{'ref only':>9}{'new':>6}{'vol same':>9}{'vol zero':>9}"
    f"{'vol up':>8}{'vol down':>9}{'bid':>7}{'ask':>7}{'mark':>7}{'last':>7}{'iv':>7}{'qtime':>7}{'under.':>8}{'oi':>7}"
)


def _table(out: ProbeDir, symbol: str, reference: Reference, rows: list[Row]) -> list[str]:
    lines = ["", f"vs {reference.label} ({_et(reference.at)} ET); p = answered in expiry ranges, summed", _HEADER]
    compared: list[tuple[datetime, Comparison]] = []
    for row in rows:
        c = row.comparison
        if c is None:
            records = ladder_records(out, timed_base(row.at, symbol))
            lines.append(f"{_et(row.at):<12}{_why(records, row.size)}")
            continue
        compared.append((row.at, c))
        contracts = f"{row.size.contracts}{'p' if row.size.state == ChainState.IN_PARTS else ''}"
        under = "-" if c.underlying_changed is None else ("changed" if c.underlying_changed else "same")
        lines.append(
            f"{_et(row.at):<12}{contracts:>11}{c.common:>8}{c.reference_only:>9}{c.sample_only:>6}"
            f"{c.volume_same:>9}{c.volume_zero:>9}{c.volume_up:>8}{c.volume_down:>9}"
            + "".join(f"{c.changed[name]:>7}" for name in QUOTE_FIELDS)
            + f"{under:>8}{c.oi_changed:>7}"
        )
    return lines + _answers(reference.label, compared)


def _answers(label: str, compared: list[tuple[datetime, Comparison]]) -> list[str]:
    if not compared:
        return [f"no later sample has been fetched yet to compare with the {label}"]
    last_time, last = compared[-1]
    lines = [
        f"volume at {_et(last_time)} ET vs {label}: {volume_verdict(last)} "
        f"(unchanged {_ratio(last.volume_same, last.volume_positive)}, "
        f"zero {_ratio(last.volume_zero, last.volume_positive)} of contracts with volume at the {label})",
    ]
    for name in QUOTE_FIELDS:
        first_change = next((t for t, c in compared if c.changed[name] > 0), None)
        lines.append(
            f"{FIELD_LABELS[name]}: first difference {_when(first_change)}; "
            f"differs at {_et(last_time)} ET on {_ratio(last.changed[name], last.common)} of common contracts"
        )
    changed_under = [t for t, c in compared if c.underlying_changed]
    lines.append(
        f"underlying price: first difference {_when(changed_under[0] if changed_under else None)}; "
        f"differs at {len(changed_under)} of {len(compared)} later samples"
    )
    lines += _oi_answers(label, compared)
    lines.append(
        f"contracts on one side only at {_et(last_time)} ET: {last.reference_only} only in the {label}, "
        f"{last.sample_only} only in the sample"
    )
    return lines


def _oi_answers(label: str, compared: list[tuple[datetime, Comparison]]) -> list[str]:
    last_time, last = compared[-1]
    first_change = next((t for t, c in compared if c.oi_changed > 0), None)
    if first_change is None:
        return [f"open interest: no contract differs from the {label} through {_et(last_time)} ET"]
    peak = max(c.oi_changed for _, c in compared)
    settled = next(t for t, c in compared if c.oi_changed >= OI_SETTLED_SHARE * peak)
    series = ", ".join(f"{_et(t)}={c.oi_changed}" for t, c in compared if c.oi_changed != 0)
    return [
        f"open interest: first difference at {_et(first_change)} ET; {OI_SETTLED_SHARE:.0%} of the change seen by "
        f"{_et(settled)} ET; at {_et(last_time)} ET {_ratio(last.oi_changed, last.common)} of common contracts differ",
        f"open interest, contracts differing from the {label} (non-zero samples): {series}",
    ]


# ---- what is missing


def _missing(out: ProbeDir, plan: Plan) -> list[str]:
    lines = ["== What is missing =="]
    tasks = survey_tasks(plan)
    unfetched = [t for t in tasks if not task_records(out, t)]
    retried = [t for t in tasks if t not in unfetched and unfinished(out, t)]
    lines.append(f"survey: {len(tasks) - len(unfetched)} of {len(tasks)} tasks have a record")
    if unfetched:
        lines.append(f"  no record: {_names([t.spellings[0] for t in unfetched])}")
    if retried:
        lines.append(f"  no usable token when tried (a restart retries): {_names([t.spellings[0] for t in retried])}")
    timed_symbols = plan.timed_tickers
    complete = 0
    gaps: list[str] = []
    for target in plan.samples:
        states: list[str] = []
        for symbol in timed_symbols:
            records = ladder_records(out, timed_base(target, symbol))
            states.append("absent" if not records else _sample_state(records, chain_size(records, symbol)))
        counts = Counter(states)
        if timed_symbols and counts["ok"] == len(timed_symbols):
            complete += 1
            continue
        gaps.append(
            f"  {_et(target)} ET: ok {counts['ok']}, no record {counts['absent']}, missed {counts['missed']}, "
            f"no token {counts['no token']}, empty {counts['empty']}, failed {counts['failed']}"
        )
    lines.append(f"timed samples: {complete} of {len(plan.samples)} complete across {len(timed_symbols)} tickers")
    lines += gaps if gaps else ["  none missing"]
    return lines


def _sample_state(records: list[CallRecord], size: ChainSize) -> str:
    if records[0].outcome == Outcome.MISSED:
        return "missed"
    if size.state == ChainState.PENDING:
        return "no token"
    if size.state == ChainState.EMPTY:
        return "empty"
    return "ok" if size.usable else "failed"


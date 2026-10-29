"""The probe's output directory: a plan, one JSON line per call, and the raw payloads.

    plan.json        what the run set out to do (written once; a restart reuses it)
    records.jsonl    one line per call outcome, appended as it happens
    raw/<key>.gz     the gzipped response body of each call that got one

A call's key names it (`chain/SPY/full`, `timed/20261028T200500Z/AAPL/split2`); the last
record written for a key wins. The record is written after its payload, so a crash leaves at
worst a payload without a record, which a restart simply fetches again.
"""

import gzip
import json
import os
import re
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

PLAN_FILE = "plan.json"
RECORDS_FILE = "records.jsonl"
RAW_DIR = "raw"


class Kind(StrEnum):
    CHAIN = "chain"
    BARS = "bars"
    TIMED = "timed"


class Outcome(StrEnum):
    HTTP = "http"  # a response, whatever its status
    TIMEOUT = "timeout"
    ERROR = "error"
    # No usable token: none stored, a dead refresh token, or the database could not be read.
    # The one outcome a restart retries.
    UNAVAILABLE = "unavailable"
    MISSED = "missed"  # a sample slot that passed unfetched


def slug(symbol: str) -> str:
    """A symbol as one safe path segment (`BRK/B` -> `BRK_B`)."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", symbol)


def error_text(exc: BaseException) -> str:
    """What to keep of an exception: its type and the start of its message."""
    return f"{type(exc).__name__}: {exc}"[:200]


def stamp(moment: datetime) -> str:
    return moment.strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True)
class CallRecord:
    """One call, after its retry: the outcome of the last attempt."""

    key: str
    kind: Kind
    symbol: str
    outcome: Outcome
    at: str  # when the call finished, UTC ISO
    status: int | None = None
    bytes: int | None = None
    latency_s: float | None = None
    attempts: int | None = None
    first_failure: str | None = None  # what the first attempt got when a retry followed
    # chain and timed
    contracts: int | None = None
    expiries: int | None = None
    truncated: bool | None = None
    chain_status: str | None = None
    variant: str | None = None  # "full", "split1" .. "split3"
    from_date: str | None = None
    to_date: str | None = None
    # bars
    candles: int | None = None
    earliest: str | None = None
    latest: str | None = None
    epoch_zero: int | None = None
    # timed
    target: str | None = None
    late_s: float | None = None
    raw: str | None = None
    detail: str | None = None

    @property
    def terminal(self) -> bool:
        """Whether a restart leaves this call alone; a 401 means the token was no good, as no token does."""
        return self.outcome != Outcome.UNAVAILABLE and self.status != 401

    @property
    def ok(self) -> bool:
        return self.outcome == Outcome.HTTP and self.status == 200

    @property
    def label(self) -> str:
        """Its status when it got a response, else its outcome."""
        return str(self.status) if self.outcome == Outcome.HTTP else str(self.outcome)

    def to_json(self) -> str:
        return json.dumps({k: v for k, v in asdict(self).items() if v is not None}, separators=(",", ":"))

    @classmethod
    def from_json(cls, line: str) -> "CallRecord":
        data = json.loads(line)
        known = {f.name for f in fields(cls)}
        record = {k: v for k, v in data.items() if k in known}
        record["kind"], record["outcome"] = Kind(record["kind"]), Outcome(record["outcome"])
        return cls(**record)


@dataclass(frozen=True)
class Plan:
    seed: int
    session_day: date
    samples: tuple[datetime, ...]  # UTC; the first is the first sample after the close
    named_chains: tuple[str, ...]  # Schwab spelling
    spelling_chains: tuple[str, ...]  # the other spellings of a class share's chain
    sample_listings: tuple[str, ...]  # Cboe spelling, in draw order
    bar_symbols: tuple[str, ...]
    fixed_tickers: tuple[str, ...]  # timed samples that need no survey result
    small_tickers: tuple[str, ...] | None = None  # chosen from the survey, then kept

    def to_json(self) -> str:
        data: dict[str, Any] = asdict(self)
        data["session_day"] = self.session_day.isoformat()
        data["samples"] = [t.isoformat() for t in self.samples]
        return json.dumps(data, indent=2)

    @classmethod
    def from_json(cls, text: str) -> "Plan":
        data = json.loads(text)
        small = data.get("small_tickers")
        return cls(
            seed=data["seed"],
            session_day=date.fromisoformat(data["session_day"]),
            samples=tuple(datetime.fromisoformat(t) for t in data["samples"]),
            named_chains=tuple(data["named_chains"]),
            spelling_chains=tuple(data["spelling_chains"]),
            sample_listings=tuple(data["sample_listings"]),
            bar_symbols=tuple(data["bar_symbols"]),
            fixed_tickers=tuple(data["fixed_tickers"]),
            small_tickers=None if small is None else tuple(small),
        )

    def with_small(self, small: tuple[str, ...]) -> "Plan":
        return replace(self, small_tickers=small)

    @property
    def timed_tickers(self) -> tuple[str, ...]:
        return self.fixed_tickers + (self.small_tickers or ())


@dataclass
class ProbeDir:
    root: Path
    _latest: dict[str, CallRecord] = field(default_factory=dict, init=False)
    _history: list[CallRecord] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self._load_records()

    def _load_records(self) -> None:
        path = self.root / RECORDS_FILE
        if not path.exists():
            return
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                record = CallRecord.from_json(line)
            except (ValueError, TypeError, KeyError):
                continue  # a line cut short by a crash
            self._history.append(record)
            self._latest[record.key] = record

    @property
    def records(self) -> list[CallRecord]:
        """Every record in the order written."""
        return list(self._history)

    def latest(self, key: str) -> CallRecord | None:
        return self._latest.get(key)

    def terminal(self, key: str) -> CallRecord | None:
        """The key's record unless it is absent or one a restart retries."""
        record = self._latest.get(key)
        return record if record is not None and record.terminal else None

    def append(self, record: CallRecord) -> None:
        path = self.root / RECORDS_FILE
        # A crash can leave a last line without its newline; end it so this record is not glued to it.
        torn = path.exists() and path.stat().st_size > 0 and not path.read_bytes().endswith(b"\n")
        with path.open("a", encoding="utf-8") as handle:
            handle.write(("\n" if torn else "") + record.to_json() + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        self._history.append(record)
        self._latest[record.key] = record

    def load_plan(self) -> Plan | None:
        path = self.root / PLAN_FILE
        return Plan.from_json(path.read_text(encoding="utf-8")) if path.exists() else None

    def save_plan(self, plan: Plan) -> None:
        self._atomic_write(self.root / PLAN_FILE, plan.to_json().encode("utf-8"))

    def write_raw(self, key: str, content: bytes) -> str:
        """Store a response body gzipped; returns its path relative to the directory."""
        relative = f"{RAW_DIR}/{key}.gz"
        self._atomic_write(self.root / relative, gzip.compress(content))
        return relative

    def read_raw(self, relative: str) -> bytes:
        return gzip.decompress((self.root / relative).read_bytes())

    @staticmethod
    def _atomic_write(path: Path, content: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(content)
        tmp.replace(path)

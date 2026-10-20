"""Readers for a dump of v1's `popularity` table.

Two formats, both produced from the v1 database (see rs_backfill.__doc__):
`pg_dump --data-only` plain SQL (a COPY block) and CSV with a header row.
Both stream: a dump can hold millions of rows.
"""

import csv
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# `name` is in the table but dropped by the import; the rest are required.
REQUIRED_COLUMNS = (
    "datetime",
    "rank",
    "ticker",
    "mentions",
    "upvotes",
    "rank_24h_ago",
    "mentions_24h_ago",
)

_COPY_START = re.compile(r'^COPY\s+(?:"?\w+"?\.)?"?(\w+)"?\s*(?:\(([^)]*)\))?\s+FROM\s+stdin;\s*$')
_COPY_END = r"\."
_COPY_NULL = r"\N"
_DEFAULT_COPY_COLUMNS = ("datetime", "rank", "ticker", "name", "mentions", "upvotes", "rank_24h_ago", "mentions_24h_ago")


class DumpError(Exception):
    """The dump is not a readable v1 popularity dump."""


@dataclass(frozen=True)
class V1Row:
    """One v1 popularity row as written: `at` is v1's naive local timestamp."""

    at: datetime
    rank: int | None
    ticker: str | None
    mentions: int | None
    upvotes: int | None
    rank_24h_ago: int | None
    mentions_24h_ago: int | None


def read_dump(path: Path, fmt: str = "auto") -> Iterator[V1Row]:
    """Stream the rows of a v1 popularity dump. `fmt` is auto, copy or csv."""
    if fmt == "auto":
        fmt = _sniff(path)
    if fmt == "copy":
        yield from _read_copy(path)
    elif fmt == "csv":
        yield from _read_csv(path)
    else:
        raise DumpError(f"unknown dump format {fmt!r} (expected auto, copy or csv)")


def _sniff(path: Path) -> str:
    with path.open(encoding="utf-8-sig") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            # pg_dump output opens with comments / SET / COPY; a CSV opens with its header.
            return "copy" if stripped.startswith(("--", "SET ", "COPY ", "SELECT ")) else "csv"
    raise DumpError(f"{path} is empty")


def _read_copy(path: Path) -> Iterator[V1Row]:
    columns: tuple[str, ...] | None = None
    skipping = False  # inside a COPY block for some other table
    found = False
    with path.open(encoding="utf-8") as handle:
        for number, raw in enumerate(handle, start=1):
            line = raw.rstrip("\n")
            if columns is None and not skipping:
                match = _COPY_START.match(line)
                if match is None:
                    continue
                if match.group(1) == "popularity":
                    found = True
                    columns = _copy_columns(match.group(2))
                else:
                    skipping = True
                continue
            if line == _COPY_END:
                columns = None
                skipping = False
                continue
            if columns is None:
                continue
            fields = [None if value == _COPY_NULL else value for value in line.split("\t")]
            yield _row(dict(zip(columns, fields, strict=False)), len(fields) == len(columns), number)
    if not found:
        raise DumpError(f"{path} has no COPY block for the popularity table")


def _copy_columns(listed: str | None) -> tuple[str, ...]:
    if listed is None:
        return _DEFAULT_COPY_COLUMNS
    columns = tuple(part.strip().strip('"') for part in listed.split(","))
    missing = [name for name in REQUIRED_COLUMNS if name not in columns]
    if missing:
        raise DumpError(f"COPY block lacks columns {missing}")
    return columns


def _read_csv(path: Path) -> Iterator[V1Row]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            raise DumpError(f"{path} is empty")
        columns = tuple(name.strip() for name in header)
        missing = [name for name in REQUIRED_COLUMNS if name not in columns]
        if missing:
            raise DumpError(f"CSV header lacks columns {missing}")
        for fields in reader:
            if not fields:
                continue
            values = [None if value in ("", _COPY_NULL) else value for value in fields]
            yield _row(dict(zip(columns, values, strict=False)), len(values) == len(columns), reader.line_num)


def _row(values: dict[str, str | None], complete: bool, line: int) -> V1Row:
    if not complete:
        raise DumpError(f"line {line}: wrong number of fields")
    try:
        return V1Row(
            at=_naive_datetime(values["datetime"], line),
            rank=_integer(values["rank"]),
            ticker=values["ticker"],
            mentions=_integer(values["mentions"]),
            upvotes=_integer(values["upvotes"]),
            rank_24h_ago=_integer(values["rank_24h_ago"]),
            mentions_24h_ago=_integer(values["mentions_24h_ago"]),
        )
    except ValueError as exc:
        raise DumpError(f"line {line}: {exc}") from exc


def _naive_datetime(value: str | None, line: int) -> datetime:
    if value is None:
        raise DumpError(f"line {line}: datetime is NULL")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        raise DumpError(f"line {line}: datetime {value!r} carries a zone; v1 wrote naive timestamps")
    return parsed


def _integer(value: str | None) -> int | None:
    return None if value is None else int(value)

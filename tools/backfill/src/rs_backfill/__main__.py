"""Backfill CLI. DSN comes from RS_DB_URL (the migrate/owner role).

    python -m rs_backfill verify-offset --dump PATH [--format auto|copy|csv]
    python -m rs_backfill popularity-import --dump PATH --zone ZONE [--format ...]

See the rs_backfill package docstring for how to produce the dump.
"""

import argparse
import asyncio
import os
import sys
from datetime import timedelta
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_backfill.dump import DumpError, read_dump
from rs_backfill.importer import import_rows
from rs_backfill.verify import (
    DEFAULT_MIN_OVERLAP,
    DEFAULT_THRESHOLD,
    LiveIndex,
    render_report,
    verify_offset,
)
from rs_backfill.zones import resolve_zone
from rs_domain import Clock, PopularityOrigin, SystemClock
from rs_persistence import (
    ImportVerification,
    ImportVerificationRepository,
    PopularityRepository,
    build_engine,
    build_session_factory,
)

_DAY = timedelta(days=1)


def main(argv: list[str] | None = None, *, clock: Clock | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rs_backfill", description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)

    verify = sub.add_parser("verify-offset", help="find and record the zone of v1's timestamps")
    _dump_args(verify)
    verify.add_argument(
        "--min-overlap",
        type=int,
        default=DEFAULT_MIN_OVERLAP,
        help="fewest overlapping rows that can verify a zone (default: %(default)s)",
    )

    import_ = sub.add_parser("popularity-import", help="import v1 popularity history")
    _dump_args(import_)
    import_.add_argument("--zone", required=True, help="the zone recorded by verify-offset")

    args = parser.parse_args(argv)

    db_url = os.environ.get("RS_DB_URL")
    if not db_url:
        print("RS_DB_URL is not set", file=sys.stderr)
        return 2
    try:
        if args.action == "verify-offset":
            return asyncio.run(_verify(db_url, args, clock or SystemClock()))
        return asyncio.run(_import(db_url, args))
    except (DumpError, ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


def _dump_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dump", type=Path, required=True, help="pg_dump SQL or CSV of v1 popularity")
    parser.add_argument("--format", choices=["auto", "copy", "csv"], default="auto")


async def _verify(db_url: str, args: argparse.Namespace, clock: Clock) -> int:
    engine = build_engine(db_url)
    try:
        factory = build_session_factory(engine)
        live = await _load_live(factory)
        if live is None:
            print("NOT VERIFIED: v2 holds no live popularity rows to compare against", file=sys.stderr)
            return 1
        report = verify_offset(
            read_dump(args.dump, args.format),
            live,
            threshold=DEFAULT_THRESHOLD,
            min_overlap=args.min_overlap,
        )
        print(render_report(report))
        winner = report.winner
        if winner is None or winner.overlap_start is None or winner.overlap_end is None:
            return 1
        async with factory() as session:
            await ImportVerificationRepository(session).record(
                ImportVerification(
                    verified_at=clock.now(),
                    zone=winner.zone,
                    overlap_rows=winner.overlap_rows,
                    matched_rows=winner.matched_rows,
                    match_rate=winner.rate,
                    overlap_start=winner.overlap_start,
                    overlap_end=winner.overlap_end,
                )
            )
            await session.commit()
        print(f"recorded zone {winner.zone}; import with: popularity-import --zone {winner.zone}")
        return 0
    finally:
        await engine.dispose()


async def _load_live(factory: async_sessionmaker[AsyncSession]) -> LiveIndex | None:
    """Every live row, read a day at a time so memory holds only the compact index."""
    async with factory() as session:
        extent = await PopularityRepository(session).extent(PopularityOrigin.LIVE)
    if extent is None:
        return None
    first, last = extent
    index = LiveIndex()
    day = first.replace(hour=0, minute=0, second=0, microsecond=0)
    while day <= last:
        async with factory() as session:
            for snap in await PopularityRepository(session).list_between(
                day, day + _DAY, PopularityOrigin.LIVE
            ):
                index.add(snap.polled_at, snap.ticker, snap.rank, snap.mentions, snap.mentions_24h_ago)
        day += _DAY
    return index


async def _import(db_url: str, args: argparse.Namespace) -> int:
    resolve_zone(args.zone)  # a typo fails here, before any database work
    engine = build_engine(db_url)
    try:
        factory = build_session_factory(engine)
        async with factory() as session:
            verified = await ImportVerificationRepository(session).latest()
        if verified is None:
            print("refusing to import: no verified zone recorded; run verify-offset first", file=sys.stderr)
            return 1
        if verified.zone != args.zone:
            print(
                f"refusing to import: --zone {args.zone} is not the verified zone {verified.zone}"
                f" (verified {verified.match_rate:.2%} of {verified.overlap_rows} rows)",
                file=sys.stderr,
            )
            return 1
        stats = await import_rows(read_dump(args.dump, args.format), args.zone, factory)
        print(
            f"parsed {stats.parsed}, inserted {stats.inserted},"
            f" already present {stats.already_present}, rejected {sum(stats.rejected.values())}"
        )
        for reason, count in sorted(stats.rejected.items()):
            print(f"  rejected {count}: {reason}")
        return 0
    finally:
        await engine.dispose()


if __name__ == "__main__":
    sys.exit(main())

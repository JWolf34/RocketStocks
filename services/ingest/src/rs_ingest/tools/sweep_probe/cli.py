"""Command line of the sweep probe: `run` captures, `summary` summarises.

`run` reads the Schwab token from the database like ingest does and never writes it, and
keeps its call rate well under the one ingest shares. It first makes one cheap Schwab call
and exits non-zero if the token or Schwab does not answer. Run it detached and restart it
freely: with the same `--out` it picks up where it stopped.
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from pydantic import ValidationError

from rs_domain import EASTERN, SystemClock
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.settings import IngestSettings
from rs_ingest.tools.sweep_probe.planning import build_plan
from rs_ingest.tools.sweep_probe.preflight import PreflightError, preflight
from rs_ingest.tools.sweep_probe.runner import ProbeRunner
from rs_ingest.tools.sweep_probe.store import Plan, ProbeDir
from rs_ingest.tools.sweep_probe.summary import build_summary
from rs_ingest.tools.sweep_probe.symbols import (
    Listing,
    download_directory,
    parse_directory,
)
from rs_ingest.tools.sweep_probe.tokens import ReadOnlyTokenStore
from rs_observability import configure_logging
from rs_persistence import SchwabTokenStore, build_engine, build_session_factory
from rs_sources import ExchangeCalendar
from rs_sources.schwab import build_schwab_client

logger = logging.getLogger(__name__)

DEFAULT_SEED = 42
DEFAULT_SAMPLE_SIZE = 50
DEFAULT_CALLS_PER_MINUTE = 20
DEFAULT_REQUEST_TIMEOUT = 60.0
USER_AGENT = "RocketStocks sweep-probe"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m rs_ingest.tools.sweep_probe", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="capture chains and bars, then the close-to-morning samples")
    run.add_argument("--out", type=Path, required=True, help="output directory; reusing one resumes its run")
    run.add_argument(
        "--start",
        type=_start,
        default=None,
        help="first (post-close) sample as ISO 8601, ET when it has no offset; "
        "default: the next session's close plus 5 minutes (early closes included)",
    )
    run.add_argument("--seed", type=int, default=DEFAULT_SEED, help="seed of the listing sample (default %(default)s)")
    run.add_argument(
        "--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE, help="listings drawn (default %(default)s)"
    )
    run.add_argument(
        "--calls-per-minute",
        type=int,
        default=DEFAULT_CALLS_PER_MINUTE,
        help="Schwab calls per minute this probe may make (default %(default)s)",
    )
    run.add_argument(
        "--request-timeout",
        type=float,
        default=DEFAULT_REQUEST_TIMEOUT,
        help="seconds before a request counts as a timeout (default %(default)s)",
    )
    run.add_argument("--symbols-file", type=Path, default=None, help="a saved Cboe directory CSV instead of downloading it")
    run.add_argument("--dry-run", action="store_true", help="print the plan and exit: no Schwab, database or output")

    summary = commands.add_parser("summary", help="print counts, times and ratios from an output directory")
    summary.add_argument("directory", type=Path)
    return parser


def _start(text: str) -> datetime:
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an ISO 8601 timestamp: {text!r}") from None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=EASTERN)


async def _listings(symbols_file: Path | None) -> list[Listing]:
    if symbols_file is not None:
        return parse_directory(symbols_file.read_text(encoding="utf-8"))
    async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT, follow_redirects=True) as http:
        return parse_directory(await download_directory(http, USER_AGENT))


async def _new_plan(args: argparse.Namespace, clock: SystemClock) -> Plan:
    return build_plan(
        now=clock.now(),
        calendar=ExchangeCalendar(),
        listings=await _listings(args.symbols_file),
        seed=args.seed,
        sample_size=args.sample_size,
        start=args.start,
    )


async def _run(args: argparse.Namespace) -> int:
    clock = SystemClock()
    if args.dry_run:
        print((await _new_plan(args, clock)).to_json())
        return 0
    settings = IngestSettings()
    engine = build_engine(settings.rs_db_url)
    tokens = ReadOnlyTokenStore(SchwabTokenStore(build_session_factory(engine)))

    async def make_client() -> Any:
        client = await build_schwab_client(
            settings.schwab_api_key, settings.schwab_api_secret.get_secret_value(), tokens
        )
        client.set_timeout(httpx.Timeout(args.request_timeout))
        return client

    try:
        await preflight(make_client)
        out = ProbeDir(args.out)
        plan = out.load_plan()
        if plan is None:
            plan = await _new_plan(args, clock)
            out.save_plan(plan)
            logger.info("plan written", extra={"out": str(args.out)})
        else:
            logger.info("resuming the plan in the output directory; --start, --seed and --sample-size are ignored")
        await ProbeRunner(
            plan=plan,
            out=out,
            make_client=make_client,
            limiter=RateLimiter(args.calls_per_minute),
            clock=clock,
            sleep=asyncio.sleep,
        ).run()
    finally:
        await engine.dispose()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "summary":
        sys.stdout.write(build_summary(args.directory))
        return 0
    configure_logging("sweep_probe")
    try:
        return asyncio.run(_run(args))
    except (PreflightError, ValueError, ValidationError, httpx.HTTPError) as exc:
        logger.error("sweep probe stopped: %s", exc)
        return 1

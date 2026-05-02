"""rocketstocks-data CLI — export market data to disk.

Entry point: rocketstocks-data  (registered in pyproject.toml)

Subcommands:
    export  TICKER [TICKER...] --to PATH  [options]
    quote   TICKER [TICKER...]
    info    TICKER
    watchlists
    version
"""
import argparse
import asyncio
import datetime
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_ALL_INCLUDES = frozenset({
    "ohlcv-daily", "ohlcv-5m", "options", "fundamentals",
    "financials", "eps", "popularity",
})


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rocketstocks-data",
        description="Export RocketStocks market data to disk.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # export
    exp = sub.add_parser("export", help="Export data for one or more tickers.")
    exp.add_argument("tickers", nargs="*", metavar="TICKER")
    exp.add_argument("--to", required=True, metavar="PATH", help="Destination directory.")
    exp.add_argument(
        "--include",
        default=None,
        metavar="LIST",
        help=(
            f"Comma-separated subset of: {', '.join(sorted(_ALL_INCLUDES))}. "
            "Defaults to all."
        ),
    )
    exp.add_argument("--history-days", type=int, default=365, metavar="N")
    exp.add_argument("--intraday-days", type=int, default=5, metavar="N")
    exp.add_argument("--watchlist", default=None, metavar="NAME",
                     help="Expand a named watchlist and export every ticker in it.")
    exp.add_argument("--format", choices=["COWORK", "RAW"], default="COWORK")
    exp.add_argument("--force-live", action="store_true",
                     help="Bypass the DB cache and fetch live data.")
    exp.add_argument("--dry-run", action="store_true",
                     help="Fetch data but write nothing to disk.")

    # quote
    qt = sub.add_parser("quote", help="Print live Schwab quotes as JSON.")
    qt.add_argument("tickers", nargs="+", metavar="TICKER")

    # info
    inf = sub.add_parser("info", help="Print ticker reference info as JSON.")
    inf.add_argument("ticker", metavar="TICKER")

    # watchlists
    sub.add_parser("watchlists", help="List named watchlists.")

    # version
    sub.add_parser("version", help="Print package version.")

    return parser


# ---------------------------------------------------------------------------
# Per-ticker export
# ---------------------------------------------------------------------------

async def _export_ticker(
    api,
    ticker: str,
    dest_dir: Path,
    include: frozenset[str],
    history_days: int,
    intraday_days: int,
    force_live: bool,
    dry_run: bool,
) -> list[Path]:
    from rocketstocks.api.exporters import (
        write_daily_csv, write_5m_csv, write_options_json,
        write_fundamentals_json, write_financials_csvs,
        write_eps_csv, write_popularity_csv,
    )

    today = datetime.date.today()
    written: list[Path] = []

    async def _fetch_write(name, fetch_coro, write_fn, *write_args):
        try:
            data = await fetch_coro
        except Exception as exc:
            logger.warning(f"[{ticker}] {name} fetch failed: {exc}")
            return
        if dry_run:
            logger.info(f"[{ticker}] dry-run: would write {name}")
            return
        try:
            result = write_fn(data, *write_args)
            if isinstance(result, list):
                written.extend(result)
            elif result is not None:
                written.append(result)
        except Exception as exc:
            logger.warning(f"[{ticker}] {name} write failed: {exc}")

    def _fetch_write_sync(name, fetch_fn, write_fn, *write_args):
        try:
            data = fetch_fn()
        except Exception as exc:
            logger.warning(f"[{ticker}] {name} fetch failed: {exc}")
            return
        if dry_run:
            logger.info(f"[{ticker}] dry-run: would write {name}")
            return
        try:
            result = write_fn(data, *write_args)
            if isinstance(result, list):
                written.extend(result)
            elif result is not None:
                written.append(result)
        except Exception as exc:
            logger.warning(f"[{ticker}] {name} write failed: {exc}")

    start_daily = today - datetime.timedelta(days=history_days)
    now = datetime.datetime.now()
    start_intraday = now - datetime.timedelta(days=intraday_days)

    if "ohlcv-daily" in include:
        await _fetch_write(
            "ohlcv-daily",
            api.get_daily_history(ticker, start_daily, today, force_live=force_live),
            write_daily_csv, dest_dir, ticker,
        )
    if "ohlcv-5m" in include:
        await _fetch_write(
            "ohlcv-5m",
            api.get_5m_history(ticker, start_intraday, now),
            write_5m_csv, dest_dir, ticker,
        )
    if "options" in include:
        await _fetch_write(
            "options",
            api.get_options_chain(ticker),
            write_options_json, dest_dir, ticker,
        )
    if "fundamentals" in include:
        await _fetch_write(
            "fundamentals",
            api.get_schwab_fundamentals(ticker),
            write_fundamentals_json, dest_dir, ticker,
        )
    if "financials" in include:
        _fetch_write_sync(
            "financials",
            lambda: api.get_financials(ticker),
            write_financials_csvs, dest_dir, ticker,
        )
    if "eps" in include:
        await _fetch_write(
            "eps",
            api.get_eps_history(ticker),
            write_eps_csv, dest_dir, ticker,
        )
    if "popularity" in include:
        await _fetch_write(
            "popularity",
            api.get_popularity(ticker),
            write_popularity_csv, dest_dir, ticker,
        )

    return written


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

async def _cmd_export(args, api) -> None:
    from rocketstocks.api.exporters import (
        write_daily_csv, write_5m_csv, write_options_json,
        write_fundamentals_json, write_financials_csvs,
        write_eps_csv, write_popularity_csv,
    )

    include = (
        frozenset(args.include.split(",")) if args.include else _ALL_INCLUDES
    )
    unknown = include - _ALL_INCLUDES
    if unknown:
        print(f"Unknown --include values: {', '.join(sorted(unknown))}", file=sys.stderr)
        print(f"Valid values: {', '.join(sorted(_ALL_INCLUDES))}", file=sys.stderr)
        sys.exit(1)

    tickers: list[str] = [t.upper() for t in args.tickers]

    if not tickers and not args.watchlist:
        print("error: provide at least one TICKER or --watchlist NAME", file=sys.stderr)
        sys.exit(1)

    if args.watchlist:
        extra = await api.get_watchlist_tickers(args.watchlist)
        tickers = list(dict.fromkeys(tickers + extra))

    dest_dir = Path(args.to)
    if not args.dry_run:
        dest_dir.mkdir(parents=True, exist_ok=True)

    total: list[Path] = []
    for ticker in tickers:
        written = await _export_ticker(
            api, ticker, dest_dir, include,
            args.history_days, args.intraday_days,
            args.force_live, args.dry_run,
        )
        total.extend(written)
        print(f"{'[dry-run] ' if args.dry_run else ''}{ticker}: {len(written)} file(s)")

    if not args.dry_run:
        print(f"\nWrote {len(total)} file(s) to {dest_dir}")


async def _cmd_quote(args, api) -> None:
    tickers = [t.upper() for t in args.tickers]
    if len(tickers) == 1:
        data = await api.get_quote(tickers[0])
    else:
        data = await api.get_quotes(tickers)
    print(json.dumps(data, indent=2, default=str))


async def _cmd_info(args, api) -> None:
    info = await api.get_ticker_info(args.ticker.upper())
    print(json.dumps(info, indent=2, default=str))


async def _cmd_watchlists(args, api) -> None:
    names = await api.get_watchlists()
    for name in names:
        print(name)


def _cmd_version() -> None:
    try:
        from importlib.metadata import version
        print(f"rocketstocks {version('rocketstocks')}")
    except Exception:
        print("rocketstocks (version unknown)")


# ---------------------------------------------------------------------------
# Async entry point
# ---------------------------------------------------------------------------

async def _async_main(args) -> None:
    if args.command == "version":
        _cmd_version()
        return

    from rocketstocks.api._factory import build_data_api
    api = await build_data_api()

    try:
        if args.command == "export":
            await _cmd_export(args, api)
        elif args.command == "quote":
            await _cmd_quote(args, api)
        elif args.command == "info":
            await _cmd_info(args, api)
        elif args.command == "watchlists":
            await _cmd_watchlists(args, api)
    finally:
        try:
            await api._sd.db.close()
        except Exception:
            pass


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()

"""Day-over-day open-interest (OI) change from chain snapshots — a named Phase-2
detection-design-session input.

OI is a once-daily *settled* figure, static intraday (the 2026-07-06 intraday diff
measured 0 OI changes across all tickers), so each session's LAST snapshot carries that
day's settled OI. For every contract present in both sessions, OI(late) - OI(early) is
the day-over-day change.

CAVEAT: when --early and --late are not consecutive captured sessions the delta is a
*multi-session cumulative* change (it spans the un-captured days), not a clean D->D+1
"day-2 confirmation"; contracts expiring in the gap churn out of the shared set.
Production (ADR-0020) captures OI nightly, so its deltas are always consecutive — this
tool's consecutive-pair output is the representative one.

    .venv/bin/python scripts/spikes/oi_daily_diff.py --early 2026-07-09 --late 2026-07-10
    .venv/bin/python scripts/spikes/oi_daily_diff.py --early 2026-07-06 --late 2026-07-09 --stdout
    .venv/bin/python scripts/spikes/oi_daily_diff.py --dry-run
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import FIXTURES_DIR
from chain_diff import contracts, load
from chain_spike import SNAP_ROOT


def total_oi(flat: dict[str, dict]) -> int:
    return sum(c.get("openInterest") or 0 for c in flat.values())


def diff_day(early: dict, late: dict) -> dict:
    """Day-over-day OI change between two settled snapshots of one ticker."""
    e_c, l_c = contracts(early), contracts(late)
    shared = e_c.keys() & l_c.keys()
    moves = []  # (delta, symbol, description, early_oi, late_oi) over shared contracts
    for sym in shared:
        e_oi = e_c[sym].get("openInterest") or 0
        l_oi = l_c[sym].get("openInterest") or 0
        if l_oi != e_oi:
            moves.append((l_oi - e_oi, sym, l_c[sym].get("description", sym), e_oi, l_oi))
    moves.sort()  # ascending delta
    return {
        "contracts_early": len(e_c),
        "contracts_late": len(l_c),
        "shared": len(shared),
        "appeared": len(l_c.keys() - e_c.keys()),
        "vanished": len(e_c.keys() - l_c.keys()),
        "moved": len(moves),
        "total_oi_early": total_oi(e_c),
        "total_oi_late": total_oi(l_c),
        "net_oi_shared": sum(m[0] for m in moves),
        "underlying_early": early.get("underlyingPrice"),
        "underlying_late": late.get("underlyingPrice"),
        "top_builds": [m for m in reversed(moves) if m[0] > 0][:5],
        "top_unwinds": [m for m in moves if m[0] < 0][:5],
    }


def last_snapshot(ticker_dir: Path) -> tuple[str, dict] | None:
    """(HHMM stamp, payload) of the last snapshot in a day/ticker dir — that day's settled OI."""
    snaps = sorted(ticker_dir.glob("*.json.gz"))
    return (snaps[-1].stem.split(".")[0], load(snaps[-1])) if snaps else None


def analyze(early_day: str, late_day: str) -> dict[str, dict]:
    early_root, late_root = SNAP_ROOT / early_day, SNAP_ROOT / late_day
    for root in (early_root, late_root):
        if not root.exists():
            sys.exit(f"no snapshots under {root}")
    per_ticker: dict[str, dict] = {}
    for ticker_dir in sorted(p for p in late_root.iterdir() if p.is_dir()):
        early_dir = early_root / ticker_dir.name
        if not early_dir.is_dir():
            continue
        e, l = last_snapshot(early_dir), last_snapshot(ticker_dir)
        if e is None or l is None:
            continue
        d = diff_day(e[1], l[1])
        d["snap_early"], d["snap_late"] = e[0], l[0]
        per_ticker[ticker_dir.name] = d
    return per_ticker


def fmt_moves(moves: list) -> list[str]:
    return [f"    {desc}: {delta:+,} ({e:,} → {l:,})" for delta, _sym, desc, e, l in moves] or ["    (none)"]


def render(early_day: str, late_day: str, per_ticker: dict[str, dict]) -> str:
    gap = (date.fromisoformat(late_day) - date.fromisoformat(early_day)).days
    lines = [
        f"# Day-over-day OI change — {early_day} → {late_day}",
        "",
        "Input to the Phase-2 detection design session: what day-over-day OI-change signal do Schwab",
        "chain snapshots support? OI is settled once daily and static intraday (2026-07-06 measured 0",
        "intraday OI changes), so each day's last snapshot is that session's settled OI.",
        "",
    ]
    if gap > 1:
        lines += [
            f"> **Multi-session delta.** {early_day}→{late_day} spans {gap} calendar days with "
            "un-captured sessions between, so the changes below are a *cumulative* multi-session build, "
            "not a clean D→D+1 confirmation, and contracts expiring in the gap churn out of the shared "
            "set. Production (ADR-0020) captures OI nightly — the consecutive-pair output is the "
            "representative signal.",
            "",
        ]
    for ticker, d in per_ticker.items():
        lines += [
            f"## {ticker}  (underlying {d['underlying_early']} → {d['underlying_late']})",
            "",
            f"- snapshots: {early_day} {d['snap_early']} → {late_day} {d['snap_late']}",
            f"- contracts: {d['contracts_early']} → {d['contracts_late']}  "
            f"(shared {d['shared']}, appeared {d['appeared']}, vanished/expired {d['vanished']})",
            f"- total OI (all contracts): {d['total_oi_early']:,} → {d['total_oi_late']:,}  "
            f"(book net {d['total_oi_late'] - d['total_oi_early']:+,})",
            f"- **net OI change in shared contracts: {d['net_oi_shared']:+,}** ({d['moved']} contracts moved)",
            "- top OI builds:",
            *fmt_moves(d["top_builds"]),
            "- top OI unwinds:",
            *fmt_moves(d["top_unwinds"]),
            "",
        ]
    return "\n".join(lines)


def run_dry() -> int:
    early = {
        "symbol": "SPY", "underlyingPrice": 500.0,
        "callExpDateMap": {"2026-07-20:11": {"560.0": [{"symbol": "SPY C560", "description": "SPY 07/20 560 C", "openInterest": 5000}]}},
        "putExpDateMap": {"2026-07-20:11": {"555.0": [{"symbol": "SPY P555", "description": "SPY 07/20 555 P", "openInterest": 9000}]}},
    }
    late = json.loads(json.dumps(early))
    late["underlyingPrice"] = 503.0
    late["callExpDateMap"]["2026-07-20:11"]["560.0"][0]["openInterest"] = 6200  # +1200 build (shared)
    late["callExpDateMap"]["2026-07-20:11"]["565.0"] = [{"symbol": "SPY C565", "description": "SPY 07/20 565 C", "openInterest": 300}]  # appeared
    del late["putExpDateMap"]["2026-07-20:11"]["555.0"]  # vanished / expired
    d = diff_day(early, late)
    print(json.dumps({k: v for k, v in d.items() if not k.startswith("top_")}, indent=2, default=str))
    assert d["shared"] == 1 and d["appeared"] == 1 and d["vanished"] == 1
    assert d["net_oi_shared"] == 1200  # only the 560 build is a shared move
    assert d["total_oi_late"] - d["total_oi_early"] == -7500  # book incl. churn: 6,500 - 14,000
    print("dry-run OK")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="day-over-day OI change from chain snapshots")
    p.add_argument("--early", help="earlier session, YYYY-MM-DD")
    p.add_argument("--late", help="later session, YYYY-MM-DD")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stdout", action="store_true", help="print the report instead of writing the committed findings md")
    a = p.parse_args()
    if a.dry_run:
        return run_dry()
    if not (a.early and a.late):
        p.error("--early and --late are required (or use --dry-run)")
    report = render(a.early, a.late, analyze(a.early, a.late))
    if a.stdout:
        print(report)
        return 0
    out = FIXTURES_DIR / "chains" / f"oi-change-{a.early}-to-{a.late}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report)
    print(f"findings → {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

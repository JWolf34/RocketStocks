"""Post-session chain-delta analysis — a named Phase-2 design-session input.

For each ticker: walk consecutive snapshots, per-contract deltas of
totalVolume (cumulative daily → delta ≈ 5-min flow), openInterest (expected
static intraday — measure whether that holds), and bid/ask drift. Emits
docs/v2/specs/fixtures/chains/findings-<date>.md (the only committed artifact;
raw snapshots stay local under data/chain-snapshots/).

    .venv/bin/python scripts/spikes/chain_diff.py [--date YYYY-MM-DD]
    .venv/bin/python scripts/spikes/chain_diff.py --dry-run   # synthetic two-snapshot diff
"""

import argparse
import gzip
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import FIXTURES_DIR, today_tag
from chain_spike import SNAP_ROOT, write_snapshot


def contracts(payload: dict) -> dict[str, dict]:
    """Flatten callExpDateMap/putExpDateMap → {contract_symbol: contract}."""
    flat: dict[str, dict] = {}
    for side in ("callExpDateMap", "putExpDateMap"):
        for strikes in payload.get(side, {}).values():
            for contract_list in strikes.values():
                for contract in contract_list:
                    flat[contract["symbol"]] = contract
    return flat


def diff_pair(prev: dict, curr: dict) -> dict:
    """Aggregate deltas between two consecutive snapshots of one ticker."""
    prev_c, curr_c = contracts(prev), contracts(curr)
    shared = prev_c.keys() & curr_c.keys()
    volume_deltas, oi_changes, quote_moves = [], 0, 0
    for symbol in shared:
        d_vol = (curr_c[symbol].get("totalVolume") or 0) - (prev_c[symbol].get("totalVolume") or 0)
        if d_vol > 0:
            volume_deltas.append((d_vol, symbol))
        if curr_c[symbol].get("openInterest") != prev_c[symbol].get("openInterest"):
            oi_changes += 1
        if (curr_c[symbol].get("bid"), curr_c[symbol].get("ask")) != (prev_c[symbol].get("bid"), prev_c[symbol].get("ask")):
            quote_moves += 1
    volume_deltas.sort(reverse=True)
    return {
        "contracts": len(shared),
        "appeared": len(curr_c.keys() - prev_c.keys()),
        "vanished": len(prev_c.keys() - curr_c.keys()),
        "active_contracts": len(volume_deltas),
        "total_volume_delta": sum(d for d, _ in volume_deltas),
        "top_flows": volume_deltas[:5],
        "oi_intraday_changes": oi_changes,
        "quote_moves": quote_moves,
        "negative_vol_deltas": sum(
            1
            for s in shared
            if (curr_c[s].get("totalVolume") or 0) < (prev_c[s].get("totalVolume") or 0)
        ),
    }


def load(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def analyze_ticker(ticker_dir: Path) -> list[tuple[str, str, dict]]:
    snaps = sorted(ticker_dir.glob("*.json.gz"))
    results = []
    for prev_path, curr_path in zip(snaps, snaps[1:]):
        results.append((prev_path.stem.split(".")[0], curr_path.stem.split(".")[0], diff_pair(load(prev_path), load(curr_path))))
    return results


def emit(day_tag: str, per_ticker: dict[str, list]) -> Path:
    lines = [
        f"# Chain-snapshot delta findings — {day_tag}",
        "",
        "Input to the Phase-2 detection design session: what flow inference do 5-min chain deltas support?",
        "",
    ]
    for ticker, diffs in per_ticker.items():
        if not diffs:
            lines.append(f"## {ticker}\n\n(one snapshot only — no deltas)\n")
            continue
        total_flow = sum(d["total_volume_delta"] for _, _, d in diffs)
        oi_total = sum(d["oi_intraday_changes"] for _, _, d in diffs)
        neg = sum(d["negative_vol_deltas"] for _, _, d in diffs)
        busiest = max(diffs, key=lambda r: r[2]["total_volume_delta"])
        lines += [
            f"## {ticker}",
            "",
            f"- snapshot pairs: {len(diffs)} · contracts/snapshot ≈ {diffs[0][2]['contracts']}",
            f"- summed 5-min volume deltas: {total_flow:,} (busiest window {busiest[0]}→{busiest[1]}: {busiest[2]['total_volume_delta']:,})",
            f"- OI changes observed intraday: {oi_total} {'(OI is NOT static intraday — matters for confirmation design)' if oi_total else '(static, as expected — OI confirmation must wait for next-day data)'}",
            f"- negative volume deltas: {neg} {'(cumulative-volume assumption violated — investigate)' if neg else '(cumulative assumption holds)'}",
            "",
        ]
    out = FIXTURES_DIR / "chains" / f"findings-{day_tag}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    return out


def run_dry() -> int:
    day = today_tag()
    base = {
        "symbol": "SPY",
        "callExpDateMap": {"2026-07-10:4": {"560.0": [{"symbol": "SPY C560", "totalVolume": 100, "openInterest": 5000, "bid": 3.1, "ask": 3.2}]}},
        "putExpDateMap": {"2026-07-10:4": {"555.0": [{"symbol": "SPY P555", "totalVolume": 40, "openInterest": 9000, "bid": 2.0, "ask": 2.1}]}},
    }
    later = json.loads(json.dumps(base))
    later["callExpDateMap"]["2026-07-10:4"]["560.0"][0].update(totalVolume=450, bid=3.4, ask=3.5)
    a = write_snapshot("DRYRUN", day, "0000", base)
    b = write_snapshot("DRYRUN", day, "0005", later)
    result = diff_pair(load(a), load(b))
    print(json.dumps(result, indent=2))
    shutil.rmtree(a.parent)
    assert result["total_volume_delta"] == 350 and result["oi_intraday_changes"] == 0
    print("dry-run diff OK")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=today_tag())
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.dry_run:
        return run_dry()

    day_dir = SNAP_ROOT / args.date
    if not day_dir.exists():
        sys.exit(f"no snapshots under {day_dir}")
    per_ticker = {}
    for ticker_dir in sorted(p for p in day_dir.iterdir() if p.is_dir()):
        if not any(ticker_dir.glob("*.json.gz")):
            continue
        per_ticker[ticker_dir.name] = analyze_ticker(ticker_dir)
    out = emit(args.date, per_ticker)
    print(f"findings → {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

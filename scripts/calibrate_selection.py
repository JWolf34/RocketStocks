"""Calibrate the selector on stored Popularity: what it would have posted, day by day.

Read-only and self-contained: standard library plus psycopg, so it runs inside the
deployed ingest container without rebuilding or restarting it, and it prints
aggregates only (no ticker, no row):

    sudo docker compose -f docker-compose.v2.yml exec -T ingest python - < scripts/calibrate_selection.py

It reads `popularity_snapshot` with RS_DB_URL (ingest's role may SELECT it) in a
read-only transaction. For each Eastern day it replays popularity-surge rule version 2
over that day's readings, scores each hit against its stock's own Baseline (median and
spread of the stock's mentions and rank at every polled boundary of the 14 days before the
day began, where a stock absent from a polled listing reads zero mentions at the bottom
rank, one past the last rank stored at that boundary), ranks
the hit among the hits of the 30 days before, and runs the selector's verdict rules
for several bar targets and reply margins.

The rule, the raw score, the percentile, the bar and the verdicts are copies of
rs_analysis.popularity_surge, rs_analysis.popularity_score, rs_analysis.strength and
rs_ingest.selector; services/ingest/tests/test_calibration_script.py keeps them equal.

Output, in order: the window; score percentiles of the hits (overall and by month);
a summary per (bar target, reply margin) setting over every day that has a reference
(Alerts and replies per day); then one line per day for the default setting. Columns:
`ref` reference days found (of 30), `hits` candidates, `stocks` stocks hit that day,
`scored` hits whose stock had a Baseline, `bar` the bar's strength, then the verdicts.
Optional arguments: `--targets 3,5,8 --margins 0.05,0.1 --budget 5 --max-replies 3`.
"""

import argparse
import math
import os
import sys
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, time, timedelta
from statistics import mean, median
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")

# popularity-surge rule version 2
MIN_MENTIONS = 15
MENTION_MULTIPLIER = 3.0
MENTION_MULTIPLIER_TOP = 5.0
TOP_RANK = 25
MENTION_MIN_PRIOR = 15
MENTION_MIN_PRIOR_TAIL = 10
TAIL_RANK = 500
RANK_JUMP_MIN_GAIN = 50
RANK_JUMP_MIN_RATIO = 1.5

# raw score
BASELINE_DAYS = 14
MIN_BASELINE_READINGS = 48
MIN_SPREAD = 0.25
MAD_TO_SIGMA = 1.4826

# strength and bar
REFERENCE_DAYS = 30
MIN_REFERENCE_DAYS = 7
MIN_POLLS_PER_DAY = 24

# selector defaults
DAILY_BUDGET = 5
BAR_TARGET_PER_DAY = 5
REPLY_STRENGTH_MARGIN = 0.05
MAX_REPLIES_PER_STOCK_DAY = 3

DEFAULT_TARGETS = (3, 4, 5, 6, 8)
DEFAULT_MARGINS = (0.02, 0.05, 0.10)


def day_start(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=EASTERN).astimezone(UTC)


def evaluate(rank, mentions, rank_24h_ago, mentions_24h_ago) -> tuple[str, ...]:
    """The sub-rules the reading fires (empty: no hit)."""
    if mentions < MIN_MENTIONS:
        return ()
    multiplier = MENTION_MULTIPLIER_TOP if rank <= TOP_RANK else MENTION_MULTIPLIER
    min_prior = MENTION_MIN_PRIOR_TAIL if rank > TAIL_RANK else MENTION_MIN_PRIOR
    gain = None if rank_24h_ago is None else rank_24h_ago - rank
    fired = []
    if (
        mentions_24h_ago is not None
        and mentions_24h_ago >= min_prior
        and mentions >= multiplier * mentions_24h_ago
    ):
        fired.append("mention_surge")
    if gain is not None and gain >= RANK_JUMP_MIN_GAIN and gain >= RANK_JUMP_MIN_RATIO * rank:
        fired.append("rank_jump")
    return tuple(fired)


def raw_score(rank, mentions, normal) -> float:
    """`normal` is (mentions_median, mentions_spread, rank_median, rank_spread) from `normal_of`."""
    mentions_median, mentions_spread, rank_median, rank_spread = normal
    mentions_distance = (math.log1p(mentions) - mentions_median) / mentions_spread
    rank_distance = (rank_median - math.log(rank)) / rank_spread
    return max(mentions_distance, 0.0) + max(rank_distance, 0.0)


def normal_of(readings, mentions_median, mentions_mad, rank_median, rank_mad):
    """A Baseline from the database's medians, or None under MIN_BASELINE_READINGS readings."""
    if readings < MIN_BASELINE_READINGS:
        return None
    return (
        mentions_median,
        max(MAD_TO_SIGMA * mentions_mad, MIN_SPREAD),
        rank_median,
        max(MAD_TO_SIGMA * rank_mad, MIN_SPREAD),
    )


def bar_score_for(per_day_best: list[list[float]], target: int) -> float:
    """The strongest score that a typical day still has `target` stocks at or above
    (the weakest score when none does). Each list is one polled day's per-stock best, ascending."""

    def typical(score: float) -> float:
        return median(len(day) - bisect_left(day, score) for day in per_day_best)

    candidates = sorted({score for day in per_day_best for score in day})
    low, high = 0, len(candidates) - 1
    if typical(candidates[0]) < target:
        high = 0
    while low < high:
        middle = (low + high + 1) // 2
        if typical(candidates[middle]) >= target:
            low = middle
        else:
            high = middle - 1
    return candidates[low]


def verdicts_of_day(hits, bar, margin, budget, max_replies) -> Counter:
    """The selector's verdict on each hit of one day, as counts. `hits` are
    (polled_at, symbol, fired, strength) with strength None for a stock with no Baseline."""
    counts: Counter = Counter()
    alerts = 0
    stocks: dict[str, tuple[set[str], float | None, int]] = {}
    ordered = sorted(hits, key=lambda h: (h[0], -(h[3] or 0.0), h[1]))
    for _, symbol, fired, strength in ordered:
        stock = stocks.get(symbol)
        if stock is not None:
            carried, announced, replies = stock
            new_rules = [rule for rule in fired if rule not in carried]
            strength_up = strength is not None and announced is not None and strength >= announced + margin
            if (new_rules or strength_up) and replies < max_replies:
                counts["reply"] += 1
                best = [s for s in (announced, strength) if s is not None]
                stocks[symbol] = (carried | set(fired), max(best, default=None), replies + 1)
            else:
                counts["repeat"] += 1
        elif strength is None or strength < bar:
            counts["below_bar"] += 1
        elif alerts >= budget:
            counts["over_budget"] += 1
        else:
            counts["posted"] += 1
            alerts += 1
            stocks[symbol] = (set(fired), strength, 0)
    return counts


def eastern_days(first: datetime, last: datetime) -> list[date]:
    days = []
    day = first.astimezone(EASTERN).date()
    while day_start(day) <= last:
        days.append(day)
        day += timedelta(days=1)
    return days


def load(conn, days, say):
    """Per Eastern day: the polls it had and its hits as (polled_at, symbol, fired, raw score or None)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT (polled_at AT TIME ZONE 'America/New_York')::date, count(DISTINCT polled_at)"
            " FROM popularity_snapshot GROUP BY 1"
        )
        polls = dict(cur.fetchall())
        cur.execute("SELECT polled_at, max(rank) + 1 FROM popularity_snapshot GROUP BY polled_at")
        bottoms = dict(cur.fetchall())
        hits_by_day: dict[date, list] = {}
        for index, day in enumerate(days):
            cur.execute(
                "SELECT polled_at, ticker, rank, mentions, rank_24h_ago, mentions_24h_ago FROM popularity_snapshot"
                " WHERE polled_at >= %s AND polled_at < %s AND mentions >= %s",
                (day_start(day), day_start(day + timedelta(days=1)), MIN_MENTIONS),
            )
            events = []
            for polled_at, ticker, rank, mentions, rank_24h_ago, mentions_24h_ago in cur.fetchall():
                fired = evaluate(rank, mentions, rank_24h_ago, mentions_24h_ago)
                if fired:
                    events.append((polled_at, ticker, fired, rank, mentions))
            normals = baselines(cur, day, {event[1] for event in events}, bottoms)
            hits_by_day[day] = [
                (
                    polled_at,
                    ticker,
                    fired,
                    None if normals.get(ticker) is None else raw_score(rank, mentions, normals[ticker]),
                )
                for polled_at, ticker, fired, rank, mentions in events
            ]
            if index % 20 == 0:
                say(f"loaded {day} ({index + 1} of {len(days)})")
    return polls, hits_by_day


def baselines(cur, day, tickers, bottoms):
    """Each ticker's Baseline as of the start of `day`, from medians computed by the database.

    A reading exists at every polled boundary of the window (`bottoms` maps each polled
    boundary to the bottom rank there); where a ticker has no snapshot it is absent: zero
    mentions at the bottom rank. A boundary with no snapshot at all is downtime and is not in
    `bottoms`."""
    start, end = day_start(day - timedelta(days=BASELINE_DAYS)), day_start(day)
    window = [(at, bottom) for at, bottom in bottoms.items() if start <= at < end]
    if not tickers or not window:
        return {}
    cur.execute(
        """
        WITH b AS (
            SELECT * FROM unnest(%s::timestamptz[], %s::int[]) AS b(polled_at, bottom)
        ), h AS (
            SELECT t.ticker, ln(1 + coalesce(s.mentions, 0)) AS lm, ln(coalesce(s.rank, b.bottom)) AS lr
            FROM b CROSS JOIN unnest(%s::text[]) AS t(ticker)
            LEFT JOIN popularity_snapshot s ON s.polled_at = b.polled_at AND s.ticker = t.ticker
        ), c AS (
            SELECT ticker, count(*) AS n,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY lm) AS mm,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY lr) AS mr
            FROM h GROUP BY ticker
        )
        SELECT h.ticker, c.n, c.mm, percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(h.lm - c.mm)),
               c.mr, percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(h.lr - c.mr))
        FROM h JOIN c USING (ticker) GROUP BY h.ticker, c.n, c.mm, c.mr
        """,
        ([at for at, _ in window], [bottom for _, bottom in window], sorted(tickers)),
    )
    return {ticker: normal_of(n, mm, madm, mr, madr) for ticker, n, mm, madm, mr, madr in cur.fetchall()}


def analyse(polls, hits_by_day, days, targets, margins, budget, max_replies):
    """Per day with a reference: its candidates and the verdicts for every setting."""
    results = []
    for day in days:
        window = [day - timedelta(days=back) for back in range(REFERENCE_DAYS, 0, -1)]
        polled = [d for d in window if polls.get(d, 0) >= MIN_POLLS_PER_DAY]
        today = hits_by_day.get(day, [])
        row = {
            "day": day,
            "ref": len(polled),
            "hits": len(today),
            "stocks": len({hit[1] for hit in today}),
            "scored": sum(hit[3] is not None for hit in today),
            "settings": {},
        }
        results.append(row)
        reference = [hit for d in window for hit in hits_by_day.get(d, []) if hit[3] is not None]
        if len(polled) < MIN_REFERENCE_DAYS or not reference:
            continue
        scores = sorted(hit[3] for hit in reference)
        best: dict[tuple[date, str], float] = {}
        for d in window:
            for _, symbol, _, score in hits_by_day.get(d, []):
                if score is not None:
                    best[(d, symbol)] = max(score, best.get((d, symbol), score))
        by_day: dict[date, list[float]] = defaultdict(list)
        for (d, _), score in best.items():
            by_day[d].append(score)
        per_day_best = [sorted(by_day[polled_day]) for polled_day in polled]
        strengthened = [
            (polled_at, symbol, fired, None if score is None else bisect_right(scores, score) / len(scores))
            for polled_at, symbol, fired, score in today
        ]
        for target in targets:
            bar_score = bar_score_for(per_day_best, target)
            bar = bisect_right(scores, bar_score) / len(scores)
            for margin in margins:
                row["settings"][(target, margin)] = (bar, verdicts_of_day(strengthened, bar, margin, budget, max_replies))
    return results


def percentile(values: list[float], share: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(share * len(ordered)))]


def report(polls, hits_by_day, results, targets, margins, budget, default, out):
    scores = [hit[3] for hits in hits_by_day.values() for hit in hits if hit[3] is not None]
    every = sum(len(hits) for hits in hits_by_day.values())
    out(f"window: {min(hits_by_day)} to {max(hits_by_day)} ({len(hits_by_day)} Eastern days)")
    out(f"hits: {every}, with a Baseline: {len(scores)} ({100 * len(scores) / max(every, 1):.1f}%)")
    out("")
    out("raw score percentiles of the hits (overall, then by month)")
    out("  scope     n       p50    p75    p90    p95    p99    max")
    by_month: dict[str, list[float]] = defaultdict(list)
    for day, hits in hits_by_day.items():
        by_month[f"{day:%Y-%m}"].extend(hit[3] for hit in hits if hit[3] is not None)
    for scope, values in [("all", scores), *sorted(by_month.items())]:
        if values:
            line = "  ".join(f"{percentile(values, p):6.2f}" for p in (0.5, 0.75, 0.9, 0.95, 0.99, 1.0))
            out(f"  {scope:8} {len(values):7d}  {line}")
    out("")
    judged = [r for r in results if r["settings"]]
    out(f"settings over {len(judged)} day(s) with a reference (a reference needs {MIN_REFERENCE_DAYS}+ polled days of 30)")
    out(f"  budget {budget} a day; alerts are posted Alerts, replies never use the budget")
    out("  target margin | alerts/day mean med p90 max | days over budget  none | replies/day mean max | replies per alert")
    for target in targets:
        for margin in margins:
            alerts = [r["settings"][(target, margin)][1]["posted"] for r in judged]
            replies = [r["settings"][(target, margin)][1]["reply"] for r in judged]
            over = sum(r["settings"][(target, margin)][1]["over_budget"] > 0 for r in judged)
            out(
                f"  {target:6d} {margin:6.2f} | {mean(alerts):10.2f} {median(alerts):3.0f} {percentile(alerts, 0.9):3.0f}"
                f" {max(alerts):3d} | {over:16d} {sum(a == 0 for a in alerts):5d} |"
                f" {mean(replies):12.2f} {max(replies):3d} | {sum(replies) / max(sum(alerts), 1):.2f}"
            )
    out("")
    out(f"per day, setting: target {default[0]}, margin {default[1]}")
    out("  day         ref  hits stocks scored    bar | alerts replies below_bar over_budget repeat")
    for r in results:
        setting = r["settings"].get(default)
        if setting is None:
            out(f"  {r['day']}  {r['ref']:3d} {r['hits']:5d} {r['stocks']:6d} {r['scored']:6d}      - | (no reference)")
            continue
        bar, counts = setting
        out(
            f"  {r['day']}  {r['ref']:3d} {r['hits']:5d} {r['stocks']:6d} {r['scored']:6d} {bar:6.3f} |"
            f" {counts['posted']:6d} {counts['reply']:7d} {counts['below_bar']:9d} {counts['over_budget']:11d} {counts['repeat']:6d}"
        )


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", default=",".join(map(str, DEFAULT_TARGETS)))
    parser.add_argument("--margins", default=",".join(map(str, DEFAULT_MARGINS)))
    parser.add_argument("--budget", type=int, default=DAILY_BUDGET)
    parser.add_argument("--max-replies", type=int, default=MAX_REPLIES_PER_STOCK_DAY)
    args = parser.parse_args(argv)
    targets = tuple(int(t) for t in args.targets.split(","))
    margins = tuple(float(m) for m in args.margins.split(","))
    default = (BAR_TARGET_PER_DAY if BAR_TARGET_PER_DAY in targets else targets[0],
               REPLY_STRENGTH_MARGIN if REPLY_STRENGTH_MARGIN in margins else margins[0])

    import psycopg

    def say(message: str) -> None:
        print(message, file=sys.stderr, flush=True)

    dsn = os.environ["RS_DB_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(dsn) as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            cur.execute("SELECT min(polled_at), max(polled_at) FROM popularity_snapshot")
            first, last = cur.fetchone()
        days = eastern_days(first, last)
        say(f"{len(days)} Eastern days from {days[0]} to {days[-1]}")
        polls, hits_by_day = load(conn, days, say)
    results = analyse(polls, hits_by_day, days, targets, margins, args.budget, args.max_replies)
    report(polls, hits_by_day, results, targets, margins, args.budget, default, print)


if __name__ == "__main__":
    main(sys.argv[1:])

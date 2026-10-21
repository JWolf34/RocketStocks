"""scripts/calibrate_selection.py is self-contained so it can run inside the deployed image;
these tests keep its copies of the rule, the score, the bar and the verdicts equal to the
code that ships, and run it end to end over synthetic history."""

import importlib.util
import itertools
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from rs_analysis.popularity_surge import (
    SubRule,
    SurgeEvent,
    SurgeInputs,
    evaluate_latest,
)
from rs_domain import PopularityOrigin, PopularitySnapshot, Verdict
from rs_ingest import selector
from rs_ingest.scoring import PopularityScorer
from rs_persistence import PopularityRepository

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "calibrate_selection.py"
spec = importlib.util.spec_from_file_location("calibrate_selection", SCRIPT)
assert spec is not None and spec.loader is not None
calibrate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calibrate)

T = datetime(2026, 10, 19, 14, 0, tzinfo=UTC)


def snapshot(rank, mentions, rank_24h_ago, mentions_24h_ago) -> PopularitySnapshot:
    return PopularitySnapshot(
        polled_at=T,
        ticker="X",
        rank=rank,
        mentions=mentions,
        rank_24h_ago=rank_24h_ago,
        mentions_24h_ago=mentions_24h_ago,
        fetched_at=T,
        origin=PopularityOrigin.LIVE,
    )


def test_the_copied_rule_and_constants_equal_the_shipped_ones():
    for rank, mentions, rank_ago, mentions_ago in itertools.product(
        (1, 10, 25, 26, 100, 500, 501, 900), (0, 14, 15, 30, 75, 76, 150, 400), (None, 5, 60, 200, 700), (None, 0, 9, 10, 15, 40)
    ):
        expected = evaluate_latest(snapshot(rank, mentions, rank_ago, mentions_ago))
        found = calibrate.evaluate(rank, mentions, rank_ago, mentions_ago)
        assert found == (() if expected is None else tuple(rule.value for rule in expected.fired))

    from rs_analysis import popularity_score, strength

    assert (calibrate.BASELINE_DAYS, calibrate.MIN_BASELINE_READINGS, calibrate.MIN_SPREAD, calibrate.MAD_TO_SIGMA) == (
        popularity_score.BASELINE_DAYS,
        popularity_score.MIN_BASELINE_READINGS,
        popularity_score.MIN_SPREAD,
        popularity_score.MAD_TO_SIGMA,
    )
    assert (calibrate.REFERENCE_DAYS, calibrate.MIN_REFERENCE_DAYS) == (strength.REFERENCE_DAYS, strength.MIN_REFERENCE_DAYS)
    assert (
        calibrate.DAILY_BUDGET,
        calibrate.BAR_TARGET_PER_DAY,
        calibrate.REPLY_STRENGTH_MARGIN,
        calibrate.MAX_REPLIES_PER_STOCK_DAY,
    ) == (
        selector.DAILY_BUDGET,
        selector.BAR_TARGET_PER_DAY,
        selector.REPLY_STRENGTH_MARGIN,
        selector.MAX_REPLIES_PER_STOCK_DAY,
    )


def shipped_counts(hits, bar, margin, budget, max_replies) -> dict[str, int]:
    """The same day run through rs_ingest.selector."""
    counts: dict[str, int] = {}
    alerts = 0
    stocks: dict[str, selector.StockDay] = {}
    for polled_at, symbol, fired, strength in sorted(hits, key=lambda h: (h[0], -(h[3] or 0.0), h[1])):
        event = SurgeEvent(
            ticker=symbol,
            polled_at=polled_at,
            fired=tuple(SubRule(rule) for rule in fired),
            inputs=SurgeInputs(
                rank=10, mentions=50, rank_24h_ago=None, mentions_24h_ago=None, rank_gain=None,
                mention_ratio=None, mention_multiplier=3.0, mention_min_prior=15,
            ),
        )
        stock = stocks.get(symbol)
        verdict = selector.select(
            event, strength=strength, bar=bar, day=selector.DayState(alerts=alerts, stock=stock), decided_at=polled_at
        )
        if verdict is Verdict.POSTED:
            alerts += 1
        if verdict in (Verdict.POSTED, Verdict.REPLY):
            stocks[symbol] = selector.announce(stock, fired, strength, reply=verdict is Verdict.REPLY)
        counts[verdict.value] = counts.get(verdict.value, 0) + 1
    return counts


def test_the_copied_verdict_rules_equal_the_selector_with_any_budget_margin_and_cap(monkeypatch):
    rng = random.Random(7)
    rules = ("mention_surge", "rank_jump")
    for _ in range(300):
        hits = [
            (
                T + timedelta(minutes=30 * rng.randrange(6)),
                f"S{rng.randrange(8)}",
                tuple(rule for rule in rules if rng.random() < 0.5) or ("mention_surge",),
                None if rng.random() < 0.1 else rng.random(),
            )
            for _ in range(rng.randrange(1, 40))
        ]
        bar, margin, budget, cap = rng.random(), rng.choice((0.02, 0.05, 0.1)), rng.randrange(1, 7), rng.randrange(1, 5)
        monkeypatch.setattr(selector, "REPLY_STRENGTH_MARGIN", margin)
        monkeypatch.setattr(selector, "DAILY_BUDGET", budget)
        monkeypatch.setattr(selector, "MAX_REPLIES_PER_STOCK_DAY", cap)

        assert dict(calibrate.verdicts_of_day(hits, bar, margin, budget, cap)) == shipped_counts(hits, bar, margin, budget, cap)


DAY = date(2026, 10, 20)
STOCKS = 12


def seeded_rows() -> list[PopularitySnapshot]:
    """45 days of steady stocks; stock s (0-9) and stock 11 surge once a day at 15:00 UTC to
    60 + 10 s mentions. Stock 11 is listed only from 16:00 UTC on, so it is absent from most of
    its Baseline window, and NEW appears for the first time at 15:00 UTC on the day before DAY."""
    rows = []
    for back in range(45, 0, -1):
        day = DAY - timedelta(days=back)
        start = datetime(day.year, day.month, day.day, tzinfo=UTC)
        for half_hour in range(48):
            polled_at = start + timedelta(minutes=30 * half_hour)
            for stock in range(STOCKS):
                surging = (stock < 10 or stock == 11) and polled_at.hour == 15 and polled_at.minute == 0
                if stock == 11 and polled_at.hour < 16 and not surging:
                    continue
                rows.append(_snapshot(stock, polled_at, 60 + 10 * stock if surging else 20 + stock % 3))
    newcomer_at = datetime(DAY.year, DAY.month, DAY.day, 15, 0, tzinfo=UTC) - timedelta(days=1)
    rows.append(
        PopularitySnapshot(
            polled_at=newcomer_at, ticker="NEW", rank=50, mentions=90, rank_24h_ago=50, mentions_24h_ago=20,
            fetched_at=newcomer_at, origin=PopularityOrigin.V1_IMPORT,
        )
    )
    return rows


def _snapshot(stock: int, polled_at: datetime, mentions: int) -> PopularitySnapshot:
    return PopularitySnapshot(
        polled_at=polled_at,
        ticker=f"T{stock:02d}",
        rank=100 + stock,
        mentions=mentions,
        rank_24h_ago=100 + stock,
        mentions_24h_ago=20 + stock % 3,
        fetched_at=polled_at,
        origin=PopularityOrigin.V1_IMPORT,
    )


async def test_run_over_stored_history_scores_and_sets_the_bar_as_the_shipped_scorer_does(session_factory, migrated_db_url):
    async with session_factory() as session:
        await PopularityRepository(session).insert_many(seeded_rows())
        await session.commit()
    day = DAY - timedelta(days=1)
    dsn = migrated_db_url.replace("postgresql+psycopg://", "postgresql://", 1)

    with psycopg.connect(dsn) as conn:
        conn.read_only = True
        days = calibrate.eastern_days(*_extent(conn))
        polls, hits_by_day = calibrate.load(conn, days, lambda _: None)
    results = calibrate.analyse(polls, hits_by_day, days, (5,), (0.05,), 5, 3)

    [result] = [r for r in results if r["day"] == day]
    bar, counts = result["settings"][(5, 0.05)]
    scorer = PopularityScorer(session_factory, bar_target=5)
    events = [
        event
        for polled_at, ticker, *_ in hits_by_day[day]
        if (event := evaluate_latest(_reading(ticker, polled_at))) is not None
    ]
    shipped = await scorer.score(events)
    assert result["ref"] == 30
    assert bar == pytest.approx(shipped[0].bar)
    assert sorted(round(score, 9) for *_, score in hits_by_day[day]) == pytest.approx(
        sorted(round(s.raw_score, 9) for s in shipped)
    )
    assert any(ticker == "NEW" for _, ticker, *_ in hits_by_day[day])  # a newcomer is scored, not skipped
    assert all(score is not None for *_, score in hits_by_day[day])
    assert counts["posted"] == 5  # the target a day


def _extent(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT min(polled_at), max(polled_at) FROM popularity_snapshot")
        return cur.fetchone()


def _reading(ticker: str, polled_at: datetime) -> PopularitySnapshot:
    if ticker == "NEW":
        return PopularitySnapshot(
            polled_at=polled_at, ticker="NEW", rank=50, mentions=90, rank_24h_ago=50, mentions_24h_ago=20,
            fetched_at=polled_at, origin=PopularityOrigin.V1_IMPORT,
        )
    stock = int(ticker[1:])
    return _snapshot(stock, polled_at, 60 + 10 * stock)

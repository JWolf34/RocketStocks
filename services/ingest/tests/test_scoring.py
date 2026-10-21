"""The scorer against stored Popularity: the reference is a replay of the rule over the
30 Eastern days before the hit's day, the Baseline is the stock's own, and neither
moves while the day goes on."""

import math
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import text

from rs_analysis.popularity_score import MIN_SPREAD
from rs_analysis.popularity_surge import SurgeEvent, evaluate_latest
from rs_analysis.strength import ReferenceUnavailable
from rs_domain import PopularityOrigin, PopularitySnapshot
from rs_ingest.scoring import PopularityScorer
from rs_persistence import PopularityRepository

DAY = date(2026, 10, 20)
HISTORY_DAYS = 45
SURGERS = 10
TARGET = 5
STEADY = 20  # mentions a stock normally has


def mentions_of(stock: int) -> int:
    """What stock `stock` has when it surges: the higher the number, the further from normal."""
    return 60 + 10 * stock


def snapshot(stock: int, polled_at: datetime, mentions: int = STEADY, prior: int = STEADY) -> PopularitySnapshot:
    return PopularitySnapshot(
        polled_at=polled_at,
        ticker=f"T{stock:02d}",
        rank=100 + stock,
        mentions=mentions,
        rank_24h_ago=100 + stock,
        mentions_24h_ago=prior,
        fetched_at=polled_at,
        origin=PopularityOrigin.V1_IMPORT,
    )


def history(*, quiet_days: frozenset[date] = frozenset(), surgers: int = SURGERS, days: int = HISTORY_DAYS):
    """Every stock steady at every half hour of `days` UTC days before DAY, except that on each
    day stock s surges once at 15:00 UTC (stock 0 also at 15:30). Days in `quiet_days` have no polls."""
    rows = []
    for back in range(days, 0, -1):
        day = DAY - timedelta(days=back)
        if day in quiet_days:
            continue
        start = datetime(day.year, day.month, day.day, tzinfo=UTC)
        for half_hour in range(48):
            polled_at = start + timedelta(minutes=30 * half_hour)
            for stock in range(surgers + 2):
                surging = stock < surgers and (
                    polled_at.hour == 15 and (polled_at.minute == 0 or (stock == 0 and polled_at.minute == 30))
                )
                rows.append(snapshot(stock, polled_at, mentions_of(stock) if surging else STEADY))
    return rows


async def store(session_factory, rows: list[PopularitySnapshot]) -> None:
    async with session_factory() as session:
        await PopularityRepository(session).insert_many(rows)
        await session.commit()


def hit(stock: int, at: datetime | None = None, mentions: int | None = None) -> SurgeEvent:
    at = at or datetime(DAY.year, DAY.month, DAY.day, 15, 0, tzinfo=UTC)
    event = evaluate_latest(snapshot(stock, at, mentions or mentions_of(stock)))
    assert event is not None
    return event


def expected_score(stock: int, steady_mentions: int = STEADY) -> float:
    return (math.log1p(mentions_of(stock)) - math.log1p(steady_mentions)) / MIN_SPREAD


class CountingFactory:
    def __init__(self, real) -> None:
        self.real = real
        self.opened = 0

    def __call__(self):
        self.opened += 1
        return self.real()


@pytest.fixture
async def seeded(session_factory):
    await store(session_factory, history())
    return PopularityScorer(session_factory, bar_target=TARGET)


async def test_the_raw_score_is_the_distance_from_the_stocks_own_normal(seeded):
    [scored] = await seeded.score([hit(7)])

    assert scored.raw_score == pytest.approx(expected_score(7))


async def test_the_same_mentions_are_further_out_for_a_quieter_stock(session_factory):
    rows = history()
    # T11 normally has 200 mentions, so the 90 that T03 surges to is nothing for it.
    rows = [r for r in rows if r.ticker != "T11"] + [
        snapshot(11, r.polled_at, mentions=200) for r in rows if r.ticker == "T11"
    ]
    await store(session_factory, rows)
    scorer = PopularityScorer(session_factory, bar_target=TARGET)

    [quiet, busy] = await scorer.score([hit(3, mentions=90), hit(11, mentions=90)])

    assert quiet.raw_score is not None and quiet.raw_score > 5
    assert busy.raw_score == 0


async def test_strength_is_the_percentile_among_the_triggers_hits_of_the_trailing_30_days(seeded):
    [weakest, strongest, beyond] = await seeded.score([hit(0), hit(9), hit(9, mentions=10_000)])

    # Eleven hits a day (stock 0 hits twice), 30 days: stock 0 is the weakest.
    assert weakest.strength == pytest.approx(2 / 11)
    assert strongest.strength == 1.0
    assert beyond.strength == 1.0


async def test_the_bar_gives_a_typical_day_the_target_number_of_stocks(seeded):
    scored = await seeded.score([hit(stock) for stock in range(SURGERS)])

    clearing = [stock for stock, s in enumerate(scored) if s.strength is not None and s.strength >= s.bar]
    assert clearing == [5, 6, 7, 8, 9]  # the target a day, each the stock's strongest hit


async def test_a_stock_with_no_baseline_has_no_score_but_still_the_days_bar(session_factory):
    await store(session_factory, history())
    scorer = PopularityScorer(session_factory, bar_target=TARGET)
    newcomer = evaluate_latest(
        PopularitySnapshot(
            polled_at=datetime(2026, 10, 20, 15, 0, tzinfo=UTC), ticker="NEWCO", rank=30, mentions=90,
            mentions_24h_ago=20, fetched_at=datetime(2026, 10, 20, 15, 0, tzinfo=UTC), origin=PopularityOrigin.LIVE,
        )
    )
    assert newcomer is not None

    [with_baseline, without] = await scorer.score([hit(9), newcomer])

    assert (without.raw_score, without.strength) == (None, None)
    assert without.bar == with_baseline.bar


async def test_the_days_own_readings_do_not_move_the_baseline(session_factory):
    await store(session_factory, history())
    control = await PopularityScorer(session_factory, bar_target=TARGET).score([hit(9)])
    # Today the stock has been surging for hours before the hit is scored.
    start = datetime(DAY.year, DAY.month, DAY.day, 5, 0, tzinfo=UTC)
    await store(session_factory, [snapshot(9, start + timedelta(minutes=30 * i), mentions=500) for i in range(20)])

    [scored] = await PopularityScorer(session_factory, bar_target=TARGET).score([hit(9)])

    assert scored == control[0]


async def test_a_stretch_without_polls_changes_neither_strengths_nor_the_bar(session_factory):
    await store(session_factory, history())
    [control] = await PopularityScorer(session_factory, bar_target=TARGET).score([hit(5)])
    gap = frozenset(DAY - timedelta(days=n) for n in range(8, 14))  # six days ingest was down
    async with session_factory() as session:
        await session.execute(text("TRUNCATE popularity_snapshot"))
        await session.commit()
    await store(session_factory, history(quiet_days=gap))

    [scored] = await PopularityScorer(session_factory, bar_target=TARGET).score([hit(5)])

    assert scored.bar == pytest.approx(control.bar)
    assert scored.strength == pytest.approx(control.strength)


async def test_too_little_history_to_rank_against_is_refused(session_factory):
    await store(session_factory, history(days=5))
    scorer = PopularityScorer(session_factory, bar_target=TARGET)

    with pytest.raises(ReferenceUnavailable, match="day"):
        await scorer.score([hit(9)])


async def test_a_days_reference_and_baselines_are_computed_once(session_factory):
    await store(session_factory, history())
    counting = CountingFactory(session_factory)
    scorer = PopularityScorer(counting, bar_target=TARGET)
    [first] = await scorer.score([hit(9)])
    opened = counting.opened

    [again] = await scorer.score([hit(9, at=datetime(DAY.year, DAY.month, DAY.day, 15, 30, tzinfo=UTC))])

    assert counting.opened == opened
    assert again == first


async def test_the_next_eastern_day_replays_one_more_day_not_thirty(session_factory):
    await store(session_factory, history())
    counting = CountingFactory(session_factory)
    scorer = PopularityScorer(counting, bar_target=TARGET)
    await scorer.score([hit(9)])
    cold = counting.opened
    tomorrow = datetime(DAY.year, DAY.month, DAY.day, 15, 0, tzinfo=UTC) + timedelta(days=1)

    await scorer.score([hit(9, at=tomorrow)])

    assert 0 < counting.opened - cold < cold / 5

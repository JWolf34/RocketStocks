"""Tests for the 15 weekly award functions and evaluate_weekly_awards."""
import datetime
import pytest

from rocketstocks.core.analysis.paper_trading import (
    evaluate_weekly_awards,
    _biggest_gainer,
    _biggest_loser,
    _sniper,
    _comeback_kid,
    _diamond_hands,
    _paper_hands,
    _yolo,
    _scared_money,
    _trendsetter,
    _contrarian,
    _bought_the_dip,
    _bought_the_top,
    _diversification,
    _day_trader,
    _ghost_trader,
)
from rocketstocks.core.content.models import WeeklyAward


# ---------------------------------------------------------------------------
# Shared fixtures / factories
# ---------------------------------------------------------------------------

_NAMES = {1: "Alice", 2: "Bob", 3: "Carol"}

_MON = datetime.date(2026, 3, 23)
_TUE = datetime.date(2026, 3, 24)
_WED = datetime.date(2026, 3, 25)
_THU = datetime.date(2026, 3, 26)
_FRI = datetime.date(2026, 3, 27)


def _snap(user_id: int, date: datetime.date, value: float) -> dict:
    return {
        'user_id': user_id,
        'snapshot_date': date,
        'portfolio_value': value,
        'cash': value * 0.5,
        'positions_value': value * 0.5,
    }


def _tx(
    user_id: int,
    ticker: str,
    side: str,
    price: float,
    shares: int = 10,
    dt: datetime.datetime | None = None,
) -> dict:
    if dt is None:
        dt = datetime.datetime(2026, 3, 24, 10, 0, tzinfo=datetime.timezone.utc)
    return {
        'user_id': user_id,
        'ticker': ticker,
        'side': side,
        'shares': shares,
        'price': price,
        'total': price * shares,
        'executed_at': dt,
    }


def _pos(ticker: str, shares: int, avg_cost: float, market_value: float) -> dict:
    return {
        'ticker': ticker,
        'shares': shares,
        'avg_cost_basis': avg_cost,
        'market_value': market_value,
    }


def _ph_row(date: datetime.date, close: float, low: float | None = None, high: float | None = None) -> dict:
    return {
        'date': date,
        'open': close,
        'high': high if high is not None else close * 1.01,
        'low': low if low is not None else close * 0.99,
        'close': close,
    }


# ---------------------------------------------------------------------------
# _biggest_gainer
# ---------------------------------------------------------------------------

def test_biggest_gainer_winner():
    snaps = {
        1: [_snap(1, _MON, 10000), _snap(1, _FRI, 11500)],  # +15%
        2: [_snap(2, _MON, 10000), _snap(2, _FRI, 10500)],  # +5%
    }
    award = _biggest_gainer(snaps, _NAMES)
    assert award.winner_name == "Alice"
    assert "+15" in award.detail


def test_biggest_gainer_no_winner_insufficient_snaps():
    snaps = {1: [_snap(1, _MON, 10000)]}  # only 1 snap
    award = _biggest_gainer(snaps, _NAMES)
    assert award.winner_name is None


def test_biggest_gainer_empty():
    award = _biggest_gainer({}, _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Biggest Gainer"


# ---------------------------------------------------------------------------
# _biggest_loser
# ---------------------------------------------------------------------------

def test_biggest_loser_winner():
    snaps = {
        1: [_snap(1, _MON, 10000), _snap(1, _FRI, 9000)],   # -10%
        2: [_snap(2, _MON, 10000), _snap(2, _FRI, 9500)],   # -5%
    }
    award = _biggest_loser(snaps, _NAMES)
    assert award.winner_name == "Alice"
    assert "-10" in award.detail


def test_biggest_loser_empty():
    award = _biggest_loser({}, _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Biggest Loser"


# ---------------------------------------------------------------------------
# _sniper
# ---------------------------------------------------------------------------

def test_sniper_basic():
    buy_dt = datetime.datetime(2026, 3, 24, 10, 0, tzinfo=datetime.timezone.utc)
    sell_dt = datetime.datetime(2026, 3, 26, 15, 0, tzinfo=datetime.timezone.utc)
    txs = [
        _tx(1, "AAPL", "BUY", 100.0, dt=buy_dt),
        _tx(1, "AAPL", "SELL", 120.0, dt=sell_dt),  # +20%
        _tx(2, "TSLA", "BUY", 200.0, dt=buy_dt),
        _tx(2, "TSLA", "SELL", 210.0, dt=sell_dt),  # +5%
    ]
    award = _sniper(txs, _NAMES)
    assert award.winner_name == "Alice"
    assert "AAPL" in award.detail
    assert "+20" in award.detail


def test_sniper_no_completed_roundtrip():
    txs = [_tx(1, "AAPL", "BUY", 100.0)]  # no matching sell
    award = _sniper(txs, _NAMES)
    assert award.winner_name is None


def test_sniper_empty():
    award = _sniper([], _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Sniper"


# ---------------------------------------------------------------------------
# _comeback_kid
# ---------------------------------------------------------------------------

def test_comeback_kid_winner():
    snaps = {
        1: [
            _snap(1, _MON, 10000),
            _snap(1, _TUE, 8000),   # mid-week low
            _snap(1, _WED, 8500),
            _snap(1, _FRI, 10200),  # recovered
        ],
        2: [
            _snap(2, _MON, 10000),
            _snap(2, _TUE, 9500),
            _snap(2, _FRI, 9800),
        ],
    }
    award = _comeback_kid(snaps, _NAMES)
    assert award.winner_name == "Alice"
    assert "Down" in award.detail


def test_comeback_kid_no_winner_not_down_midweek():
    snaps = {
        1: [
            _snap(1, _MON, 10000),
            _snap(1, _WED, 10500),  # always going up
            _snap(1, _FRI, 11000),
        ]
    }
    award = _comeback_kid(snaps, _NAMES)
    assert award.winner_name is None


def test_comeback_kid_needs_3_snaps():
    snaps = {1: [_snap(1, _MON, 10000), _snap(1, _FRI, 9000)]}
    award = _comeback_kid(snaps, _NAMES)
    assert award.winner_name is None


def test_comeback_kid_empty():
    award = _comeback_kid({}, _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Comeback Kid"


# ---------------------------------------------------------------------------
# _diamond_hands
# ---------------------------------------------------------------------------

def test_diamond_hands_winner():
    positions = {1: [_pos("AAPL", 10, 100.0, 950.0)]}
    ph = {
        "AAPL": [
            _ph_row(_MON, 100.0, low=93.0),   # -7% below cost → qualifies
            _ph_row(_FRI, 102.0),
        ]
    }
    award = _diamond_hands(positions, ph, _NAMES)
    assert award.winner_name == "Alice"
    assert "AAPL" in award.detail


def test_diamond_hands_no_winner_shallow_drawdown():
    positions = {1: [_pos("AAPL", 10, 100.0, 970.0)]}
    ph = {"AAPL": [_ph_row(_MON, 100.0, low=97.0)]}  # only -3%
    award = _diamond_hands(positions, ph, _NAMES)
    assert award.winner_name is None


def test_diamond_hands_no_price_history():
    positions = {1: [_pos("AAPL", 10, 100.0, 1000.0)]}
    award = _diamond_hands(positions, {}, _NAMES)
    assert award.winner_name is None


# ---------------------------------------------------------------------------
# _paper_hands
# ---------------------------------------------------------------------------

def test_paper_hands_winner():
    sell_dt = datetime.datetime(2026, 3, 24, 15, 0, tzinfo=datetime.timezone.utc)
    txs = [_tx(1, "TSLA", "SELL", 200.0, dt=sell_dt)]
    ph = {
        "TSLA": [
            _ph_row(_MON, 200.0),
            _ph_row(_WED, 215.0),  # +7.5% after sell
        ]
    }
    award = _paper_hands(txs, ph, _NAMES)
    assert award.winner_name == "Alice"
    assert "TSLA" in award.detail


def test_paper_hands_no_winner_small_rise():
    sell_dt = datetime.datetime(2026, 3, 24, 15, 0, tzinfo=datetime.timezone.utc)
    txs = [_tx(1, "TSLA", "SELL", 200.0, dt=sell_dt)]
    ph = {"TSLA": [_ph_row(_MON, 200.0), _ph_row(_WED, 203.0)]}  # +1.5% only
    award = _paper_hands(txs, ph, _NAMES)
    assert award.winner_name is None


def test_paper_hands_no_buys_counted():
    buy_dt = datetime.datetime(2026, 3, 24, 10, 0, tzinfo=datetime.timezone.utc)
    txs = [_tx(1, "TSLA", "BUY", 200.0, dt=buy_dt)]
    ph = {"TSLA": [_ph_row(_WED, 220.0)]}
    award = _paper_hands(txs, ph, _NAMES)
    assert award.winner_name is None


# ---------------------------------------------------------------------------
# _yolo
# ---------------------------------------------------------------------------

def test_yolo_winner():
    positions = {
        1: [_pos("AAPL", 10, 100.0, 8000.0)],   # 80% of 10000
        2: [_pos("TSLA", 5, 200.0, 3000.0)],    # 30% of 10000
    }
    values = {1: 10000.0, 2: 10000.0}
    award = _yolo(positions, values, _NAMES)
    assert award.winner_name == "Alice"
    assert "AAPL" in award.detail
    assert "80" in award.detail


def test_yolo_no_positions():
    award = _yolo({}, {}, _NAMES)
    assert award.winner_name is None


def test_yolo_zero_portfolio_value():
    positions = {1: [_pos("AAPL", 10, 100.0, 1000.0)]}
    values = {1: 0.0}
    award = _yolo(positions, values, _NAMES)
    assert award.winner_name is None


# ---------------------------------------------------------------------------
# _scared_money
# ---------------------------------------------------------------------------

def test_scared_money_winner():
    # Alice has tiny daily swings; Bob has large swings
    snaps = {
        1: [
            _snap(1, _MON, 10000),
            _snap(1, _TUE, 10001),
            _snap(1, _WED, 9999),
            _snap(1, _FRI, 10002),
        ],
        2: [
            _snap(2, _MON, 10000),
            _snap(2, _TUE, 11000),
            _snap(2, _WED, 9000),
            _snap(2, _FRI, 10500),
        ],
    }
    award = _scared_money(snaps, _NAMES)
    assert award.winner_name == "Alice"


def test_scared_money_needs_3_snaps():
    snaps = {1: [_snap(1, _MON, 10000), _snap(1, _FRI, 10100)]}
    award = _scared_money(snaps, _NAMES)
    assert award.winner_name is None


def test_scared_money_empty():
    award = _scared_money({}, _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Scared Money"


# ---------------------------------------------------------------------------
# _trendsetter
# ---------------------------------------------------------------------------

def test_trendsetter_winner():
    t0 = datetime.datetime(2026, 3, 23, 10, 0, tzinfo=datetime.timezone.utc)
    t1 = datetime.datetime(2026, 3, 24, 10, 0, tzinfo=datetime.timezone.utc)
    t2 = datetime.datetime(2026, 3, 25, 10, 0, tzinfo=datetime.timezone.utc)
    txs = [
        _tx(1, "NVDA", "BUY", 500.0, dt=t0),   # Alice first
        _tx(2, "NVDA", "BUY", 510.0, dt=t1),
        _tx(3, "NVDA", "BUY", 505.0, dt=t2),   # 3 total buyers
    ]
    names = {1: "Alice", 2: "Bob", 3: "Carol"}
    award = _trendsetter(txs, names)
    assert award.winner_name == "Alice"
    assert "NVDA" in award.detail


def test_trendsetter_no_winner_fewer_than_3_buyers():
    t0 = datetime.datetime(2026, 3, 23, 10, 0, tzinfo=datetime.timezone.utc)
    t1 = datetime.datetime(2026, 3, 24, 10, 0, tzinfo=datetime.timezone.utc)
    txs = [
        _tx(1, "NVDA", "BUY", 500.0, dt=t0),
        _tx(2, "NVDA", "BUY", 510.0, dt=t1),
    ]
    award = _trendsetter(txs, _NAMES)
    assert award.winner_name is None


def test_trendsetter_empty():
    award = _trendsetter([], _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Trendsetter"


# ---------------------------------------------------------------------------
# _contrarian
# ---------------------------------------------------------------------------

def test_contrarian_winner():
    txs = [
        # Majority buying AAPL (net +2)
        _tx(1, "AAPL", "BUY", 150.0),
        _tx(2, "AAPL", "BUY", 150.0),
        _tx(3, "AAPL", "BUY", 150.0),
        # Alice sells AAPL (contrarian)
        _tx(1, "AAPL", "SELL", 155.0),
        # Majority selling TSLA (net -2)
        _tx(2, "TSLA", "SELL", 200.0),
        _tx(3, "TSLA", "SELL", 200.0),
        # Alice buys TSLA (contrarian)
        _tx(1, "TSLA", "BUY", 195.0),
    ]
    names = {1: "Alice", 2: "Bob", 3: "Carol"}
    award = _contrarian(txs, names)
    assert award.winner_name == "Alice"


def test_contrarian_no_contrarian_trades():
    # Everyone does the same thing
    txs = [_tx(1, "AAPL", "BUY", 150.0), _tx(2, "AAPL", "BUY", 151.0)]
    award = _contrarian(txs, _NAMES)
    assert award.winner_name is None


def test_contrarian_empty():
    award = _contrarian([], _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Contrarian"


# ---------------------------------------------------------------------------
# _bought_the_dip
# ---------------------------------------------------------------------------

def test_bought_the_dip_winner():
    txs = [
        _tx(1, "AAPL", "BUY", 101.0),  # bought very near weekly low of 100
        _tx(2, "AAPL", "BUY", 115.0),  # bought higher
    ]
    ph = {"AAPL": [_ph_row(_MON, 115.0, low=100.0), _ph_row(_FRI, 120.0)]}
    award = _bought_the_dip(txs, ph, _NAMES)
    assert award.winner_name == "Alice"
    assert "AAPL" in award.detail


def test_bought_the_dip_only_sells():
    txs = [_tx(1, "AAPL", "SELL", 150.0)]
    ph = {"AAPL": [_ph_row(_MON, 150.0, low=140.0)]}
    award = _bought_the_dip(txs, ph, _NAMES)
    assert award.winner_name is None


def test_bought_the_dip_empty():
    award = _bought_the_dip([], {}, _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Bought the Dip"


# ---------------------------------------------------------------------------
# _bought_the_top
# ---------------------------------------------------------------------------

def test_bought_the_top_winner():
    txs = [
        _tx(1, "AAPL", "BUY", 149.0),  # very near weekly high of 150
        _tx(2, "AAPL", "BUY", 120.0),  # bought lower
    ]
    ph = {"AAPL": [_ph_row(_MON, 130.0, high=150.0)]}
    award = _bought_the_top(txs, ph, _NAMES)
    assert award.winner_name == "Alice"
    assert "AAPL" in award.detail


def test_bought_the_top_empty():
    award = _bought_the_top([], {}, _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Bought the Top"


# ---------------------------------------------------------------------------
# _diversification
# ---------------------------------------------------------------------------

def test_diversification_winner():
    positions = {
        1: [
            _pos("AAPL", 10, 150.0, 1500.0),
            _pos("JPM", 5, 200.0, 1000.0),
            _pos("XOM", 8, 120.0, 960.0),
        ],
        2: [_pos("TSLA", 3, 300.0, 900.0)],
    }
    sectors = {"AAPL": "Technology", "JPM": "Financials", "XOM": "Energy", "TSLA": "Consumer Discretionary"}
    award = _diversification(positions, sectors, _NAMES)
    assert award.winner_name == "Alice"
    assert "3" in award.detail


def test_diversification_no_sector_data():
    positions = {1: [_pos("AAPL", 10, 150.0, 1500.0)]}
    award = _diversification(positions, {}, _NAMES)
    assert award.winner_name is None


def test_diversification_empty():
    award = _diversification({}, {}, _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Diversification Award"


# ---------------------------------------------------------------------------
# _day_trader
# ---------------------------------------------------------------------------

def test_day_trader_winner():
    buy1 = datetime.datetime(2026, 3, 24, 10, 0, tzinfo=datetime.timezone.utc)
    sell1 = datetime.datetime(2026, 3, 24, 11, 0, tzinfo=datetime.timezone.utc)   # 1h hold
    buy2 = datetime.datetime(2026, 3, 24, 10, 0, tzinfo=datetime.timezone.utc)
    sell2 = datetime.datetime(2026, 3, 26, 10, 0, tzinfo=datetime.timezone.utc)   # 48h hold
    txs = [
        _tx(1, "AAPL", "BUY", 150.0, dt=buy1),
        _tx(1, "AAPL", "SELL", 155.0, dt=sell1),
        _tx(2, "TSLA", "BUY", 200.0, dt=buy2),
        _tx(2, "TSLA", "SELL", 210.0, dt=sell2),
    ]
    award = _day_trader(txs, _NAMES)
    assert award.winner_name == "Alice"
    assert "h" in award.detail  # formatted as Avg hold X.Xh or XhYm


def test_day_trader_no_completed_roundtrip():
    txs = [_tx(1, "AAPL", "BUY", 150.0)]
    award = _day_trader(txs, _NAMES)
    assert award.winner_name is None


def test_day_trader_empty():
    award = _day_trader([], _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Day Trader"


# ---------------------------------------------------------------------------
# _ghost_trader
# ---------------------------------------------------------------------------

def test_ghost_trader_winner():
    # Alice and Bob made trades; Carol did not
    txs = [_tx(1, "AAPL", "BUY", 150.0), _tx(2, "TSLA", "SELL", 200.0)]
    all_ids = [1, 2, 3]
    names = {1: "Alice", 2: "Bob", 3: "Carol"}
    award = _ghost_trader(txs, all_ids, names)
    assert award.winner_name == "Carol"
    assert "0 trades" in award.detail


def test_ghost_trader_no_winner_all_traded():
    txs = [_tx(1, "AAPL", "BUY", 150.0), _tx(2, "TSLA", "BUY", 200.0)]
    award = _ghost_trader(txs, [1, 2], _NAMES)
    assert award.winner_name is None


def test_ghost_trader_no_users():
    award = _ghost_trader([], [], _NAMES)
    assert award.winner_name is None
    assert award.award_name == "Ghost Trader"


def test_ghost_trader_picks_alphabetically_when_multiple():
    txs = []  # nobody traded
    all_ids = [1, 2, 3]
    names = {1: "Zara", 2: "Alice", 3: "Bob"}
    award = _ghost_trader(txs, all_ids, names)
    assert award.winner_name == "Alice"


# ---------------------------------------------------------------------------
# evaluate_weekly_awards — integration
# ---------------------------------------------------------------------------

def test_evaluate_weekly_awards_returns_15():
    awards = evaluate_weekly_awards(
        snapshots_by_user={},
        transactions=[],
        positions_by_user={},
        portfolio_values_by_user={},
        price_history={},
        ticker_sectors={},
        all_user_ids=[],
        user_names={},
    )
    assert len(awards) == 15


def test_evaluate_weekly_awards_all_are_weekly_award():
    awards = evaluate_weekly_awards(
        snapshots_by_user={},
        transactions=[],
        positions_by_user={},
        portfolio_values_by_user={},
        price_history={},
        ticker_sectors={},
        all_user_ids=[],
        user_names={},
    )
    for award in awards:
        assert isinstance(award, WeeklyAward)
        assert award.award_name
        assert award.description


def test_evaluate_weekly_awards_order():
    """Award names appear in the canonical order from the plan."""
    awards = evaluate_weekly_awards(
        snapshots_by_user={},
        transactions=[],
        positions_by_user={},
        portfolio_values_by_user={},
        price_history={},
        ticker_sectors={},
        all_user_ids=[],
        user_names={},
    )
    expected_names = [
        "Biggest Gainer", "Biggest Loser", "Sniper", "Comeback Kid",
        "Diamond Hands", "Paper Hands", "YOLO", "Scared Money",
        "Trendsetter", "Contrarian", "Bought the Dip", "Bought the Top",
        "Diversification Award", "Day Trader", "Ghost Trader",
    ]
    assert [a.award_name for a in awards] == expected_names


def test_evaluate_weekly_awards_no_winners_when_no_data():
    awards = evaluate_weekly_awards(
        snapshots_by_user={},
        transactions=[],
        positions_by_user={},
        portfolio_values_by_user={},
        price_history={},
        ticker_sectors={},
        all_user_ids=[1, 2],
        user_names={1: "Alice", 2: "Bob"},
    )
    # With no transactions/snapshots, most awards have no winner
    # Ghost Trader should fire since all_user_ids has entries but no transactions
    ghost = next(a for a in awards if a.award_name == "Ghost Trader")
    assert ghost.winner_name is not None

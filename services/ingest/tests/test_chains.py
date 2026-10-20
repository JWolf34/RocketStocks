"""S008 step-2: chain-response normalization + client-side window/cap filtering.

Field mapping is bound against a hand-built full-shape synthetic chain (every
Schwab field present, invented values; no vendor data is committed);
window/cap semantics against synthetic chains; and, when the gitignored
full-day captures are present locally, the whole pipeline against the
measured real books (SPY 2,654 rows / 13 expiries at ±10%/≤30DTE, QQQ
2,600 / 13, cap 4,000 non-binding on every name)."""

import gzip
import json
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path

import pytest

from rs_ingest.chains import normalize_chain

REPO_ROOT = Path(__file__).parents[3]
CAPTURES = REPO_ROOT / "data/chain-snapshots/2026-07-06"

WINDOW = 0.10
CAP = 4000

# Overrides that give contract() the SYN full-shape payload used by synthetic_payload.
SYN_SHAPE = {
    "description": "SYN 07/06/2026 100.00 C",
    "optionRoot": "SYN",
    "expirationDate": "2026-07-06T20:00:00.000+00:00",
    "daysToExpiration": 0,
    "bidSize": 64,
    "askSize": 62,
    "bidAskSize": "64X62",
    "lastSize": 2,
    "gamma": 0.006,
    "theta": -0.008,
    "vega": 0.014,
    "rho": 0.001,
}

needs_captures = pytest.mark.skipif(
    not CAPTURES.exists(),
    reason=f"full-day chain captures not present at {CAPTURES} (gitignored, local-only)",
)


@pytest.fixture(scope="module")
def synthetic_payload():
    """A synthetic SYN chain at 100.00: one expiry, two strikes per side, full
    Schwab field shape. Deliberately built out of strike order."""
    c = partial(contract, expiry_ms=1783382400000, **SYN_SHAPE)
    calls = {
        "2026-07-06:0": {
            "99.0": [c("SYN   260706C00099000", 99.0, "CALL", bid=1.65, ask=1.79, last=1.77,
                       totalVolume=2782, openInterest=8345, volatility=24.769, delta=0.959)],
            "98.0": [c("SYN   260706C00098000", 98.0, "CALL", bid=2.68, ask=2.83, last=2.88,
                       totalVolume=5805, openInterest=5190, volatility=22.867, delta=0.955)],
        }
    }
    puts = {
        "2026-07-06:0": {
            "101.0": [c("SYN   260706P00101000", 101.0, "PUT", bid=1.4, ask=1.5, last=1.45,
                        totalVolume=1200, openInterest=900, volatility=21.5, delta=-0.5)],
            "102.0": [c("SYN   260706P00102000", 102.0, "PUT", bid=2.2, ask=2.3, last=2.25,
                        totalVolume=800, openInterest=700, volatility=20.1, delta=-0.6)],
        }
    }
    return {
        "symbol": "SYN",
        "status": "SUCCESS",
        "strategy": "SINGLE",
        "isDelayed": False,
        "isIndex": False,
        "underlyingPrice": 100.0,
        "volatility": 29.0,
        "numberOfContracts": 4,
        "isChainTruncated": False,
        "callExpDateMap": calls,
        "putExpDateMap": puts,
    }


def contract(
    symbol="XYZ   260717C00100000",
    strike=100.0,
    put_call="CALL",
    expiry_ms=1784332800000,
    **overrides,
):
    """A contract carrying the full Schwab field set (including fields the
    normalizer ignores), with invented values. Keyword overrides replace any
    field."""
    bid = overrides.get("bid", 1.0)
    ask = overrides.get("ask", 1.2)
    last = overrides.get("last", 1.1)
    fields = {
        "putCall": put_call,
        "symbol": symbol,
        "description": f"XYZ 07/17/2026 {strike:.2f} {put_call[0]}",
        "exchangeName": "OPR",
        "bid": bid,
        "ask": ask,
        "last": last,
        "mark": round((bid + ask) / 2, 2),
        "bidSize": 10,
        "askSize": 12,
        "bidAskSize": "10X12",
        "lastSize": 1,
        "highPrice": ask + 0.2,
        "lowPrice": bid - 0.2,
        "openPrice": bid,
        "closePrice": bid,
        "totalVolume": 50,
        "tradeTimeInLong": 1783353653132,
        "quoteTimeInLong": 1783353656792,
        "netChange": 0.5,
        "volatility": 25.0,
        "delta": 0.5,
        "gamma": 0.01,
        "theta": -0.02,
        "vega": 0.1,
        "rho": 0.03,
        "openInterest": 500,
        "timeValue": 0.04,
        "theoreticalOptionValue": last,
        "theoreticalVolatility": 29.0,
        "optionDeliverablesList": [
            {"symbol": "XYZ", "assetType": "STOCK", "deliverableUnits": 100.0, "currencyType": None}
        ],
        "strikePrice": strike,
        "expirationDate": "2026-07-17T20:00:00.000+00:00",
        "daysToExpiration": 11,
        "expirationType": "W",
        "lastTradingDay": expiry_ms,
        "multiplier": 100.0,
        "settlementType": "P",
        "deliverableNote": "100 XYZ",
        "percentChange": 1.0,
        "markChange": 0.5,
        "markPercentChange": 1.0,
        "intrinsicValue": 0.0,
        "extrinsicValue": 0.0,
        "optionRoot": "XYZ",
        "exerciseType": "A",
        "high52Week": 9.0,
        "low52Week": 1.0,
        "breakEven": 100.0,
        "ssid": 1,
        "inTheMoney": True,
        "mini": False,
        "nonStandard": False,
        "pennyPilot": True,
    }
    fields.update(overrides)
    return fields


def chain_payload(underlying_price, expiry_map):
    """expiry_map: {'YYYY-MM-DD': [contract, ...]} — contracts get grouped
    per side/strike the way Schwab shapes callExpDateMap/putExpDateMap."""
    calls: dict = {}
    puts: dict = {}
    for expiry, contracts in expiry_map.items():
        for c in contracts:
            side = calls if c["putCall"] == "CALL" else puts
            side.setdefault(f"{expiry}:1", {}).setdefault(str(c["strikePrice"]), []).append(c)
    return {
        "symbol": "XYZ",
        "status": "SUCCESS",
        "underlyingPrice": underlying_price,
        "callExpDateMap": calls,
        "putExpDateMap": puts,
    }


class TestFieldMapping:
    def test_full_shape_maps_every_bound_field(self, synthetic_payload):
        result = normalize_chain(synthetic_payload, window=WINDOW, row_cap=CAP)
        assert result.underlying_price == 100.0
        assert result.skipped_contracts == 0
        assert result.dropped_by_cap == 0
        assert len(result.rows) == 4

        q = next(r for r in result.rows if r.option_symbol == "SYN   260706C00099000")
        assert q.expiry == date(2026, 7, 6)
        assert q.strike == 99.0
        assert q.put_call == "C"
        assert q.quote_time == datetime.fromtimestamp(1783353656792 / 1000, tz=UTC)
        assert q.bid == 1.65
        assert q.ask == 1.79
        assert q.last == 1.77
        assert q.mark == 1.72
        assert q.bid_size == 64
        assert q.ask_size == 62
        assert q.last_size == 2
        assert q.trade_time == datetime.fromtimestamp(1783353653132 / 1000, tz=UTC)
        assert q.volume == 2782
        assert q.open_interest == 8345
        assert q.iv == 24.769  # Schwab's `volatility`, stored as-received (percent)
        assert q.delta == 0.959
        assert q.gamma == 0.006
        assert q.theta == -0.008
        assert q.vega == 0.014
        assert q.rho == 0.001
        assert q.underlying_price == 100.0
        assert q.multiplier == 100
        assert q.expiration_type == "W"
        assert q.non_standard is False

    def test_full_shape_covers_both_sides(self, synthetic_payload):
        rows = normalize_chain(synthetic_payload, window=WINDOW, row_cap=CAP).rows
        assert {r.put_call for r in rows} == {"C", "P"}

    def test_rows_ordered_by_expiry_strike_side(self, synthetic_payload):
        rows = normalize_chain(synthetic_payload, window=WINDOW, row_cap=CAP).rows
        keys = [(r.expiry, r.strike, r.put_call) for r in rows]
        assert keys == sorted(keys)

    def test_epoch_zero_times_are_null_not_1970(self):
        payload = chain_payload(
            100.0, {"2026-07-17": [contract(quoteTimeInLong=0, tradeTimeInLong=0)]}
        )
        (row,) = normalize_chain(payload, window=WINDOW, row_cap=CAP).rows
        assert row.quote_time is None
        assert row.trade_time is None

    def test_minus_999_greeks_are_null(self):
        # Schwab's documented "no value" sentinel for vol/greeks; a stored -999
        # would poison every aggregate over REAL columns. Never observed in the
        # real full-day captures, so this is a defensive mapping only.
        payload = chain_payload(
            100.0,
            {"2026-07-17": [contract(volatility=-999.0, delta=-999.0, rho=-999.0)]},
        )
        (row,) = normalize_chain(payload, window=WINDOW, row_cap=CAP).rows
        assert row.iv is None
        assert row.delta is None
        assert row.rho is None
        assert row.gamma == 0.01  # untouched neighbours

    def test_malformed_contract_skipped_not_fatal(self):
        bad = contract()
        del bad["symbol"]
        payload = chain_payload(100.0, {"2026-07-17": [bad, contract(strike=101.0)]})
        result = normalize_chain(payload, window=WINDOW, row_cap=CAP)
        assert result.skipped_contracts == 1
        assert [r.strike for r in result.rows] == [101.0]


class TestMoneynessWindow:
    def test_window_bounds_inclusive_against_response_price(self):
        strikes = [89.9, 90.0, 100.0, 110.0, 110.1]
        payload = chain_payload(
            100.0,
            {"2026-07-17": [contract(strike=s, symbol=f"XYZ{s}") for s in strikes]},
        )
        rows = normalize_chain(payload, window=WINDOW, row_cap=CAP).rows
        assert [r.strike for r in rows] == [90.0, 100.0, 110.0]

    def test_missing_underlying_price_disables_window(self):
        # No price to window against -> capture everything the (already
        # expiry-bounded) response contains rather than guess; the cap still
        # guards. The scanner decides what status such a snapshot gets.
        payload = chain_payload(None, {"2026-07-17": [contract(strike=5.0)]})
        result = normalize_chain(payload, window=WINDOW, row_cap=CAP)
        assert result.underlying_price is None
        assert len(result.rows) == 1


class TestRowCap:
    @staticmethod
    def three_expiry_payload(rows_per_expiry=(4, 4, 4)):
        expiries = ["2026-07-10", "2026-07-17", "2026-07-24"]
        return chain_payload(
            100.0,
            {
                e: [
                    contract(strike=95.0 + i, symbol=f"XYZ {e} {i}")
                    for i in range(n)
                ]
                for e, n in zip(expiries, rows_per_expiry, strict=True)
            },
        )

    def test_cap_non_binding_keeps_everything(self):
        result = normalize_chain(self.three_expiry_payload(), window=WINDOW, row_cap=12)
        assert len(result.rows) == 12
        assert result.dropped_by_cap == 0

    def test_cap_drops_whole_farthest_expiries_first(self):
        result = normalize_chain(self.three_expiry_payload(), window=WINDOW, row_cap=11)
        # 4+4=8 fits, adding the 07-24 expiry (4 more) would exceed 11: the
        # whole expiry goes, never a partial slice of it.
        assert len(result.rows) == 8
        assert result.dropped_by_cap == 4
        assert {r.expiry for r in result.rows} == {date(2026, 7, 10), date(2026, 7, 17)}

    def test_nearest_expiry_always_kept_even_over_cap(self):
        result = normalize_chain(self.three_expiry_payload((6, 4, 4)), window=WINDOW, row_cap=5)
        assert len(result.rows) == 6
        assert result.dropped_by_cap == 8
        assert {r.expiry for r in result.rows} == {date(2026, 7, 10)}


@needs_captures
class TestMeasuredBooks:
    """The acceptance numbers: real captured books through the real pipeline.

    The 2026-07-06 capture was expiry-bounded wider than production's <=30 DTE
    request, so the harness pre-applies the server-side bound normalize_chain
    will never see (production requests are bounded via fromDate/toDate)."""

    CUTOFF = date(2026, 7, 6) + timedelta(days=30)

    @classmethod
    def load_bounded(cls, name, hhmm):
        with gzip.open(CAPTURES / name / f"{hhmm}.json.gz") as f:
            payload = json.load(f)
        for side in ("callExpDateMap", "putExpDateMap"):
            payload[side] = {
                k: v
                for k, v in payload[side].items()
                if date.fromisoformat(k.split(":")[0]) <= cls.CUTOFF
            }
        return payload

    @pytest.mark.parametrize(
        ("name", "hhmm", "expected_rows", "expected_expiries"),
        [("SPY", "1600", 2654, 13), ("QQQ", "1601", 2600, 13)],
    )
    def test_reproduces_measured_index_books(self, name, hhmm, expected_rows, expected_expiries):
        result = normalize_chain(self.load_bounded(name, hhmm), window=WINDOW, row_cap=CAP)
        assert result.skipped_contracts == 0
        assert result.dropped_by_cap == 0, "4,000 cap must be non-binding on measured books"
        assert len(result.rows) == expected_rows
        assert len({r.expiry for r in result.rows}) == expected_expiries

    @pytest.mark.parametrize(
        "name", ["AAPL", "AMD", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "TSLA"]
    )
    def test_single_names_in_measured_range_cap_non_binding(self, name):
        result = normalize_chain(self.load_bounded(name, "1601"), window=WINDOW, row_cap=CAP)
        assert result.dropped_by_cap == 0
        assert 250 <= len(result.rows) <= 900

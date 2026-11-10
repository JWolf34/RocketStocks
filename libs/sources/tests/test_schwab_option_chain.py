"""Schwab option chains through a fake schwab-py client; payloads are synthetic."""

from datetime import UTC, date, datetime

import pytest
from authlib.integrations.base_client import OAuthError

from rs_sources.schwab import (
    SchwabAuthError,
    SchwabOptionChains,
    SchwabResponseError,
    normalize_chain,
    schwab_chain_symbol,
)

QUOTED_MS = int(datetime(2026, 11, 9, 21, 0, tzinfo=UTC).timestamp() * 1000)


def contract(side="CALL", strike=100.0, symbol=None, **overrides):
    letter = "C" if side == "CALL" else "P"
    raw = {
        "putCall": side,
        "symbol": symbol or f"AAA   261120{letter}{int(strike * 1000):08d}",
        "strikePrice": strike,
        "bid": 1.1,
        "ask": 1.2,
        "last": 1.15,
        "mark": 1.15,
        "bidSize": 10,
        "askSize": 12,
        "lastSize": 1,
        "totalVolume": 250,
        "openInterest": 4000,
        "quoteTimeInLong": QUOTED_MS,
        "tradeTimeInLong": QUOTED_MS - 60_000,
        "volatility": 31.5,
        "delta": 0.5,
        "gamma": 0.04,
        "theta": -0.05,
        "vega": 0.1,
        "rho": 0.01,
        "multiplier": 100.0,
        "expirationType": "R",
        "nonStandard": False,
    }
    return raw | overrides


def chain_payload(calls=(), puts=(), **overrides):
    def by_expiry(contracts):
        expiries = {}
        for raw in contracts:
            key = "2026-11-27:18" if "261127" in raw["symbol"] else "2026-11-20:11"
            expiries.setdefault(key, {}).setdefault(str(raw["strikePrice"]), []).append(raw)
        return expiries

    payload = {
        "symbol": "AAA",
        "status": "SUCCESS",
        "underlyingPrice": 99.5,
        "isChainTruncated": False,
        "callExpDateMap": by_expiry(calls),
        "putExpDateMap": by_expiry(puts),
    }
    return payload | overrides


class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSchwabClient:
    class Options:
        class ContractType:
            ALL = "ALL"

    def __init__(self, response):
        self.response = response
        self.calls = []

    async def get_option_chain(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def chains(response):
    client = FakeSchwabClient(response)
    return SchwabOptionChains(client), client


class TestSpelling:
    @pytest.mark.parametrize(
        ("symbol", "spelled"),
        [("BRK.B", "BRK/B"), ("aapl", "AAPL"), (" BF.B ", "BF/B"), ("SPY", "SPY")],
    )
    def test_class_shares_are_slashed(self, symbol, spelled):
        assert schwab_chain_symbol(symbol) == spelled


class TestFetch:
    async def test_an_unfiltered_chain_asks_for_every_contract_type_and_no_expiry_bounds(self):
        adapter, client = chains(FakeResponse(chain_payload(calls=[contract()])))

        chain = await adapter.chain("aaa")

        assert client.calls == [("AAA", {"contract_type": "ALL", "from_date": None, "to_date": None})]
        assert chain.underlying == "AAA" and len(chain.contracts) == 1

    async def test_a_class_share_is_requested_slashed_but_stored_under_the_universe_symbol(self):
        adapter, client = chains(FakeResponse(chain_payload(calls=[contract()], symbol="BRK/B")))

        chain = await adapter.chain("BRK.B")

        assert client.calls[0][0] == "BRK/B"
        assert chain.underlying == "BRK.B"
        assert {c.underlying for c in chain.contracts} == {"BRK.B"}

    async def test_an_expiry_range_is_sent_and_an_open_ended_one_has_no_end(self):
        adapter, client = chains(FakeResponse(chain_payload()))

        await adapter.chain("AAA", date(2026, 11, 10), date(2027, 1, 15))
        await adapter.chain("AAA", date(2027, 1, 16))

        assert client.calls[0][1]["from_date"] == date(2026, 11, 10)
        assert client.calls[0][1]["to_date"] == date(2027, 1, 15)
        assert client.calls[1][1]["from_date"] == date(2027, 1, 16)
        assert client.calls[1][1]["to_date"] is None

    async def test_a_backwards_range_is_a_programming_error(self):
        adapter, client = chains(FakeResponse(chain_payload()))
        with pytest.raises(ValueError, match="after end"):
            await adapter.chain("AAA", date(2027, 1, 1), date(2026, 1, 1))
        assert client.calls == []

    @pytest.mark.parametrize("status", [400, 404, 429, 502])
    async def test_a_failed_answer_raises_with_its_status(self, status):
        adapter, _ = chains(FakeResponse({}, status))
        with pytest.raises(SchwabResponseError) as caught:
            await adapter.chain("AAA")
        assert caught.value.status_code == status

    async def test_a_body_that_is_not_json_raises(self):
        adapter, _ = chains(FakeResponse(ValueError("no json")))
        with pytest.raises(SchwabResponseError, match="not JSON"):
            await adapter.chain("AAA")

    async def test_a_dead_refresh_token_is_an_auth_error(self):
        adapter, _ = chains(OAuthError(error="invalid_grant"))
        with pytest.raises(SchwabAuthError):
            await adapter.chain("AAA")


class TestNormalize:
    def test_a_chain_is_ordered_by_expiry_strike_and_side(self):
        later = contract(strike=90.0, symbol="AAA   261127C00090000")
        payload = chain_payload(
            calls=[contract(strike=105.0), contract(strike=100.0), later],
            puts=[contract("PUT", strike=100.0)],
        )

        chain = normalize_chain("AAA", payload)

        assert [(c.expiry, c.strike, c.put_call) for c in chain.contracts] == [
            (date(2026, 11, 20), 100.0, "C"),
            (date(2026, 11, 20), 100.0, "P"),
            (date(2026, 11, 20), 105.0, "C"),
            (date(2026, 11, 27), 90.0, "C"),
        ]
        assert chain.skipped == 0 and chain.truncated is False

    def test_a_contract_carries_schwabs_values_as_sent(self):
        [row] = normalize_chain("AAA", chain_payload(calls=[contract()])).contracts

        assert row.model_dump() == {
            "underlying": "AAA",
            "option_symbol": "AAA   261120C00100000",
            "expiry": date(2026, 11, 20),
            "strike": 100.0,
            "put_call": "C",
            "bid": 1.1,
            "ask": 1.2,
            "last": 1.15,
            "mark": 1.15,
            "bid_size": 10,
            "ask_size": 12,
            "last_size": 1,
            "volume": 250,
            "open_interest": 4000,
            "quote_time": datetime(2026, 11, 9, 21, 0, tzinfo=UTC),
            "trade_time": datetime(2026, 11, 9, 20, 59, tzinfo=UTC),
            "iv": 31.5,  # percent, not a fraction
            "delta": 0.5,
            "gamma": 0.04,
            "theta": -0.05,
            "vega": 0.1,
            "rho": 0.01,
            "underlying_price": 99.5,
            "multiplier": 100,
            "expiration_type": "R",
            "non_standard": False,
        }

    @pytest.mark.parametrize("field", ["volatility", "delta", "gamma", "theta", "vega", "rho"])
    @pytest.mark.parametrize("missing", [-999, -999.0, "NaN", None, float("nan")])
    def test_a_missing_value_sentinel_is_none_not_a_number(self, field, missing):
        [row] = normalize_chain("AAA", chain_payload(calls=[contract(**{field: missing})])).contracts
        column = "iv" if field == "volatility" else field
        assert getattr(row, column) is None

    def test_a_real_zero_greek_stays_zero(self):
        [row] = normalize_chain("AAA", chain_payload(calls=[contract(delta=0.0, volatility=0)])).contracts
        assert row.delta == 0.0 and row.iv == 0.0

    @pytest.mark.parametrize("epoch", [0, -1, None, "x"])
    def test_a_non_positive_epoch_time_is_none_not_1970(self, epoch):
        [row] = normalize_chain(
            "AAA", chain_payload(calls=[contract(quoteTimeInLong=epoch, tradeTimeInLong=epoch)])
        ).contracts
        assert row.quote_time is None and row.trade_time is None

    def test_context_fields_schwab_did_not_send_are_none(self):
        raw = contract()
        for key in ("multiplier", "expirationType", "nonStandard"):
            del raw[key]
        [row] = normalize_chain("AAA", chain_payload(calls=[raw], underlyingPrice=0.0)).contracts
        assert (row.multiplier, row.expiration_type, row.non_standard, row.underlying_price) == (None,) * 4

    def test_an_unusable_contract_is_skipped_and_counted(self):
        payload = chain_payload(
            calls=[
                contract(strike=100.0),
                contract(strike=101.0, bid=None),
                contract(strike=102.0, totalVolume=-1),
                contract(strike=103.0, putCall="STRADDLE"),
            ]
        )
        del payload["callExpDateMap"]["2026-11-20:11"]["100.0"][0]["lastSize"]

        chain = normalize_chain("AAA", payload)

        assert [c.strike for c in chain.contracts] == []
        assert chain.skipped == 4

    def test_one_bad_contract_does_not_cost_the_rest(self):
        chain = normalize_chain(
            "AAA", chain_payload(calls=[contract(strike=100.0), contract(strike=101.0, bid=None)])
        )
        assert [c.strike for c in chain.contracts] == [100.0]
        assert chain.skipped == 1

    def test_a_repeated_option_symbol_is_kept_once(self):
        payload = chain_payload(calls=[contract()])
        payload["putExpDateMap"] = payload["callExpDateMap"]
        chain = normalize_chain("AAA", payload)
        assert len(chain.contracts) == 1 and chain.skipped == 1

    def test_a_bad_expiry_key_skips_its_contracts(self):
        payload = chain_payload(calls=[contract()])
        payload["callExpDateMap"] = {"soon": payload["callExpDateMap"]["2026-11-20:11"]}
        chain = normalize_chain("AAA", payload)
        assert chain.contracts == () and chain.skipped == 1

    def test_a_chain_with_no_contracts_is_an_empty_chain_not_an_error(self):
        chain = normalize_chain("AAA", chain_payload(status="FAILED", underlyingPrice=0.0))
        assert chain.contracts == () and chain.skipped == 0 and chain.underlying_price is None

    def test_the_chains_own_underlying_price_and_truncation_flag_are_kept(self):
        chain = normalize_chain("AAA", chain_payload(calls=[contract()], isChainTruncated=True))
        assert chain.underlying_price == 99.5 and chain.truncated is True

    @pytest.mark.parametrize("payload", [{}, {"error": "boom"}, {"callExpDateMap": {}}])
    def test_a_body_without_the_chain_envelope_is_an_error_not_an_empty_chain(self, payload):
        with pytest.raises(SchwabResponseError, match="not an option chain"):
            normalize_chain("AAA", payload)

    def test_an_expiry_map_that_is_not_a_mapping_is_an_error(self):
        with pytest.raises(SchwabResponseError, match="not a mapping"):
            normalize_chain("AAA", chain_payload(callExpDateMap=[1]))


class TestContextFields:
    @pytest.mark.parametrize("code", ["", "RW", 5, None])
    def test_an_odd_expiration_type_is_none_and_costs_no_contract(self, code):
        chain = normalize_chain("AAA", chain_payload(calls=[contract(expirationType=code)]))
        assert [c.expiration_type for c in chain.contracts] == [None] and chain.skipped == 0

    def test_a_non_boolean_non_standard_flag_is_none(self):
        chain = normalize_chain("AAA", chain_payload(calls=[contract(nonStandard="no")]))
        assert chain.contracts[0].non_standard is None

    def test_tied_contracts_are_ordered_by_option_symbol(self):
        payload = chain_payload(calls=[contract(symbol="AAA1  261120C00100000"), contract()])
        chain = normalize_chain("AAA", payload)
        assert [c.option_symbol for c in chain.contracts] == ["AAA   261120C00100000", "AAA1  261120C00100000"]

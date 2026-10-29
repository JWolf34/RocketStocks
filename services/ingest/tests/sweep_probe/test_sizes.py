"""How a chain's size is read from its records: whole, in parts, empty, rejected or unanswered."""

from rs_ingest.tools.sweep_probe.sizes import ChainState, chain_size
from rs_ingest.tools.sweep_probe.store import CallRecord, Kind, Outcome


def call(variant, status=200, *, outcome=Outcome.HTTP, contracts=None, expiries=None, size=None, chain_status="SUCCESS"):
    ok = outcome == Outcome.HTTP and status == 200
    return CallRecord(
        f"chain/X/{variant}",
        Kind.CHAIN,
        "X",
        outcome,
        at="2026-10-28T20:05:00+00:00",
        status=status if outcome == Outcome.HTTP else None,
        bytes=size,
        contracts=contracts if ok else None,
        expiries=expiries if ok else None,
        chain_status=chain_status if ok else None,
        variant=variant,
    )


def failed(variant):
    return call(variant, 502)


def test_a_whole_chain_carries_its_own_numbers():
    size = chain_size([call("full", contracts=100, expiries=4, size=5_000)], "X")

    assert (size.state, size.contracts, size.expiries, size.bytes, size.usable) == (ChainState.WHOLE, 100, 4, 5_000, True)
    assert size.chain_statuses == ("SUCCESS",)


def test_a_200_with_status_failed_or_with_no_contracts_is_empty_and_not_usable():
    failed_status = chain_size([call("full", contracts=7, size=10, chain_status="FAILED")], "X")
    no_contracts = chain_size([call("full", contracts=0, size=10)], "X")

    assert failed_status.state == no_contracts.state == ChainState.EMPTY
    assert not failed_status.usable and not no_contracts.usable


def test_a_chain_answered_in_three_ranges_counts_with_them_summed_and_flagged():
    records = [
        failed("full"),
        call("split1", contracts=10, expiries=2, size=100),
        call("split2", contracts=20, expiries=3, size=200),
        call("split3", contracts=30, expiries=5, size=300),
    ]

    size = chain_size(records, "X")

    assert (size.state, size.contracts, size.expiries, size.bytes) == (ChainState.IN_PARTS, 60, 10, 600)
    assert size.laddered and size.usable


def test_a_chain_with_a_range_that_did_not_answer_is_partial_and_not_usable():
    records = [failed("full"), call("split1", contracts=10, size=100), failed("split2"), call("split3", contracts=5, size=50)]

    size = chain_size(records, "X")

    assert (size.state, size.contracts, size.parts_ok, size.usable) == (ChainState.PARTIAL, 15, 2, False)


def test_a_chain_whose_ranges_all_failed_failed():
    assert chain_size([failed("full"), failed("split1"), failed("split2"), failed("split3")], "X").state == ChainState.FAILED


def test_a_chain_whose_ladder_is_still_running_or_waits_for_a_token_is_pending():
    assert chain_size([failed("full"), call("split1", contracts=1, size=1)], "X").state == ChainState.PENDING
    assert chain_size([call("full", outcome=Outcome.UNAVAILABLE)], "X").state == ChainState.PENDING
    assert chain_size([], "X").state == ChainState.PENDING


def test_a_400_is_rejected_and_another_error_is_a_failure():
    assert chain_size([call("full", 400)], "X").state == ChainState.REJECTED
    assert chain_size([call("full", 403)], "X").state == ChainState.FAILED


def test_a_timeout_goes_down_the_ladder_like_a_502():
    records = [call("full", outcome=Outcome.TIMEOUT)] + [call(f"split{n}", contracts=1, size=1) for n in (1, 2, 3)]

    assert chain_size(records, "X").state == ChainState.IN_PARTS

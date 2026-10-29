"""How big one chain is, whole or in the parts it had to be split into."""

from dataclasses import dataclass, replace
from enum import StrEnum

from rs_ingest.tools.sweep_probe.store import CallRecord, Outcome

# Responses that send a chain down the ladder: fetched again, then fetched in expiry ranges.
LADDER_STATUSES = (502, 503, 504)
PARTS = 3


class ChainState(StrEnum):
    WHOLE = "whole"  # one response carried the chain
    IN_PARTS = "in parts"  # only the three expiry ranges answered, all of them
    PARTIAL = "partial"  # some expiry ranges answered, some did not
    EMPTY = "empty"  # a 200 with status FAILED or no contracts
    REJECTED = "rejected"  # 400: Schwab does not know the spelling
    FAILED = "failed"  # nothing answered
    PENDING = "pending"  # not fetched yet, or only partly


def needs_ladder(record: CallRecord) -> bool:
    """A response worth fetching again and then in parts: a gateway error, a timeout or a transport error."""
    return record.outcome in (Outcome.TIMEOUT, Outcome.ERROR) or record.status in LADDER_STATUSES


@dataclass(frozen=True)
class ChainSize:
    symbol: str  # the spelling that answered, or the last one tried
    state: ChainState
    contracts: int = 0
    expiries: int = 0
    bytes: int = 0
    truncated: bool = False
    parts_ok: int = 0
    retried: bool = False  # an attempt failed and a later one answered
    laddered: bool = False  # the whole chain failed twice, so it was fetched in expiry ranges
    chain_statuses: tuple[str, ...] = ()
    latency_s: float | None = None  # of the whole-chain request

    @property
    def usable(self) -> bool:
        """Whether its numbers describe the whole chain."""
        return self.state in (ChainState.WHOLE, ChainState.IN_PARTS)


def chain_size(records: list[CallRecord], symbol: str) -> ChainSize:
    """The size of one chain from its records: the full request and, when it failed, the ranges.

    A chain that answered only in parts counts with its parts summed.
    """
    if not records:
        return ChainSize(symbol, ChainState.PENDING)
    full = records[0]
    parts = records[1:]
    if full.ok:
        if full.contracts is None:  # a 200 whose body is no chain
            return ChainSize(full.symbol, ChainState.FAILED)
        contracts = full.contracts
        empty = full.chain_status == "FAILED" or contracts == 0
        return ChainSize(
            full.symbol,
            ChainState.EMPTY if empty else ChainState.WHOLE,
            contracts=contracts,
            expiries=full.expiries or 0,
            bytes=full.bytes or 0,
            truncated=bool(full.truncated),
            retried=full.first_failure is not None,
            chain_statuses=(full.chain_status or "absent",),
            latency_s=full.latency_s,
        )
    if full.status == 400:
        return ChainSize(full.symbol, ChainState.REJECTED)
    if not needs_ladder(full):
        state = ChainState.PENDING if not full.terminal else ChainState.FAILED
        return ChainSize(full.symbol, state)
    answered = [p for p in parts if p.ok]
    contracts = sum(p.contracts or 0 for p in answered)
    summed = ChainSize(
        full.symbol,
        ChainState.PENDING,
        contracts=contracts,
        expiries=sum(p.expiries or 0 for p in answered),
        bytes=sum(p.bytes or 0 for p in answered),
        truncated=any(p.truncated for p in answered),
        parts_ok=len(answered),
        laddered=True,
        retried=any(p.first_failure is not None for p in answered),
        chain_statuses=tuple(p.chain_status or "absent" for p in answered),
    )
    if len(parts) < PARTS or not all(p.terminal for p in parts):
        return summed
    if not answered:
        return replace(summed, state=ChainState.FAILED)
    if len(answered) < PARTS:
        return replace(summed, state=ChainState.PARTIAL)
    empty = contracts == 0 or all(p.chain_status == "FAILED" for p in answered)
    return replace(summed, state=ChainState.EMPTY if empty else ChainState.IN_PARTS)

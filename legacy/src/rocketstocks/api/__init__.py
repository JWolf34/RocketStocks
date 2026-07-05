"""rocketstocks.api — public data SDK.

Public surface:
    DataAPI              — async facade over all data-layer repositories and clients
    build_data_api       — factory: opens DB pool, boots StockData, returns DataAPI
    DataNotAvailable     — raised when DB-only access finds no data
    TickerBatch          — deep-dive batch (all fields for a single ticker)
    PeerComparisonBatch  — history + fundamentals + stats across a peer group
    OptionsSnapshot      — chain + IV history + quote + stats
    FundamentalsSnapshot — financials + EPS + analyst + insider data
"""

from rocketstocks.api.client import DataAPI, DataNotAvailable
from rocketstocks.api._factory import build_data_api
from rocketstocks.api.batches import (
    TickerBatch,
    PeerComparisonBatch,
    OptionsSnapshot,
    FundamentalsSnapshot,
)

__all__ = [
    "DataAPI",
    "DataNotAvailable",
    "build_data_api",
    "TickerBatch",
    "PeerComparisonBatch",
    "OptionsSnapshot",
    "FundamentalsSnapshot",
]

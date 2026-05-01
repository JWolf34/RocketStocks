"""rocketstocks.api — public data SDK.

Public surface:
    DataAPI       — async facade over all data-layer repositories and clients
    build_data_api — factory: opens DB pool, boots StockData, returns DataAPI
    DataNotAvailable — raised when DB-only access finds no data
"""

from rocketstocks.api.client import DataAPI, DataNotAvailable
from rocketstocks.api._factory import build_data_api

__all__ = ["DataAPI", "DataNotAvailable", "build_data_api"]

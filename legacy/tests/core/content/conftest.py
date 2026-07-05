"""Shared fixtures for tests/core/content/."""
import datetime

import pandas as pd
import pytest


@pytest.fixture
def quote_up():
    return {
        'symbol': 'GME',
        'quote': {
            'netPercentChange': 7.5,
            'openPrice': 50.0, 'highPrice': 56.0, 'lowPrice': 49.0,
            'totalVolume': 5_000_000,
        },
        'regular': {'regularMarketLastPrice': 54.0},
        'assetSubType': 'CS',
        'reference': {'exchangeName': 'NYSE', 'isShortable': True, 'isHardToBorrow': False},
    }


@pytest.fixture
def quote_down():
    return {
        'symbol': 'GME',
        'quote': {
            'netPercentChange': -4.2,
            'openPrice': 100.0, 'highPrice': 101.0, 'lowPrice': 93.0,
            'totalVolume': 3_000_000,
        },
        'regular': {'regularMarketLastPrice': 96.0},
        'assetSubType': 'CS',
        'reference': {'exchangeName': 'NASDAQ', 'isShortable': True, 'isHardToBorrow': False},
    }


@pytest.fixture
def ticker_info():
    return {'ticker': 'GME', 'name': 'GameStop Corp', 'sector': 'Consumer',
            'industry': 'Retail', 'country': 'US'}


@pytest.fixture
def price_history():
    dates = [datetime.date.today() - datetime.timedelta(days=i) for i in range(100)]
    return pd.DataFrame({
        'date': dates, 'close': [50.0 + i * 0.01 for i in range(100)],
        'high': [51.0] * 100, 'low': [49.0] * 100, 'volume': [500_000] * 100,
    })

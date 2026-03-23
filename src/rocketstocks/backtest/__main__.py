"""Entry point: python -m rocketstocks.backtest"""
import asyncio
import sys
import logging

from rocketstocks.core.utils.logging_config import setup_logging
from rocketstocks.backtest.cli import main


def _entry():
    setup_logging(level=logging.INFO)
    sys.exit(asyncio.run(main()))


if __name__ == '__main__':
    _entry()

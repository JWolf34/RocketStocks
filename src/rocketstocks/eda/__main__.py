"""Entry point: python -m rocketstocks.eda"""
import asyncio
import logging
import sys

from rocketstocks.core.utils.logging_config import setup_logging
from rocketstocks.eda.cli import main


def _entry():
    setup_logging(level=logging.INFO)
    sys.exit(asyncio.run(main()))


if __name__ == '__main__':
    _entry()

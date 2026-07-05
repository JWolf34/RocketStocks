import logging

from rocketstocks.core.utils.logging_config import setup_logging
from src.rocketstocks.data.stockdata import StockData
from rocketstocks.bot import bot as Discord
from rocketstocks.core.notifications import EventEmitter, NotificationConfig

logger = logging.getLogger(__name__)


def main():
    setup_logging()
    logger.info('**********[START LOG]**********')

    # Init StockData object
    stock_data = StockData()

    # Init notification emitter
    emitter = EventEmitter()
    notification_config = NotificationConfig.from_env()

    try:
        Discord.run_bot(stock_data=stock_data, emitter=emitter, notification_config=notification_config)
    except Exception:
        logger.critical("Bot crashed with unhandled exception", exc_info=True)
        raise


if __name__ == '__main__':
    main()

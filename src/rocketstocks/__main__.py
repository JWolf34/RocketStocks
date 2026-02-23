import logging
import threading

from rocketstocks.core.utils.logging_config import setup_logging
from src.rocketstocks.data.stockdata import StockData
from rocketstocks.bot import bot as Discord
from rocketstocks.core.scheduler import jobs as scheduler
from rocketstocks.core.notifications import EventEmitter, NotificationConfig

logger = logging.getLogger(__name__)


def main():
    setup_logging()
    logger.info('**********[START LOG]**********')

    # Init StockData object
    stock_data = StockData()

    # Init notification emitter (cross-thread queue bridge)
    emitter = EventEmitter()
    notification_config = NotificationConfig.from_env()

    # Build threads - one for bot and one for scheduler
    bot_thread = threading.Thread(target=lambda: Discord.run_bot(stock_data=stock_data, emitter=emitter, notification_config=notification_config))
    scheduler_thread = threading.Thread(target=lambda: scheduler.scheduler(stock_data=stock_data, emitter=emitter))

    bot_thread.start()
    logger.debug("Bot thread initialized")

    scheduler_thread.start()
    logger.debug("Scheduler thread initialized")

    bot_thread.join()
    logger.debug("Threads joined")


if __name__ == '__main__':
    main()

import sys
sys.path.append('discord')
import bot as Discord
import stockdata as sd
import logging
from logging.handlers import RotatingFileHandler
import threading
import scheduler

# Logging configuration
format = '%(asctime)s [%(levelname)-8s] [%(thread)-5d] %(module)s.%(funcName)-20s > %(message)s'
logfile_handler = RotatingFileHandler(filename="rocketstocks.log", maxBytes=1073741824, backupCount=10)
logfile_handler.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(stream=sys.stdout)
stdout_handler.setLevel(logging.INFO)
stdout_handler.addFilter(logging.Filter(__name__))
handlers = [logfile_handler, stdout_handler]
logging.basicConfig(level=logging.DEBUG, format=format, handlers=handlers)
logger = logging.getLogger(__name__)

def rocketStocks():
    logger.info('**********[START LOG]**********')

    bot_thread = threading.Thread(target=Discord.run_bot)
    scheduler_thread = threading.Thread(target=scheduler.scheduler)

    bot_thread.start()
    logger.debug("Bot thread initialized")

    scheduler_thread.start()
    logger.debug("Scheduler thread initialized")
    
    bot_thread.join()
    logger.debug("Threads joined")

if (__name__ == '__main__'):
    rocketStocks()
    

    

    
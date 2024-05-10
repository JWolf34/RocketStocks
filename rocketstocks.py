import sys
sys.path.append('discord')
import bot as Discord
import stockdata as sd
import logging
import threading
import scheduler

logger = logging.getLogger(__name__)
format = '%(asctime)s [%(levelname)-8s] [%(thread)-8d] %(module)s.%(funcName)s: %(message)s'
logging.basicConfig(filename="rocketstocks.log", level=logging.DEBUG, format=format)
logger.info('**********[START LOG]**********')

def rocketStocks():
    
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
    

    

    
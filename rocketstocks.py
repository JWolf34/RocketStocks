import sys
sys.path.append('discord')
import bot as Discord
import stockdata as sd
import logging
import threading
import scheduler

# Logging configuration
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
format = '%(asctime)s [%(levelname)-8s] [%(thread)-5d] %(module)s.%(funcName)s: %(message)s'
logging.basicConfig(filename="rocketstocks.log", level=logging.DEBUG, format=format)

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
    logger.info('**********[START LOG]**********')
    rocketStocks()
    

    

    
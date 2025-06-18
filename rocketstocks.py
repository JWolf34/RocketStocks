import sys
sys.path.append('discord')
sys.path.append('discord/cogs')
sys.path.append('stockdata')
import bot as Discord
from stock_data import StockData
import logging
import logging.config
import logging.handlers
import threading
import scheduler
import json
import os

# Logging configuration
logger = logging.getLogger(__name__)

class moduleFilter(logging.Filter):
    def filter(self, record: logging.LogRecord):
        module = record.module
        modules = []
        for filename in os.listdir("./"): # append all files in root folder
            if filename.endswith(".py"):
                modules.append(filename[:-3])
        for filename in os.listdir("./discord"): # append all files in discord folder
            if filename.endswith(".py"):
                modules.append(filename[:-3])
        for filename in os.listdir("./discord/cogs"): # append all files in discord/cogs folder
            if filename.endswith(".py"):
                modules.append(filename[:-3])
        for filename in os.listdir("./stockdata"): # append all files in stockdata folder
            if filename.endswith(".py"):
                modules.append(filename[:-3])
        return module in modules
    
class MyLogFormatter(logging.Formatter):
    def format(self, record):
        location = '%s.%s' % (record.name, record.funcName)
        msg = '%s [%-8s] [%-5s] %-40s > %s' % (self.formatTime(record), record.levelname, record.thread, location, record.msg)
        #record.msg = msg
        return super(MyLogFormatter, self).format(record)

def setup_logging():
    log_path = "logs/"
    if not (os.path.isdir(log_path)):
        os.makedirs(log_path) 

    config_file = "logconfig.json"
    with open(config_file) as f:
        config = json.load(f)

    logging.config.dictConfig(config)


def rocketStocks():
    logger.info('**********[START LOG]**********')

    # Init StockData object
    stock_data = StockData()

    # Build threads - one for bot and one for scheduler
    bot_thread = threading.Thread(target=lambda: Discord.run_bot(stock_data=stock_data))
    scheduler_thread = threading.Thread(target=lambda:scheduler.scheduler(stock_data=stock_data))

    bot_thread.start()
    logger.debug("Bot thread initialized")

    scheduler_thread.start()
    logger.debug("Scheduler thread initialized")
    
    bot_thread.join()
    logger.debug("Threads joined")

def test():
    import asyncio
    sd = StockData()
    asyncio.run(sd.update_5m_price_history())
    

if (__name__ == '__main__'):
    setup_logging()
    #rocketStocks()
    test()
    
    

    
import sys
sys.path.append('discord')
import bot as Discord
import stockdata as sd
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
        modules = ['bot', 'analysis', 'stockdata', 'scheduler', 'rocketstocks']
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

    bot_thread = threading.Thread(target=Discord.run_bot)
    scheduler_thread = threading.Thread(target=scheduler.scheduler)

    bot_thread.start()
    logger.debug("Bot thread initialized")

    scheduler_thread.start()
    logger.debug("Scheduler thread initialized")
    
    bot_thread.join()
    logger.debug("Threads joined")

if (__name__ == '__main__'):
    setup_logging()
    rocketStocks()
    

    

    
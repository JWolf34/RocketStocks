import json
import logging
import logging.config
import logging.handlers
import os

logger = logging.getLogger(__name__)


class moduleFilter(logging.Filter):
    """Only pass log records from modules within the rocketstocks package."""
    def filter(self, record: logging.LogRecord):
        return record.name.startswith('rocketstocks') or record.name == '__main__'


class MyLogFormatter(logging.Formatter):
    def format(self, record):
        location = '%s.%s' % (record.name, record.funcName)
        msg = '%s [%-8s] [%-5s] %-40s > %s' % (
            self.formatTime(record), record.levelname, record.thread, location, record.msg
        )
        return super(MyLogFormatter, self).format(record)


def setup_logging():
    log_path = "logs/"
    if not os.path.isdir(log_path):
        os.makedirs(log_path)

    config_file = "logconfig.json"
    with open(config_file) as f:
        config = json.load(f)

    logging.config.dictConfig(config)

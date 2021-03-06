import eventlet
import logging
import sys

from . import settings

LOGGERS = ['Server', 'Client', 'Node', 'Log', 'STM']


def config_log(level=logging.DEBUG, CONF=None):
    if CONF:
        log_filename = CONF.get('logging', 'logfile')
        level = getattr(logging, CONF.get('logging', 'level'))

    def _set_logger(logger_name, level, log_filename):
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

        format_str = ("%(asctime)s - %(name)s - %(levelname)s"
                      " - %(funcName)s -  %(message)s")
        formatter = logging.Formatter(format_str)
        handler = logging.FileHandler(log_filename)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    NUM_LOGGERS = len(LOGGERS)
    map(_set_logger,
        LOGGERS,
        [level] * NUM_LOGGERS,
        [log_filename] * NUM_LOGGERS)


def batch_fetch_from_queue(queue):
    elements = []
    try:
        for i in range(settings.MAX_BATCH_ITEMS):
            elements.append(queue.get(block=False))
    except eventlet.queue.Empty:
        pass
    finally:
        return elements

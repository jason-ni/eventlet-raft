import logging
import sys


LOGGERS = ['Server', 'Client', 'Node']


def config_log(output='STDOUT', level=logging.DEBUG, CONF=None):
    if CONF:
        output = CONF.get('logging', 'output')
        level = getattr(logging, CONF.get('logging', 'level'))

    def _set_logger(logger_name, level, handler):
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        handler.setLevel(level)
        logger.addHandler(handler)

    NUM_LOGGERS = len(LOGGERS)
    if isinstance(output, str) and output == 'STDOUT':
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(output)
    map(_set_logger,
        LOGGERS,
        [level] * NUM_LOGGERS,
        [handler] * NUM_LOGGERS)

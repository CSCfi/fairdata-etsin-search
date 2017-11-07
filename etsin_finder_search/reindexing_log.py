import logging
import logging.handlers
from etsin_finder_search.utils import get_config_from_file


def get_logger(logger_name):
    conf = get_config_from_file()
    logger = logging.getLogger(logger_name if logger_name else 'reindexing')
    handler = logging.handlers.RotatingFileHandler(conf.get('SEARCH_APP_LOG_PATH',
                                                            '/var/log/etsin_finder/this_should_not_be_here.log'),
                                                   maxBytes=10000000, mode='a', backupCount=30)
    handler.setLevel(conf.get('SEARCH_APP_LOG_LEVEL', 'INFO'))
    formatter = logging.Formatter("[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

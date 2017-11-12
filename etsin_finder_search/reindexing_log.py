import logging
import logging.handlers
from etsin_finder_search.utils import get_config_from_file, executing_travis


def get_logger(logger_name):
    conf = get_config_from_file() if not executing_travis() else {}
    logger = logging.getLogger(logger_name if logger_name else 'etsin_finder_search')
    logger.setLevel(conf.get('SEARCH_APP_LOG_LEVEL', 'DEBUG'))
    handler = logging.handlers.RotatingFileHandler(conf.get('SEARCH_APP_LOG_PATH',
                                                            '/var/log/etsin_finder_search/etsin_finder_search.log'),
                                                   maxBytes=10000000, mode='a', backupCount=30)
    formatter = logging.Formatter("[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

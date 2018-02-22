#!/usr/bin/python

import signal
import sys
import time

from etsin_finder_search.rabbitmq.rabbitmq_client import MetaxConsumer
from etsin_finder_search.reindexing_log import get_logger


log = get_logger(__name__)
log.info("Initializing RabbitMQ consumer..")
consumer = MetaxConsumer()


def signal_term_handler(signal, frame):
    """
    Handler for the sigterm event occurring when the running of this code is being terminated by systemd.
    First cancel consumers in order not to receive any more messages. After that for 10 seconds
    wait for the current reindexing operation to finish. After that exit the program anyway.

    :param signal:
    :param frame:
    :return:
    """

    consumer.before_stop()
    reindexing_ongoing = not consumer.event_processing_completed
    i = 0
    while reindexing_ongoing and i < 10:
        log.info("Waiting for reindexing operation to finish before exiting RabbitMQ consumer..")
        time.sleep(1)
        reindexing_ongoing = not consumer.event_processing_completed
        i += 1

    log.info("Exiting RabbitMQ consumer")
    sys.exit(0)


# If consumer initialized ok (i.e. finished the __init__ without returning), start consuming
if consumer.init_ok:
    log.info("RabbitMQ consumer initialized")
    signal.signal(signal.SIGTERM, signal_term_handler)
    consumer.run()
else:
    log.error("Consumer not initialized, exiting service")
    sys.exit(1)

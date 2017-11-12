#!/usr/bin/python

from etsin_finder_search.rabbitmq.rabbitmq_client import MetaxConsumer

consumer = MetaxConsumer()
consumer.run()

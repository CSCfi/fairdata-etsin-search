"""
Consumer connects to Metax RabbitMQ and listens for changes in Metax.
When metadata is created, updated or deleted, consumer calls appropriate
functions to propagate the change to Etsin search index.

This script should be run as a standalone. It's not part of the Flask app.
(ssh to server or Vagrant)
sudo su - etsin-user
source_pyenv
python /etsin/etsin_finder/rabbitmq_client.py

Press CTRL+C to exit script.
"""

import json
import pika

from elasticsearch.exceptions import RequestError

from etsin_finder_search.catalog_record_converter import CRConverter
from etsin_finder_search.elastic.service.es_service import ElasticSearchService
from etsin_finder_search.reindexing_log import get_logger
from etsin_finder_search.utils import get_metax_rabbit_mq_config
from etsin_finder_search.utils import get_elasticsearch_config

class MetaxConsumer():
    def __init__(self):
        self.log = get_logger(__name__)

        # Get configs
        # If these raise errors, let consumer init fail
        rabbit_settings = get_metax_rabbit_mq_config()
        es_settings = get_elasticsearch_config()

        # Set up RabbitMQ connection, channel, exchange and queues
        credentials = pika.PlainCredentials(
            rabbit_settings['USER'], rabbit_settings['PASSWORD'])
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                rabbit_settings['HOST'],
                rabbit_settings['PORT'],
                rabbit_settings['VHOST'],
                credentials))

        self.channel = connection.channel()

        exchange = rabbit_settings['EXCHANGE']
        queue_1 = 'etsin-create'
        queue_2 = 'etsin-update'
        queue_3 = 'etsin-delete'

        self.channel.queue_declare(queue_1, durable=True)
        self.channel.queue_declare(queue_2, durable=True)
        self.channel.queue_declare(queue_3, durable=True)

        self.channel.queue_bind(exchange=exchange, queue=queue_1, routing_key='create')
        self.channel.queue_bind(exchange=exchange, queue=queue_2, routing_key='update')
        self.channel.queue_bind(exchange=exchange, queue=queue_3, routing_key='delete')

        # Set up ElasticSearch client
        es_client = ElasticSearchService(es_settings)
        if not es_client.index_exists():
            if not es_client.create_index_and_mapping():
                # If there's no ES index, don't create consumer
                raise Exception

        def callback_reindex(ch, method, properties, body):
            converter = CRConverter()
            es_data_model = converter.convert_metax_catalog_record_json_to_es_data_model(
                json.loads(body))
            reindex_success = False
            try:
                reindex_success = es_client.reindex_dataset(es_data_model)
            except RequestError:
                reindex_success = False
            finally:
                if reindex_success:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    self.log.info('Failed to reindex %s', json.loads(
                        body).get('urn_identifier', 'unknown identifier'))
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        def callback_delete(ch, method, properties, body):
            delete_success = False
            try:
                delete_success = es_client.delete_dataset(json.loads(body).get('urn_identifier'))
            except RequestError:
                delete_success = False
            finally:
                if delete_success:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    self.log.info('Failed to delete %s', json.loads(
                        body).get('urn_identifier', 'unknown identifier'))
                    # TODO: If delete fails because there's no such id in index,
                    # no need to requeue
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        # Set up consumer
        self.channel.basic_consume(callback_reindex, queue=queue_1)
        self.channel.basic_consume(callback_reindex, queue=queue_2)
        self.channel.basic_consume(callback_delete, queue=queue_3)

    def run(self):
        self.log.info('[*] RabbitMQ client started')
        print('[*] RabbitMQ is running. To exit press CTRL+C. See logs for indexing details.')
        self.channel.start_consuming()

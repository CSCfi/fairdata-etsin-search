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
import time

from elasticsearch.exceptions import RequestError

from etsin_finder_search.catalog_record_converter import CRConverter
from etsin_finder_search.elastic.service.es_service import ElasticSearchService
from etsin_finder_search.reindexing_log import get_logger
from etsin_finder_search.utils import get_metax_rabbit_mq_config, get_elasticsearch_config, get_config_from_file


class MetaxConsumer():

    def __init__(self):
        self.log = get_logger(__name__)
        self.indexing_operation_complete = True
        self.init_ok = False

        # Get configs
        # If these raise errors, let consumer init fail
        rabbit_settings = get_metax_rabbit_mq_config()
        es_settings = get_elasticsearch_config()

        if not rabbit_settings or not es_settings:
            self.log.error("Unable to load RabbitMQ configuration or Elasticsearch configuration")
            return

        # Set up RabbitMQ connection, channel, exchange and queues
        credentials = pika.PlainCredentials(
            rabbit_settings['USER'], rabbit_settings['PASSWORD'])

        # Try connecting every one minute 30 times
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    rabbit_settings['HOST'],
                    rabbit_settings['PORT'],
                    rabbit_settings['VHOST'],
                    credentials,
                    connection_attempts=30,
                    retry_delay=60))
        except Exception as e:
            self.log.error(e)
            self.log.error("Unable to open RabbitMQ connection")
            return

        # Set up ElasticSearch client. In case connection cannot be established, try every 2 seconds for 30 seconds
        es_conn_ok = False
        i = 0
        while not es_conn_ok and i < 15:
            self.es_client = ElasticSearchService(es_settings)
            if self.es_client.client_ok():
                es_conn_ok = True
            else:
                time.sleep(2)
                i += 1

        if not es_conn_ok or not self._ensure_index_existence():
            return

        self.channel = connection.channel()
        self.exchange = rabbit_settings['EXCHANGE']
        self.create_queue = 'etsin-create'
        self.update_queue = 'etsin-update'
        self.delete_queue = 'etsin-delete'

        self._create_and_bind_queues()

        def callback_reindex(ch, method, properties, body):
            self.indexing_operation_complete = False
            self.log.debug("Received create or update message from Metax RabbitMQ")

            if not self._ensure_index_existence():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                self.indexing_operation_complete = True
                return

            body_as_json = self._get_message_body_as_json(body)
            if not body_as_json:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                self.indexing_operation_complete = True
                return

            converter = CRConverter()
            es_data_model = converter.convert_metax_catalog_record_json_to_es_data_model(body_as_json)

            if not es_data_model:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                self.indexing_operation_complete = True
                return

            es_reindex_success = False
            try:
                es_reindex_success = self.es_client.reindex_dataset(es_data_model)
            except RequestError:
                es_reindex_success = False
            finally:
                if es_reindex_success:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    self.log.error('Failed to reindex %s', json.loads(
                        body).get('urn_identifier', 'unknown identifier'))
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

                self.indexing_operation_complete = True

        def callback_delete(ch, method, properties, body):
            self.indexing_operation_complete = False
            self.log.debug("Received delete message from Metax RabbitMQ")

            if not self._ensure_index_existence():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                self.indexing_operation_complete = True
                return

            body_as_json = self._get_message_body_as_json(body)
            if not body_as_json:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                self.indexing_operation_complete = True
                return

            delete_success = False
            try:
                delete_success = self.es_client.delete_dataset(body_as_json.get('urn_identifier'))
            except RequestError:
                delete_success = False
            finally:
                if delete_success:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    self.log.error('Failed to delete %s', json.loads(
                        body).get('urn_identifier', 'unknown identifier'))
                    # TODO: If delete fails because there's no such id in index,
                    # no need to requeue
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

                self.indexing_operation_complete = True

        # Set up consumers
        self.create_consumer_tag = self.channel.basic_consume(callback_reindex, queue=self.create_queue)
        self.update_consumer_tag = self.channel.basic_consume(callback_reindex, queue=self.update_queue)
        self.delete_consumer_tag = self.channel.basic_consume(callback_delete, queue=self.delete_queue)

        self.init_ok = True

    def run(self):
        self.log.info('RabbitMQ client starting to consume messages..')
        print('[*] RabbitMQ is running. To exit press CTRL+C. See logs for indexing details.')
        self.channel.start_consuming()

    def before_stop(self):
        self._cancel_consumers()

    def _get_message_body_as_json(self, body):
        try:
            return json.loads(body)
        except ValueError:
            self.log.error("RabbitMQ message cannot be interpreted as json")

        return None

    def _cancel_consumers(self):
        self.channel.basic_cancel(consumer_tag=self.create_consumer_tag)
        self.channel.basic_cancel(consumer_tag=self.update_consumer_tag)
        self.channel.basic_cancel(consumer_tag=self.delete_consumer_tag)

    def _create_and_bind_queues(self):
        self.channel.queue_declare(self.create_queue, durable=True)
        self.channel.queue_declare(self.update_queue, durable=True)
        self.channel.queue_declare(self.delete_queue, durable=True)

        self.channel.queue_bind(exchange=self.exchange, queue=self.create_queue, routing_key='create')
        self.channel.queue_bind(exchange=self.exchange, queue=self.update_queue, routing_key='update')
        self.channel.queue_bind(exchange=self.exchange, queue=self.delete_queue, routing_key='delete')

    def _ensure_index_existence(self):
        if not self.es_client.index_exists():
            if not self.es_client.create_index_and_mapping():
                # If there's no ES index, don't create consumer
                self.log.error("Unable to create Elasticsearch index and type mapping")
                return False

        return True

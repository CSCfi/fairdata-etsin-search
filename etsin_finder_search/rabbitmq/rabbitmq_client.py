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
import os

from elasticsearch.exceptions import RequestError

from etsin_finder_search.catalog_record_converter import CRConverter
from etsin_finder_search.elastic.service.es_service import ElasticSearchService
from etsin_finder_search.reindexing_log import get_logger
from etsin_finder_search.utils import \
    get_metax_rabbit_mq_config, \
    get_elasticsearch_config, \
    get_catalog_record_previous_version_identifier


class MetaxConsumer():

    def __init__(self):
        self.log = get_logger(__name__)
        self.event_processing_completed = True
        self.init_ok = False

        # Get configs
        # If these raise errors, let consumer init fail
        rabbit_settings = get_metax_rabbit_mq_config()
        es_settings = get_elasticsearch_config()

        is_local_dev = True if os.path.isdir("/etsin/ansible") else False

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

        self._set_queue_names(is_local_dev)
        self._create_and_bind_queues(is_local_dev)

        def callback_create(ch, method, properties, body):
            if not self._init_event_callback_ok("create", ch, method):
                return

            body_as_json = self._get_event_json_body(ch, method, body)
            if not body_as_json:
                return

            self._convert_to_es_doc_and_reindex(ch, method, body_as_json)

        def callback_update(ch, method, properties, body):
            if not self._init_event_callback_ok("update", ch, method):
                return

            body_as_json = self._get_event_json_body(ch, method, body)
            if not body_as_json:
                return

            self._convert_to_es_doc_and_reindex(ch, method, body_as_json)

        def callback_delete(ch, method, properties, body):
            if not self._init_event_callback_ok("delete", ch, method):
                return

            body_as_json = self._get_event_json_body(ch, method, body)
            if not body_as_json:
                return

            delete_success = False
            try:
                delete_success = self.es_client.delete_dataset(body_as_json.get('urn_identifier'))
                # TODO: Move the document to the "dead dataset" index maybe via Reindex API?
            except RequestError:
                delete_success = False
            finally:
                if delete_success:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    self.log.error('Failed to delete %s', body_as_json.get('urn_identifier'))
                    # TODO: If delete fails because there's no such id in index, no need to requeue
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

                self.event_processing_completed = True

        # Set up consumers so that acks are required
        self.create_consumer_tag = self.channel.basic_consume(callback_create, queue=self.create_queue, no_ack=False)
        self.update_consumer_tag = self.channel.basic_consume(callback_update, queue=self.update_queue, no_ack=False)
        self.delete_consumer_tag = self.channel.basic_consume(callback_delete, queue=self.delete_queue, no_ack=False)

        self.init_ok = True

    def run(self):
        self.log.info('RabbitMQ client starting to consume messages..')
        print('[*] RabbitMQ is running. To exit press CTRL+C. See logs for indexing details.')
        self.channel.start_consuming()

    def before_stop(self):
        self._cancel_consumers()

    def _convert_to_es_doc_and_reindex(self, ch, method, body_as_json):
        prev_version_id = get_catalog_record_previous_version_identifier(body_as_json)
        converter = CRConverter()
        es_data_model = converter.convert_metax_cr_json_to_es_data_model(body_as_json)

        if not es_data_model:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.event_processing_completed = True
            return False

        es_reindex_success = False
        try:
            es_reindex_success = self.es_client.reindex_dataset(es_data_model) and \
                                 (prev_version_id is None or self.es_client.delete_dataset(prev_version_id))
        except Exception:
            es_reindex_success = False
        finally:
            if es_reindex_success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                self.log.error('Failed to reindex %s', body_as_json.get('research_dataset').get('urn_identifier'))
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            self.event_processing_completed = True

    def _init_event_callback_ok(self, callback_type, ch, method):
        self.event_processing_completed = False
        self.log.debug("Received {0} message from Metax RabbitMQ".format(callback_type))

        if not self._ensure_index_existence():
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.event_processing_completed = True
            return False

        return True

    def _get_event_json_body(self, ch, method, body):
        body_as_json = self._get_message_body_as_json(body)
        if not body_as_json:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.event_processing_completed = True
            return None
        return body_as_json

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

    def _set_queue_names(self, is_local_dev):
        self.create_queue = 'etsin-create'
        self.update_queue = 'etsin-update'
        self.delete_queue = 'etsin-delete'

        if is_local_dev:
            # The point of this section is to give unique names to the queues created in local dev env
            # This is done to prevent the target rabbitmq server from having multiple consumers consuming the same
            # queue (this would result in round-robin type of delivering of messages).
            #
            # Below, also a time-to-live for a local dev queue is set to automatically delete the queues from the
            # target rabbitmq server after a set period of time so as not to make the rabbitmq virtual host to be
            # filled with local dev queues. Cf. http://www.rabbitmq.com/ttl.html#queue-ttl

            import json
            if os.path.isfile('/srv/etsin/rabbitmq_queues.json'):
                with open('/srv/etsin/rabbitmq_queues.json') as json_data:
                    queues = json.load(json_data)
                    self.create_queue = queues.get('create', None)
                    self.update_queue = queues.get('update', None)
                    self.delete_queue = queues.get('delete', None)
            else:
                import time
                timestamp = str(time.time())

                self.create_queue += '-' + timestamp
                self.update_queue += '-' + timestamp
                self.delete_queue += '-' + timestamp

                queues = {'create': self.create_queue,
                          'update': self.update_queue,
                          'delete': self.delete_queue}

                with open('/srv/etsin/rabbitmq_queues.json', 'w') as outfile:
                    json.dump(queues, outfile)

    def _create_and_bind_queues(self, is_local_dev):
        args = {}
        if is_local_dev:
            # Expire local dev created queues after 8 hours of inactivity
            args['x-expires'] = 28800000

        self.channel.queue_declare(self.create_queue, durable=True, arguments=args)
        self.channel.queue_declare(self.update_queue, durable=True, arguments=args)
        self.channel.queue_declare(self.delete_queue, durable=True, arguments=args)

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

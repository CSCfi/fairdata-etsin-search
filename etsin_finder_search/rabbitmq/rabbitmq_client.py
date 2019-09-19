# This file is part of the Etsin service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

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
import os
import pika
import random
from time import sleep

from elasticsearch.exceptions import RequestError

from etsin_finder_search.catalog_record_converter import CRConverter
from etsin_finder_search.elastic.domain.es_dataset_data_model import ESDatasetModel
from etsin_finder_search.elastic.service.es_service import ElasticSearchService
from etsin_finder_search.reindexing_log import get_logger
from etsin_finder_search.utils import \
    get_metax_rabbit_mq_config, \
    get_elasticsearch_config, \
    get_catalog_record_previous_dataset_version_identifier, \
    catalog_record_has_next_dataset_version, \
    catalog_record_has_previous_dataset_version, \
    catalog_record_is_deprecated, \
    catalog_has_preservation_dataset_origin_version, \
    catalog_record_has_preferred_identifier, \
    catalog_record_has_identifier, \
    catalog_record_should_be_indexed, \
    get_catalog_record_identifier


class MetaxConsumer():

    def __init__(self):
        self.log = get_logger(__name__)
        self.event_processing_completed = True
        self.init_ok = False

        # Get configs
        # If these raise errors, let consumer init fail
        self.rabbit_settings = get_metax_rabbit_mq_config()
        es_settings = get_elasticsearch_config()

        self.is_local_dev = True if os.path.isdir("/etsin/ansible") else False

        if not self.rabbit_settings or not es_settings:
            self.log.error("Unable to load RabbitMQ configuration or Elasticsearch configuration")
            return

        self.credentials = pika.PlainCredentials(self.rabbit_settings['USER'], self.rabbit_settings['PASSWORD'])
        self.exchange = self.rabbit_settings['EXCHANGE']
        self._set_queue_names(self.is_local_dev)

        self.es_client = ElasticSearchService.get_elasticsearch_service(es_settings)
        if self.es_client is None:
            return

        if not self.es_client.ensure_index_existence():
            return

        self.init_ok = True

    def run(self):
        if self._setup_connection() and self._setup_consumers():
            self.log.info('RabbitMQ client starting to consume messages..')
            print('[*] RabbitMQ is running. To exit press CTRL+C. See logs for indexing details.')
            try:
                self.channel.start_consuming()
            except Exception as e:
                self.log.error(e)
                self.log.error('An error occurred while consuming')
                return
        else:
            self.log.error('Unable to setup RabbitMQ connection or consumers')
            return

    def _setup_connection(self):
        self.log.info("Setting up connection to RabbitMQ server..")
        hosts = self.rabbit_settings['HOSTS']

        # Connection retries are needed as long as there is no load balancer in front of rabbitmq-server VMs
        sleep_time = 30
        num_conn_retries = 3000

        for x in range(0, num_conn_retries):
            # Choose host randomly so that different hosts are tried out in case of connection problems
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                    random.choice(hosts),
                    self.rabbit_settings['PORT'],
                    self.rabbit_settings['VHOST'],
                    self.credentials))

                self.channel = self.connection.channel()
                self._create_and_bind_queues(self.is_local_dev)
                str_error = None
            except Exception as e:
                self.log.error(e)
                self.log.error("Problem connecting to RabbitMQ server, trying to reconnect in {0} seconds...".
                               format(str(sleep_time)))
                str_error = e

            if str_error:
                sleep(sleep_time)  # wait before trying to connect again
            else:
                break

        if not self.connection:
            return False

        self.log.info("Connection OK")
        return True

    def _setup_consumers(self):
        self.log.info("Setting up consumers..")

        def callback_create(ch, method, properties, body):
            if not self._init_event_callback_ok("create", ch, method):
                return

            body_as_json = self._get_event_json_body(ch, method, body)
            if not body_as_json:
                return

            # If cr har previous dataset version on create message, try to delete the previous document from the index
            if catalog_record_has_identifier(body_as_json) and \
                    catalog_record_has_previous_dataset_version(body_as_json):
                incoming_cr_id = get_catalog_record_identifier(body_as_json)
                prev_version_cr_id = get_catalog_record_previous_dataset_version_identifier(body_as_json)

                self.log.info(
                    "Identifier {0} has a previous dataset version {1}. Trying to delete the previous dataset version "
                    "from index...".format(incoming_cr_id, prev_version_cr_id))
                self.es_client.delete_dataset_from_index(prev_version_cr_id)

            # If catalog_has_preservation_dataset_origin_version is found, it means the dataset is stored in PAS and has an original version.
            # This original version will be displayed in the dataset list instead, so this PAS dataset version should be excluded.
            if catalog_has_preservation_dataset_origin_version(body_as_json):
                self.log.info("Identifier {0} is a PAS dataset, and has a dataset in original version. "
                                "Trying to delete from index if it exists..".format(incoming_cr_id))
                self._delete_from_index(ch, method, body_as_json)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.event_processing_completed = True
                return

            self._convert_to_es_doc_and_reindex(ch, method, body_as_json)

        def callback_update(ch, method, properties, body):
            if not self._init_event_callback_ok("update", ch, method):
                return

            body_as_json = self._get_event_json_body(ch, method, body)
            if not body_as_json:
                return

            if catalog_record_has_identifier(body_as_json):
                incoming_cr_id = get_catalog_record_identifier(body_as_json)

                # If catalog record has been deprecated, delete it from index
                if catalog_record_is_deprecated(body_as_json):
                    self.log.info("Identifier {0} is deprecated. "
                                  "Trying to delete from index if it exists..".format(incoming_cr_id))
                    self._delete_from_index(ch, method, body_as_json)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.event_processing_completed = True
                    return

                # If catalog_has_preservation_dataset_origin_version is found, it means the dataset is stored in PAS and has an original version.
                # This original version will be displayed in the dataset list instead, so this PAS dataset version should be excluded.
                # Dataset must be excluded here in callback_update as well, to prevent it appearning in the list again, after an update of the dataset
                if catalog_has_preservation_dataset_origin_version(body_as_json):
                    self.log.info("Identifier {0} is a PAS dataset, and has a dataset in original version. "
                                    "Trying to delete from index if it exists..".format(incoming_cr_id))
                    self._delete_from_index(ch, method, body_as_json)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.event_processing_completed = True
                    return

                # If catalog record has next dataset version, do not index it
                if catalog_record_has_next_dataset_version(body_as_json):
                    self.log.info("Identifier {0} has a next dataset version. Skipping reindexing..."
                                  .format(incoming_cr_id))
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.event_processing_completed = True
                    return

            self._convert_to_es_doc_and_reindex(ch, method, body_as_json)

        def callback_delete(ch, method, properties, body):
            if not self._init_event_callback_ok("delete", ch, method):
                return

            body_as_json = self._get_event_json_body(ch, method, body)
            if not body_as_json:
                return

            self._delete_from_index(ch, method, body_as_json)

        # Set up consumers so that acks are required
        try:
            self.create_consumer_tag = self.channel.basic_consume(
                self.create_queue, callback_create, auto_ack=False)
            self.update_consumer_tag = self.channel.basic_consume(
                self.update_queue, callback_update, auto_ack=False)
            self.delete_consumer_tag = self.channel.basic_consume(
                self.delete_queue, callback_delete, auto_ack=False)
        except Exception as e:
            self.log.error(e)
            self.log.error("Unable to setup consumers")
            return False

        self.log.info("Consumers OK")
        return True

    def before_stop(self):
        self._cancel_consumers()

    def _delete_from_index(self, ch, method, body_as_json):
        try:
            cr_id_for_doc_to_delete = get_catalog_record_identifier(body_as_json)
            if cr_id_for_doc_to_delete:
                delete_success = self.es_client.delete_dataset_from_index(cr_id_for_doc_to_delete)
                if delete_success:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    self.log.error('Failed to delete document from index: %s', cr_id_for_doc_to_delete)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                self.log.error('No identifier found from RabbitMQ message, ignoring')
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except RequestError:
            self.log.error('Request error on trying to delete from index triggered by RabbitMQ')
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        finally:
            self.event_processing_completed = True

    def _convert_to_es_doc_and_reindex(self, ch, method, body_as_json):
        if not catalog_record_should_be_indexed(body_as_json):
            self.log.debug('Catalog record not indexed due to defined rules')
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.event_processing_completed = True
            return

        if not catalog_record_has_preferred_identifier(body_as_json) or not catalog_record_has_identifier(body_as_json):
            self.log.error('No preferred_identifier or identifier found from RabbitMQ message, ignoring')
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.event_processing_completed = True
            return

        converter = CRConverter()
        es_doc = converter.convert_metax_cr_json_to_es_data_model(body_as_json)
        if not es_doc:
            self.log.error("Unable to convert Metax catalog record to es data model, not requeing message")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.event_processing_completed = True
            return

        try:
            es_reindex_success = self.es_client.reindex_dataset(ESDatasetModel(es_doc))
            if es_reindex_success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                self.log.error('Failed to reindex %s', get_catalog_record_identifier(body_as_json))
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        finally:
            self.event_processing_completed = True

    def _init_event_callback_ok(self, callback_type, ch, method):
        self.event_processing_completed = False
        self.log.debug("Received {0} message from Metax RabbitMQ".format(callback_type))

        if not self.es_client.ensure_index_existence():
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
            if os.path.isfile('/home/etsin-user/rabbitmq_queues.json'):
                with open('/home/etsin-user/rabbitmq_queues.json') as json_data:
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

                with open('/home/etsin-user/rabbitmq_queues.json', 'w') as outfile:
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

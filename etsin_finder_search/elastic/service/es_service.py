# This file is part of the Etsin service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import json
from os import path
from time import sleep

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from etsin_finder_search.reindexing_log import get_logger

log = get_logger(__name__)


class ElasticSearchService:
    """
    Service for operating with Elasticsearch APIs
    """

    INDEX_NAME = 'metax'
    INDEX_CONFIG_FILENAME = 'metax_index_definition.json'
    INDEX_DOC_TYPE_NAME = 'dataset'
    INDEX_DOC_TYPE_MAPPING_FILENAME = 'dataset_type_mapping.json'
    BULK_OPERATION_ROW_SIZE = 300

    def __init__(self, es_settings):
        self.es = Elasticsearch(es_settings.get('HOSTS'), timeout=180, **self._get_connection_parameters(es_settings))

    @classmethod
    def get_elasticsearch_service(cls, es_config):
        if es_config:
            # Set up ElasticSearch client. In case connection cannot be established, try every 2 seconds 30 times
            log.info("Trying to establish connection with Elasticsearch instance..")
            i = 0
            while i < 30:
                es_client = cls(es_config)
                if es_client._client_ok():
                    log.info("Connection established with Elasticsearch instance")
                    return es_client
                else:
                    log.error("Connection not established with Elasticsearch instance, trying again..")
                    sleep(2)
                    i += 1

            log.error("Unable to establish connection with Elasticsearch instance, stopped trying")
            return None
        else:
            log.error("Unable to get Elasticsearch config")
            return None

    def ensure_index_existence(self):
        if not self._index_exists():
            if not self._create_index_and_mapping():
                log.error("Unable to create Elasticsearch index and type mapping")
                return False
        return True

    def delete_index(self):
        log.info("Trying to delete index " + self.INDEX_NAME)
        return self._operation_ok(self.es.indices.delete(index=self.INDEX_NAME, ignore=[404]))

    def reindex_dataset(self, dataset_data_model):
        log.info("{0} {1} into index {2}".format(
            "Trying to reindex data with doc id {0} having type".format(dataset_data_model.get_es_document_id()),
            self.INDEX_DOC_TYPE_NAME, self.INDEX_NAME))

        return self._operation_ok(self.es.index(
            index=self.INDEX_NAME, doc_type=self.INDEX_DOC_TYPE_NAME,
            id=dataset_data_model.get_es_document_id(),
            body=dataset_data_model.to_es_document_string()))

    def delete_dataset_from_index(self, doc_id):
        log.info("{0}{1} from index {2}".format(
            "Trying to delete data with doc id {0} having type ".format(doc_id), self.INDEX_DOC_TYPE_NAME,
            self.INDEX_NAME))

        if self._doc_exists_in_index(doc_id):
            return self._operation_ok(self.es.delete(index=self.INDEX_NAME, doc_type=self.INDEX_DOC_TYPE_NAME, id=doc_id))
        else:
            log.info("The document does not exist in the index, ignoring")
            return True

    def get_all_doc_ids_from_index(self):
        if not self._index_exists():
            log.error("No index exists")
            return None

        all_rows = scan(self.es, query={'query': {'match_all': {}}, "_source": False}, index=self.INDEX_NAME)
        all_doc_ids = []
        for row in all_rows:
            if row.get('_id', False):
                all_doc_ids.append(row['_id'])

        return all_doc_ids

    def do_bulk_request_for_datasets(self, dataset_models_to_reindex, doc_ids_to_delete):
        bulk_request_str = ''

        log.info("Reindexing {0} documents and trying to delete {1} documents".format(
            str(len(dataset_models_to_reindex)), str(len(doc_ids_to_delete))))

        if dataset_models_to_reindex:
            for item_no, dataset_data in enumerate(dataset_models_to_reindex, start=1):
                bulk_request_str += self._create_bulk_update_row(dataset_data) + "\n"
                if item_no % self.BULK_OPERATION_ROW_SIZE == 0:
                    self._do_bulk_request(bulk_request_str)
                    bulk_request_str = ''

        if doc_ids_to_delete:
            for item_no, doc_id in enumerate(doc_ids_to_delete, start=1):
                bulk_request_str += self._create_bulk_delete_row(doc_id) + "\n"
                if item_no % self.BULK_OPERATION_ROW_SIZE == 0:
                    self._do_bulk_request(bulk_request_str)
                    bulk_request_str = ''

        if bulk_request_str:
            self._do_bulk_request(bulk_request_str)

    def _do_bulk_request(self, bulk_request_str):
        log.info("Trying to perform bulk request for data with type " + self.INDEX_DOC_TYPE_NAME +
                 " into index " + self.INDEX_NAME)

        if not self._operation_ok(self.es.bulk(body=bulk_request_str, request_timeout=30)):
            log.error("Something went wrong with the following bulk request: \n{0}".format(bulk_request_str))
            return False

        return True

    def _empty_all_documents_from_index(self):
        log.info("Trying to delete all documents from index " + self.INDEX_NAME)
        return self._operation_ok(self.es.delete_by_query(index=self.INDEX_NAME,
                                                          body="{\"query\": { \"match_all\": {}}}"))

    def _create_bulk_update_row(self, dataset_data_model):
        return "{\"index\":{\"_index\": \"" + self.INDEX_NAME + "\", \"_type\": \"" + self.INDEX_DOC_TYPE_NAME \
               + "\", \"_id\":\"" + dataset_data_model.get_es_document_id() + "\"}}\n" + \
               dataset_data_model.to_es_document_string()

    def _create_bulk_delete_row(self, doc_id):
        return "{\"delete\":{\"_index\": \"" + self.INDEX_NAME + "\", \"_type\": \"" + self.INDEX_DOC_TYPE_NAME + \
               "\", \"_id\":\"" + doc_id + "\"}}"

    def _create_index_and_mapping(self):
        log.info("Trying to create index " + self.INDEX_NAME)
        is_ok = self._operation_ok(self.es.indices.create(index=self.INDEX_NAME,
                                                         body=self._get_json_file_as_str(self.INDEX_CONFIG_FILENAME)))
        if is_ok:
            log.info("Trying to create mapping type " + self.INDEX_DOC_TYPE_NAME + " for index " + self.INDEX_NAME)
            return self._operation_ok(
                self.es.indices.put_mapping(index=self.INDEX_NAME, doc_type=self.INDEX_DOC_TYPE_NAME,
                                            body=self._get_json_file_as_str(self.INDEX_DOC_TYPE_MAPPING_FILENAME)))
        return False

    def _client_ok(self):
        try:
            is_ok = self.es and self.es.ping()
        except Exception:
            is_ok = False

        return is_ok

    def _index_exists(self):
        return self.es.indices.exists(index=self.INDEX_NAME)

    def _doc_exists_in_index(self, doc_id):
        return self.es.exists(self.INDEX_NAME, self.INDEX_DOC_TYPE_NAME, doc_id)

    @staticmethod
    def _operation_ok(op_response):
        if ('errors' in op_response and op_response.get('errors')) or \
                ('acknowledged' in op_response and not op_response.get('acknowledged')):
            log.error('The performed operation had errors: \n{0}'.format(op_response))
            return False

        log.info('Operation OK')
        return True

    @staticmethod
    def _get_json_file_as_str(filename):
        with open(path.dirname(__file__) + '/../resources/' + filename) as json_data:
            return json.load(json_data)

    @staticmethod
    def _get_connection_parameters(settings):
        """
        https://docs.objectrocket.com/elastic_python_examples.html
        """
        if settings['HOSTS'][0] != 'localhost':
            conf = {'send_get_body_as': 'GET'}
            if settings.get('USE_SSL', False):
                conf.update({'port': 443, 'use_ssl': True, 'verify_certs': True})
            if settings.get('PORT', False):
                conf.update({'port': 9200})
            return conf
        return {}

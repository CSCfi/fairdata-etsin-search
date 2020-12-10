# This file is part of the Etsin service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from etsin_finder_search.elastic.domain.es_dataset_data_model import ESDatasetModel
from etsin_finder_search.elastic.service.es_service import ElasticSearchService
from etsin_finder_search.metax.metax_api import MetaxAPIService
from etsin_finder_search.catalog_record_converter import CRConverter
from etsin_finder_search.reindexing_log import get_logger
from etsin_finder_search.utils import \
    get_metax_api_config, \
    get_elasticsearch_config, \
    start_rabbitmq_consumer, \
    stop_rabbitmq_consumer, \
    rabbitmq_consumer_is_running, \
    catalog_record_is_deprecated, \
    catalog_has_preservation_dataset_origin_version, \
    catalog_record_should_be_indexed, \
    catalog_record_is_pas_catalog, \
    get_catalog_preservation_state


log = get_logger(__name__)

metax_api_config = get_metax_api_config()
es_config = get_elasticsearch_config()


def reindex_all_without_emptying_index():
    task = ReindexScheduledTask()
    task.run_task(False)
    _start_rabbitmq_service_if_not_running()


def reindex_all_by_emptying_index():
    task = ReindexScheduledTask()
    task.run_task(True)
    _start_rabbitmq_service_if_not_running()


def _start_rabbitmq_service_if_not_running():
    if not rabbitmq_consumer_is_running():
        log.info("Starting RabbitMQ consumer..")
        if start_rabbitmq_consumer():
            log.info("Started")
        else:
            log.error("Unable to start RabbitMQ consumer service")


def create_search_index_and_doc_type_mapping_if_not_exist():
    es_client = ElasticSearchService.get_elasticsearch_service(es_config)
    if es_client is None:
        log.error("Unable to initialize Elasticsearch client")
        return False

    if not es_client.ensure_index_existence():
        return False

    return True


def delete_search_index():
    es_client = ElasticSearchService.get_elasticsearch_service(es_config)
    if es_client is None:
        log.error("Unable to initialize Elasticsearch client")
        return

    es_client.delete_index()


def load_test_data_into_es(dataset_amt):
    log.info("Loading test data into Elasticsearch..")

    es_client = ElasticSearchService.get_elasticsearch_service(es_config)
    if es_client is None:
        log.error("Unable to initialize Elastisearch client")
        return False

    metax_api = MetaxAPIService.get_metax_api_service(metax_api_config)
    if metax_api is None:
        log.error("Unable to initialize Metax API client")
        return False

    if not es_client.ensure_index_existence():
        return False

    cr_identifiers = metax_api.get_latest_catalog_record_identifiers()
    if cr_identifiers:
        identifiers_to_load = cr_identifiers[0:min(len(cr_identifiers), dataset_amt)]
        identifiers_to_delete = []
        es_data_models = convert_identifiers_to_es_data_models(metax_api, identifiers_to_load, identifiers_to_delete)
        es_client.do_bulk_request_for_datasets(es_data_models, identifiers_to_delete)
        log.info("Test data loaded into Elasticsearch")
        return True

    log.error("No catalog record identifiers to load")
    return False


def convert_identifiers_to_es_data_models(metax_api, identifiers_to_convert, identifiers_to_delete, metax_crs_dict=None):
    """
    Takes in Metax catalog record identifiers, fetches their json from Metax, converts them to an ESDatasetModel
    object and adds to es_data_models list. Also checks if the dataset has been deprecated in which case also add it to
    identifiers_to_delete list

    :param identifiers_to_convert: Metax identifiers to fetch from Metax potentially to be reindexed
    :param identifiers_to_delete: list of Metax identifiers that will be sent to es bulk request
    :return: List of ESDatasetModel objects
    """

    es_dataset_models = []
    converter = CRConverter()
    log.info("Trying to convert {0} Metax catalog records to Elasticsearch documents. "
             "If catalog record is deprecated, try to delete it from index.".format(len(identifiers_to_convert)))

    for identifier in identifiers_to_convert:
        if metax_crs_dict:
            metax_cr_json = metax_crs_dict.get(identifier, None)
        else:
            metax_cr_json = metax_api.get_catalog_record(identifier)
            if not catalog_record_should_be_indexed(metax_cr_json):
                continue

        if metax_cr_json:
            # 1. If catalog record has been deprecated, delete its identifier from index
            # 2. If catalog_has_preservation_dataset_origin_version is found, it means the dataset is stored in PAS and has an original version.
            #    This original version will be displayed in the dataset list instead, so this PAS dataset version identifier should be excluded.
            if ((catalog_record_is_deprecated(metax_cr_json)) or (catalog_has_preservation_dataset_origin_version(metax_cr_json))):
                identifiers_to_delete.append(identifier)
                continue

            if (catalog_record_is_pas_catalog(metax_cr_json) and get_catalog_preservation_state(metax_cr_json) != 120):
                identifiers_to_delete.append(identifier)
                continue

            es_dataset_json = converter.convert_metax_cr_json_to_es_data_model(metax_cr_json)
            if es_dataset_json:
                # The below 3 lines is in case you want to print the metax cr json and es dataset json to a file
                # from etsin_finder_search.utils import append_json_to_file
                # append_json_to_file(metax_cr_json, 'data.txt')
                # append_json_to_file(es_dataset_json, 'data.txt')

                es_dataset_models.append(ESDatasetModel(es_dataset_json))
            else:
                log.error("Something went wrong when converting {0} to es data model".format(identifier))
                continue

    log.info("Converted finally {0} Metax catalog records to Elasticsearch documents".format(len(es_dataset_models)))
    return es_dataset_models


class ReindexScheduledTask:

    def __init__(self):
        self.metax_api = MetaxAPIService.get_metax_api_service(metax_api_config)
        self.es_client = ElasticSearchService.get_elasticsearch_service(es_config)

    def run_task(self, delete_index_first):
        # 1a. Check elasticsearch client ok
        if self.es_client is None:
            log.error("Unable to create Elasticsearch client")
            return

        if self.metax_api is None:
            log.error("Unable to create Metax API client")
            return

        # 1b. Stop RabbitMQ consumer
        if rabbitmq_consumer_is_running():
            log.info("Trying to stop RabbitMQ consumer service for the length of reindexing operation..")
            if stop_rabbitmq_consumer():
                log.info("RabbitMQ consumer service stopped")
            else:
                log.error("Unable to stop RabbitMQ consumer service, but continuing with reindexing operation..")

        # 2a. Get all latest catalog records from Metax
        log.info("Trying to bulk fetch the latest catalog records from Metax..")
        metax_crs = self.metax_api.get_latest_catalog_records()
        if not metax_crs:
            log.error("Unable to fetch catalog records from Metax, aborting reindexing operation")
            return
        log.info("Done")

        # 2b. Decide whether catalog record is to be indexed, and for those to be indexed, change catalog record array
        # to dictionary with catalog record identifier as the key
        metax_crs_dict = {}
        for cr_json in metax_crs:
            if catalog_record_should_be_indexed(cr_json):
                metax_crs_dict[cr_json['identifier']] = cr_json

        # 3. Create a list containing all catalog record identifiers
        metax_identifiers = list(metax_crs_dict.keys())
        ids_to_create = list(metax_identifiers) if metax_crs_dict else []

        # 4. If necessary, delete search index. Check index and mapping existence and create if necessary.
        if delete_index_first:
            if not self.es_client.delete_index():
                log.error("Unable to delete search index. Aborting reindexing operation")
                return

        if not create_search_index_and_doc_type_mapping_if_not_exist():
            log.error("Unable to create search index and/or mapping. Aborting reindexing operation")
            return

        # 5. Get all document identifiers (equivalent to Metax catalog record identifiers) from search index
        es_identifiers = self.es_client.get_all_doc_ids_from_index() or []

        # 6.
        # If metax_id in Metax and in es index -> index
        # If metax_id in Metax but not in es index -> index
        # If metax_id not in Metax but in es index -> delete
        ids_to_delete = []
        ids_to_index = []
        for es_id in es_identifiers:
            if es_id in metax_identifiers:
                ids_to_index.append(es_id)
                ids_to_create.remove(es_id)
            else:
                ids_to_delete.append(es_id)

        log.info("Amount of identifiers to delete: {0}".format(len(ids_to_delete)))
        log.info("Amount of identifiers to create: {0}".format(len(ids_to_create)))
        log.info("Amount of identifiers to update: {0}".format(len(ids_to_index)))
        ids_to_index.extend(ids_to_create)

        # 7. Convert catalog records to es documents and for those records add their previous version ids to delete list
        es_data_models = convert_identifiers_to_es_data_models(self.metax_api, ids_to_index, ids_to_delete,
                                                               metax_crs_dict)

        # 8. Run bulk requests to search index
        # a. Create or update documents that are either new or already exist in search index
        # b. Delete documents from index no longer in metax
        self.es_client.do_bulk_request_for_datasets(es_data_models, ids_to_delete)

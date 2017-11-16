from etsin_finder_search.elastic.service.es_service import ElasticSearchService
from etsin_finder_search.metax.metax_api import MetaxAPIService
from etsin_finder_search.catalog_record_converter import CRConverter
from etsin_finder_search.reindexing_log import get_logger
from etsin_finder_search.utils import \
    get_metax_api_config, \
    get_elasticsearch_config, \
    start_rabbitmq_consumer, \
    stop_rabbitmq_consumer, \
    rabbitmq_consumer_is_running


log = get_logger(__name__)

metax_api_config = get_metax_api_config()
es_config = get_elasticsearch_config()


def reindex_all_without_emptying_index():
    task = ReindexScheduledTask(False)
    task.run_task()


def reindex_all_by_emptying_index():
    task = ReindexScheduledTask(True)
    task.run_task()


def create_search_index_and_doc_type_mapping_if_not_exist():
    es_client = _create_es_client()

    if not es_client:
        return False

    if not es_client.index_exists():
        if not es_client.create_index_and_mapping():
            log.error("Unable to create index or document type mapping")
            return False

    return True


def delete_search_index():
    es_client = _create_es_client()
    if es_client:
        es_client.delete_index()


def reindex_metax_catalog_record(metax_catalog_record_json):
    es_client = _create_es_client()
    if es_client and create_search_index_and_doc_type_mapping_if_not_exist(es_client):
        converter = CRConverter()
        es_data_model = converter.convert_metax_cr_json_to_es_data_model(metax_catalog_record_json)
        es_client.reindex_dataset(es_data_model)


def delete_es_document_using_urn_identifier(urn_id):
    es_client = _create_es_client()
    if es_client and es_client.index_exists():
        es_client.delete_dataset(urn_id)


def load_test_data_into_es(dataset_amt):
    log.info("Loading test data into Elasticsearch..")

    es_client = ElasticSearchService(es_config)
    metax_api = MetaxAPIService(metax_api_config)
    converter = CRConverter()

    if not es_client or not metax_api:
        log.error("Loading test data into Elasticsearch failed")

    if not es_client.index_exists():
        log.info("Index does not exist, trying to create")
        if not es_client.create_index_and_mapping():
            log.error("Unable to create index")
            return False

    all_metax_urn_identifiers = metax_api.get_all_catalog_record_urn_identifiers()
    if all_metax_urn_identifiers:
        urn_ids_to_load = all_metax_urn_identifiers[0:min(len(all_metax_urn_identifiers), dataset_amt)]

        es_client.do_bulk_request_for_datasets([], converter.convert_metax_cr_urn_ids_to_es_data_model(urn_ids_to_load, metax_api))
        log.info("Test data loaded into Elasticsearch")
        return True

    return False


def _create_es_client():
    if es_config:
        es_client = ElasticSearchService(es_config)
        if not es_client.client_ok():
            log.error("Unable to create Elasticsearch client instance")
            return False
        return es_client

    return False


class ReindexScheduledTask:

    def __init__(self, delete_index_first):
        self.converter = CRConverter()

        if metax_api_config:
            self.metax_api = MetaxAPIService(metax_api_config)
            self.es_client = _create_es_client()
            if self.es_client and delete_index_first:
                self.es_client.delete_index()

    def run_task(self):
        if not create_search_index_and_doc_type_mapping_if_not_exist():
            return

        urn_ids_to_delete = []
        urn_ids_to_index = []

        # 1. Stop RabbitMQ consumer
        if rabbitmq_consumer_is_running():
            if not stop_rabbitmq_consumer():
                log.error("Unable to stop RabbitMQ consumer service, continuing with reindexing")

        # 2. Get all dataset urn_identifiers from metax
        metax_urn_identifiers = self.metax_api.get_all_catalog_record_urn_identifiers()
        urn_ids_to_create = list(metax_urn_identifiers)

        # 3. Get all urn_identifiers from search index
        es_urn_identifiers = self.es_client.get_all_doc_ids_from_index() or []

        # 4.
        # If urn_id in metax and in ex index -> index
        # If urn_id in metax but not in es index -> index
        # If urn_id not in metax but in es index -> delete
        for es_urn_id in es_urn_identifiers:
            if es_urn_id in metax_urn_identifiers:
                urn_ids_to_index.append(es_urn_id)
                urn_ids_to_create.remove(es_urn_id)
            else:
                urn_ids_to_delete.append(es_urn_id)

        log.info("urn identifiers to delete: \n{0}".format(urn_ids_to_delete))
        log.info("urn identifiers to create: \n{0}".format(urn_ids_to_create))
        log.info("urn identifiers to update: \n{0}".format(urn_ids_to_index))
        urn_ids_to_index.extend(urn_ids_to_create)

        # 5. Run bulk requests to search index
        # A. Delete documents from index no longer in metax
        # B. Create or update documents that are either new or already exist in search index
        self.es_client.do_bulk_request_for_datasets(urn_ids_to_delete,
                                                    self.converter.convert_metax_cr_urn_ids_to_es_data_model(
                                                        urn_ids_to_index, self.metax_api))

        # 6. Start RabbitMQ consumer
        if not rabbitmq_consumer_is_running():
            if not start_rabbitmq_consumer():
                log.error("Unable to start RabbitMQ consumer service")

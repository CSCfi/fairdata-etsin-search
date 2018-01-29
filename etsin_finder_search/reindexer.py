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
    get_catalog_record_previous_version_identifier


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


def load_test_data_into_es(dataset_amt):
    log.info("Loading test data into Elasticsearch..")

    es_client = ElasticSearchService(es_config)
    metax_api = MetaxAPIService(metax_api_config)

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

        identifiers_to_delete = []
        es_data_models = []
        convert_identifiers_to_es_data_models(metax_api, urn_ids_to_load, es_data_models, identifiers_to_delete)
        es_client.do_bulk_request_for_datasets(es_data_models, identifiers_to_delete)
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


def convert_identifiers_to_es_data_models(metax_api, identifiers, es_dataset_models, identifiers_to_delete):
    """
    Takes in Metax catalog record identifiers, fetches their json from Metax, converts them to an ESDatasetModel
    object and adds to es_data_models list. At the same time checks if catalog record has a previous version and
    if it does, adds the previous version's identifier to identifiers_to_delete list.

    :param identifiers: Metax identifiers
    :param es_dataset_models: List of ESDatasetModel objects that will be sent to es bulk request
    :param identifiers_to_delete: list of Metax identifiers that will be sent to es bulk request
    :return:
    """

    converter = CRConverter()
    log.info("Converting {0} Metax catalog records to Elasticsearch documents..".format(len(identifiers)))
    for identifier in identifiers:
        metax_cr_json = metax_api.get_catalog_record(identifier)
        if metax_cr_json:
            prev_version_id = get_catalog_record_previous_version_identifier(metax_cr_json)
            if prev_version_id:
                identifiers_to_delete.append(prev_version_id)
            es_dataset_json = converter.convert_metax_cr_json_to_es_data_model(metax_cr_json)

            if es_dataset_json:
                # The below 3 lines is in case you want to print the metax cr json and es dataset json to a file
                from etsin_finder_search.utils import append_json_to_file
                append_json_to_file(metax_cr_json, 'data.txt')
                append_json_to_file(es_dataset_json, 'data.txt')

                es_dataset_models.append(ESDatasetModel(es_dataset_json))
            else:
                log.error("Something went wrong when converting {0} to es data model".format(identifier))
                continue


class ReindexScheduledTask:

    def __init__(self, delete_index_first):
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
        # If urn_id in metax and in es index -> index
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

        # 5. Convert catalog records to es documents and add previous version ids, for those records to be added to
        # index, on delete list.
        es_data_models = []
        convert_identifiers_to_es_data_models(self.metax_api, urn_ids_to_index, es_data_models, urn_ids_to_delete)

        # 6. Run bulk requests to search index
        # A. Create or update documents that are either new or already exist in search index
        # B. Delete documents from index no longer in metax
        self.es_client.do_bulk_request_for_datasets(es_data_models, urn_ids_to_delete)

        # 7. Start RabbitMQ consumer
        if not rabbitmq_consumer_is_running():
            if not start_rabbitmq_consumer():
                log.error("Unable to start RabbitMQ consumer service")


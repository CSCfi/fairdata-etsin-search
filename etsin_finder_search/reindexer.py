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
    catalog_record_is_deprecated


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

    metax_identifiers = metax_api.get_latest_catalog_record_preferred_identifiers()
    if metax_identifiers:
        identifiers_to_load = metax_identifiers[0:min(len(metax_identifiers), dataset_amt)]

        identifiers_to_delete = []
        es_data_models = convert_identifiers_to_es_data_models(metax_api, identifiers_to_load, identifiers_to_delete)
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


def convert_identifiers_to_es_data_models(metax_api, identifiers_to_convert, identifiers_to_delete):
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
             "If a catalog record is deprecated, try to delete from index instead.".format(len(identifiers_to_convert)))

    for identifier in identifiers_to_convert:
        metax_cr_json = metax_api.get_catalog_record(identifier)
        if metax_cr_json:
            if catalog_record_is_deprecated(metax_cr_json):
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

    def __init__(self, delete_index_first):
        if metax_api_config:
            self.metax_api = MetaxAPIService(metax_api_config)
            self.es_client = _create_es_client()
            if self.es_client and delete_index_first:
                self.es_client.delete_index()

    def run_task(self):
        if not create_search_index_and_doc_type_mapping_if_not_exist():
            return

        ids_to_delete = []
        ids_to_index = []

        # 1. Stop RabbitMQ consumer
        if rabbitmq_consumer_is_running():
            if not stop_rabbitmq_consumer():
                log.error("Unable to stop RabbitMQ consumer service, continuing with reindexing")

        # 2. Get all dataset identifiers from metax
        # Fetch only the latest dataset versions
        metax_identifiers = self.metax_api.get_latest_catalog_record_preferred_identifiers()
        ids_to_create = list(metax_identifiers) if metax_identifiers else []

        # 3. Get all identifiers from search index
        es_identifiers = self.es_client.get_all_doc_ids_from_index() or []

        # 4.
        # If metax_id in metax and in es index -> index
        # If metax_id in metax but not in es index -> index
        # If metax_id not in metax but in es index -> delete
        for es_id in es_identifiers:
            if es_id in metax_identifiers:
                ids_to_index.append(es_id)
                ids_to_create.remove(es_id)
            else:
                ids_to_delete.append(es_id)

        log.info("Identifiers to delete: \n{0}".format(ids_to_delete))
        log.info("Identifiers to create: \n{0}".format(ids_to_create))
        log.info("Identifiers to update: \n{0}".format(ids_to_index))
        ids_to_index.extend(ids_to_create)

        # 5. Convert catalog records to es documents and for those records add their previous version ids to delete list
        es_data_models = convert_identifiers_to_es_data_models(self.metax_api, ids_to_index, ids_to_delete)

        # 6. Run bulk requests to search index
        # A. Create or update documents that are either new or already exist in search index
        # B. Delete documents from index no longer in metax
        self.es_client.do_bulk_request_for_datasets(es_data_models, ids_to_delete)

        # 7. Start RabbitMQ consumer
        if not rabbitmq_consumer_is_running():
            if not start_rabbitmq_consumer():
                log.error("Unable to start RabbitMQ consumer service")

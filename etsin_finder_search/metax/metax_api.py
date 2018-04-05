import requests
from requests import HTTPError, ConnectionError, Timeout
import json
from time import sleep

from etsin_finder_search.reindexing_log import get_logger

log = get_logger(__name__)
TIMEOUT = 30
NUM_RETRIES = 3


class MetaxAPIService:

    def __init__(self, metax_api_config):
        self.METAX_CATALOG_RECORDS_BASE_URL = 'https://{0}/rest/datasets'.format(metax_api_config['HOST'])
        self.METAX_GET_PIDS_URL = self.METAX_CATALOG_RECORDS_BASE_URL + '/unique_preferred_identifiers?latest'
        self.METAX_GET_CATALOG_RECORD_URL = self.METAX_CATALOG_RECORDS_BASE_URL + '?preferred_identifier={0}'

    @staticmethod
    def _do_request(request_func, arg=None):
        sleep_time = 4
        for x in range(0, NUM_RETRIES):
            try:
                if arg:
                    response = request_func(arg)
                else:
                    response = request_func()
                str_error = None
            except (ConnectionError, Timeout) as e:
                str_error = e

            if str_error:
                sleep(sleep_time)  # wait before trying to fetch the data again
                sleep_time *= 2  # exponential backoff
            else:
                break

        if not str_error and response:
            return response
        return None

    def get_catalog_record(self, preferred_identifier):
        """ Get a catalog record with the given preferred_identifier from MetaX API.

        :return: Metax catalog record as json
        """

        def get(identifier):
            return requests.get(self.METAX_GET_CATALOG_RECORD_URL.format(identifier),
                         headers={'Content-Type': 'application/json'},
                         timeout=TIMEOUT)

        response = self._do_request(get, preferred_identifier)
        if not response:
            log.error("Unable to connect to Metax API with pref_id {0}".format(preferred_identifier))
            return None

        try:
            response.raise_for_status()
        except HTTPError as e:
            log.error('Failed to get catalog record: \nidentifier={id}, \nerror={error}, \njson={json}'.format(
                id=preferred_identifier, error=repr(e), json=self.json_or_empty(response)))
            log.error('Response text: %s', response.text)
            return None

        return json.loads(response.text)

    def get_latest_catalog_record_preferred_identifiers(self):
        """
        Get a list of latest catalog record preferred_identifiers in terms of dataset versioning from MetaX API.

        :return: List of preferred_identifiers
        """

        def get():
            return requests.get(self.METAX_GET_PIDS_URL,
                                headers={'Content-Type': 'application/json'},
                                timeout=TIMEOUT)

        response = self._do_request(get)
        if not response:
            log.error("Unable to connect to Metax API")
            return None

        try:
            response.raise_for_status()
        except HTTPError as e:
            log.error('Failed to get identifiers from Metax: \nerror={error}, \njson={json}'.format(
                error=repr(e), json=self.json_or_empty(response)))
            log.error('Response text: %s', response.text)
            return None

        return json.loads(response.text)

    @staticmethod
    def json_or_empty(response):
        response_json = ""
        try:
            response_json = response.json()
        except Exception:
            pass
        return response_json

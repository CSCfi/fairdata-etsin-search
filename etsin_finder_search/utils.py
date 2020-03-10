# This file is part of the Etsin service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import json
import yaml
import os
import subprocess


def get_config_from_file():
    with open('/home/etsin-user/app_config') as app_config_file:
        return yaml.load(app_config_file, Loader=yaml.FullLoader)


def get_elasticsearch_config():
    es_conf = get_config_from_file().get('ELASTICSEARCH', False)
    if not es_conf or not isinstance(es_conf, dict):
        return None

    if 'HOSTS' not in es_conf or 'PORT' not in es_conf or 'USE_SSL' not in es_conf:
        return None

    return es_conf


def get_metax_api_config():
    metax_api_conf = get_config_from_file().get('METAX_API', False)
    if not metax_api_conf or not isinstance(metax_api_conf, dict):
        return None

    if 'HOST' not in metax_api_conf or 'USER' not in metax_api_conf \
            or 'PASSWORD' not in metax_api_conf or 'VERIFY_SSL' not in metax_api_conf:
        return None

    return metax_api_conf


def get_metax_rabbit_mq_config():
    metax_rabbitmq_conf = get_config_from_file().get('METAX_RABBITMQ', False)
    if not metax_rabbitmq_conf or not isinstance(metax_rabbitmq_conf, dict):
        return None

    if 'HOSTS' not in metax_rabbitmq_conf or 'PORT' not in metax_rabbitmq_conf \
            or 'VHOST' not in metax_rabbitmq_conf or 'EXCHANGE' not in metax_rabbitmq_conf \
            or 'USER' not in metax_rabbitmq_conf or 'PASSWORD' not in metax_rabbitmq_conf:
        return None

    return metax_rabbitmq_conf


def append_json_to_file(json_data, filename):
    with open(filename, "a") as output_file:
        json.dump(json_data, output_file, indent=4, sort_keys=True)


def write_string_to_file(string, filename):
    with open(filename, "w") as output_file:
        print(f"{string}", file=output_file)


def executing_travis():
    """
    Returns True whenever code is being executed by travis
    """
    return True if os.getenv('TRAVIS', False) else False


def stop_rabbitmq_consumer():
    """
    Stop rabbitmq-consumer systemd service. Waits for exit or raises an error
    :return:
    """
    try:
        subprocess.check_call("sudo service rabbitmq-consumer stop".split())
        return True
    except subprocess.CalledProcessError:
        return False


def start_rabbitmq_consumer():
    """
    Start rabbitmq-consumer systemd service.
    :return:
    """
    try:
        subprocess.check_call("sudo service rabbitmq-consumer start".split())
        return True
    except subprocess.CalledProcessError:
        return False


def rabbitmq_consumer_is_running():
    output = str(subprocess.check_output(['ps', 'aux']))
    if 'run_rabbitmq_consumer.py' in output:
        return True
    return False


def catalog_record_has_preferred_identifier(cr_json):
    if cr_json.get('research_dataset') and cr_json['research_dataset'].get('preferred_identifier'):
        return True
    return False


def get_catalog_record_preferred_identifier(cr_json):
    if cr_json.get('research_dataset') and cr_json['research_dataset'].get('preferred_identifier'):
        return cr_json['research_dataset']['preferred_identifier']
    return None


def catalog_record_has_identifier(cr_json):
    if cr_json.get('identifier'):
        return True
    return False


def get_catalog_record_identifier(cr_json):
    if cr_json.get('identifier'):
        return cr_json['identifier']
    return None


def catalog_record_has_previous_dataset_version(cr_json):
    if cr_json.get('previous_dataset_version') and cr_json['previous_dataset_version'].get('identifier'):
        return True
    return False


def get_catalog_record_previous_dataset_version_identifier(cr_json):
    if cr_json.get('previous_dataset_version') and cr_json['previous_dataset_version'].get('identifier'):
        return cr_json['previous_dataset_version']['identifier']
    return None


def catalog_record_has_next_dataset_version(cr_json):
    if cr_json.get('next_dataset_version') and cr_json['next_dataset_version'].get('identifier'):
        return True
    return False


def catalog_record_is_deprecated(cr_json):
    return cr_json.get('deprecated', False)

def catalog_has_preservation_dataset_origin_version(cr_json):
    return cr_json.get('preservation_dataset_origin_version', False)

def get_catalog_preservation_state(cr_json):
    return cr_json.get('preservation_state', 0)

def catalog_record_is_pas_catalog(cr_json):
    return get_catalog_record_data_catalog_identifier(cr_json) == 'urn:nbn:fi:att:data-catalog-pas'

def catalog_record_should_be_indexed(cr_json):
    dc_identifier = get_catalog_record_data_catalog_identifier(cr_json)
    if dc_identifier is None:
        return False
    if dc_identifier == 'urn:nbn:fi:att:data-catalog-legacy':
        return False
    return True


def get_catalog_record_dataset_version_set(cr_json):
    ret_val = []
    for dvs_obj in cr_json.get('dataset_version_set', []):
        prev_pref_id = dvs_obj.get('preferred_identifier')
        if prev_pref_id:
            ret_val.append(prev_pref_id)
    return ret_val


def get_catalog_record_data_catalog_identifier(cr_json):
    dc_identifier = cr_json.get('data_catalog', {}).get('catalog_json', {}).get('identifier', False) or \
        cr_json.get('data_catalog', {}).get('identifier', False) or None
    return dc_identifier


def get_catalog_record_data_catalog_title(cr_json):
    dc_title = cr_json.get('data_catalog', {}).get('catalog_json', {}).get('title', False)
    if dc_title:
        return dc_title

    # Use identifier as a fallback
    dc_identifier = get_catalog_record_data_catalog_identifier(cr_json)
    if dc_identifier is not None:
        return {'en': dc_identifier, 'fi': dc_identifier}

    return None

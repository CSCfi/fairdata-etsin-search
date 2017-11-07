import json
import yaml


def get_config_from_file():
    with open('/home/etsin-user/app_config') as app_config_file:
        return yaml.load(app_config_file)


def get_elasticsearch_config():
    es_conf = get_config_from_file().get('ELASTICSEARCH', None)
    if not es_conf or not isinstance(es_conf, dict):
        return None

    return es_conf


def get_metax_api_config():
    metax_api_conf = get_config_from_file().get('METAX_API')
    if not metax_api_conf or not isinstance(metax_api_conf, dict):
        return None

    return metax_api_conf


def get_metax_rabbit_mq_config():
    metax_rabbitmq_conf = get_config_from_file().get('METAX_RABBITMQ')
    if not metax_rabbitmq_conf or not isinstance(metax_rabbitmq_conf, dict):
        return None

    return metax_rabbitmq_conf


def write_json_to_file(json_data, filename):
    with open(filename, "w") as output_file:
        json.dump(json_data, output_file)


def write_string_to_file(string, filename):
    with open(filename, "w") as output_file:
        print(f"{string}", file=output_file)

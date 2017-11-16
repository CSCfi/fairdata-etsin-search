from etsin_finder_search.catalog_record_converter import CRConverter
from .test_utils import get_test_object_from_file


def test_get_es_person_or_org_common_data_from_metax_obj():
    converter = CRConverter()
    output = converter.convert_metax_cr_json_to_es_data_model(get_test_object_from_file('metax_catalog_record.json'))
    assert output == get_test_object_from_file('es_document.json')

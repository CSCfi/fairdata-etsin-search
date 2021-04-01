# This file is part of the Etsin service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from etsin_finder_search.catalog_record_converter import CRConverter
from .helpers import get_test_object_from_file


def test_get_es_person_or_org_common_data_from_metax_obj():
    converter = CRConverter()
    output = converter.convert_metax_cr_json_to_es_data_model(get_test_object_from_file('metax_catalog_record.json'))
    assert output == get_test_object_from_file('es_document.json')

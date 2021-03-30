# This file is part of the Etsin service
#
# Copyright 2017-2021 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import pytest

from etsin_finder_search.utils import catalog_record_should_be_indexed
from .helpers import get_test_object_from_file

@pytest.fixture
def cr():
    return get_test_object_from_file('metax_catalog_record.json')

class TestCatalogRecordShouldBeIndexed:
    def test_published_record_should_be_indexed(self, cr):
        assert catalog_record_should_be_indexed(cr)

    def test_record_without_catalog_id_should_not_be_indexed(self, cr):
        del cr['data_catalog']['catalog_json']['identifier']
        assert not catalog_record_should_be_indexed(cr)

    def test_record_in_legacy_catalog_should_not_be_indexed(self, cr):
        cr['data_catalog']['catalog_json']['identifier'] = 'urn:nbn:fi:att:data-catalog-legacy'
        assert not catalog_record_should_be_indexed(cr)

    def test_draft_record_should_not_be_indexed(self, cr):
        cr['state'] = 'draft'
        assert not catalog_record_should_be_indexed(cr)

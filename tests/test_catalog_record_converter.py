from etsin_finder_search.catalog_record_converter import CRConverter

test_metax_agent_obj = {
    'name': {
        'fi': 'TestiOrg',
        'en': 'TestOrg',
    },
    '@type': 'Organization',
    'identifier': 'urn:identifier:1',
    'email': 'test@test.com',
    'telephone': ['12345'],
    'homepage': {
        'title': {
            'en': 'Homepages'
        },
        'identifier': 'http://test.com'
    },
    'is_part_of': {
        'name': {
            'und': 'Aalto yliopisto'
        },
        '@type': 'Organization',
        'email': 'info@csc.fi',
        'homepage': {
            'title': {
                'en': 'Publisher parent website',
                'fi': 'Julkaisijan yläorganisaation kotisivu'
            },
            'identifier': 'http://www.publisher_parent.fi/'
        },
        'telephone': [
            '+234234'
        ],
        'identifier': 'http://purl.org/att/es/organization_data/organization/organization_10076'
    }
}

expected_es_metax_agent_output = {
    'identifier': 'urn:identifier:1',
    'name': {'fi': 'TestiOrg', 'en': 'TestOrg'},
    'email': 'test@test.com',
    'telephone': ['12345'],
    'agent_type': 'Organization',
    'homepage': {'title': {'en': 'Homepages'}, 'identifier': 'http://test.com'},
    'belongs_to_org': {
        'identifier': 'http://purl.org/att/es/organization_data/organization/organization_10076',
        'name': {'und': 'Aalto yliopisto'},
        'email': 'info@csc.fi',
        'telephone': ['+234234'],
        'agent_type': 'Organization',
        'homepage': {
            'title':{
                'en': 'Publisher parent website',
                'fi': 'Julkaisijan yläorganisaation kotisivu'
            },
            'identifier': 'http://www.publisher_parent.fi/'
        }
    }
}


def test_get_es_person_or_org_common_data_from_metax_obj():
    converter = CRConverter()
    output = converter.get_converted_single_org_or_person_es_model(test_metax_agent_obj)
    print(output)
    assert output == expected_es_metax_agent_output
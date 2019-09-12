# This file is part of the Etsin service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from etsin_finder_search.reindexing_log import get_logger
from etsin_finder_search.utils import \
    catalog_record_has_preferred_identifier, \
    get_catalog_record_preferred_identifier, \
    catalog_record_has_identifier, \
    get_catalog_record_identifier, \
    get_catalog_record_dataset_version_set, \
    get_catalog_record_data_catalog_title, \
    get_catalog_record_data_catalog_identifier

log = get_logger(__name__)


class CRConverter:

    def convert_metax_cr_json_to_es_data_model(self, metax_cr_json):
        es_dataset = {}
        if metax_cr_json.get('research_dataset', False) and \
                catalog_record_has_identifier(metax_cr_json) and \
                catalog_record_has_preferred_identifier(metax_cr_json):

            es_dataset['identifier'] = get_catalog_record_identifier(metax_cr_json)
            es_dataset['preferred_identifier'] = get_catalog_record_preferred_identifier(metax_cr_json)
            es_dataset['dataset_version_set'] = get_catalog_record_dataset_version_set(metax_cr_json)
            es_dataset['data_catalog'] = get_catalog_record_data_catalog_title(metax_cr_json)
            es_dataset['data_catalog_identifier'] = get_catalog_record_data_catalog_identifier(metax_cr_json)

            m_rd = metax_cr_json['research_dataset']

            if 'organization_name_fi' not in es_dataset:
                es_dataset['organization_name_fi'] = []

            if 'organization_name_en' not in es_dataset:
                es_dataset['organization_name_en'] = []

            if metax_cr_json.get('date_modified', False):
                es_dataset['date_modified'] = metax_cr_json.get('date_modified')
            else:
                es_dataset['date_modified'] = metax_cr_json.get('date_created')

            if m_rd.get('title', False):
                es_dataset['title'] = m_rd.get('title')

            if m_rd.get('description', False):
                es_dataset['description'] = m_rd.get('description')

            if m_rd.get('keyword', False):
                es_dataset['keyword'] = m_rd.get('keyword')

            if metax_cr_json.get('preservation_state', False):
                es_dataset['preservation_state'] = metax_cr_json.get('preservation_state')

            if metax_cr_json.get('preservation_identifier', False):
                es_dataset['preservation_identifier'] = metax_cr_json.get('preservation_identifier')

            if metax_cr_json.get('preservation_dataset_version', False):
                es_dataset['preservation_dataset_version'] = metax_cr_json.get('preservation_dataset_version')

            if metax_cr_json.get('preservation_dataset_origin_version', False):
                es_dataset['preservation_dataset_origin_version'] = metax_cr_json.get('preservation_dataset_origin_version')

            for m_other_identifier_item in m_rd.get('other_identifier', []):
                if 'other_identifier' not in es_dataset:
                    es_dataset['other_identifier'] = []

                es_other_identifier = {}

                if m_other_identifier_item.get('notation'):
                    es_other_identifier['notation'] = m_other_identifier_item.get('notation')

                if m_other_identifier_item.get('type', False):
                    es_other_identifier['type'] = {}
                    self._convert_metax_obj_containing_identifier_and_label_to_es_model(
                        m_other_identifier_item.get('type'), es_other_identifier['type'], 'pref_label')

                es_dataset['other_identifier'].append(es_other_identifier)

            if m_rd.get('access_rights', False):
                if 'access_rights' not in es_dataset:
                    es_dataset['access_rights'] = {}

                es_access_rights = es_dataset['access_rights']

                CRConverter._add_descriptive_field_to_output_obj( m_rd.get('access_rights'), es_access_rights)

                if m_rd.get('access_rights').get('license', False):
                    m_license = m_rd.get('access_rights').get('license')
                    self._convert_metax_obj_containing_identifier_and_label_to_es_model(m_license, es_access_rights,
                                                                                        'title', 'license')

                if m_rd.get('access_rights').get('access_type', False):
                    es_dataset['access_rights']['access_type'] = {}
                    es_access_type = es_dataset['access_rights']['access_type']

                    m_type = m_rd.get('access_rights').get('access_type')
                    self._convert_metax_obj_containing_identifier_and_label_to_es_model(m_type, es_access_type,
                                                                                        'pref_label')

            if m_rd.get('theme', False):
                if 'theme' not in es_dataset:
                    es_dataset['theme'] = []

                m_theme = m_rd.get('theme')
                self._convert_metax_obj_containing_identifier_and_label_to_es_model(m_theme, es_dataset, 'pref_label',
                                                                                    'theme')

            if m_rd.get('field_of_science', False):
                if 'field_of_science' not in es_dataset:
                    es_dataset['field_of_science'] = []

                m_field_of_science = m_rd.get('field_of_science')
                self._convert_metax_obj_containing_identifier_and_label_to_es_model(m_field_of_science, es_dataset,
                                                                                    'pref_label', 'field_of_science')

            for m_is_output_of_item in m_rd.get('infrastructure', []):
                if 'infrastructure' not in es_dataset:
                    es_dataset['infrastructure'] = []

                m_infrastructure = {}
                self._convert_metax_obj_containing_identifier_and_label_to_es_model(m_is_output_of_item, m_infrastructure,
                                                                                    'pref_label')

                es_dataset['infrastructure'].append(m_infrastructure)

            for m_is_output_of_item in m_rd.get('is_output_of', []):
                if m_is_output_of_item.get('has_funding_agency', []):
                    self._convert_metax_langstring_name_to_es_model(m_is_output_of_item.get('has_funding_agency'), es_dataset, 'organization_name')

                if m_is_output_of_item.get('source_organization', []):
                    self._convert_metax_langstring_name_to_es_model(m_is_output_of_item.get('source_organization'), es_dataset, 'organization_name')

            if m_rd.get('is_output_of', []):
                if 'project_name_fi' not in es_dataset:
                    es_dataset['project_name_fi'] = []

                if 'project_name_en' not in es_dataset:
                    es_dataset['project_name_en'] = []

                self._convert_metax_langstring_name_to_es_model(m_rd.get('is_output_of'), es_dataset, 'project_name')

                if 'is_output_of' not in es_dataset:
                    es_dataset['is_output_of'] = []

                for project in m_rd.get('is_output_of'):
                    es_dataset['is_output_of'].append({'name': project['name']})

            if 'file_type' not in es_dataset and (m_rd.get('files', False) or m_rd.get('remote_resources', False)):
                es_dataset['file_type'] = []

            for m_is_output_of_item in m_rd.get('files', []) + m_rd.get('remote_resources', []):
                if 'file_type' in m_is_output_of_item:
                    m_file_type = {}
                    self._convert_metax_obj_containing_identifier_and_label_to_es_model(m_is_output_of_item['file_type'], m_file_type,
                                                                                        'pref_label')
                    es_dataset['file_type'].append(m_file_type)

            if m_rd.get('contributor', False):
                es_dataset['contributor'] = []
                self._convert_metax_org_or_person_to_es_model(m_rd.get('contributor'), es_dataset, 'contributor')
                self._convert_metax_langstring_name_to_es_model(m_rd.get('contributor'), es_dataset, 'organization_name')

            if m_rd.get('publisher', False):
                es_dataset['publisher'] = []
                self._convert_metax_org_or_person_to_es_model(m_rd.get('publisher'), es_dataset, 'publisher')
                self._convert_metax_langstring_name_to_es_model(m_rd.get('publisher'), es_dataset, 'organization_name')

            if m_rd.get('curator', False):
                es_dataset['curator'] = []
                self._convert_metax_org_or_person_to_es_model(m_rd.get('curator'), es_dataset, 'curator')
                self._convert_metax_langstring_name_to_es_model(m_rd.get('curator'), es_dataset, 'organization_name')

            if m_rd.get('creator', False):
                es_dataset['creator'] = []
                self._convert_metax_org_or_person_to_es_model(m_rd.get('creator'), es_dataset, 'creator')
                self._convert_metax_creator_name_to_es_model(m_rd.get('creator'), es_dataset, 'creator_name')
                self._convert_metax_langstring_name_to_es_model(m_rd.get('creator'), es_dataset, 'organization_name')

            if m_rd.get('rights_holder', False):
                es_dataset['rights_holder'] = []
                self._convert_metax_org_or_person_to_es_model(m_rd.get('rights_holder'), es_dataset, 'rights_holder')
                self._convert_metax_langstring_name_to_es_model(m_rd.get('rights_holder'), es_dataset, 'organization_name')

        return es_dataset

    @staticmethod
    def _convert_metax_obj_containing_identifier_and_label_to_es_model(m_input, es_output, m_input_label_field,
                                                                       es_array_relation_name=''):
        """

        If m_input is not array, set identifier and label directly on es_output.
        If m_input is array, add a es_array_relation_name array relation to es_output, which will contain objects
        having identifier and label each

        :param m_input:
        :param es_output:
        :param m_input_label_field:
        :param es_array_relation_name:
        :return:
        """

        if isinstance(m_input, list) and es_array_relation_name:
            output = []
            for obj in m_input:

                m_input_label_is_array = isinstance(obj.get(m_input_label_field), list)
                out_obj = {
                    'identifier': obj.get('identifier', ''),
                    m_input_label_field: obj.get(m_input_label_field, [] if m_input_label_is_array else {})
                }
                CRConverter._add_descriptive_field_to_output_obj(obj, out_obj)
                output.append(out_obj)
            es_output[es_array_relation_name] = output
        elif isinstance(m_input, dict):
            m_input_label_is_array = isinstance(m_input.get(m_input_label_field), list)
            es_output['identifier'] = m_input.get('identifier', '')
            es_output[m_input_label_field] = m_input.get(m_input_label_field, [] if m_input_label_is_array else {})
            CRConverter._add_descriptive_field_to_output_obj(m_input, es_output)

    @staticmethod
    def _add_descriptive_field_to_output_obj(input_obj, output_obj):
        if 'description' in input_obj:
            output_obj['description'] = input_obj['description']
        if 'definition' in input_obj:
            output_obj['definition'] = input_obj['definition']

    def _convert_metax_org_or_person_to_es_model(self, m_input, es_output, relation_name):
        """

        :param m_input:
        :param es_output:
        :param relation_name:
        :return:
        """

        if isinstance(m_input, list):
            output = []
            for m_obj in m_input:
                org_or_person = self._get_converted_single_org_or_person_es_model(m_obj)
                if org_or_person is not None:
                    output.append(org_or_person)
        else:
            output = {}
            if m_input:
                org_or_person = self._get_converted_single_org_or_person_es_model(m_input)
                if org_or_person is not None:
                    output = org_or_person

        es_output[relation_name] = output

    def _convert_metax_creator_name_to_es_model(self, m_input, es_output, relation_name):
        """

        :param m_input:
        :param es_output:
        :param relation_name:
        :return:
        """

        output = []
        if isinstance(m_input, list):
            for m_obj in m_input:
                name = self._get_converted_creator_name_es_model(m_obj)
                if name is not None:
                    output.extend(name)
        else:
            if m_input:
                name = self._get_converted_creator_name_es_model(m_input)
                if name is not None:
                    output = name

        es_output[relation_name] = output

    def _convert_metax_langstring_name_to_es_model(self, m_input, es_output, relation_name_base):
        """
        Converts an object with langstring name to two lists, one for Finnish and one for English name.

        :param m_input:
        :param es_output:
        :param relation_name:
        :return:
        """

        output_fi = []
        output_en = []
        if isinstance(m_input, list):
            for m_obj in m_input:
                name_fi = self._get_converted_langstring_name_es_model(m_obj, 'fi')
                name_en = self._get_converted_langstring_name_es_model(m_obj, 'en')
                if name_fi is not None and name_en is not None:
                    output_fi.append(name_fi)
                    output_en.append(name_en)

                if 'is_part_of' in m_obj:
                    self._convert_metax_langstring_name_to_es_model(m_obj['is_part_of'], es_output, relation_name_base)
                if 'member_of' in m_obj:
                    self._convert_metax_langstring_name_to_es_model(m_obj['member_of'], es_output, relation_name_base)
        else:
            if m_input:
                output_fi.append(self._get_converted_langstring_name_es_model(m_input, 'fi'))
                output_en.append(self._get_converted_langstring_name_es_model(m_input, 'en'))

                if 'is_part_of' in m_input:
                    self._convert_metax_langstring_name_to_es_model(m_input['is_part_of'], es_output, relation_name_base)
                if 'member_of' in m_input:
                    self._convert_metax_langstring_name_to_es_model(m_input['member_of'], es_output, relation_name_base)

        if output_fi is not None and output_en is not None:
            es_output[relation_name_base + '_fi'].extend(output_fi)
            es_output[relation_name_base + '_en'].extend(output_en)

    def _get_converted_single_org_or_person_es_model(self, m_obj):
        out_obj = self._get_es_person_or_org_common_data_obj_from_metax_agent_obj(m_obj)
        if out_obj is None:
            return None

        agent_type = m_obj.get('@type')
        if agent_type == 'Person' and m_obj.get('member_of', False):
            org = self._get_es_person_or_org_common_data_obj_from_metax_agent_obj(m_obj.get('member_of'))
            if org is not None:
                out_obj.update({
                    'belongs_to_org': org
                })
        elif agent_type == 'Organization' and m_obj.get('is_part_of', False):
            org = self._get_es_person_or_org_common_data_obj_from_metax_agent_obj(m_obj.get('is_part_of'))
            if org is not None:
                out_obj.update({
                    'belongs_to_org': org
                })

        return out_obj

    def _get_converted_creator_name_es_model(self, m_obj):
        person_or_org = self._get_es_person_or_org_common_data_obj_from_metax_agent_obj(m_obj)
        if person_or_org is None:
            return None

        out_obj = list(person_or_org['name'].values())
        return out_obj

    def _get_converted_langstring_name_es_model(self, m_obj, lang):
        if not isinstance(m_obj.get('name'), dict):
            return None

        if lang == 'fi':
            preferred_order = ['fi', 'und', 'en']
        elif lang == 'en':
            preferred_order = ['en', 'und', 'fi']
        else:
            return None

        for language in preferred_order:
            try:
                return m_obj['name'][language]
            except KeyError:
                continue

        # If name is not available in preferred languages, choose any name
        out_obj = list(m_obj['name'].values())[0]

        return out_obj

    @staticmethod
    def _get_es_person_or_org_common_data_obj_from_metax_agent_obj(m_obj):
        if not m_obj or 'name' not in m_obj or '@type' not in m_obj:
            log.warning("Agent object does not have either name or @type")
            return None

        if m_obj['@type'] not in ['Agent', 'Person', 'Organization']:
            log.warning("Agent object's @type is not one of allowed values")
            return None

        # Name should be langstring
        name = m_obj['name']
        if not isinstance(name, dict):
            name = {'und': m_obj['name']}

        ret_obj = {
            'name': name,
            'agent_type': m_obj['@type']
        }
        if m_obj.get('identifier', False):
            ret_obj['identifier'] = m_obj['identifier']

        return ret_obj

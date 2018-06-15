# This file is part of the Etsin service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from etsin_finder_search.reindexer import create_search_index_and_doc_type_mapping_if_not_exist


def main():
    create_search_index_and_doc_type_mapping_if_not_exist()


if __name__ == '__main__':
    # calling main function
    main()

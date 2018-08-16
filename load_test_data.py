# This file is part of the Etsin service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import sys
from etsin_finder_search.reindexing_log import get_logger

from etsin_finder_search.reindexer import load_test_data_into_es

log = get_logger(__name__)

AMOUNT_OF_DATASETS = "amount_of_datasets"


def main():

    instructions = """\nRun the program as etsin-user with pyenv activated using 'python load_test_data.py
    amount_of_datasets=Y, where Y is a positive integer for the amount of datasets to load into the search index"""

    run_args = dict([arg.split('=', maxsplit=1) for arg in sys.argv[1:]])

    if AMOUNT_OF_DATASETS not in run_args:
        print(instructions)
        log.error(instructions)
        sys.exit(1)

    try:
        int(run_args[AMOUNT_OF_DATASETS])
    except ValueError:
        print(instructions)
        log.error(instructions)
        sys.exit(1)

    if int(run_args[AMOUNT_OF_DATASETS]) < 1:
        print(instructions)
        log.error(instructions)
        sys.exit(1)

    if not load_test_data_into_es(int(run_args[AMOUNT_OF_DATASETS])):
        log.error("Something went wrong when loading test data")


if __name__ == '__main__':
    # calling main function
    main()

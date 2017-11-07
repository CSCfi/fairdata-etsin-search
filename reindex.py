import sys

from etsin_finder_search.reindexer import reindex_all_without_emptying_index
from etsin_finder_search.reindexer import reindex_all_by_emptying_index
from etsin_finder_search.reindexing_log import get_logger

log = get_logger(__name__)

NO = 'no'
YES = 'yes'
RECREATE_INDEX = "recreate_index"


def main():

    instructions = """\nRun the program as etsin-user with pyenv activated using 'python reindex.py recreate_index=X
    where X = yes or X = no"""

    run_args = dict([arg.split('=', maxsplit=1) for arg in sys.argv[1:]])

    if RECREATE_INDEX not in run_args:
        print(instructions)
        log.error(instructions)
        sys.exit(1)

    if run_args[RECREATE_INDEX] not in [NO, YES]:
        print(instructions)
        log.error(instructions)
        sys.exit(1)

    if run_args[RECREATE_INDEX] == NO:
        reindex_all_without_emptying_index()

    if run_args[RECREATE_INDEX] == YES:
        reindex_all_by_emptying_index()


if __name__ == '__main__':
    # calling main function
    main()

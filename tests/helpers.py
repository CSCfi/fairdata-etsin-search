# This file is part of the Etsin service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import json
import os


def get_test_object_from_file(filename):
    json_data = open('{0}/test_objects/{1}'.format(os.path.dirname(os.path.realpath(__file__)), filename)).read()
    return json.loads(json_data)

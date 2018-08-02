# This file is part of the Etsin service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from setuptools import setup, find_packages

setup(
    name='etsin_finder_search',
    description='Etsin finder search index related scripts',
    author='CSC - IT Center for Science Ltd.',
    packages=find_packages(),
    setup_requires=[
        'pytest-runner'
    ]
)

# etsin-finder-search

This repository contains code for Etsin Finder Search, which is used for dataset searching functionalities in Etsin. This repository has been developed using RabbitMQ and ElasticSearch.

## 1. Development setup prerequisites

1. If not installed, install docker on your computer
    - `docker.com/get-started`
2. If not installed, install docker-compose
    - `docs.docker.com/compose/install`

## 2. Development setup

This repository functions as part of the Etsin-Qvain setup. See: `https://github.com/CSCfi/fairdata-docker`

## 3. How to use the scripts

1. First, the Docker image must be built: `docker build -f python.dockerfile -t etsin-search-python ./`

2. Then, you can use the image, to run python script commands, as follows (command = <PYTHON_COMMAND>):
    `docker run --network=elastic-network etsin-search-python python <PYTHON_COMMAND>`
    - <PYTHON_COMMAND> should be one of:
        - `create_empty_index.py`
        - `load_test_data.py amount_of_datasets=<AMOUNT>`
        - `reindex.py reacreate_index=<yes/no>`
        - `delete_index.py`
3. Elasticsearch status can be inspected with:
    - `curl -X GET elasticsearch:9200/_cat/indices`

# Build status

## Test branch
[![Build Status](https://travis-ci.com/CSCfi/etsin-finder-search.svg?branch=test)](https://travis-ci.com/CSCfi/etsin-finder-search)

## Stable branch
[![Build Status](https://travis-ci.com/CSCfi/etsin-finder-search.svg?branch=stable)](https://travis-ci.com/CSCfi/etsin-finder-search)

License
-------
Copyright (c) 2018-2020 Ministry of Education and Culture, Finland

Licensed under [MIT License](LICENSE)

# etsin-finder-search

This repository contains code for Etsin Finder Search, which is used for dataset searching functionalities in Etsin. This repository has been developed using RabbitMQ (consumer, listening to messages from Metax) and ElasticSearch (dataset search index).

## 1. Development setup prerequisites

1. If not installed, install docker on your computer
    - `docker.com/get-started`
2. If not installed, install docker-compose
    - `docs.docker.com/compose/install`

## 2. Development setup

This repository functions as part of the Etsin-Qvain setup. See: `https://github.com/CSCfi/fairdata-docker`

## 3. How to use the scripts

1. First, pull the Docker image:
`docker pull fairdata-docker.artifactory.ci.csc.fi/fairdata-etsin-search-rabbitmq`

2. To load test data, you can run:
    - `docker exec $(docker ps -q -f name=etsin-qvain_rabbitmq) python load_test_data.py amount_of_datasets=1000`
    - Other scripts than can be run:
        - `python create_empty_index.py`
        - `python load_test_data.py amount_of_datasets=<AMOUNT>`
        - `python reindex.py reacreate_index=<yes/no>`
        - `python delete_index.py`
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

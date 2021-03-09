# etsin-finder-search

This repository contains code for Etsin Finder Search, which is used for dataset searching functionalities in Etsin. This repository has been developed using RabbitMQ (consumer, listening to messages from Metax) and ElasticSearch (dataset search index).

## 1. Development setup prerequisites

1. If not installed, install docker on your computer
    - `docker.com/get-started`
2. If not installed, install docker-compose
    - `docs.docker.com/compose/install`

## 2. Development setup

This repository functions as part of the Etsin-Qvain setup. See: `https://github.com/CSCfi/fairdata-docker`

For instructions, see the link above.

The default behavior of the Dockerized version of etsin-finder-search within Etsin-Qvain is to:
    - 1. Reindex all datasets from Metax
    - 2. Start a rabbitmq-consumer, listening to any changes from Metax
    - These details are specified in `reindex_and_start_rabbitmq_consumer.sh`

## 3. How to manually use the scripts

When the Etsin-Qvain stack is running, scripts can be run manually as follows:
    - `docker exec $(docker ps -q -f name=etsin-qvain_rabbitmq) python load_test_data.py amount_of_datasets=<AMOUNT>`
    - `docker exec $(docker ps -q -f name=etsin-qvain_rabbitmq) python create_empty_index.py`
    - `docker exec $(docker ps -q -f name=etsin-qvain_rabbitmq) python reindex.py recreate_index=yes`
    - `docker exec $(docker ps -q -f name=etsin-qvain_rabbitmq) python reindex.py recreate_index=no`
    - `docker exec $(docker ps -q -f name=etsin-qvain_rabbitmq) python delete_index.py`

Elasticsearch status can be inspected with:
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

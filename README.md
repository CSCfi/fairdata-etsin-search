# etsin-finder-search

This repository contains code for Etsin Finder Search, which is used for dataset searching functionalities in Etsin. This repository has been developed using RabbitMQ (consumer, listening to messages from Metax) and ElasticSearch (dataset search index).

## Development setup

This repository functions as part of the Etsin-Qvain setup, together with `etsin-finder` (github.com/CSCfi/etsin-finder).

For a development setup of Etsin-Qvain (`etsin-finder` and `etsin-finder-search`) using Docker, see repository https://gitlab.ci.csc.fi/fairdata/fairdata-docker

The default behavior of the Dockerized version of `etsin-finder-search` within Etsin-Qvain is to:
- Reindex all datasets from Metax
- Start a rabbitmq-consumer, listening to any changes from Metax
- These details are specified in `reindex_and_start_rabbitmq_consumer.sh`

Elasticsearch status can be inspected with:
- `docker exec $(docker ps -q -f name=metax-etsin-qvain-dev_etsin-qvain-elasticsearch) curl -X GET etsin-qvain-elasticsearch:9201/_cat/indices`

# Updating docker image

The Docker image (etsin-search-rabbitmq) is built manually (and can thus be edited) 

First, login:
`docker login fairdata-docker.artifactory.ci.csc.fi`

Then, the service specific images can be pushed (see below)

## Updating etsin-search-rabbitmq-consumer

1 Build image:
- `docker build -f rabbitmq-consumer.dockerfile -t etsin-search-rabbitmq-consumer ./`

2 Tag image:
- `docker tag etsin-search-rabbitmq-consumer fairdata-docker.artifactory.ci.csc.fi/fairdata-etsin-search-rabbitmq-consumer`

3 Push image:
- `docker push fairdata-docker.artifactory.ci.csc.fi/fairdata-etsin-search-rabbitmq-consumer`

# Build status

## Test branch
[![Build Status](https://travis-ci.com/CSCfi/etsin-finder-search.svg?branch=test)](https://travis-ci.com/CSCfi/etsin-finder-search)

## Stable branch
[![Build Status](https://travis-ci.com/CSCfi/etsin-finder-search.svg?branch=stable)](https://travis-ci.com/CSCfi/etsin-finder-search)

License
-------
Copyright (c) 2018-2020 Ministry of Education and Culture, Finland

Licensed under [MIT License](LICENSE)

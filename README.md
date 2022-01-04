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
```
# Generic
- `docker exec $(docker ps -q -f name={{docker_stack_name}}_etsin-qvain-elasticsearch) curl -X GET etsin-qvain-elasticsearch:9201/_cat/indices`

# Example:
docker exec $(docker ps -q -f name=fairdata_etsin-qvain-elasticsearch) curl -X GET etsin-qvain-elasticsearch:9201/_cat/indices
```

# Updating docker image

The Docker image (etsin-search-rabbitmq) is built manually (and can thus be edited) 

First, login:
`docker login fairdata-docker.artifactory.ci.csc.fi`

Then, the service specific images can be pushed (see below)

## Updating etsin-search-rabbitmq-consumer

```
# Build image:
docker build -f rabbitmq-consumer.dockerfile -t etsin-search-rabbitmq-consumer ./

# Tag image:
docker tag etsin-search-rabbitmq-consumer fairdata-docker.artifactory.ci.csc.fi/fairdata-etsin-search-rabbitmq-consumer`

# Push image:
docker push fairdata-docker.artifactory.ci.csc.fi/fairdata-etsin-search-rabbitmq-consumer
```

License
-------
Copyright (c) 2018-2021 Ministry of Education and Culture, Finland

Licensed under [MIT License](LICENSE)

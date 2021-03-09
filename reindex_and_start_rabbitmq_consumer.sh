#!/bin/bash

# This script will reindex data from the configured Metax version and start a rabbitMQ consumer
python ./reindex.py recreate_index=yes &
python ./run_rabbitmq_consumer.py
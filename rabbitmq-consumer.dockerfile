FROM python:3.6

RUN apt-get update && apt-get -y install sudo

# Bundle app source into Docker container
COPY . ./etsin_finder_search

# Create log file directory
RUN mkdir /var/log/etsin_finder_search/ 

WORKDIR /etsin_finder_search

# Install dependencies
RUN pip install --upgrade pip wheel
RUN pip install -r requirements.txt

CMD ["./reindex_and_start_rabbitmq_consumer.sh"]

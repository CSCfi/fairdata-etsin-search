FROM python:3.6

RUN apt-get update && apt-get -y install sudo

# Bundle app source into Docker container
COPY . .

COPY rabbitmq-consumer.service /lib/systemd/system

# Create log file directory
RUN mkdir /var/log/etsin_finder_search/ 

# Install dependencies
RUN pip install --upgrade pip wheel
RUN pip install -r requirements.txt

# Default command, used as entrypoint for .py script commands 
RUN chmod a+x reindex_and_start_rabbitmq_consumer.sh

CMD ["./reindex_and_start_rabbitmq_consumer.sh"]

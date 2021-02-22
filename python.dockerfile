FROM python:3.6

RUN apt-get update && apt-get -y install sudo

# Bundle app source into Docker container
COPY . .

# Copy app_config to Docker container
COPY ../app_config /home/etsin-user/app_config

# Create log file directory
RUN mkdir /var/log/etsin_finder_search/ 

# Install dependencies
RUN pip install --upgrade pip wheel
RUN pip install -r requirements.txt

# Default command, used as entrypoint for .py script commands 
CMD ["python"]

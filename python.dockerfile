FROM python:3.6

# Bundle app source into Docker container
COPY . .

# Copy app_config to Docker container
COPY app_config /home/etsin-user/app_config

# Create log file directory
RUN mkdir /var/log/etsin_finder_search/ 

# install dependencies
RUN pip install --upgrade pip wheel
RUN pip install -r requirements.txt

# Define default command
CMD ["python", "create_empty_index.py"]

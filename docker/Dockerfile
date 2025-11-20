###############################################################################
###############################################################################
#################### RUN DOCKER BUILD IN PARENT DIR   #########################
## docker build --no-cache -t east01/rcimetrics:tag -f docker/Dockerfile .  ##
###############################################################################

# Use official Python 3.13 image from Docker Hub
FROM python:3.13-slim AS base

# Set working directory inside the container
WORKDIR /app

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/east-01/AutoMetrics.git /app/

# Install dependencies
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY rci_plugins/ /app/plugins/rci_plugins/
COPY configs /app/configs/

CMD ["bash", "-c", "echo Running $CONFIG_FILE; python /app/src/main.py $CONFIG_FILE"]
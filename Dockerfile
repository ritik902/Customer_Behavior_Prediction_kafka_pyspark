# Use a base image with Python installed
FROM python:3.8-slim

# Install Kafka Python library
RUN pip install kafka-python

# Set the working directory
WORKDIR /app

# Copy the producer script to the container
COPY producer.py /app/

# Command to run the script
CMD ["python3", "/app/producer.py"]

# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables for Spark and Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.0
ENV PYSPARK_PYTHON=python3

# Install system dependencies (Java 17 is required for Spark 3.5)
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the environment file and install Python dependencies
COPY environment.yml .
RUN pip install --no-cache-dir pyspark==3.5.0 pandas numpy scikit-learn psutil pyyaml

# Copy the rest of the project code
COPY . .

# Create directory structure for local data testing
RUN mkdir -p data/schemas data/samples

# Command to run the test pipeline by default
CMD ["python", "scripts/test_pipeline.py"]
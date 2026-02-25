# Use a lightweight Python image with Java (required for PySpark)
FROM openjdk:11-jre-slim-buster

# Install Python and essential build tools
RUN apt-get update && apt-get install -y python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the environment requirements first (optimizes Docker caching)
COPY environment.yml .
COPY scripts/ ./scripts/
COPY notebooks/ ./notebooks/
COPY config/ ./config/

# Install PySpark and dependencies
# Requirement: Choice of storage format (PyArrow for fast Parquet)
RUN pip3 install pyspark pandas pyarrow scikit-learn matplotlib seaborn

# Environment variables for Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Run the master pipeline script by default
CMD ["python3", "scripts/run_pipeline.py"]
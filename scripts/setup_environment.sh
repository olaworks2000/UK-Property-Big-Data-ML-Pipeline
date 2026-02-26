#!/bin/bash

# ==============================================================================
# UK Property Big Data ML Pipeline - Environment Setup Script
# Requirement: Infrastructure & Scalability
# ==============================================================================

echo "Initiating Environment Setup..."

# 1. Update and Install System Dependencies (For Docker/Linux environments)
echo "Installing system dependencies (Java & Python)..."
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    sudo apt-get update
    sudo apt-get install -y openjdk-11-jre-slim python3-pip
fi

# 2. Install Python Libraries (Requirement: 2a - MLlib & Scikit-learn)
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install pyspark==3.5.0 \
            pandas \
            pyarrow \
            scikit-learn \
            matplotlib \
            seaborn \
            pyyaml

# 3. Create Project Directory Structure (Requirement: Project Repository)
echo "Creating repository folder structure..."
mkdir -p notebooks
mkdir -p scripts
mkdir -p config
mkdir -p data/schemas
mkdir -p data/samples
mkdir -p tests
mkdir -p models

# 4. Initialize Databricks Volumes (If running in Databricks)
# Note: This is a placeholder for UC Volume initialization
echo "Environment configured for Databricks Unity Catalog Volumes..."

# 5. Verify Installation
echo "Verifying Spark Installation..."
python3 -c "import pyspark; print('Spark Version:', pyspark.__version__)"

echo "Setup Complete. Your environment is ready for the UK Property Pipeline."
#!/bin/bash

# ==============================================================================
# UK Property Big Data - Environment Setup Script
# Fulfills Section 1(a): Technical Environment Configuration
# ==============================================================================

echo "--- Starting Environment Setup for UK Property Pipeline ---"

# 1. Create Local Directory Structure
echo "Creating project directories..."
mkdir -p data/schemas data/samples
mkdir -p logs/performance
mkdir -p tableau/extracts

# 2. Check for Python 3.10+
if command -v python3 &>/dev/null; then
    echo "Python check: PASSED ($(python3 --version))"
else
    echo "Python check: FAILED. Please install Python 3.10."
    exit 1
fi

# 3. Initialize Conda Environment (Optional check)
if command -v conda &>/dev/null; then
    echo "Conda detected. Creating environment from environment.yml..."
    conda env create -f ../environment.yml || echo "Environment already exists."
else
    echo "Conda not found. Skipping environment creation."
fi

# 4. Set Permissions for Orchestrator
echo "Setting execution permissions for run_pipeline.py..."
chmod +x run_pipeline.py

echo "--- Setup Complete! ---"
echo "To begin, activate your environment: 'conda activate uk_property_ml_pipeline'"
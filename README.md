# UK Land Registry: Big Data ML Pipeline (30.9M Rows)
**Student Name:** OLAYEMI ODOBO
**Dataset:** 6GB UK Property Sales (1995-2026)  
**Architecture:** Distributed Medallion (Bronze/Silver/Gold)

## Project Overview
This project implements a scalable, distributed machine learning pipeline to analyze and predict property classifications across England and Wales. Processing **30,906,560 rows**, the system demonstrates Big Data architecture principles using PySpark and Unity Catalog.

## Technical Architecture
### 1. Data Engineering (Medallion Flow)
- **Bronze:** Raw ingestion from S3 into Parquet format with schema enforcement.
- **Silver:** Data cleaning, `StandardScaler` normalization, and temporal feature engineering.
- **Gold:** Aggregated business summaries and ML model performance metrics.

### 2. Distributed Machine Learning
I implemented a multiclass classification suite using Spark MLlib:
- **Algorithms:** Logistic Regression, Decision Tree, Random Forest, and K-Means.
- **Optimization:** Hyperparameter tuning via `ParamGridBuilder` and 3-fold `CrossValidator`.
- **Performance:** Achieved an accuracy of **37.25%** with a stability variance of **0.0005**.

## Dashboard Insights (Tableau)
The project includes an interactive dashboard with 4 distinct views:
1. **Pipeline Monitoring:** Data quality and ingestion metrics.
2. **Model Evaluation:** Confusion matrix and feature importance (Entropy/Information Gain).
3. **Market Analysis:** National price heatmap vs. the mean (£234,011.17).
4. **Scalability:** Resource allocation and "Strong Scaling" analysis.

## Repository Structure
- **`/notebooks`**: Modularized PySpark logic following the Medallion Architecture (Ingestion, Engineering, Training, Evaluation).
- **`/tableau`**: Contains the `dashboard_final.twbx` file. The workbook features 4 distinct views: Pipeline Monitoring, Model Performance, Business Insights, and Scalability Analysis.
- **`/scripts`**: Automation including the `performance_profiler.py` for Strong/Weak scaling analysis and the master `run_pipeline.py`.
- **`/config`**: Configuration files (`spark_config.yaml`) documenting the cluster specifications (Standard_DS3_v2, 2 Workers) used for the 30.9M row processing.
- **`/data`**: 
    - `/schemas`: JSON/Text definition of the Land Registry schema used for fail-fast ingestion.
    - `/samples`: A 100-row representative sample of the processed Silver layer for rapid review.
- **`/tests`**: Automated unit tests (`test_pipeline.py`) verifying data integrity and schema compliance.

## Requirements & Setup
- **Spark Version:** 3.5.0
- **Hardware:** HP Omen 14 (Student Cluster: Standard_DS3_v2)
- **Environment:** See `environment.yml` and `Dockerfile`.
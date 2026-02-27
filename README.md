# UK Land Registry: Big Data ML Pipeline (30.9M Rows)
**Student Name:** OLAYEMI ODOBO
**Dataset:** 6GB UK Property Sales (1995-2026)  
**Architecture:** Distributed Medallion (Bronze/Silver/Gold)

## Project Overview
This project implements a scalable, distributed machine learning pipeline to analyze and predict property classifications across England and Wales. Processing **30,906,560 rows**, the system demonstrates Big Data architecture principles using PySpark, Databricks, and Unity Catalog.

## Technical Architecture
### 1. Data Engineering (Medallion Flow)
- **Bronze:** Raw ingestion from Unity Catalog Volumes into Parquet format with partition-by-County logic.
- **Silver:** Data cleaning, `StandardScaler` normalization, and Geographic Feature Encoding (indexing 1,173 unique towns).
- **Gold:** Aggregated business summaries (30.9M records) and ML model performance metrics exported as CSV for Tableau.

### 2. Distributed Machine Learning (Iterative Optimization)
I implemented a multiclass classification suite using Spark MLlib. To overcome initial baseline limitations, the model was iterated:
- **Algorithms:** Logistic Regression, Decision Tree, Random Forest, and Linear Regression.
- **Feature Engineering:** Added high-cardinality categorical encoding for `Town_City` and utilized `maxBins=1200`.
- **Performance:** Accuracy improved from a baseline of ~5% to **41.12%** (Random Forest) by incorporating geographic context into the feature vector.

## Dashboard Insights (Tableau)
The project features a **4x4 Dashboard Strategy** (4 Dashboards, each with 4 distinct views):
1. **D1: Pipeline Monitoring:** Volume analysis (30.9M rows) and ingestion health checks.
2. **D2: Model Evaluation:** Confusion matrix, accuracy trends, and error distribution.
3. **D3: Market Analysis:** National price trends and property type distributions.
4. **D4: Scalability:** Performance profiling showing a total pipeline execution of ~445 seconds.

## Repository Structure
- **`/notebooks`**: Modularized PySpark logic (Ingestion, Engineering, Training, Evaluation).
- **`/scripts`**: `run_pipeline.py` - Single-entry master script for full data-to-model synchronization.
- **`/data`**: Schema definitions and Gold layer samples for external visualization.

## Requirements & Setup
- **Spark Version:** 3.5.0
- **Environment:** Databricks Runtime 14.3 LTS (Standard_DS3_v2, 2 Workers).
- **Hardware Profile:** Optimization tested on HP Omen 14.
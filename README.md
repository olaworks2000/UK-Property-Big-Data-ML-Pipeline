# UK Property Big Data ML Pipeline (30.9M Records)

## Project Overview
This repository contains a production-grade **Medallion Architecture** implemented on Databricks Serverless to process and analyze 30.9 million UK Land Registry records. The project demonstrates high-performance data engineering, distributed machine learning, and interactive big data visualization.



## Interactive Dashboards
The final analysis is presented across four distinct Tableau dashboards. 
**[VIEW THE FULL TABLEAU REPOSITORY HERE] https://public.tableau.com/app/profile/olayemi.odobo/viz/All_Dashboards_17723367352830/4_ScalabilityCloudEconomics?publish=yes**

- **Dashboard 1:** Pipeline Health & Data Quality Audit
- **Dashboard 2:** Model Performance & Cost-Performance Tradeoff
- **Dashboard 3:** Business Insights & UK Property Trends
- **Dashboard 4:** Scalability Analysis (Spark vs. Scikit-Learn)

## 🛠 Technical Requirements Met
### 1. Data Engineering (PySpark)
- **Ingestion:** Explicit schema definition for 6GB CSV to prevent OOM errors.
- **Storage:** Use of **Parquet** format with partitioning by `County` for optimized query patterns.
- **Lineage:** Automated metadata injection (`source_file`, `ingest_timestamp`) for audit trails.
- **Validation:** Distributed QA gate using `test_pipeline.py` to enforce data integrity.

### 2. Scalability & Distributed ML
- **MLlib Implementation:** Comparison of 4 algorithms: Linear Regression, Decision Tree, Random Forest, and GBT.
- **Efficiency:** Parallel data cleaning using `mapInPandas` for $O(n/p)$ computational complexity.
- **Baseline Comparison:** Benchmarked Spark MLlib against a single-node Scikit-Learn baseline, demonstrating a **309x scalability advantage**.

## Repository Structure
```text
├── notebooks/          # End-to-end Medallion notebooks (1-4)
├── scripts/            # Orchestration (run_pipeline.py) & Profiling
├── tests/              # Automated Data Quality Gate
├── tableau/            # Packaged Tableau files (.twbx)
├── config/             # Spark and environment configurations
├── Dockerfile          # Containerization for reproducibility
└── environment.yml     # Conda environment specification

Performance Summary

Metric,Scikit-Learn (Baseline),PySpark MLlib (Project)
Data Volume,"100,000 Rows","30,906,560 Rows"
Execution,Single-Node (RAM Bound),Distributed (Scalable)
Linear Reg Time,~0.10s,~30.28s
Scalability,1x,309x

Author: Odobo Olayemi Osazuwa

Institution: Coventry University

Module: 7006SCN - Big Data Systems and Machine Learning
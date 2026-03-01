# UK Property Big Data - Visualization Strategy
This folder contains the packaged Tableau workbooks (`.twbx`) for the UK Property ML Pipeline. These dashboards are designed to provide stakeholders with actionable insights into pipeline health, model performance, and market trends.

## Dashboard Overviews

### 1. Pipeline Monitoring & Data Quality
- **Purpose:** Real-time audit of the Medallion architecture.
- **Key Metrics:** Record throughput (30.9M rows), stage-wise execution time, and Pass/Fail status of the QA gate.
- **Technical Highlight:** Visualizes the $O(n/p)$ complexity by showing linear scaling in processing time relative to row volume.

### 2. Model Performance & Evaluation
- **Purpose:** Comparison of MLlib algorithms (LR, DT, RF, GBT).
- **Key Metrics:** RMSE vs. Training Time (sec).
- **Technical Highlight:** Uses a **Scatter Plot** to identify the cost-performance tradeoff, justifying the selection of GBT for accuracy vs. LR for latency.

### 3. Business Insights & UK Trends
- **Purpose:** Geospatial and temporal analysis of the UK housing market.
- **Key Metrics:** Average price by County, 30-year price trends, and property type distribution.
- **Technical Highlight:** Employs **Action Filters** on the UK map to allow drill-down into specific regional statistics.

### 4. Scalability & Cost Analysis
- **Purpose:** Proving the necessity of Big Data systems.
- **Key Metrics:** Spark MLlib vs. Scikit-Learn (Single-node) benchmarking.
- **Technical Highlight:** Demonstrates the **309x scalability advantage** and identifies RAM-limit bottlenecks in traditional single-node processing.

## 🛠 Best Practices Applied (Requirement 3b)
- **Tableau Extracts:** Used for high-performance interaction with 100k+ sample records.
- **Level of Detail (LOD):** Applied to calculate regional price variances independent of the view filter.
- **Interactivity:** Integrated global dashboard actions for a seamless narrative flow.
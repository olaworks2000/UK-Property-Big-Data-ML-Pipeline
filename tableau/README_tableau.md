# Tableau Dashboard: UK Land Registry Big Data Analytics
**Link to Live Dashboard:** https://public.tableau.com/app/profile/olayemi.odobo/viz/UK_Property_BigData_Project/Dashboard1?publish=yes

## Summary of Project Views
This workbook provides a 4-quadrant analysis of the 30,906,560 row dataset, synchronized with the PySpark Medallion pipeline.

### 1. Pipeline Monitoring: 30.9M Records
- **Technical Goal:** Data Engineering Validation.
- **Insight:** Visualizes the successful ingestion and distribution of the full 6GB dataset across property classifications (D, S, T, F, O) after cleaning.

### 2. Model Performance: Feature Importance (Price)
- **Technical Goal:** Machine Learning Evaluation.
- **Insight:** Demonstrates why 'Price' was the primary feature for the MLlib classifier. Shows the "Price Signature" of different categories, identifying "O" (Other) as the highest value predictor (£701,613).

### 3. Business Insights: National Price Heatmap
- **Technical Goal:** Geospatial Urban Economics.
- **Insight:** A high-contrast heatmap centered at the national mean (£234,011.17). It reveals the North-South economic divide and identifies high-value clusters in London and the South East.

### 4. Scalability Analysis: Data Volume by Node
- **Technical Goal:** Distributed System Performance.
- **Insight:** A Treemap visualizing the computational load per property category. This represents how Spark partitions the massive volume of data for parallel processing.

## Interactivity
The **National Price Heatmap** is enabled as a 'Global Filter.' Clicking any geographic data point will instantly update the metrics in the other three quadrants to reflect that specific region's data.
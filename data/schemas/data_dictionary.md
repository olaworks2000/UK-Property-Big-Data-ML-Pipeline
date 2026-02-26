# UK Land Registry - Data Dictionary

This document defines the schema and business logic for the 30.9 million row UK Property dataset used in this ML Pipeline.

## 1. Bronze Layer (Raw Ingested Columns)
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| **Transaction_ID** | String | Unique identifier for the sale. |
| **Price** | Integer | The sale price in GBP (£). |
| **Date** | Timestamp | Date and time the transfer took place. |
| **Postcode** | String | UK Postcode (e.g., CV1 5FB). |
| **Property_Type** | String | Type of property: D=Detached, S=Semi, T=Terraced, P=Flat, O=Other. |
| **Old_New** | String | Age of property: Y=Newly built, N=Established residential. |
| **Duration** | String | Tenure type: F=Freehold, L=Leasehold. |
| **PAON / SAON** | String | Primary/Secondary Addressable Object Name (House number/Flat name). |
| **Street / Locality** | String | Street and local area name. |
| **Town_City** | String | The primary Town or City (Used for Geographic Analysis). |
| **District / County** | String | Administrative district and county (Partition Key). |
| **PPD_Category** | String | A=Standard Price Paid, B=Additional Price Paid (Repo/Part-exchange). |
| **Record_Status** | String | A=Addition, C=Change, D=Delete. |

## 2. Silver Layer (Engineered Features)
| Column Name | Source / Logic | Purpose |
| :--- | :--- | :--- |
| **Sale_Year** | `year(Date)` | Extracted for temporal splitting (Train: <2023). |
| **type_label** | `StringIndexer` | Numeric encoding of Property_Type for ML models. |
| **Market_Segment** | `PriceSegmenter` | Custom Feature: Budget (<150k), Standard, or Premium (>450k). |
| **scaled_features** | `StandardScaler` | Z-score normalized Price vector for model stability. |
| **Type_Description**| `Broadcast Join` | Human-readable mapping (e.g., 'D' -> 'Detached'). |

## 3. Gold Layer (Tableau Exports)
- **gold_tableau_data**: 100k row sample for Business Intelligence (Dashboards 1 & 3).
- **gold_model_performance**: Prediction results vs Actuals for ML Evaluation (Dashboard 2).
- **gold_pipeline_performance**: Execution time logs for Scalability Analysis (Dashboard 4).
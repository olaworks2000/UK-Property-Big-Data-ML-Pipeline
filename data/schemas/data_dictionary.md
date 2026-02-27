# UK Land Registry - Data Dictionary

This document defines the schema and business logic for the 30,906,560 row UK Property dataset used in this ML Pipeline.

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
| **Town_City** | String | The primary Town or City (Used for Geographic Feature Engineering). |
| **District / County** | String | Administrative district and county (Partition Key). |
| **PPD_Category** | String | A=Standard Price Paid, B=Additional Price Paid. |
| **Record_Status** | String | A=Addition, C=Change, D=Delete. |

## 2. Silver Layer (Engineered Features)
| Column Name | Source / Logic | Purpose |
| :--- | :--- | :--- |
| **Sale_Year** | `year(Date)` | Extracted for temporal splitting (Train: <2023, Test: >=2023). |
| **type_label** | `StringIndexer` | Numeric encoding of Property_Type (The Target Variable). |
| **city_label** | `StringIndexer` | Numeric index of 1,173 unique towns for Geographic context. |
| **Market_Segment** | `PriceSegmenter` | Custom Feature: Budget (<150k), Standard, or Premium (>450k). |
| **scaled_features** | `StandardScaler` | Z-score normalized Price vector to improve model convergence. |
| **final_features** | `VectorAssembler` | The combined feature vector (Price + City) used for Training. |
| **Type_Description**| `Broadcast Join` | Human-readable mapping joined from mapping dataframe. |

## 3. Gold Layer (Tableau Exports)
| File Name | Content | Dashboard Use Case |
| :--- | :--- | :--- |
| **gold_tableau_data** | 100k Clean Sample | Business Intelligence & Heatmaps (Dash
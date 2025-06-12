# Architecture Documentation

## System Overview
ETL/ELT pipeline processing 61,500 synthetic records into BigQuery data warehouse.

## Data Flow
```
Sources → Data Lake → Staging → Transform → Warehouse → External Access
   ↓         ↓          ↓          ↓          ↓           ↓
CSV File → raw/csv → staging_area → transform → data_warehouse → external_access
API Data → raw/api → staging_area → transform → data_warehouse → external_access  
Firestore → raw/firestore → staging_area → transform → data_warehouse → external_access
```

## Components

### Data Sources
- **CSV**: `gs://synthetic-data-csv-data-demo-etl/` (10K customers)
- **API**: `https://europe-west1-data-demo-etl.cloudfunctions.net/products-api` (500 products)
- **Firestore**: Collections `marketing_campaigns`, `sales_transactions` (51K docs)

### Orchestration
- **Composer Environment**: `etl-demo-env`
- **Location**: `europe-west1`
- **Airflow Version**: 2.10.5
- **Composer Version**: 2.13.2

### BigQuery Datasets
- **staging_area**: Raw data ingestion
- **data_warehouse**: Star schema (3 dims + 1 fact)
- **metadata**: ETL audit logs
- **external_access**: BI tool views

### External Access
- **Power BI**: Customer analytics dashboards
- **Tableau**: Product performance analytics
- **API Endpoints**: REST API for custom apps

## Star Schema
```
dim_customers (10K) ──┐
                      │
dim_products (500) ───┼─── fact_sales_transactions (50K)
                      │
dim_campaigns (1K) ───┘
```

## Technology Stack
- **Orchestration**: Apache Airflow (Cloud Composer)
- **Data Warehouse**: Google BigQuery
- **Data Lake**: Google Cloud Storage
- **NoSQL**: Google Firestore
- **External Access**: Power BI, Tableau, REST API
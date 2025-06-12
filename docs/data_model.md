# Data Model - Star Schema Design

## Overview

This document defines the star schema design for our ETL/ELT pipeline processing 61,500 synthetic records from three data sources into a BigQuery data warehouse.

## Data Sources Summary

| Source | Type | Volume | Primary Entity |
|--------|------|--------|----------------|
| CSV Files | Customer Data | 10,000 records | Customers |
| REST API | Product Catalog | 500 products | Products |
| Firestore | Campaigns | 1,000 documents | Marketing Campaigns |
| Firestore | Transactions | 50,000 documents | Sales Transactions |

**Total Records**: 61,500

## Star Schema Design

```
                    ┌─────────────────┐
                    │  dim_customers  │
                    │                 │
                    │ customer_id (PK)│◄──┐
                    │ first_name      │   │
                    │ last_name       │   │
                    │ email           │   │
                    │ country         │   │
                    │ created_date    │   │
                    └─────────────────┘   │
                                          │
    ┌─────────────────┐                  │    ┌─────────────────────┐
    │  dim_products   │                  │    │ fact_sales_transactions │
    │                 │                  │    │                     │
    │ product_id (PK) │◄─────────────────┼────┤ transaction_id (PK) │
    │ name            │                  │    │ customer_id (FK)    │
    │ category        │                  └────┤ product_id (FK)     │
    │ price           │                       │ campaign_id (FK)    │
    │ description     │                       │ quantity            │
    │ created_date    │                       │ unit_price          │
    └─────────────────┘                       │ total_amount        │
                                              │ transaction_date    │
    ┌─────────────────┐                       │ created_timestamp   │
    │ dim_campaigns   │                       └─────────────────────┘
    │                 │                              ▲
    │ campaign_id (PK)│◄─────────────────────────────┘
    │ name            │
    │ type            │
    │ status          │
    │ budget          │
    │ start_date      │
    │ end_date        │
    └─────────────────┘
```

## Dimension Tables

### dim_customers
**Source**: CSV files (`gs://synthetic-data-csv-data-demo-etl/data/synthetic_customers_*.csv`)
**Records**: ~10,000

| Column | Type | Description | Source Field |
|--------|------|-------------|--------------|
| customer_id | STRING | Unique customer identifier (PK) | customer_id |
| first_name | STRING | Customer first name | first_name |
| last_name | STRING | Customer last name | last_name |
| email | STRING | Customer email address | email |
| country | STRING | Customer country | country |
| phone | STRING | Customer phone number | phone |
| address | STRING | Customer address | address |
| city | STRING | Customer city | city |
| postal_code | STRING | Customer postal code | postal_code |
| registration_date | DATE | Customer registration date | registration_date |
| created_date | TIMESTAMP | ETL load timestamp | CURRENT_TIMESTAMP() |

### dim_products
**Source**: REST API (`https://europe-west1-data-demo-etl.cloudfunctions.net/products-api`)
**Records**: ~500

| Column | Type | Description | Source Field |
|--------|------|-------------|--------------|
| product_id | STRING | Unique product identifier (PK) | product_id |
| name | STRING | Product name | name |
| category | STRING | Product category | category |
| price | NUMERIC(10,2) | Product price in EUR | price |
| description | STRING | Product description | description |
| brand | STRING | Product brand | brand |
| weight | NUMERIC(8,3) | Product weight | weight |
| dimensions | STRING | Product dimensions | dimensions |
| created_date | TIMESTAMP | ETL load timestamp | CURRENT_TIMESTAMP() |

### dim_campaigns
**Source**: Firestore collection `marketing_campaigns`
**Records**: ~1,000

| Column | Type | Description | Source Field |
|--------|------|-------------|--------------|
| campaign_id | STRING | Unique campaign identifier (PK) | document_id |
| name | STRING | Campaign name | name |
| type | STRING | Campaign type | type |
| status | STRING | Campaign status | status |
| budget | NUMERIC(12,2) | Campaign budget | budget |
| start_date | DATE | Campaign start date | start_date |
| end_date | DATE | Campaign end date | end_date |
| target_audience | STRING | Target audience | target_audience |
| created_date | TIMESTAMP | ETL load timestamp | CURRENT_TIMESTAMP() |

## Fact Table

### fact_sales_transactions
**Source**: Firestore collection `sales_transactions`
**Records**: ~50,000

| Column | Type | Description | Source Field |
|--------|------|-------------|--------------|
| transaction_id | STRING | Unique transaction identifier (PK) | document_id |
| customer_id | STRING | Foreign key to dim_customers | customer_id |
| product_id | STRING | Foreign key to dim_products | product_id |
| campaign_id | STRING | Foreign key to dim_campaigns | campaign_id |
| quantity | INTEGER | Quantity purchased | quantity |
| unit_price | NUMERIC(10,2) | Unit price at time of sale | unit_price |
| total_amount | NUMERIC(12,2) | Total transaction amount | total_amount |
| discount_amount | NUMERIC(10,2) | Discount applied | discount_amount |
| tax_amount | NUMERIC(10,2) | Tax amount | tax_amount |
| transaction_date | DATE | Date of transaction | transaction_date |
| transaction_time | TIME | Time of transaction | transaction_time |
| payment_method | STRING | Payment method used | payment_method |
| order_status | STRING | Order status | order_status |
| created_timestamp | TIMESTAMP | ETL load timestamp | CURRENT_TIMESTAMP() |

## Foreign Key Relationships

1. **fact_sales_transactions.customer_id** → **dim_customers.customer_id**
   - Links transactions to customer information
   - Enables customer segmentation and analysis

2. **fact_sales_transactions.product_id** → **dim_products.product_id**
   - Links transactions to product details
   - Enables product performance analysis

3. **fact_sales_transactions.campaign_id** → **dim_campaigns.campaign_id**
   - Links transactions to marketing campaigns
   - Enables campaign ROI analysis

## Data Quality Rules

### Referential Integrity
- All foreign keys in fact table must have corresponding records in dimension tables
- Orphaned records will be logged and handled during ETL process

### Data Validation
- **Dates**: transaction_date must be between campaign start_date and end_date
- **Amounts**: total_amount = (quantity × unit_price) - discount_amount + tax_amount
- **Status**: Only valid status values allowed per business rules

### Null Handling
- Primary keys: NOT NULL (enforced)
- Foreign keys: NOT NULL for fact table
- Business fields: NULL allowed with default handling rules

## Indexing Strategy

### BigQuery Clustering
- **dim_customers**: Clustered by country, registration_date
- **dim_products**: Clustered by category, price
- **dim_campaigns**: Clustered by type, start_date
- **fact_sales_transactions**: Clustered by transaction_date, customer_id

### Partitioning
- **fact_sales_transactions**: Partitioned by transaction_date (daily)
- **dim_campaigns**: Partitioned by start_date (monthly)
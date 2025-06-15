# Data Model - Star Schema Design

## Overview

This document defines the star schema design for our ETL/ELT pipeline processing 61,500 synthetic records from three data sources into a BigQuery data warehouse with integrated quality monitoring and governance.

## Data Sources Summary

| Source | Type | Volume | Primary Entity | Status |
|--------|------|--------|----------------|---------|
| CSV Files | Customer Data | 10,000 records | Customers | ✅ Operational |
| REST API | Product Catalog | 500 products | Products | ✅ Operational |
| Firestore | Campaigns | 1,000 documents | Marketing Campaigns | ✅ Operational |
| Firestore | Transactions | 50,000 documents | Sales Transactions | ✅ Operational |

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
**Source**: CSV files (`gs://synthetic-data-csv-data-demo-etl/synthetic_customers_*.csv`)
**Records**: 10,000

| Column | Type | Description | Business Usage |
|--------|------|-------------|----------------|
| customer_id | STRING | Unique customer identifier (PK) | Customer segmentation, retention analysis |
| first_name | STRING | Customer first name | Personalization, customer service |
| last_name | STRING | Customer last name | Customer identification, reporting |
| email | STRING | Customer email address | Marketing campaigns, communication |
| country | STRING | Customer country | Geographic analysis, regional performance |
| phone | STRING | Customer phone number | Customer service, contact management |
| address | STRING | Customer address | Shipping analysis, location insights |
| city | STRING | Customer city | Urban vs rural analysis, logistics |
| registration_date | DATE | Customer registration date | Cohort analysis, customer lifecycle |
| age | INTEGER | Customer age | Demographic segmentation, targeting |
| created_date | TIMESTAMP | ETL load timestamp | Data freshness tracking |

### dim_products
**Source**: REST API (`https://europe-west1-data-demo-etl.cloudfunctions.net/products-api`)
**Records**: 500

| Column | Type | Description | Business Usage |
|--------|------|-------------|----------------|
| product_id | STRING | Unique product identifier (PK) | Product performance, inventory management |
| name | STRING | Product name | Product catalog, search functionality |
| category | STRING | Product category | Category performance, merchandising |
| subcategory | STRING | Product subcategory | Detailed product analysis |
| brand | STRING | Product brand | Brand performance, supplier analysis |
| price | NUMERIC(10,2) | Product price in EUR | Pricing analysis, revenue calculation |
| currency | STRING | Price currency | Multi-currency support |
| description | STRING | Product description | Product information, search optimization |
| availability | BOOLEAN | Product availability | Inventory status, availability analysis |
| stock_quantity | INTEGER | Stock quantity | Inventory management, stockout analysis |
| rating | NUMERIC(3,2) | Product rating | Quality analysis, customer satisfaction |
| reviews_count | INTEGER | Number of reviews | Product popularity, social proof |
| weight_kg | NUMERIC(8,3) | Product weight in kg | Shipping cost calculation, logistics |
| country_origin | STRING | Country of origin | Supply chain analysis, sourcing insights |
| created_date | TIMESTAMP | ETL load timestamp | Data freshness tracking |

### dim_campaigns
**Source**: Firestore collection `marketing_campaigns`
**Records**: 1,000

| Column | Type | Description | Business Usage |
|--------|------|-------------|----------------|
| campaign_id | STRING | Unique campaign identifier (PK) | Campaign performance, ROI analysis |
| name | STRING | Campaign name | Campaign identification, reporting |
| channel | STRING | Marketing channel | Channel effectiveness, budget allocation |
| status | STRING | Campaign status | Campaign lifecycle management |
| target_audience | STRING | Target audience | Audience analysis, targeting effectiveness |
| budget | NUMERIC(12,2) | Campaign budget | Budget management, cost control |
| actual_spend | NUMERIC(12,2) | Actual spend | Budget variance analysis, cost tracking |
| start_date | DATE | Campaign start date | Campaign timeline, seasonal analysis |
| end_date | DATE | Campaign end date | Campaign duration, effectiveness period |
| conversion_rate | NUMERIC(6,4) | Conversion rate | Campaign effectiveness, optimization |
| impressions | INTEGER | Total impressions | Reach analysis, brand awareness |
| clicks | INTEGER | Total clicks | Engagement analysis, CTR calculation |
| conversions | INTEGER | Total conversions | Campaign success, ROI measurement |
| created_date | TIMESTAMP | ETL load timestamp | Data freshness tracking |

## Fact Table

### fact_sales_transactions
**Source**: Firestore collection `sales_transactions`
**Records**: 50,000

| Column | Type | Description | Business Usage |
|--------|------|-------------|----------------|
| transaction_id | STRING | Unique transaction identifier (PK) | Transaction tracking, order management |
| customer_id | STRING | Foreign key to dim_customers | Customer analysis, segmentation |
| product_id | STRING | Foreign key to dim_products | Product performance, sales analysis |
| campaign_id | STRING | Foreign key to dim_campaigns | Campaign attribution, ROI analysis |
| quantity | INTEGER | Quantity purchased | Volume analysis, demand forecasting |
| unit_price | NUMERIC(10,2) | Unit price at time of sale | Price analysis, discount effectiveness |
| amount | NUMERIC(12,2) | Total transaction amount | Revenue analysis, financial reporting |
| discount_amount | NUMERIC(10,2) | Discount applied | Promotion effectiveness, pricing strategy |
| tax_amount | NUMERIC(10,2) | Tax amount | Tax reporting, compliance |
| transaction_date | DATE | Date of transaction | Time series analysis, seasonal trends |
| payment_method | STRING | Payment method used | Payment analysis, processing costs |
| status | STRING | Transaction status | Order fulfillment, customer service |
| shipping_address | STRING | Shipping address | Logistics analysis, delivery optimization |
| order_notes | STRING | Order notes | Customer service, order details |
| created_timestamp | TIMESTAMP | ETL load timestamp | Data freshness tracking |

## Business Relationships

### Primary Business Processes
1. **Sales Transactions**: Central fact measuring business performance
2. **Customer Acquisition**: New customer registration and first purchase
3. **Campaign Effectiveness**: Marketing campaign impact on sales
4. **Product Performance**: Product-specific sales and popularity metrics

### Key Performance Indicators (KPIs)
- **Revenue Metrics**: Total sales, average order value, revenue per customer
- **Customer Metrics**: Customer lifetime value, acquisition cost, retention rate
- **Product Metrics**: Product profitability, inventory turnover, category performance
- **Campaign Metrics**: Return on ad spend (ROAS), cost per acquisition, conversion rates

## Data Quality Framework

### Quality Monitoring
- **Uniqueness**: 100% for all primary keys
- **Completeness**: 95%+ for critical business fields
- **Referential Integrity**: 99%+ for foreign key relationships
- **Data Freshness**: All tables updated within defined SLAs
- **Volume Anomaly**: Record counts within expected ranges

### Data Validation Rules
- **Business Logic**: amount = (quantity × unit_price) - discount_amount + tax_amount
- **Date Consistency**: transaction_date between campaign start_date and end_date
- **Status Validation**: Only valid status values per business rules
- **Price Validation**: Positive values for prices, amounts, and quantities

## Performance Optimization

### BigQuery Clustering Strategy
- **dim_customers**: Clustered by country, registration_date (geographic and temporal queries)
- **dim_products**: Clustered by category, price (product analysis queries)
- **dim_campaigns**: Clustered by channel, start_date (campaign performance queries)
- **fact_sales_transactions**: Clustered by transaction_date, customer_id (time-based and customer analysis)

### Partitioning Strategy
- **fact_sales_transactions**: Partitioned by transaction_date (daily) for time-based analysis
- **dim_campaigns**: Partitioned by start_date (monthly) for campaign lifecycle analysis

## Governance & Lineage

### Metadata Management
The data model includes comprehensive lineage tracking and metadata management:

- **Source Mapping**: Complete source-to-target field mapping with confidence scores
- **Transformation Documentation**: Business rule documentation for all transformations
- **Quality Impact**: Integration between data quality results and lineage metadata
- **Execution Tracking**: Pipeline execution logs with timestamp and status information

### Views for Analysis
- **Complete Lineage**: Full data flow visualization from sources to targets
- **Field Lineage**: Field-level transformation mapping for impact analysis
- **Quality Impact**: Quality score integration with lineage for data trust metrics
- **Pipeline Dependencies**: Dependency analysis for operational management

## Analytical Use Cases

### Customer Analytics
- Customer segmentation by demographics and behavior
- Customer lifetime value analysis and prediction
- Churn analysis and retention strategies
- Geographic performance and expansion opportunities

### Product Analytics
- Product performance analysis by category and brand
- Inventory optimization and demand forecasting
- Price elasticity analysis and optimization
- Product recommendation engine data

### Campaign Analytics
- Campaign ROI and effectiveness measurement
- Channel performance and budget optimization
- Customer acquisition cost analysis
- Attribution modeling and customer journey analysis

### Financial Analytics
- Revenue reporting and forecasting
- Profitability analysis by product and customer
- Discount and promotion effectiveness
- Tax reporting and compliance
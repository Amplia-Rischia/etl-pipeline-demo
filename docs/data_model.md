# Data Model - Star Schema Design

## Overview

Comprehensive star schema implementation processing 61,500 synthetic records from three heterogeneous data sources into an optimized BigQuery analytical data warehouse with integrated quality monitoring and complete data lineage.

## Data Sources Summary

| Source | Type | Volume | Primary Entity | Location | Status |
|--------|------|--------|----------------|----------|---------|
| CSV Files | Customer Data | 10,000 records | Customers | `gs://synthetic-data-csv-data-demo-etl/` | ✅ Operational |
| REST API | Product Catalog | 500 products | Products | `https://europe-west1-data-demo-etl.cloudfunctions.net/products-api` | ✅ Operational |
| Firestore | Marketing Data | 1,000 documents | Campaigns | Collection: `marketing_campaigns` | ✅ Operational |
| Firestore | Transaction Data | 50,000 documents | Sales | Collection: `sales_transactions` | ✅ Operational |

**Total Records**: 61,500  
**Target Schema**: Star schema with 3 dimensions + 1 fact table

## Star Schema Architecture

```
                    ┌─────────────────┐
                    │  dim_customers  │
                    │    (10,000)     │
                    │  customer_id    │◄──┐
                    │  first_name     │   │
                    │  last_name      │   │
                    │  email          │   │
                    │  country        │   │
                    │  ...            │   │
                    └─────────────────┘   │
                                          │
    ┌─────────────────┐                  │    ┌─────────────────────┐
    │  dim_products   │                  │    │ fact_sales_trans    │
    │     (500)       │                  │    │      (20,000+)      │
    │  product_id     │◄─────────────────┼────┤  transaction_id     │
    │  name           │                  │    │  customer_id (FK)   │
    │  category       │                  └────┤  product_id (FK)    │
    │  price          │                       │  campaign_id (FK)   │
    │  ...            │                       │  quantity           │
    └─────────────────┘                       │  amount             │
                                              │  transaction_date   │
    ┌─────────────────┐                       │  ...                │
    │  dim_campaigns  │                       └─────────────────────┘
    │    (1,000)      │                              ▲
    │  campaign_id    │◄─────────────────────────────┘
    │  name           │
    │  channel        │
    │  budget         │
    │  ...            │
    └─────────────────┘
```

## Dimension Tables Specification

### dim_customers
**Source**: CSV files (`gs://synthetic-data-csv-data-demo-etl/synthetic_customers_*.csv`)  
**Records**: 10,000  
**Grain**: One record per unique customer

| Column | Data Type | Constraints | Description | Business Purpose |
|--------|-----------|-------------|-------------|------------------|
| customer_id | STRING | PRIMARY KEY, NOT NULL | Unique customer identifier | Customer segmentation, retention analysis |
| first_name | STRING | NOT NULL | Customer first name | Personalization, customer service |
| last_name | STRING | NOT NULL | Customer last name | Customer identification, reporting |
| email | STRING | NOT NULL, UNIQUE | Customer email address | Marketing campaigns, communication |
| country | STRING | NOT NULL | Customer country | Geographic analysis, regional performance |
| phone | STRING | | Customer phone number | Customer service, contact management |
| address | STRING | | Customer address | Shipping analysis, location insights |
| city | STRING | | Customer city | Urban vs rural analysis, logistics |
| registration_date | DATE | NOT NULL | Customer registration date | Cohort analysis, customer lifecycle |
| age | INTEGER | CHECK (age > 0) | Customer age | Demographic segmentation, targeting |
| created_date | TIMESTAMP | NOT NULL, DEFAULT CURRENT_TIMESTAMP | ETL load timestamp | Data freshness tracking |

**Clustering**: `country`, `registration_date` for geographic and temporal queries  
**Business Key**: `email` (natural business identifier)

### dim_products
**Source**: REST API (`https://europe-west1-data-demo-etl.cloudfunctions.net/products-api`)  
**Records**: 500  
**Grain**: One record per unique product

| Column | Data Type | Constraints | Description | Business Purpose |
|--------|-----------|-------------|-------------|------------------|
| product_id | STRING | PRIMARY KEY, NOT NULL | Unique product identifier | Product performance, inventory management |
| name | STRING | NOT NULL | Product name | Product catalog, search functionality |
| category | STRING | NOT NULL | Product category | Category performance, merchandising |
| subcategory | STRING | | Product subcategory | Detailed product analysis |
| brand | STRING | | Product brand | Brand performance, supplier analysis |
| price | NUMERIC(10,2) | CHECK (price >= 0) | Product price in EUR | Pricing analysis, revenue calculation |
| currency | STRING | DEFAULT 'EUR' | Price currency | Multi-currency support |
| description | STRING | | Product description | Product information, search optimization |
| availability | BOOLEAN | NOT NULL | Product availability status | Inventory status, availability analysis |
| stock_quantity | INTEGER | CHECK (stock_quantity >= 0) | Current stock quantity | Inventory management, stockout analysis |
| rating | NUMERIC(3,2) | CHECK (rating BETWEEN 0 AND 5) | Product rating (0-5) | Quality analysis, customer satisfaction |
| reviews_count | INTEGER | CHECK (reviews_count >= 0) | Number of reviews | Product popularity, social proof |
| weight_kg | NUMERIC(8,3) | CHECK (weight_kg > 0) | Product weight in kg | Shipping cost calculation, logistics |
| country_origin | STRING | | Country of origin | Supply chain analysis, sourcing insights |
| created_date | TIMESTAMP | NOT NULL, DEFAULT CURRENT_TIMESTAMP | ETL load timestamp | Data freshness tracking |

**Clustering**: `category`, `price` for product analysis queries  
**Business Key**: `name`, `brand` combination

### dim_campaigns
**Source**: Firestore collection `marketing_campaigns`  
**Records**: 1,000  
**Grain**: One record per unique marketing campaign

| Column | Data Type | Constraints | Description | Business Purpose |
|--------|-----------|-------------|-------------|------------------|
| campaign_id | STRING | PRIMARY KEY, NOT NULL | Unique campaign identifier | Campaign performance, ROI analysis |
| name | STRING | NOT NULL | Campaign name | Campaign identification, reporting |
| channel | STRING | NOT NULL | Marketing channel | Channel effectiveness, budget allocation |
| status | STRING | NOT NULL | Campaign status | Campaign lifecycle management |
| target_audience | STRING | | Target audience description | Audience analysis, targeting effectiveness |
| budget | NUMERIC(12,2) | CHECK (budget >= 0) | Campaign budget | Budget management, cost control |
| actual_spend | NUMERIC(12,2) | CHECK (actual_spend >= 0) | Actual spend amount | Budget variance analysis, cost tracking |
| start_date | DATE | NOT NULL | Campaign start date | Campaign timeline, seasonal analysis |
| end_date | DATE | CHECK (end_date >= start_date) | Campaign end date | Campaign duration, effectiveness period |
| conversion_rate | NUMERIC(6,4) | CHECK (conversion_rate BETWEEN 0 AND 1) | Conversion rate (0-1) | Campaign effectiveness, optimization |
| impressions | INTEGER | CHECK (impressions >= 0) | Total impressions | Reach analysis, brand awareness |
| clicks | INTEGER | CHECK (clicks >= 0) | Total clicks | Engagement analysis, CTR calculation |
| conversions | INTEGER | CHECK (conversions >= 0) | Total conversions | Campaign success, ROI measurement |
| created_date | TIMESTAMP | NOT NULL, DEFAULT CURRENT_TIMESTAMP | ETL load timestamp | Data freshness tracking |

**Partitioning**: Monthly partitioning on `start_date`  
**Clustering**: `channel`, `start_date` for campaign performance queries

## Fact Table Specification

### fact_sales_transactions
**Source**: Firestore collection `sales_transactions`  
**Records**: 50,000  
**Grain**: One record per unique sales transaction

| Column | Data Type | Constraints | Description | Business Purpose |
|--------|-----------|-------------|-------------|------------------|
| transaction_id | STRING | PRIMARY KEY, NOT NULL | Unique transaction identifier | Transaction tracking, order management |
| customer_id | STRING | FOREIGN KEY → dim_customers.customer_id | Customer reference | Customer analysis, segmentation |
| product_id | STRING | FOREIGN KEY → dim_products.product_id | Product reference | Product performance, sales analysis |
| campaign_id | STRING | FOREIGN KEY → dim_campaigns.campaign_id | Campaign reference | Campaign attribution, ROI analysis |
| quantity | INTEGER | CHECK (quantity > 0) | Quantity purchased | Volume analysis, demand forecasting |
| unit_price | NUMERIC(10,2) | CHECK (unit_price >= 0) | Unit price at sale time | Price analysis, discount effectiveness |
| amount | NUMERIC(12,2) | CHECK (amount >= 0) | Total transaction amount | Revenue analysis, financial reporting |
| discount_amount | NUMERIC(10,2) | CHECK (discount_amount >= 0) | Discount applied | Promotion effectiveness, pricing strategy |
| tax_amount | NUMERIC(10,2) | CHECK (tax_amount >= 0) | Tax amount | Tax reporting, compliance |
| transaction_date | DATE | NOT NULL | Date of transaction | Time series analysis, seasonal trends |
| payment_method | STRING | | Payment method used | Payment analysis, processing costs |
| status | STRING | NOT NULL | Transaction status | Order fulfillment, customer service |
| shipping_address | STRING | | Shipping address | Logistics analysis, delivery optimization |
| order_notes | STRING | | Order notes | Customer service, order details |
| created_timestamp | TIMESTAMP | NOT NULL, DEFAULT CURRENT_TIMESTAMP | ETL load timestamp | Data freshness tracking |

**Partitioning**: Daily partitioning on `transaction_date`  
**Clustering**: `transaction_date`, `customer_id` for time-based and customer analysis

**Business Rules**:
- `amount = (quantity × unit_price) - discount_amount + tax_amount`
- `transaction_date` must be between campaign `start_date` and `end_date` when `campaign_id` is not null
- All foreign key references must exist in respective dimension tables

## Data Quality Framework

### Quality Monitoring Implementation

**Primary Quality Checks**:

| Quality Dimension | Table | Threshold | Implementation | Business Impact |
|------------------|-------|-----------|----------------|-----------------|
| **Uniqueness** | All tables | 100% | Primary key validation | Data integrity, no duplicates |
| **Completeness** | dim_customers | 95%+ | Required field validation | Customer analysis accuracy |
| **Completeness** | dim_products | 95%+ | Required field validation | Product analysis accuracy |
| **Completeness** | fact_sales_transactions | 99%+ | Transaction completeness | Revenue accuracy |
| **Referential Integrity** | fact_sales_transactions | 99%+ | Foreign key validation | Analytical query reliability |
| **Business Logic** | fact_sales_transactions | 100% | Amount calculation validation | Financial accuracy |

### Data Validation Rules

**Cross-table Validation**:
- All `customer_id` in fact table must exist in `dim_customers`
- All `product_id` in fact table must exist in `dim_products`  
- All `campaign_id` in fact table must exist in `dim_campaigns`
- Transaction dates must fall within campaign date ranges when campaign is specified

**Business Logic Validation**:
- `quantity` and `unit_price` must be positive
- `amount` must equal calculated value: `(quantity × unit_price) - discount_amount + tax_amount`
- `discount_amount` cannot exceed `quantity × unit_price`
- Campaign `end_date` must be greater than or equal to `start_date`

## Performance Optimization Strategy

### BigQuery Optimization

**Partitioning Strategy**:
- **fact_sales_transactions**: Daily partitioning on `transaction_date` for time-based queries
- **dim_campaigns**: Monthly partitioning on `start_date` for campaign lifecycle analysis

**Clustering Strategy**:
- **dim_customers**: `country`, `registration_date` for geographic and cohort analysis
- **dim_products**: `category`, `price` for product performance analysis
- **dim_campaigns**: `channel`, `start_date` for campaign effectiveness analysis
- **fact_sales_transactions**: `transaction_date`, `customer_id` for customer journey analysis

**Query Optimization**:
- **Star Schema Joins**: Optimized for analytical query patterns
- **Materialized Views**: Pre-computed aggregations for common analytical queries
- **Clustering Benefits**: 70-90% query cost reduction for properly clustered queries

## Business Intelligence Capabilities

### Analytical Use Cases

**Customer Analytics**:
- **Customer Segmentation**: Demographics, behavior, and value-based segmentation
- **Customer Lifetime Value**: Predictive CLV modeling and high-value customer identification
- **Churn Analysis**: Customer retention patterns and churn prediction
- **Geographic Analysis**: Regional performance and market expansion opportunities

**Product Analytics**:
- **Product Performance**: Sales analysis by category, brand, and individual product
- **Inventory Optimization**: Stock level optimization and demand forecasting
- **Price Analysis**: Price elasticity and optimization opportunities
- **Cross-sell Analysis**: Product affinity and recommendation engine data

**Campaign Analytics**:
- **ROI Analysis**: Campaign return on investment and effectiveness measurement
- **Attribution Modeling**: Multi-touch attribution and customer journey analysis
- **Channel Performance**: Marketing channel effectiveness and budget optimization
- **Audience Analysis**: Target audience performance and optimization

**Financial Analytics**:
- **Revenue Analysis**: Revenue trends, seasonality, and forecasting
- **Profitability Analysis**: Margin analysis by product, customer, and campaign
- **Discount Effectiveness**: Promotion impact and pricing strategy optimization

### Key Performance Indicators (KPIs)

**Revenue Metrics**:
- Total revenue: `SUM(amount)`
- Average order value: `AVG(amount)`
- Revenue per customer: `SUM(amount) / COUNT(DISTINCT customer_id)`

**Customer Metrics**:
- Customer acquisition cost: Campaign spend / new customers
- Customer lifetime value: Total customer revenue over relationship duration
- Customer retention rate: Repeat customers / total customers

**Product Metrics**:
- Product profitability: `(unit_price - cost) × quantity`
- Inventory turnover: Sales volume / average inventory
- Category performance: Revenue by product category

**Campaign Metrics**:
- Return on ad spend (ROAS): Revenue / campaign spend
- Cost per acquisition: Campaign spend / conversions
- Conversion rate: Conversions / clicks

## Data Lineage and Governance

### Lineage Tracking

**Source-to-Target Mapping**:
- Complete field-level lineage from source systems to star schema
- Transformation logic documentation for all business rules
- Quality impact analysis integrated with lineage metadata
- Execution audit trail with timestamps and status tracking

**Metadata Management**:
- **Source Registration**: All 4 data sources with complete metadata
- **Transformation Documentation**: Business rule documentation for 24+ transformations
- **Relationship Mapping**: Foreign key and business relationship documentation
- **Execution Tracking**: Real-time pipeline execution monitoring

### Governance Framework

**Data Stewardship**:
- **Data Quality**: Automated monitoring with threshold-based alerting
- **Data Security**: Role-based access control and encryption
- **Data Privacy**: PII handling and data retention policies
- **Compliance**: Audit trail and regulatory reporting capabilities

**Change Management**:
- **Schema Evolution**: Procedures for adding new fields and tables
- **Backward Compatibility**: Maintaining compatibility with existing analytical queries
- **Version Control**: Git-based version control for all transformation logic
- **Documentation Updates**: Automated documentation generation from metadata
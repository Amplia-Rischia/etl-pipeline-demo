-- Data Lineage Metadata Schema
-- Purpose: Create tables to store comprehensive data lineage information

-- Table 1: Data Sources Registry
CREATE OR REPLACE TABLE `data-demo-etl.data_warehouse.lineage_sources` (
  source_id STRING NOT NULL,
  source_name STRING NOT NULL,
  source_type STRING NOT NULL, -- CSV, API, FIRESTORE
  source_location STRING, -- Bucket path, API endpoint, collection name
  description STRING,
  business_owner STRING,
  technical_owner STRING,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table 2: Data Targets Registry  
CREATE OR REPLACE TABLE `data-demo-etl.data_warehouse.lineage_targets` (
  target_id STRING NOT NULL,
  target_name STRING NOT NULL,
  target_type STRING NOT NULL, -- STAGING_TABLE, DIMENSION_TABLE, FACT_TABLE
  dataset_name STRING,
  table_name STRING,
  description STRING,
  business_purpose STRING,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table 3: Transformation Rules
CREATE OR REPLACE TABLE `data-demo-etl.data_warehouse.lineage_transformations` (
  transformation_id STRING NOT NULL,
  transformation_name STRING NOT NULL,
  transformation_type STRING NOT NULL, -- INGESTION, CLEANING, AGGREGATION, ENRICHMENT
  source_field STRING,
  target_field STRING,
  transformation_logic STRING, -- SQL snippet or description
  business_rule STRING,
  dag_id STRING,
  task_id STRING,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Table 4: Lineage Relationships (Source-to-Target Mappings)
CREATE OR REPLACE TABLE `data-demo-etl.data_warehouse.lineage_relationships` (
  relationship_id STRING NOT NULL,
  source_id STRING NOT NULL,
  target_id STRING NOT NULL,
  transformation_id STRING,
  relationship_type STRING, -- DIRECT_COPY, TRANSFORMED, AGGREGATED, FILTERED
  confidence_score FLOAT64, -- 0.0 to 1.0 indicating lineage accuracy
  pipeline_name STRING,
  dag_id STRING,
  task_id STRING,
  execution_date TIMESTAMP,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  is_active BOOLEAN DEFAULT TRUE
);

-- Table 5: Lineage Execution Log
CREATE OR REPLACE TABLE `data-demo-etl.data_warehouse.lineage_execution_log` (
  execution_id STRING NOT NULL,
  dag_id STRING NOT NULL,
  task_id STRING NOT NULL,
  run_id STRING NOT NULL,
  source_table STRING,
  target_table STRING,
  records_processed INT64,
  execution_status STRING, -- SUCCESS, FAILED, RUNNING
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  execution_duration_seconds INT64,
  error_message STRING,
  lineage_captured BOOLEAN DEFAULT FALSE,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Insert predefined source systems
INSERT INTO `data-demo-etl.data_warehouse.lineage_sources` 
(source_id, source_name, source_type, source_location, description, business_owner, technical_owner) VALUES
('SRC_CSV_CUSTOMERS', 'Customer CSV Files', 'CSV', 'gs://synthetic-data-csv-data-demo-etl/', 'Customer master data from CSV files', 'Business Team', 'Data Engineering'),
('SRC_API_PRODUCTS', 'Products REST API', 'API', 'https://europe-west1-data-demo-etl.cloudfunctions.net/products-api', 'Product catalog from REST API', 'Product Team', 'Data Engineering'),
('SRC_FIRESTORE_CAMPAIGNS', 'Marketing Campaigns', 'FIRESTORE', 'marketing_campaigns', 'Marketing campaign data from Firestore', 'Marketing Team', 'Data Engineering'),
('SRC_FIRESTORE_TRANSACTIONS', 'Sales Transactions', 'FIRESTORE', 'sales_transactions', 'Sales transaction data from Firestore', 'Sales Team', 'Data Engineering');

-- Insert predefined target systems
INSERT INTO `data-demo-etl.data_warehouse.lineage_targets`
(target_id, target_name, target_type, dataset_name, table_name, description, business_purpose) VALUES
('TGT_STAGING_CUSTOMERS', 'Staging Customers', 'STAGING_TABLE', 'staging_area', 'customers', 'Raw customer data for processing', 'Data ingestion staging'),
('TGT_STAGING_PRODUCTS', 'Staging Products', 'STAGING_TABLE', 'staging_area', 'products', 'Raw product data for processing', 'Data ingestion staging'),
('TGT_STAGING_CAMPAIGNS', 'Staging Campaigns', 'STAGING_TABLE', 'staging_area', 'marketing_campaigns', 'Raw campaign data for processing', 'Data ingestion staging'),
('TGT_STAGING_TRANSACTIONS', 'Staging Transactions', 'STAGING_TABLE', 'staging_area', 'sales_transactions', 'Raw transaction data for processing', 'Data ingestion staging'),
('TGT_DIM_CUSTOMERS', 'Customer Dimension', 'DIMENSION_TABLE', 'data_warehouse', 'dim_customers', 'Clean customer dimension for analytics', 'Customer analytics and segmentation'),
('TGT_DIM_PRODUCTS', 'Product Dimension', 'DIMENSION_TABLE', 'data_warehouse', 'dim_products', 'Clean product dimension for analytics', 'Product performance analysis'),
('TGT_DIM_CAMPAIGNS', 'Campaign Dimension', 'DIMENSION_TABLE', 'data_warehouse', 'dim_campaigns', 'Clean campaign dimension for analytics', 'Campaign effectiveness analysis'),
('TGT_FACT_TRANSACTIONS', 'Sales Fact Table', 'FACT_TABLE', 'data_warehouse', 'fact_sales_transactions', 'Transaction facts for business intelligence', 'Sales reporting and analytics');
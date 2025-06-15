-- Data Lineage: Source-to-Target Mapping Population
-- Purpose: Populate lineage relationships and transformations for our ETL pipeline

-- Insert Ingestion Transformations (Phase 3: Sources → Staging)
INSERT INTO `data-demo-etl.data_warehouse.lineage_transformations`
(transformation_id, transformation_name, transformation_type, source_field, target_field, transformation_logic, business_rule, dag_id, task_id) VALUES

-- Customer CSV Ingestion Transformations
('TRANS_CUST_001', 'Customer Data Ingestion', 'INGESTION', 'customer_id', 'customer_id', 'DIRECT_COPY', 'Primary key preserved during ingestion', 'csv_ingestion_pipeline', 'load_to_bigquery'),
('TRANS_CUST_002', 'Customer Email Ingestion', 'INGESTION', 'email', 'email', 'DIRECT_COPY', 'Email preserved for customer identification', 'csv_ingestion_pipeline', 'load_to_bigquery'),
('TRANS_CUST_003', 'Customer Metadata Addition', 'ENRICHMENT', 'NULL', 'ingestion_timestamp', 'CURRENT_TIMESTAMP()', 'Add ingestion timestamp for audit trail', 'csv_ingestion_pipeline', 'validate_transform'),

-- Product API Ingestion Transformations  
('TRANS_PROD_001', 'Product Data Ingestion', 'INGESTION', 'product_id', 'product_id', 'DIRECT_COPY', 'Primary key preserved during ingestion', 'api_ingestion_pipeline', 'load_to_bigquery'),
('TRANS_PROD_002', 'Product Price Validation', 'CLEANING', 'price', 'price', 'CASE WHEN price > 0 THEN price ELSE 0 END', 'Ensure non-negative prices', 'api_ingestion_pipeline', 'validate_transform_api'),
('TRANS_PROD_003', 'Product API Metadata', 'ENRICHMENT', 'NULL', 'api_source', 'CONSTANT_VALUE', 'Track API source for data provenance', 'api_ingestion_pipeline', 'validate_transform_api'),

-- Firestore Campaign Ingestion Transformations
('TRANS_CAMP_001', 'Campaign Data Ingestion', 'INGESTION', 'campaign_id', 'campaign_id', 'DIRECT_COPY', 'Primary key preserved during ingestion', 'firestore_ingestion_pipeline', 'load_campaigns_to_bigquery'),
('TRANS_CAMP_002', 'Campaign Timestamp Conversion', 'CLEANING', 'start_date', 'start_date', 'TIMESTAMP(start_date)', 'Convert Firestore timestamp to BigQuery timestamp', 'firestore_ingestion_pipeline', 'validate_transform_campaigns'),
('TRANS_CAMP_003', 'Campaign Source Tracking', 'ENRICHMENT', 'NULL', 'source_collection', 'CONSTANT_VALUE', 'Track Firestore collection source', 'firestore_ingestion_pipeline', 'validate_transform_campaigns'),

-- Firestore Transaction Ingestion Transformations
('TRANS_TRANS_001', 'Transaction Data Ingestion', 'INGESTION', 'transaction_id', 'transaction_id', 'DIRECT_COPY', 'Primary key preserved during ingestion', 'firestore_ingestion_pipeline', 'load_transactions_to_bigquery'),
('TRANS_TRANS_002', 'Transaction Amount Validation', 'CLEANING', 'amount', 'amount', 'CASE WHEN amount >= 0 THEN amount ELSE 0 END', 'Ensure non-negative transaction amounts', 'firestore_ingestion_pipeline', 'validate_transform_transactions'),
('TRANS_TRANS_003', 'Campaign ID Null Handling', 'CLEANING', 'campaign_id', 'campaign_id', 'COALESCE(campaign_id, "")', 'Handle null campaign references for BigQuery compatibility', 'firestore_ingestion_pipeline', 'validate_transform_transactions');

-- Insert Transformation Rules (Phase 4: Staging → Warehouse)
INSERT INTO `data-demo-etl.data_warehouse.lineage_transformations`
(transformation_id, transformation_name, transformation_type, source_field, target_field, transformation_logic, business_rule, dag_id, task_id) VALUES

-- Customer Dimension Transformations
('TRANS_DIM_CUST_001', 'Customer Name Standardization', 'CLEANING', 'first_name', 'first_name', 'TRIM(INITCAP(first_name))', 'Standardize name formatting for consistency', 'transformation_dag', 'transform_dim_customers'),
('TRANS_DIM_CUST_002', 'Customer Email Validation', 'CLEANING', 'email', 'email_valid', 'REGEXP_CONTAINS(email, email_pattern)', 'Validate email format for data quality', 'transformation_dag', 'transform_dim_customers'),
('TRANS_DIM_CUST_003', 'Customer Deduplication', 'CLEANING', 'customer_id', 'customer_id', 'ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY email_valid DESC)', 'Remove duplicate customers keeping best record', 'transformation_dag', 'transform_dim_customers'),

-- Product Dimension Transformations
('TRANS_DIM_PROD_001', 'Product Price Currency Conversion', 'CLEANING', 'price', 'price', 'CASE WHEN currency = "USD" THEN price * 0.85 ELSE price END', 'Standardize all prices to EUR', 'transformation_dag', 'transform_dim_products'),
('TRANS_DIM_PROD_002', 'Product Rating Validation', 'CLEANING', 'rating', 'rating', 'CASE WHEN rating BETWEEN 0 AND 5 THEN rating ELSE NULL END', 'Ensure valid rating range', 'transformation_dag', 'transform_dim_products'),
('TRANS_DIM_PROD_003', 'Product Category Standardization', 'CLEANING', 'category', 'category', 'TRIM(UPPER(category))', 'Standardize category formatting', 'transformation_dag', 'transform_dim_products'),

-- Campaign Dimension Transformations
('TRANS_DIM_CAMP_001', 'Campaign Budget Validation', 'CLEANING', 'budget', 'budget', 'CASE WHEN budget >= 0 THEN budget ELSE 0 END', 'Ensure non-negative budget values', 'transformation_dag', 'transform_dim_campaigns'),
('TRANS_DIM_CAMP_002', 'Campaign Duration Calculation', 'ENRICHMENT', 'start_date,end_date', 'campaign_duration_days', 'DATE_DIFF(end_date, start_date, DAY)', 'Calculate campaign duration for analysis', 'transformation_dag', 'transform_dim_campaigns'),
('TRANS_DIM_CAMP_003', 'Campaign Performance Calculation', 'ENRICHMENT', 'actual_spend,budget', 'spend_percentage', 'ROUND((actual_spend / budget) * 100, 2)', 'Calculate budget utilization percentage', 'transformation_dag', 'transform_dim_campaigns'),

-- Fact Table Transformations
('TRANS_FACT_001', 'Transaction Foreign Key Validation', 'CLEANING', 'customer_id,product_id,campaign_id', 'customer_exists,product_exists,campaign_exists', 'EXISTS() subqueries for FK validation', 'Ensure referential integrity', 'transformation_dag', 'transform_fact_transactions'),
('TRANS_FACT_002', 'Transaction Amount Calculation', 'CLEANING', 'quantity,unit_price,discount,tax', 'calculated_total_amount', '(quantity * unit_price) - discount + tax', 'Validate transaction amount calculations', 'transformation_dag', 'transform_fact_transactions'),
('TRANS_FACT_003', 'Transaction Quality Filtering', 'FILTERING', 'status', 'status', 'WHERE status IN ("COMPLETED", "PENDING")', 'Include only valid transactions for analytics', 'transformation_dag', 'transform_fact_transactions');

-- Insert Source-to-Target Relationships (Ingestion: Phase 3)
INSERT INTO `data-demo-etl.data_warehouse.lineage_relationships`
(relationship_id, source_id, target_id, transformation_id, relationship_type, confidence_score, pipeline_name, dag_id, task_id) VALUES

-- CSV to Staging Relationships
('REL_001', 'SRC_CSV_CUSTOMERS', 'TGT_STAGING_CUSTOMERS', 'TRANS_CUST_001', 'DIRECT_COPY', 1.0, 'CSV Ingestion Pipeline', 'csv_ingestion_pipeline', 'load_to_bigquery'),

-- API to Staging Relationships  
('REL_002', 'SRC_API_PRODUCTS', 'TGT_STAGING_PRODUCTS', 'TRANS_PROD_001', 'TRANSFORMED', 0.95, 'API Ingestion Pipeline', 'api_ingestion_pipeline', 'load_to_bigquery'),

-- Firestore to Staging Relationships
('REL_003', 'SRC_FIRESTORE_CAMPAIGNS', 'TGT_STAGING_CAMPAIGNS', 'TRANS_CAMP_001', 'TRANSFORMED', 0.95, 'Firestore Ingestion Pipeline', 'firestore_ingestion_pipeline', 'load_campaigns_to_bigquery'),
('REL_004', 'SRC_FIRESTORE_TRANSACTIONS', 'TGT_STAGING_TRANSACTIONS', 'TRANS_TRANS_001', 'TRANSFORMED', 0.90, 'Firestore Ingestion Pipeline', 'firestore_ingestion_pipeline', 'load_transactions_to_bigquery'),

-- Staging to Warehouse Relationships (Transformation: Phase 4)
('REL_005', 'TGT_STAGING_CUSTOMERS', 'TGT_DIM_CUSTOMERS', 'TRANS_DIM_CUST_003', 'TRANSFORMED', 0.98, 'Data Warehouse Transformation', 'transformation_dag', 'transform_dim_customers'),
('REL_006', 'TGT_STAGING_PRODUCTS', 'TGT_DIM_PRODUCTS', 'TRANS_DIM_PROD_003', 'TRANSFORMED', 0.95, 'Data Warehouse Transformation', 'transformation_dag', 'transform_dim_products'),
('REL_007', 'TGT_STAGING_CAMPAIGNS', 'TGT_DIM_CAMPAIGNS', 'TRANS_DIM_CAMP_003', 'TRANSFORMED', 0.95, 'Data Warehouse Transformation', 'transformation_dag', 'transform_dim_campaigns'),
('REL_008', 'TGT_STAGING_TRANSACTIONS', 'TGT_FACT_TRANSACTIONS', 'TRANS_FACT_003', 'FILTERED', 0.85, 'Data Warehouse Transformation', 'transformation_dag', 'transform_fact_transactions');
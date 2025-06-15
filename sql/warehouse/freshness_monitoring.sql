-- Data Quality: Data Freshness Monitoring
-- Purpose: Monitor data age and detect stale data

WITH freshness_analysis AS (
  SELECT 
    'dim_customers' as table_name,
    COUNT(*) as total_records,
    MAX(created_date) as latest_data_timestamp,
    MIN(created_date) as oldest_data_timestamp,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(MAX(created_date)), HOUR) as hours_since_latest,
    DATE_DIFF(DATE(MAX(created_date)), DATE(MIN(created_date)), DAY) as data_age_span_days,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_customers`
  
  UNION ALL
  
  SELECT 
    'dim_products' as table_name,
    COUNT(*) as total_records,
    MAX(created_date) as latest_data_timestamp,
    MIN(created_date) as oldest_data_timestamp,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(MAX(created_date)), HOUR) as hours_since_latest,
    DATE_DIFF(DATE(MAX(created_date)), DATE(MIN(created_date)), DAY) as data_age_span_days,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_products`
  
  UNION ALL
  
  SELECT 
    'dim_campaigns' as table_name,
    COUNT(*) as total_records,
    MAX(created_date) as latest_data_timestamp,
    MIN(created_date) as oldest_data_timestamp,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_date), HOUR) as hours_since_latest,
    TIMESTAMP_DIFF(MAX(created_date), MIN(created_date), DAY) as data_age_span_days,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_campaigns`
  
  UNION ALL
  
  SELECT 
    'fact_sales_transactions' as table_name,
    COUNT(*) as total_records,
    MAX(created_timestamp) as latest_data_timestamp,
    MIN(created_timestamp) as oldest_data_timestamp,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_timestamp), HOUR) as hours_since_latest,
    TIMESTAMP_DIFF(MAX(created_timestamp), MIN(created_timestamp), DAY) as data_age_span_days,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.fact_sales_transactions`
)

SELECT 
  table_name,
  total_records,
  latest_data_timestamp,
  hours_since_latest,
  data_age_span_days,
  check_timestamp,
  'FRESHNESS_CHECK' as check_type,
  CASE 
    WHEN hours_since_latest <= 24 THEN 'FRESH'
    WHEN hours_since_latest <= 48 THEN 'ACCEPTABLE'
    WHEN hours_since_latest <= 168 THEN 'AGING'
    ELSE 'STALE'
  END as freshness_status,
  CASE 
    WHEN hours_since_latest <= 24 THEN 'Data is fresh (< 24 hours)'
    WHEN hours_since_latest <= 48 THEN 'Data is acceptable (< 48 hours)'
    WHEN hours_since_latest <= 168 THEN 'Data is aging (< 1 week)'
    ELSE 'Data is stale (> 1 week)'
  END as status_description
FROM freshness_analysis
ORDER BY hours_since_latest DESC;
-- Data Quality: Volume Anomaly Detection
-- Purpose: Detect unusual data volumes and missing data

WITH expected_volumes AS (
  SELECT 'dim_customers' as table_name, 8000 as min_expected, 12000 as max_expected
  UNION ALL
  SELECT 'dim_products' as table_name, 5 as min_expected, 1000 as max_expected
  UNION ALL
  SELECT 'dim_campaigns' as table_name, 5 as min_expected, 2000 as max_expected
  UNION ALL
  SELECT 'fact_sales_transactions' as table_name, 1000 as min_expected, 100000 as max_expected
),

actual_volumes AS (
  SELECT 'dim_customers' as table_name, COUNT(*) as actual_count
  FROM `data-demo-etl.data_warehouse.dim_customers`
  
  UNION ALL
  
  SELECT 'dim_products' as table_name, COUNT(*) as actual_count
  FROM `data-demo-etl.data_warehouse.dim_products`
  
  UNION ALL
  
  SELECT 'dim_campaigns' as table_name, COUNT(*) as actual_count
  FROM `data-demo-etl.data_warehouse.dim_campaigns`
  
  UNION ALL
  
  SELECT 'fact_sales_transactions' as table_name, COUNT(*) as actual_count
  FROM `data-demo-etl.data_warehouse.fact_sales_transactions`
),

volume_analysis AS (
  SELECT 
    a.table_name,
    a.actual_count,
    e.min_expected,
    e.max_expected,
    ROUND((a.actual_count / e.max_expected) * 100, 2) as volume_percentage,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM actual_volumes a
  JOIN expected_volumes e ON a.table_name = e.table_name
)

SELECT 
  table_name,
  actual_count,
  min_expected,
  max_expected,
  volume_percentage,
  check_timestamp,
  'VOLUME_CHECK' as check_type,
  CASE 
    WHEN actual_count = 0 THEN 'CRITICAL'
    WHEN actual_count < min_expected THEN 'LOW'
    WHEN actual_count > max_expected THEN 'HIGH'
    ELSE 'NORMAL'
  END as volume_status,
  CASE 
    WHEN actual_count = 0 THEN 'No data found - critical issue'
    WHEN actual_count < min_expected THEN 'Volume below expected minimum'
    WHEN actual_count > max_expected THEN 'Volume exceeds expected maximum'
    ELSE 'Volume within expected range'
  END as status_description,
  CASE 
    WHEN actual_count = 0 THEN 'Check data ingestion pipeline'
    WHEN actual_count < min_expected THEN 'Investigate potential data loss'
    WHEN actual_count > max_expected THEN 'Check for data duplication'
    ELSE 'No action required'
  END as recommended_action
FROM volume_analysis
ORDER BY 
  CASE 
    WHEN actual_count = 0 THEN 1
    WHEN actual_count < min_expected THEN 2
    WHEN actual_count > max_expected THEN 3
    ELSE 4
  END,
  table_name;
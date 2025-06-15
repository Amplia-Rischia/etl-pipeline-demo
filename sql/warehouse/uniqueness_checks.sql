-- Data Quality: Uniqueness Checks
-- Purpose: Validate primary key uniqueness across all tables

-- Check 1: Customer ID Uniqueness
WITH customer_duplicates AS (
  SELECT 
    customer_id,
    COUNT(*) as duplicate_count,
    'dim_customers' as table_name,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_customers`
  GROUP BY customer_id
  HAVING COUNT(*) > 1
),

-- Check 2: Product ID Uniqueness  
product_duplicates AS (
  SELECT 
    product_id as entity_id,
    COUNT(*) as duplicate_count,
    'dim_products' as table_name,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_products`
  GROUP BY product_id
  HAVING COUNT(*) > 1
),

-- Check 3: Campaign ID Uniqueness
campaign_duplicates AS (
  SELECT 
    campaign_id as entity_id,
    COUNT(*) as duplicate_count,
    'dim_campaigns' as table_name,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_campaigns`
  GROUP BY campaign_id
  HAVING COUNT(*) > 1
),

-- Check 4: Transaction ID Uniqueness
transaction_duplicates AS (
  SELECT 
    transaction_id as entity_id,
    COUNT(*) as duplicate_count,
    'fact_sales_transactions' as table_name,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.fact_sales_transactions`
  GROUP BY transaction_id
  HAVING COUNT(*) > 1
),

-- Check 5: Email Uniqueness (Business Rule)
email_duplicates AS (
  SELECT 
    email as entity_id,
    COUNT(*) as duplicate_count,
    'dim_customers_email' as table_name,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_customers`
  WHERE email IS NOT NULL
  GROUP BY email
  HAVING COUNT(*) > 1
),

-- Combine all uniqueness violations
all_violations AS (
  SELECT customer_id as entity_id, duplicate_count, table_name, check_timestamp FROM customer_duplicates
  UNION ALL
  SELECT entity_id, duplicate_count, table_name, check_timestamp FROM product_duplicates
  UNION ALL
  SELECT entity_id, duplicate_count, table_name, check_timestamp FROM campaign_duplicates
  UNION ALL
  SELECT entity_id, duplicate_count, table_name, check_timestamp FROM transaction_duplicates
  UNION ALL
  SELECT entity_id, duplicate_count, table_name, check_timestamp FROM email_duplicates
)

-- Final Results
SELECT 
  table_name,
  COUNT(*) as violation_count,
  SUM(duplicate_count) as total_duplicates,
  check_timestamp,
  'UNIQUENESS_CHECK' as check_type,
  CASE 
    WHEN COUNT(*) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END as check_status
FROM all_violations
GROUP BY table_name, check_timestamp

UNION ALL

-- Summary row when no violations exist (fix the WHERE without FROM issue)
SELECT 
  'ALL_TABLES' as table_name,
  CAST(0 AS INT64) as violation_count,
  CAST(0 AS INT64) as total_duplicates,
  CURRENT_TIMESTAMP() as check_timestamp,
  'UNIQUENESS_CHECK' as check_type,
  'PASS' as check_status
FROM (SELECT 1 as dummy) -- Add dummy FROM clause
WHERE (SELECT COUNT(*) FROM all_violations) = 0

ORDER BY table_name;
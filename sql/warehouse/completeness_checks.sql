-- Data Quality: Completeness Checks
-- Purpose: Validate data completeness across critical fields

WITH customer_completeness AS (
  SELECT 
    'dim_customers' as table_name,
    COUNT(*) as total_records,
    
    -- Critical field completeness
    ROUND((COUNT(customer_id) / COUNT(*)) * 100, 2) as customer_id_completeness,
    ROUND((COUNT(first_name) / COUNT(*)) * 100, 2) as first_name_completeness,
    ROUND((COUNT(last_name) / COUNT(*)) * 100, 2) as last_name_completeness,
    ROUND((COUNT(email) / COUNT(*)) * 100, 2) as email_completeness,
    ROUND((COUNT(country) / COUNT(*)) * 100, 2) as country_completeness,
    
    -- Calculate overall completeness score
    ROUND((
      COUNT(customer_id) + COUNT(first_name) + COUNT(last_name) + 
      COUNT(email) + COUNT(country)
    ) / (COUNT(*) * 5) * 100, 2) as overall_completeness_pct,
    
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_customers`
),

product_completeness AS (
  SELECT 
    'dim_products' as table_name,
    COUNT(*) as total_records,
    
    -- Critical field completeness
    ROUND((COUNT(product_id) / COUNT(*)) * 100, 2) as product_id_completeness,
    ROUND((COUNT(name) / COUNT(*)) * 100, 2) as name_completeness,
    ROUND((COUNT(category) / COUNT(*)) * 100, 2) as category_completeness,
    ROUND((COUNT(price) / COUNT(*)) * 100, 2) as price_completeness,
    ROUND((COUNT(availability) / COUNT(*)) * 100, 2) as availability_completeness,
    
    -- Calculate overall completeness score
    ROUND((
      COUNT(product_id) + COUNT(name) + COUNT(category) + 
      COUNT(price) + COUNT(availability)
    ) / (COUNT(*) * 5) * 100, 2) as overall_completeness_pct,
    
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_products`
),

campaign_completeness AS (
  SELECT 
    'dim_campaigns' as table_name,
    COUNT(*) as total_records,
    
    -- Critical field completeness
    ROUND((COUNT(campaign_id) / COUNT(*)) * 100, 2) as campaign_id_completeness,
    ROUND((COUNT(name) / COUNT(*)) * 100, 2) as name_completeness,
    ROUND((COUNT(channel) / COUNT(*)) * 100, 2) as channel_completeness,
    ROUND((COUNT(budget) / COUNT(*)) * 100, 2) as budget_completeness,
    ROUND((COUNT(start_date) / COUNT(*)) * 100, 2) as start_date_completeness,
    
    -- Calculate overall completeness score
    ROUND((
      COUNT(campaign_id) + COUNT(name) + COUNT(channel) + 
      COUNT(budget) + COUNT(start_date)
    ) / (COUNT(*) * 5) * 100, 2) as overall_completeness_pct,
    
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_campaigns`
),

transaction_completeness AS (
  SELECT 
    'fact_sales_transactions' as table_name,
    COUNT(*) as total_records,
    
    -- Critical field completeness
    ROUND((COUNT(transaction_id) / COUNT(*)) * 100, 2) as transaction_id_completeness,
    ROUND((COUNT(customer_id) / COUNT(*)) * 100, 2) as customer_id_completeness,
    ROUND((COUNT(product_id) / COUNT(*)) * 100, 2) as product_id_completeness,
    ROUND((COUNT(amount) / COUNT(*)) * 100, 2) as amount_completeness,
    ROUND((COUNT(transaction_date) / COUNT(*)) * 100, 2) as transaction_date_completeness,
    
    -- Calculate overall completeness score
    ROUND((
      COUNT(transaction_id) + COUNT(customer_id) + COUNT(product_id) + 
      COUNT(amount) + COUNT(transaction_date)
    ) / (COUNT(*) * 5) * 100, 2) as overall_completeness_pct,
    
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.fact_sales_transactions`
),

-- Completeness standards (business rules)
completeness_results AS (
  SELECT 
    table_name,
    total_records,
    overall_completeness_pct,
    check_timestamp,
    'COMPLETENESS_CHECK' as check_type,
    CASE 
      WHEN overall_completeness_pct >= 95.0 THEN 'PASS'
      WHEN overall_completeness_pct >= 90.0 THEN 'WARNING'
      ELSE 'FAIL'
    END as check_status,
    CASE 
      WHEN overall_completeness_pct >= 95.0 THEN 'Excellent data completeness'
      WHEN overall_completeness_pct >= 90.0 THEN 'Acceptable with minor gaps'
      ELSE 'Critical completeness issues'
    END as status_description
  FROM (
    SELECT table_name, total_records, overall_completeness_pct, check_timestamp FROM customer_completeness
    UNION ALL
    SELECT table_name, total_records, overall_completeness_pct, check_timestamp FROM product_completeness
    UNION ALL
    SELECT table_name, total_records, overall_completeness_pct, check_timestamp FROM campaign_completeness
    UNION ALL
    SELECT table_name, total_records, overall_completeness_pct, check_timestamp FROM transaction_completeness
  )
)

SELECT * FROM completeness_results
ORDER BY overall_completeness_pct DESC;
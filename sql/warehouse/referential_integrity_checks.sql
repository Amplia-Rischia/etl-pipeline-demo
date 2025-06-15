-- Data Quality: Referential Integrity Checks
-- Purpose: Validate foreign key relationships in star schema

WITH fact_customer_integrity AS (
  SELECT 
    'fact_transactions_to_dim_customers' as relationship_name,
    COUNT(*) as total_fact_records,
    COUNT(c.customer_id) as valid_foreign_keys,
    COUNT(*) - COUNT(c.customer_id) as orphaned_records,
    ROUND((COUNT(c.customer_id) / COUNT(*)) * 100, 2) as integrity_percentage,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.fact_sales_transactions` f
  LEFT JOIN `data-demo-etl.data_warehouse.dim_customers` c 
    ON f.customer_id = c.customer_id
),

fact_product_integrity AS (
  SELECT 
    'fact_transactions_to_dim_products' as relationship_name,
    COUNT(*) as total_fact_records,
    COUNT(p.product_id) as valid_foreign_keys,
    COUNT(*) - COUNT(p.product_id) as orphaned_records,
    ROUND((COUNT(p.product_id) / COUNT(*)) * 100, 2) as integrity_percentage,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.fact_sales_transactions` f
  LEFT JOIN `data-demo-etl.data_warehouse.dim_products` p 
    ON f.product_id = p.product_id
),

fact_campaign_integrity AS (
  SELECT 
    'fact_transactions_to_dim_campaigns' as relationship_name,
    COUNT(*) as total_fact_records,
    COUNT(c.campaign_id) as valid_foreign_keys,
    COUNT(*) - COUNT(c.campaign_id) as orphaned_records,
    ROUND((COUNT(c.campaign_id) / COUNT(*)) * 100, 2) as integrity_percentage,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.fact_sales_transactions` f
  LEFT JOIN `data-demo-etl.data_warehouse.dim_campaigns` c 
    ON f.campaign_id = c.campaign_id
),

-- Check for orphaned dimension records (dimensions without fact references)
orphaned_customers AS (
  SELECT 
    'dim_customers_orphaned' as relationship_name,
    COUNT(*) as total_dimension_records,
    COUNT(*) - COUNT(f.customer_id) as orphaned_dimension_records,
    COUNT(f.customer_id) as referenced_records,
    ROUND((COUNT(f.customer_id) / COUNT(*)) * 100, 2) as utilization_percentage,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_customers` c
  LEFT JOIN `data-demo-etl.data_warehouse.fact_sales_transactions` f
    ON c.customer_id = f.customer_id
),

orphaned_products AS (
  SELECT 
    'dim_products_orphaned' as relationship_name,
    COUNT(*) as total_dimension_records,
    COUNT(*) - COUNT(f.product_id) as orphaned_dimension_records,
    COUNT(f.product_id) as referenced_records,
    ROUND((COUNT(f.product_id) / COUNT(*)) * 100, 2) as utilization_percentage,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_products` p
  LEFT JOIN `data-demo-etl.data_warehouse.fact_sales_transactions` f
    ON p.product_id = f.product_id
),

orphaned_campaigns AS (
  SELECT 
    'dim_campaigns_orphaned' as relationship_name,
    COUNT(*) as total_dimension_records,
    COUNT(*) - COUNT(f.campaign_id) as orphaned_dimension_records,
    COUNT(f.campaign_id) as referenced_records,
    ROUND((COUNT(f.campaign_id) / COUNT(*)) * 100, 2) as utilization_percentage,
    CURRENT_TIMESTAMP() as check_timestamp
  FROM `data-demo-etl.data_warehouse.dim_campaigns` c
  LEFT JOIN `data-demo-etl.data_warehouse.fact_sales_transactions` f
    ON c.campaign_id = f.campaign_id
),

-- Combine all integrity checks
integrity_results AS (
  -- Foreign key integrity (fact pointing to dimensions)
  SELECT 
    relationship_name,
    total_fact_records as total_records,
    orphaned_records,
    integrity_percentage as percentage,
    check_timestamp,
    'FOREIGN_KEY_INTEGRITY' as check_type,
    CASE 
      WHEN integrity_percentage = 100.0 THEN 'PASS'
      WHEN integrity_percentage >= 95.0 THEN 'WARNING'
      ELSE 'FAIL'
    END as check_status
  FROM (
    SELECT * FROM fact_customer_integrity
    UNION ALL
    SELECT * FROM fact_product_integrity
    UNION ALL
    SELECT * FROM fact_campaign_integrity
  )
  
  UNION ALL
  
  -- Dimension utilization (dimensions referenced by facts)
  SELECT 
    relationship_name,
    total_dimension_records as total_records,
    orphaned_dimension_records as orphaned_records,
    utilization_percentage as percentage,
    check_timestamp,
    'DIMENSION_UTILIZATION' as check_type,
    CASE 
      WHEN orphaned_dimension_records = 0 THEN 'OPTIMAL'
      WHEN utilization_percentage >= 50.0 THEN 'GOOD'
      WHEN utilization_percentage >= 25.0 THEN 'WARNING'
      ELSE 'POOR'
    END as check_status
  FROM (
    SELECT * FROM orphaned_customers
    UNION ALL
    SELECT * FROM orphaned_products
    UNION ALL
    SELECT * FROM orphaned_campaigns
  )
)

SELECT 
  relationship_name,
  total_records,
  orphaned_records,
  percentage,
  check_type,
  check_status,
  check_timestamp,
  CASE 
    WHEN check_type = 'FOREIGN_KEY_INTEGRITY' AND check_status = 'PASS' 
      THEN 'All foreign keys valid'
    WHEN check_type = 'FOREIGN_KEY_INTEGRITY' AND check_status = 'WARNING' 
      THEN 'Minor integrity violations detected'
    WHEN check_type = 'FOREIGN_KEY_INTEGRITY' AND check_status = 'FAIL' 
      THEN 'Critical integrity violations'
    WHEN check_type = 'DIMENSION_UTILIZATION' AND check_status = 'OPTIMAL' 
      THEN 'All dimensions fully utilized'
    WHEN check_type = 'DIMENSION_UTILIZATION' AND check_status = 'GOOD' 
      THEN 'Good dimension utilization'
    WHEN check_type = 'DIMENSION_UTILIZATION' AND check_status = 'WARNING' 
      THEN 'Some unused dimension records'
    ELSE 'Poor dimension utilization'
  END as status_description
FROM integrity_results
ORDER BY check_type, check_status;
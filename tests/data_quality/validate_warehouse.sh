# Task 1: Validate BigQuery Data Warehouse (30 minutes)
# Execute these commands in order to complete all validation steps

# ============================================================================
# Step 1.1: Verify Star Schema Integrity (8 minutes)
# ============================================================================

echo "=== Step 1.1: Verifying Star Schema Tables ==="

# Check all required tables exist
bq query --use_legacy_sql=false "
SELECT 
  table_name,
  table_type,
  row_count,
  size_bytes
FROM \`data-demo-etl.data_warehouse.INFORMATION_SCHEMA.TABLES\`
WHERE table_name IN ('dim_customers', 'dim_products', 'dim_campaigns', 'fact_sales_transactions')
ORDER BY table_name
"

# Get record counts for each table
bq query --use_legacy_sql=false "
SELECT 'dim_customers' as table_name, COUNT(*) as record_count FROM \`data-demo-etl.data_warehouse.dim_customers\`
UNION ALL
SELECT 'dim_products' as table_name, COUNT(*) as record_count FROM \`data-demo-etl.data_warehouse.dim_products\`
UNION ALL
SELECT 'dim_campaigns' as table_name, COUNT(*) as record_count FROM \`data-demo-etl.data_warehouse.dim_campaigns\`
UNION ALL
SELECT 'fact_sales_transactions' as table_name, COUNT(*) as record_count FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\`
ORDER BY table_name
"

# Check table schemas
bq show data-demo-etl:data_warehouse.dim_customers
bq show data-demo-etl:data_warehouse.dim_products
bq show data-demo-etl:data_warehouse.dim_campaigns
bq show data-demo-etl:data_warehouse.fact_sales_transactions

# ============================================================================
# Step 1.2: Test Referential Integrity (7 minutes)
# ============================================================================

echo "=== Step 1.2: Testing Referential Integrity ==="

# Check customer references
bq query --use_legacy_sql=false "
SELECT 
  COUNT(*) as total_transactions,
  SUM(CASE WHEN c.customer_id IS NOT NULL THEN 1 ELSE 0 END) as valid_customer_refs,
  ROUND(SUM(CASE WHEN c.customer_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as customer_integrity_pct
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\` f
LEFT JOIN \`data-demo-etl.data_warehouse.dim_customers\` c ON f.customer_id = c.customer_id
"

# Check product references
bq query --use_legacy_sql=false "
SELECT 
  COUNT(*) as total_transactions,
  SUM(CASE WHEN p.product_id IS NOT NULL THEN 1 ELSE 0 END) as valid_product_refs,
  ROUND(SUM(CASE WHEN p.product_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as product_integrity_pct
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\` f
LEFT JOIN \`data-demo-etl.data_warehouse.dim_products\` p ON f.product_id = p.product_id
"

# Check campaign references (including NO_CAMPAIGN)
bq query --use_legacy_sql=false "
SELECT 
  COUNT(*) as total_transactions,
  SUM(CASE WHEN c.campaign_id IS NOT NULL OR f.campaign_id = 'NO_CAMPAIGN' THEN 1 ELSE 0 END) as valid_campaign_refs,
  ROUND(SUM(CASE WHEN c.campaign_id IS NOT NULL OR f.campaign_id = 'NO_CAMPAIGN' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as campaign_integrity_pct,
  SUM(CASE WHEN f.campaign_id = 'NO_CAMPAIGN' THEN 1 ELSE 0 END) as no_campaign_count
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\` f
LEFT JOIN \`data-demo-etl.data_warehouse.dim_campaigns\` c ON f.campaign_id = c.campaign_id
"

# Find orphaned records
bq query --use_legacy_sql=false "
SELECT 'orphaned_customers' as issue, COUNT(*) as count
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\` f
LEFT JOIN \`data-demo-etl.data_warehouse.dim_customers\` c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT 'orphaned_products' as issue, COUNT(*) as count
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\` f
LEFT JOIN \`data-demo-etl.data_warehouse.dim_products\` p ON f.product_id = p.product_id
WHERE p.product_id IS NULL
UNION ALL
SELECT 'orphaned_campaigns' as issue, COUNT(*) as count
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\` f
LEFT JOIN \`data-demo-etl.data_warehouse.dim_campaigns\` c ON f.campaign_id = c.campaign_id
WHERE c.campaign_id IS NULL AND f.campaign_id != 'NO_CAMPAIGN'
"

# ============================================================================
# Step 1.3: Check Data Quality Standards (8 minutes)
# ============================================================================

echo "=== Step 1.3: Checking Data Quality Standards ==="

# Primary key uniqueness check
bq query --use_legacy_sql=false "
SELECT 
  'dim_customers' as table_name,
  COUNT(*) as total_records,
  COUNT(DISTINCT customer_id) as unique_ids,
  CASE WHEN COUNT(*) = COUNT(DISTINCT customer_id) THEN 'PASS' ELSE 'FAIL' END as uniqueness_check
FROM \`data-demo-etl.data_warehouse.dim_customers\`
UNION ALL
SELECT 
  'dim_products' as table_name,
  COUNT(*) as total_records,
  COUNT(DISTINCT product_id) as unique_ids,
  CASE WHEN COUNT(*) = COUNT(DISTINCT product_id) THEN 'PASS' ELSE 'FAIL' END as uniqueness_check
FROM \`data-demo-etl.data_warehouse.dim_products\`
UNION ALL
SELECT 
  'dim_campaigns' as table_name,
  COUNT(*) as total_records,
  COUNT(DISTINCT campaign_id) as unique_ids,
  CASE WHEN COUNT(*) = COUNT(DISTINCT campaign_id) THEN 'PASS' ELSE 'FAIL' END as uniqueness_check
FROM \`data-demo-etl.data_warehouse.dim_campaigns\`
UNION ALL
SELECT 
  'fact_sales_transactions' as table_name,
  COUNT(*) as total_records,
  COUNT(DISTINCT transaction_id) as unique_ids,
  CASE WHEN COUNT(*) = COUNT(DISTINCT transaction_id) THEN 'PASS' ELSE 'FAIL' END as uniqueness_check
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\`
"

# Completeness check for critical fields
bq query --use_legacy_sql=false "
SELECT 
  'dim_customers' as table_name,
  ROUND(SUM(CASE WHEN first_name IS NOT NULL AND first_name != '' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as first_name_completeness,
  ROUND(SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as email_completeness,
  ROUND(SUM(CASE WHEN country IS NOT NULL AND country != '' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as country_completeness
FROM \`data-demo-etl.data_warehouse.dim_customers\`
UNION ALL
SELECT 
  'dim_products' as table_name,
  ROUND(SUM(CASE WHEN name IS NOT NULL AND name != '' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as name_completeness,
  ROUND(SUM(CASE WHEN category IS NOT NULL AND category != '' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as category_completeness,
  ROUND(SUM(CASE WHEN price IS NOT NULL AND price > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as price_completeness
FROM \`data-demo-etl.data_warehouse.dim_products\`
"

# Check for NULL values in required business fields
bq query --use_legacy_sql=false "
SELECT 
  'fact_transactions_nulls' as check_type,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
  SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_product_ids,
  SUM(CASE WHEN amount IS NULL OR amount <= 0 THEN 1 ELSE 0 END) as null_or_zero_amounts,
  SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) as null_transaction_dates
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\`
"

# Date field validation
bq query --use_legacy_sql=false "
SELECT 
  'date_validation' as check_type,
  MIN(transaction_date) as earliest_transaction,
  MAX(transaction_date) as latest_transaction,
  COUNT(CASE WHEN transaction_date < '2020-01-01' THEN 1 END) as dates_before_2020,
  COUNT(CASE WHEN transaction_date > CURRENT_DATE() THEN 1 END) as future_dates
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\`
"

# ============================================================================
# Step 1.4: Test Key Analytical Joins (4 minutes)
# ============================================================================

echo "=== Step 1.4: Testing Analytical Joins ==="

# Test simple fact-to-dimension joins
echo "Testing customer analysis join..."
time bq query --use_legacy_sql=false "
SELECT 
  c.country,
  COUNT(*) as transaction_count,
  SUM(f.amount) as total_revenue,
  AVG(f.amount) as avg_transaction_value
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\` f
JOIN \`data-demo-etl.data_warehouse.dim_customers\` c ON f.customer_id = c.customer_id
GROUP BY c.country
ORDER BY total_revenue DESC
LIMIT 10
"

# Test multi-dimensional analysis
echo "Testing complex multi-dimensional join..."
time bq query --use_legacy_sql=false "
SELECT 
  p.category,
  c.country,
  cam.channel,
  COUNT(*) as transaction_count,
  SUM(f.amount) as total_revenue,
  AVG(f.amount) as avg_transaction_value
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\` f
JOIN \`data-demo-etl.data_warehouse.dim_customers\` c ON f.customer_id = c.customer_id
JOIN \`data-demo-etl.data_warehouse.dim_products\` p ON f.product_id = p.product_id
LEFT JOIN \`data-demo-etl.data_warehouse.dim_campaigns\` cam ON f.campaign_id = cam.campaign_id
WHERE f.transaction_date >= '2024-01-01'
GROUP BY p.category, c.country, cam.channel
HAVING COUNT(*) > 10
ORDER BY total_revenue DESC
LIMIT 20
"

# Test time-based analysis performance
echo "Testing time-series analysis..."
time bq query --use_legacy_sql=false "
SELECT 
  DATE_TRUNC(f.transaction_date, MONTH) as month,
  COUNT(*) as transaction_count,
  SUM(f.amount) as monthly_revenue,
  COUNT(DISTINCT f.customer_id) as unique_customers
FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\` f
WHERE f.transaction_date >= '2024-01-01'
GROUP BY month
ORDER BY month DESC
"

# ============================================================================
# Step 1.5: Document Validation Results (3 minutes)
# ============================================================================

echo "=== Step 1.5: Final Validation Summary ==="

# Generate comprehensive summary
bq query --use_legacy_sql=false "
SELECT 
  'VALIDATION_SUMMARY' as report_type,
  CURRENT_TIMESTAMP() as validation_timestamp,
  (SELECT COUNT(*) FROM \`data-demo-etl.data_warehouse.dim_customers\`) as customer_count,
  (SELECT COUNT(*) FROM \`data-demo-etl.data_warehouse.dim_products\`) as product_count,
  (SELECT COUNT(*) FROM \`data-demo-etl.data_warehouse.dim_campaigns\`) as campaign_count,
  (SELECT COUNT(*) FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\`) as transaction_count,
  (SELECT SUM(amount) FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\`) as total_revenue
"

echo ""
echo "=== Task 1 Complete ==="
echo "All validation commands executed. Review results above for data warehouse validation status."
echo "Expected completion time: 30 minutes"
echo ""
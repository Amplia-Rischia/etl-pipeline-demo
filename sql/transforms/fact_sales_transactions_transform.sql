-- Fact Sales Transactions Transformation
-- Source: staging_area.sales_transactions -> data_warehouse.fact_sales_transactions
-- Purpose: Clean, validate foreign keys, and calculate derived measures
-- UPDATED: Allow transactions without campaigns (using 'NO_CAMPAIGN' default)

CREATE OR REPLACE TABLE `data-demo-etl.data_warehouse.fact_sales_transactions`
PARTITION BY transaction_date
CLUSTER BY customer_id, product_id
AS
WITH validated_transactions AS (
  SELECT
    -- Primary Key
    t.transaction_id,
    
    -- Foreign Keys (validated against dimensions)
    t.customer_id,
    t.product_id,
    COALESCE(t.campaign_id, 'NO_CAMPAIGN') AS campaign_id,  -- Handle NULL campaigns
    
    -- Transaction Measures (validated and standardized)
    CASE 
      WHEN t.quantity IS NULL OR t.quantity <= 0 THEN 1
      ELSE t.quantity 
    END AS quantity,
    
    CASE 
      WHEN t.unit_price IS NULL OR t.unit_price < 0 THEN 0.00
      ELSE ROUND(t.unit_price, 2)
    END AS unit_price,
    
    CASE 
      WHEN t.amount IS NULL OR t.amount < 0 THEN 0.00
      ELSE ROUND(t.amount, 2)
    END AS amount,
    
    CASE 
      WHEN t.discount_amount IS NULL OR t.discount_amount < 0 THEN 0.00
      ELSE ROUND(t.discount_amount, 2)
    END AS discount_amount,
    
    CASE 
      WHEN t.tax_amount IS NULL OR t.tax_amount < 0 THEN 0.00
      ELSE ROUND(t.tax_amount, 2)
    END AS tax_amount,
    
    -- Transaction Details
    CASE 
      WHEN t.transaction_date IS NULL THEN DATE('1900-01-01')
      ELSE DATE(t.transaction_date)
    END AS transaction_date,
    
    TRIM(UPPER(COALESCE(t.payment_method, 'UNKNOWN'))) AS payment_method,
    TRIM(UPPER(COALESCE(t.status, 'PENDING'))) AS status,
    TRIM(COALESCE(t.shipping_address, 'Not Provided')) AS shipping_address,
    TRIM(COALESCE(t.order_notes, '')) AS order_notes,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_timestamp,
    
    -- Foreign Key Validation Flags
    CASE 
      WHEN EXISTS (SELECT 1 FROM `data-demo-etl.data_warehouse.dim_customers` c WHERE c.customer_id = t.customer_id)
      THEN TRUE ELSE FALSE 
    END AS customer_exists,
    
    CASE 
      WHEN EXISTS (SELECT 1 FROM `data-demo-etl.data_warehouse.dim_products` p WHERE p.product_id = t.product_id)
      THEN TRUE ELSE FALSE 
    END AS product_exists,
    
    CASE 
      WHEN t.campaign_id IS NULL OR t.campaign_id = '' 
      THEN TRUE  -- Allow transactions without campaigns
      WHEN EXISTS (SELECT 1 FROM `data-demo-etl.data_warehouse.dim_campaigns` c WHERE c.campaign_id = t.campaign_id)
      THEN TRUE ELSE FALSE 
    END AS campaign_exists,
    
    -- Deduplication ranking
    ROW_NUMBER() OVER (
      PARTITION BY t.transaction_id 
      ORDER BY 
        CASE WHEN t.status = 'COMPLETED' THEN 1 ELSE 2 END,
        t.transaction_date DESC
    ) AS row_rank
    
  FROM `data-demo-etl.staging_area.sales_transactions` t
  WHERE t.transaction_id IS NOT NULL
),

-- Calculate derived measures and validate business rules
calculated_transactions AS (
  SELECT
    transaction_id,
    customer_id,
    product_id,
    campaign_id,
    quantity,
    unit_price,
    amount,
    discount_amount,
    tax_amount,
    transaction_date,
    payment_method,
    status,
    shipping_address,
    order_notes,
    created_timestamp,
    customer_exists,
    product_exists,
    campaign_exists,
    
    -- Calculated measures
    ROUND(quantity * unit_price, 2) AS calculated_gross_amount,
    ROUND((quantity * unit_price) - discount_amount + tax_amount, 2) AS calculated_total_amount,
    ROUND(discount_amount / NULLIF(quantity * unit_price, 0) * 100, 2) AS discount_percentage,
    ROUND(tax_amount / NULLIF(amount - tax_amount, 0) * 100, 2) AS tax_rate,
    
    -- Data quality flags
    CASE 
      WHEN ABS(amount - ((quantity * unit_price) - discount_amount + tax_amount)) > 0.01 
      THEN FALSE ELSE TRUE 
    END AS amount_calculation_valid,
    
    CASE 
      WHEN status NOT IN ('PENDING', 'COMPLETED', 'CANCELLED', 'REFUNDED') 
      THEN FALSE ELSE TRUE 
    END AS status_valid,
    
    CASE 
      WHEN payment_method NOT IN ('CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER', 'CASH', 'UNKNOWN')
      THEN FALSE ELSE TRUE 
    END AS payment_method_valid
    
  FROM validated_transactions
  WHERE row_rank = 1  -- Keep only the best record per transaction_id
),

-- Final fact table with only valid transactions
final_transactions AS (
  SELECT
    transaction_id,
    customer_id,
    product_id,
    campaign_id,
    quantity,
    unit_price,
    amount,
    discount_amount,
    tax_amount,
    transaction_date,
    payment_method,
    status,
    shipping_address,
    order_notes,
    created_timestamp,
    calculated_gross_amount,
    calculated_total_amount,
    discount_percentage,
    tax_rate,
    amount_calculation_valid,
    status_valid,
    payment_method_valid
    
  FROM calculated_transactions
  WHERE customer_exists = TRUE  -- Only include transactions with valid foreign keys
    AND product_exists = TRUE
    -- REMOVED: AND campaign_exists = TRUE  -- Now allowing transactions without campaigns
    AND status IN ('COMPLETED', 'PENDING')  -- Exclude cancelled/refunded for analytics
    AND amount > 0  -- Exclude zero-value transactions
)

SELECT * FROM final_transactions;
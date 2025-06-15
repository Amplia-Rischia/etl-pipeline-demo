-- Dimension Product Transformation
-- Source: staging_area.products -> data_warehouse.dim_products
-- Purpose: Clean, standardize and deduplicate product data

DROP TABLE IF EXISTS `data-demo-etl.data_warehouse.dim_products`;

CREATE OR REPLACE TABLE `data-demo-etl.data_warehouse.dim_products`
CLUSTER BY category
AS
WITH cleaned_products AS (
  SELECT
    -- Primary Key
    product_id,
    
    -- Product Information (standardized)
    TRIM(name) AS name,
    TRIM(UPPER(category)) AS category,
    TRIM(UPPER(subcategory)) AS subcategory,
    TRIM(INITCAP(brand)) AS brand,
    
    -- Pricing (standardized to EUR)
    CASE 
      WHEN price IS NULL OR price <= 0 THEN NULL
      WHEN currency = 'USD' THEN ROUND(price * 0.85, 2) -- Simple USD to EUR conversion
      WHEN currency = 'GBP' THEN ROUND(price * 1.15, 2) -- Simple GBP to EUR conversion
      ELSE price -- Assume EUR if not specified
    END AS price,
    
    'EUR' AS currency, -- Standardize all prices to EUR
    
    -- Product Details
    TRIM(description) AS description,
    COALESCE(availability, FALSE) AS availability,
    
    -- Stock and Metrics (handle negatives and nulls)
    CASE 
      WHEN stock_quantity IS NULL OR stock_quantity < 0 THEN 0
      ELSE stock_quantity 
    END AS stock_quantity,
    
    CASE 
      WHEN rating IS NULL OR rating < 0 OR rating > 5 THEN NULL
      ELSE ROUND(rating, 2)
    END AS rating,
    
    CASE 
      WHEN reviews_count IS NULL OR reviews_count < 0 THEN 0
      ELSE reviews_count 
    END AS reviews_count,
    
    -- Physical Properties
    CASE 
      WHEN weight_kg IS NULL OR weight_kg <= 0 THEN NULL
      ELSE ROUND(weight_kg, 3)
    END AS weight_kg,
    
    TRIM(UPPER(country_origin)) AS country_origin,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_date,
    CURRENT_TIMESTAMP() AS last_updated_date,
    
    -- Data quality flags
    CASE 
      WHEN name IS NULL OR LENGTH(TRIM(name)) = 0 THEN FALSE
      ELSE TRUE 
    END AS name_valid,
    
    CASE 
      WHEN category IS NULL OR LENGTH(TRIM(category)) = 0 THEN FALSE
      ELSE TRUE 
    END AS category_valid,
    
    CASE 
      WHEN price IS NULL OR price <= 0 THEN FALSE
      ELSE TRUE 
    END AS price_valid,
    
    -- Deduplication ranking
    ROW_NUMBER() OVER (
      PARTITION BY product_id 
      ORDER BY 
        CASE WHEN name IS NOT NULL THEN 1 ELSE 2 END,
        CASE WHEN price IS NOT NULL AND price > 0 THEN 1 ELSE 2 END,
        CASE WHEN availability = TRUE THEN 1 ELSE 2 END
    ) AS row_rank
    
  FROM `data-demo-etl.staging_area.products`
  WHERE product_id IS NOT NULL
),

-- Handle missing values with business rules
final_products AS (
  SELECT
    product_id,
    
    -- Handle missing product information
    COALESCE(name, 'Unknown Product') AS name,
    COALESCE(category, 'UNCATEGORIZED') AS category,
    COALESCE(subcategory, 'GENERAL') AS subcategory,
    COALESCE(brand, 'Generic') AS brand,
    
    -- Pricing with business defaults
    COALESCE(price, 0.00) AS price,
    currency,
    
    -- Product details
    COALESCE(description, 'No description available') AS description,
    availability,
    stock_quantity,
    
    -- Metrics with defaults
    COALESCE(rating, 0.0) AS rating,
    reviews_count,
    weight_kg,
    COALESCE(country_origin, 'UNKNOWN') AS country_origin,
    
    created_date,
    last_updated_date,
    name_valid,
    category_valid,
    price_valid
    
  FROM cleaned_products
  WHERE row_rank = 1  -- Keep only the best record per product_id
)

SELECT * FROM final_products;
-- Dimension Customer Transformation
-- Source: staging_area.customers -> data_warehouse.dim_customers
-- Purpose: Clean, standardize and deduplicate customer data

DROP TABLE IF EXISTS `data-demo-etl.data_warehouse.dim_customers`;

CREATE OR REPLACE TABLE `data-demo-etl.data_warehouse.dim_customers`
CLUSTER BY country, registration_date
AS
WITH cleaned_customers AS (
  SELECT
    -- Primary Key
    customer_id,
    
    -- Personal Information (standardized)
    TRIM(INITCAP(first_name)) AS first_name,
    TRIM(INITCAP(last_name)) AS last_name,
    TRIM(LOWER(email)) AS email,
    
    -- Contact Information (standardized)
    REGEXP_REPLACE(TRIM(phone), r'[^\d+\-\s\(\)]', '') AS phone,
    TRIM(address) AS address,
    TRIM(INITCAP(city)) AS city,
    TRIM(UPPER(country)) AS country,
    
    -- Demographics
    CASE 
      WHEN age IS NULL OR age < 0 OR age > 120 THEN NULL
      ELSE age 
    END AS age,
    
    -- Dates (standardized)
    CASE 
      WHEN registration_date IS NULL THEN DATE('1900-01-01')
      ELSE DATE(registration_date)
    END AS registration_date,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_date,
    CURRENT_TIMESTAMP() AS last_updated_date,
    
    -- Data quality flags
    CASE 
      WHEN email IS NULL OR NOT REGEXP_CONTAINS(email, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') 
      THEN FALSE 
      ELSE TRUE 
    END AS email_valid,
    
    CASE 
      WHEN phone IS NULL OR LENGTH(TRIM(phone)) < 10 
      THEN FALSE 
      ELSE TRUE 
    END AS phone_valid,
    
    -- Deduplication ranking
    ROW_NUMBER() OVER (
      PARTITION BY customer_id 
      ORDER BY 
        CASE WHEN email IS NOT NULL THEN 1 ELSE 2 END,
        CASE WHEN phone IS NOT NULL THEN 1 ELSE 2 END,
        registration_date DESC
    ) AS row_rank
    
  FROM `data-demo-etl.staging_area.customers`
  WHERE customer_id IS NOT NULL
),

-- Handle missing values with business rules
final_customers AS (
  SELECT
    customer_id,
    
    -- Handle missing names
    COALESCE(first_name, 'Unknown') AS first_name,
    COALESCE(last_name, 'Unknown') AS last_name,
    
    -- Email is required for business logic
    email,
    
    -- Contact information
    COALESCE(phone, 'Not Provided') AS phone,
    COALESCE(address, 'Not Provided') AS address,
    COALESCE(city, 'Not Provided') AS city,
    COALESCE(country, 'UNKNOWN') AS country,
    
    -- Demographics with defaults
    COALESCE(age, 30) AS age, -- Default to 30 if missing
    
    registration_date,
    created_date,
    last_updated_date,
    email_valid,
    phone_valid
    
  FROM cleaned_customers
  WHERE row_rank = 1  -- Keep only the best record per customer_id
    AND email_valid = TRUE  -- Business rule: valid email required
)

SELECT * FROM final_customers;
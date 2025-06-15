-- Dimension Campaign Transformation
-- Source: staging_area.marketing_campaigns -> data_warehouse.dim_campaigns
-- Purpose: Clean, standardize and deduplicate campaign data

CREATE OR REPLACE TABLE `data-demo-etl.data_warehouse.dim_campaigns`
CLUSTER BY channel, start_date
AS
WITH cleaned_campaigns AS (
  SELECT
    -- Primary Key
    campaign_id,
    
    -- Campaign Information (standardized)
    TRIM(name) AS name,
    TRIM(UPPER(channel)) AS channel,
    TRIM(UPPER(status)) AS status,
    TRIM(target_audience) AS target_audience,
    
    -- Financial Data (standardized)
    CASE 
      WHEN budget IS NULL OR budget < 0 THEN 0.00
      ELSE ROUND(budget, 2)
    END AS budget,
    
    CASE 
      WHEN actual_spend IS NULL OR actual_spend < 0 THEN 0.00
      ELSE ROUND(actual_spend, 2)
    END AS actual_spend,
    
    -- Performance Metrics (validated)
    CASE 
      WHEN conversion_rate IS NULL OR conversion_rate < 0 OR conversion_rate > 1 THEN NULL
      ELSE ROUND(conversion_rate, 4)
    END AS conversion_rate,
    
    CASE 
      WHEN impressions IS NULL OR impressions < 0 THEN 0
      ELSE impressions 
    END AS impressions,
    
    CASE 
      WHEN clicks IS NULL OR clicks < 0 THEN 0
      ELSE clicks 
    END AS clicks,
    
    CASE 
      WHEN conversions IS NULL OR conversions < 0 THEN 0
      ELSE conversions 
    END AS conversions,
    
    -- Date Handling (standardized) - FIX: Ensure both sides are DATE type
    CASE 
      WHEN start_date IS NULL THEN DATE('1900-01-01')
      ELSE DATE(start_date)
    END AS start_date,
    
    CASE 
      WHEN end_date IS NULL THEN DATE('2099-12-31')
      WHEN DATE(end_date) < DATE(start_date) THEN DATE(start_date) -- Both sides are DATE now
      ELSE DATE(end_date)
    END AS end_date,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_date,
    CURRENT_TIMESTAMP() AS last_updated_date,
    
    -- Data quality flags
    CASE 
      WHEN name IS NULL OR LENGTH(TRIM(name)) = 0 THEN FALSE
      ELSE TRUE 
    END AS name_valid,
    
    CASE 
      WHEN channel IS NULL OR LENGTH(TRIM(channel)) = 0 THEN FALSE
      ELSE TRUE 
    END AS channel_valid,
    
    CASE 
      WHEN status NOT IN ('ACTIVE', 'PAUSED', 'COMPLETED', 'DRAFT') THEN FALSE
      ELSE TRUE 
    END AS status_valid,
    
    CASE 
      WHEN actual_spend > budget AND budget > 0 THEN TRUE
      ELSE FALSE 
    END AS over_budget,
    
    -- Calculated fields
    CASE 
      WHEN budget > 0 THEN ROUND((actual_spend / budget) * 100, 2)
      ELSE NULL 
    END AS spend_percentage,
    
    CASE 
      WHEN clicks > 0 THEN ROUND((conversions / clicks) * 100, 4)
      ELSE NULL 
    END AS calculated_conversion_rate,
    
    DATE_DIFF(
      CASE WHEN DATE(end_date) = DATE('2099-12-31') THEN CURRENT_DATE() ELSE DATE(end_date) END,
      DATE(start_date), 
      DAY
    ) AS campaign_duration_days,
    
    -- Deduplication ranking
    ROW_NUMBER() OVER (
      PARTITION BY campaign_id 
      ORDER BY 
        CASE WHEN name IS NOT NULL THEN 1 ELSE 2 END,
        CASE WHEN status IN ('ACTIVE', 'COMPLETED') THEN 1 ELSE 2 END,
        DATE(start_date) DESC
    ) AS row_rank
    
  FROM `data-demo-etl.staging_area.marketing_campaigns`
  WHERE campaign_id IS NOT NULL
),

-- Handle missing values with business rules
final_campaigns AS (
  SELECT
    campaign_id,
    
    -- Handle missing campaign information
    COALESCE(name, 'Unknown Campaign') AS name,
    COALESCE(channel, 'UNKNOWN') AS channel,
    COALESCE(status, 'DRAFT') AS status,
    COALESCE(target_audience, 'General Audience') AS target_audience,
    
    -- Financial data
    budget,
    actual_spend,
    
    -- Performance metrics
    conversion_rate,
    impressions,
    clicks,
    conversions,
    
    -- Dates
    start_date,
    end_date,
    
    created_date,
    last_updated_date,
    name_valid,
    channel_valid,
    status_valid,
    over_budget,
    spend_percentage,
    calculated_conversion_rate,
    campaign_duration_days
    
  FROM cleaned_campaigns
  WHERE row_rank = 1  -- Keep only the best record per campaign_id
)

SELECT * FROM final_campaigns;
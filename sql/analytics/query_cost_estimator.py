#!/usr/bin/env python3
"""
ETL Pipeline Demo - Query Cost Estimator
Estimates BigQuery costs for all demonstration queries before execution
"""

import subprocess
import json
import sys
from datetime import datetime

# Configuration
PROJECT_ID = "data-demo-etl"
BIGQUERY_PRICING_PER_TB = 6.25  # USD per TB processed (on-demand pricing)

# Demonstration Queries Dictionary
DEMO_QUERIES = {
    "customer_lifetime_value": """
    SELECT 
      c.customer_id,
      c.first_name,
      c.last_name,
      c.country,
      c.registration_date,
      COUNT(DISTINCT f.transaction_id) as total_transactions,
      SUM(f.amount) as total_revenue,
      AVG(f.amount) as avg_transaction_value,
      MAX(f.transaction_date) as last_purchase_date,
      DATE_DIFF(CURRENT_DATE(), MAX(f.transaction_date), DAY) as days_since_last_purchase,
      ROUND(SUM(f.amount) * COUNT(DISTINCT f.transaction_id) / 
        GREATEST(DATE_DIFF(CURRENT_DATE(), MAX(f.transaction_date), DAY), 1), 2) as clv_score
    FROM `data-demo-etl.data_warehouse.dim_customers` c
    JOIN `data-demo-etl.data_warehouse.fact_sales_transactions` f ON c.customer_id = f.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.country, c.registration_date
    HAVING total_revenue > 1000
    ORDER BY clv_score DESC
    LIMIT 20
    """,
    
    "customer_segmentation": """
    WITH customer_metrics AS (
      SELECT 
        c.customer_id,
        c.age,
        c.country,
        COUNT(DISTINCT f.transaction_id) as transaction_count,
        SUM(f.amount) as total_spent,
        AVG(f.amount) as avg_order_value,
        DATE_DIFF(CURRENT_DATE(), MAX(f.transaction_date), DAY) as recency_days
      FROM `data-demo-etl.data_warehouse.dim_customers` c
      JOIN `data-demo-etl.data_warehouse.fact_sales_transactions` f ON c.customer_id = f.customer_id
      GROUP BY c.customer_id, c.age, c.country
    ),
    customer_segments AS (
      SELECT 
        *,
        CASE 
          WHEN recency_days <= 30 AND transaction_count >= 5 AND total_spent >= 2000 THEN 'Champions'
          WHEN recency_days <= 60 AND transaction_count >= 3 AND total_spent >= 1000 THEN 'Loyal Customers'
          WHEN recency_days <= 90 AND transaction_count >= 2 THEN 'Potential Loyalists'
          WHEN recency_days <= 30 AND transaction_count < 3 THEN 'New Customers'
          WHEN recency_days > 90 AND total_spent >= 1000 THEN 'At Risk'
          WHEN recency_days > 180 THEN 'Cannot Lose Them'
          ELSE 'Others'
        END as customer_segment,
        CASE 
          WHEN age < 25 THEN '18-24'
          WHEN age < 35 THEN '25-34'
          WHEN age < 45 THEN '35-44'
          WHEN age < 55 THEN '45-54'
          ELSE '55+'
        END as age_group
      FROM customer_metrics
    )
    SELECT 
      customer_segment,
      age_group,
      country,
      COUNT(*) as customer_count,
      AVG(total_spent) as avg_total_spent,
      AVG(avg_order_value) as avg_order_value,
      AVG(recency_days) as avg_recency_days
    FROM customer_segments
    GROUP BY customer_segment, age_group, country
    ORDER BY customer_count DESC
    """,
    
    "geographic_performance": """
    SELECT 
      c.country,
      COUNT(DISTINCT c.customer_id) as total_customers,
      COUNT(DISTINCT f.transaction_id) as total_transactions,
      SUM(f.amount) as total_revenue,
      AVG(f.amount) as avg_transaction_value,
      SUM(f.amount) / COUNT(DISTINCT c.customer_id) as revenue_per_customer,
      COUNT(DISTINCT f.transaction_id) / COUNT(DISTINCT c.customer_id) as transactions_per_customer,
      COUNT(DISTINCT CASE WHEN f.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) 
        THEN c.customer_id END) as active_customers_90d,
      ROUND(COUNT(DISTINCT CASE WHEN f.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) 
        THEN c.customer_id END) / COUNT(DISTINCT c.customer_id) * 100, 2) as active_customer_rate
    FROM `data-demo-etl.data_warehouse.dim_customers` c
    JOIN `data-demo-etl.data_warehouse.fact_sales_transactions` f ON c.customer_id = f.customer_id
    GROUP BY c.country
    ORDER BY total_revenue DESC
    """,
    
    "campaign_roi": """
    WITH campaign_performance AS (
      SELECT 
        COALESCE(cam.name, 'No Campaign Attribution') as campaign_name,
        COALESCE(cam.channel, 'DIRECT') as channel,
        COALESCE(cam.budget, 0) as campaign_budget,
        COALESCE(cam.actual_spend, 0) as actual_spend,
        COUNT(DISTINCT f.transaction_id) as transactions,
        COUNT(DISTINCT f.customer_id) as unique_customers,
        SUM(f.amount) as total_revenue,
        AVG(f.amount) as avg_transaction_value,
        CASE 
          WHEN COALESCE(cam.actual_spend, 0) > 0 
          THEN ROUND((SUM(f.amount) - COALESCE(cam.actual_spend, 0)) / COALESCE(cam.actual_spend, 0) * 100, 2)
          ELSE NULL 
        END as roi_percentage,
        CASE 
          WHEN COUNT(DISTINCT f.customer_id) > 0 
          THEN ROUND(COALESCE(cam.actual_spend, 0) / COUNT(DISTINCT f.customer_id), 2)
          ELSE 0 
        END as cost_per_acquisition
      FROM `data-demo-etl.data_warehouse.fact_sales_transactions` f
      LEFT JOIN `data-demo-etl.data_warehouse.dim_campaigns` cam ON f.campaign_id = cam.campaign_id
      GROUP BY cam.name, cam.channel, cam.budget, cam.actual_spend
    )
    SELECT 
      campaign_name,
      channel,
      campaign_budget,
      actual_spend,
      transactions,
      unique_customers,
      total_revenue,
      avg_transaction_value,
      roi_percentage,
      cost_per_acquisition,
      CASE 
        WHEN roi_percentage IS NULL THEN 'Direct Sales'
        WHEN roi_percentage >= 200 THEN 'Excellent'
        WHEN roi_percentage >= 100 THEN 'Good'
        WHEN roi_percentage >= 50 THEN 'Average'
        WHEN roi_percentage >= 0 THEN 'Poor'
        ELSE 'Loss Making'
      END as performance_rating
    FROM campaign_performance
    ORDER BY COALESCE(roi_percentage, 999) DESC, total_revenue DESC
    """,
    
    "channel_performance": """
    SELECT 
      COALESCE(cam.channel, 'DIRECT') as channel,
      COUNT(DISTINCT f.transaction_id) as total_transactions,
      SUM(f.amount) as total_revenue,
      COUNT(DISTINCT f.customer_id) as unique_customers,
      AVG(f.amount) as avg_transaction_value,
      SUM(COALESCE(cam.actual_spend, 0)) as total_spend,
      ROUND(SUM(f.amount) / SUM(COALESCE(cam.actual_spend, 1)), 2) as revenue_per_dollar_spent,
      ROUND(COUNT(DISTINCT f.transaction_id) / SUM(COALESCE(cam.actual_spend, 1)), 4) as transactions_per_dollar_spent,
      ROUND(SUM(f.amount) / (SELECT SUM(amount) FROM `data-demo-etl.data_warehouse.fact_sales_transactions`) * 100, 2) as revenue_share_pct
    FROM `data-demo-etl.data_warehouse.fact_sales_transactions` f
    LEFT JOIN `data-demo-etl.data_warehouse.dim_campaigns` cam ON f.campaign_id = cam.campaign_id
    GROUP BY cam.channel
    ORDER BY total_revenue DESC
    """,
    
    "product_performance": """
    SELECT 
      p.category,
      p.brand,
      COUNT(DISTINCT f.transaction_id) as transaction_count,
      SUM(f.quantity) as total_quantity_sold,
      SUM(f.amount) as total_revenue,
      AVG(f.unit_price) as avg_selling_price,
      AVG(p.price) as catalog_price,
      ROUND(AVG(f.unit_price) / AVG(p.price) * 100, 2) as price_realization_pct,
      SUM(f.amount) / SUM(f.quantity) as revenue_per_unit,
      COUNT(DISTINCT f.customer_id) as unique_buyers,
      RANK() OVER (ORDER BY SUM(f.amount) DESC) as revenue_rank,
      RANK() OVER (ORDER BY SUM(f.quantity) DESC) as volume_rank
    FROM `data-demo-etl.data_warehouse.dim_products` p
    JOIN `data-demo-etl.data_warehouse.fact_sales_transactions` f ON p.product_id = f.product_id
    GROUP BY p.category, p.brand
    HAVING transaction_count >= 10
    ORDER BY total_revenue DESC
    LIMIT 25
    """,
    
    "inventory_analysis": """
    WITH product_performance AS (
      SELECT 
        p.product_id,
        p.name,
        p.category,
        p.stock_quantity,
        p.price as catalog_price,
        COUNT(DISTINCT f.transaction_id) as transaction_count,
        SUM(f.quantity) as total_sold,
        SUM(f.amount) as total_revenue,
        MAX(f.transaction_date) as last_sale_date,
        DATE_DIFF(CURRENT_DATE(), MAX(f.transaction_date), DAY) as days_since_last_sale
      FROM `data-demo-etl.data_warehouse.dim_products` p
      LEFT JOIN `data-demo-etl.data_warehouse.fact_sales_transactions` f ON p.product_id = f.product_id
      GROUP BY p.product_id, p.name, p.category, p.stock_quantity, p.price
    )
    SELECT 
      product_id,
      name,
      category,
      stock_quantity,
      catalog_price,
      COALESCE(total_sold, 0) as total_sold,
      COALESCE(total_revenue, 0) as total_revenue,
      days_since_last_sale,
      CASE 
        WHEN total_sold IS NULL THEN 'No Sales'
        WHEN days_since_last_sale <= 30 AND total_sold >= 10 THEN 'Fast Moving'
        WHEN days_since_last_sale <= 60 AND total_sold >= 5 THEN 'Regular Moving'
        WHEN days_since_last_sale <= 90 THEN 'Slow Moving'
        ELSE 'Dead Stock'
      END as inventory_category,
      CASE 
        WHEN stock_quantity = 0 THEN 'Out of Stock'
        WHEN stock_quantity <= 5 THEN 'Low Stock'
        WHEN stock_quantity <= 20 THEN 'Normal Stock'
        ELSE 'Overstocked'
      END as stock_status
    FROM product_performance
    ORDER BY 
      CASE inventory_category 
        WHEN 'Fast Moving' THEN 1 
        WHEN 'Regular Moving' THEN 2 
        WHEN 'Slow Moving' THEN 3 
        WHEN 'Dead Stock' THEN 4 
        ELSE 5 
      END,
      total_revenue DESC
    """,
    
    "seasonal_analysis": """
    WITH monthly_sales AS (
      SELECT 
        DATE_TRUNC(f.transaction_date, MONTH) as month,
        COUNT(DISTINCT f.transaction_id) as transactions,
        SUM(f.amount) as revenue,
        COUNT(DISTINCT f.customer_id) as active_customers,
        AVG(f.amount) as avg_transaction_value
      FROM `data-demo-etl.data_warehouse.fact_sales_transactions` f
      GROUP BY month
    ),
    sales_with_trends AS (
      SELECT 
        month,
        transactions,
        revenue,
        active_customers,
        avg_transaction_value,
        LAG(revenue) OVER (ORDER BY month) as prev_month_revenue,
        ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) / 
          LAG(revenue) OVER (ORDER BY month) * 100, 2) as mom_revenue_growth,
        AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as revenue_3month_avg
      FROM monthly_sales
    )
    SELECT 
      month,
      transactions,
      revenue,
      active_customers,
      avg_transaction_value,
      COALESCE(mom_revenue_growth, 0) as mom_revenue_growth_pct,
      ROUND(revenue_3month_avg, 2) as revenue_3month_avg,
      CASE 
        WHEN mom_revenue_growth >= 20 THEN 'High Growth'
        WHEN mom_revenue_growth >= 5 THEN 'Moderate Growth'
        WHEN mom_revenue_growth >= -5 THEN 'Stable'
        WHEN mom_revenue_growth >= -20 THEN 'Moderate Decline'
        ELSE 'Significant Decline'
      END as growth_category
    FROM sales_with_trends
    ORDER BY month
    """,
    
    "executive_summary": """
    WITH business_metrics AS (
      SELECT 
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        SUM(f.amount) as total_revenue,
        COUNT(DISTINCT f.customer_id) as total_customers,
        COUNT(DISTINCT p.product_id) as products_sold,
        COUNT(DISTINCT CASE WHEN cam.campaign_id != 'NO_CAMPAIGN' THEN cam.campaign_id END) as active_campaigns,
        AVG(f.amount) as avg_transaction_value,
        COUNT(DISTINCT CASE WHEN f.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
          THEN f.transaction_id END) as transactions_last_30d,
        SUM(CASE WHEN f.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
          THEN f.amount ELSE 0 END) as revenue_last_30d
      FROM `data-demo-etl.data_warehouse.fact_sales_transactions` f
      JOIN `data-demo-etl.data_warehouse.dim_customers` c ON f.customer_id = c.customer_id
      JOIN `data-demo-etl.data_warehouse.dim_products` p ON f.product_id = p.product_id
      LEFT JOIN `data-demo-etl.data_warehouse.dim_campaigns` cam ON f.campaign_id = cam.campaign_id
    )
    SELECT 
      'Business Performance Summary' as metric_category,
      total_transactions,
      ROUND(total_revenue, 2) as total_revenue,
      total_customers,
      products_sold,
      active_campaigns,
      ROUND(avg_transaction_value, 2) as avg_transaction_value,
      ROUND(total_revenue / total_customers, 2) as revenue_per_customer,
      transactions_last_30d,
      ROUND(revenue_last_30d, 2) as revenue_last_30d,
      ROUND(revenue_last_30d / NULLIF(transactions_last_30d, 0), 2) as avg_transaction_value_30d
    FROM business_metrics
    """,
    
    "attribution_matrix": """
    SELECT 
      c.country,
      p.category,
      COALESCE(cam.channel, 'DIRECT') as channel,
      COUNT(DISTINCT f.transaction_id) as transactions,
      SUM(f.amount) as revenue,
      COUNT(DISTINCT f.customer_id) as unique_customers,
      AVG(f.amount) as avg_transaction_value,
      SUM(COALESCE(cam.actual_spend, 0)) as marketing_spend,
      ROUND(SUM(f.amount) / COUNT(DISTINCT f.customer_id), 2) as revenue_per_customer,
      ROUND(COUNT(DISTINCT f.transaction_id) / COUNT(DISTINCT f.customer_id), 2) as transactions_per_customer,
      CASE 
        WHEN SUM(COALESCE(cam.actual_spend, 0)) > 0 
        THEN ROUND((SUM(f.amount) - SUM(COALESCE(cam.actual_spend, 0))) / SUM(COALESCE(cam.actual_spend, 0)) * 100, 2)
        ELSE NULL 
      END as roi_percentage
    FROM `data-demo-etl.data_warehouse.fact_sales_transactions` f
    JOIN `data-demo-etl.data_warehouse.dim_customers` c ON f.customer_id = c.customer_id
    JOIN `data-demo-etl.data_warehouse.dim_products` p ON f.product_id = p.product_id
    LEFT JOIN `data-demo-etl.data_warehouse.dim_campaigns` cam ON f.campaign_id = cam.campaign_id
    GROUP BY c.country, p.category, cam.channel
    HAVING transactions >= 5
    ORDER BY revenue DESC
    LIMIT 50
    """
}

def estimate_query_cost(query_name, query_sql):
    """Estimate the cost of a BigQuery query using dry run"""
    try:
        # Create temporary file for query
        with open(f"/tmp/query_{query_name}.sql", "w") as f:
            f.write(query_sql.strip())
        
        # Run dry run to get bytes processed
        cmd = [
            "bq", "query",
            "--use_legacy_sql=false",
            "--dry_run",
            "--format=json",
            query_sql.strip()
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            return None, f"Error: {result.stderr}"
        
        # Parse JSON output to get bytes processed
        if result.stdout.strip():
            try:
                # Extract bytes processed from bq output
                output_lines = result.stdout.strip().split('\n')
                for line in output_lines:
                    if "bytes processed" in line.lower():
                        # Extract number of bytes
                        bytes_str = line.split()[-2].replace(',', '')
                        bytes_processed = int(bytes_str)
                        break
                else:
                    # Try to parse as JSON
                    output_data = json.loads(result.stdout)
                    bytes_processed = int(output_data.get('totalBytesProcessed', 0))
            except (json.JSONDecodeError, ValueError, IndexError):
                # Fallback: run another command to get job info
                return None, "Could not parse bytes processed from output"
        else:
            bytes_processed = 0
        
        # Calculate cost (BigQuery charges $6.25 per TB)
        tb_processed = bytes_processed / (1024**4)  # Convert bytes to TB
        estimated_cost = tb_processed * BIGQUERY_PRICING_PER_TB
        
        return {
            'bytes_processed': bytes_processed,
            'tb_processed': round(tb_processed, 6),
            'estimated_cost_usd': round(estimated_cost, 4)
        }, None
        
    except Exception as e:
        return None, f"Exception: {str(e)}"

def main():
    print("=" * 60)
    print("ETL Pipeline Demo - Query Cost Estimator")
    print("=" * 60)
    print(f"Project: {PROJECT_ID}")
    print(f"Pricing: ${BIGQUERY_PRICING_PER_TB}/TB processed")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    total_cost = 0
    total_bytes = 0
    results = []
    errors = []
    
    print("Estimating costs for all demonstration queries...")
    print()
    
    for i, (query_name, query_sql) in enumerate(DEMO_QUERIES.items(), 1):
        print(f"[{i:2d}/11] Analyzing: {query_name}...", end=" ")
        
        cost_info, error = estimate_query_cost(query_name, query_sql)
        
        if error:
            print("❌ ERROR")
            errors.append(f"{query_name}: {error}")
        else:
            print("✅ OK")
            results.append({
                'name': query_name,
                'bytes': cost_info['bytes_processed'],
                'tb': cost_info['tb_processed'],
                'cost': cost_info['estimated_cost_usd']
            })
            total_cost += cost_info['estimated_cost_usd']
            total_bytes += cost_info['bytes_processed']
    
    print()
    print("=" * 60)
    print("COST ESTIMATION RESULTS")
    print("=" * 60)
    
    if results:
        print(f"{'Query Name':<25} {'Bytes Processed':<15} {'TB':<10} {'Cost (USD)':<12}")
        print("-" * 62)
        
        for result in sorted(results, key=lambda x: x['cost'], reverse=True):
            bytes_formatted = f"{result['bytes']:,}"
            print(f"{result['name']:<25} {bytes_formatted:<15} {result['tb']:<10.6f} ${result['cost']:<11.4f}")
        
        print("-" * 62)
        total_tb = total_bytes / (1024**4)
        print(f"{'TOTAL':<25} {total_bytes:,<15} {total_tb:<10.6f} ${total_cost:<11.4f}")
        
        print()
        print("💰 COST SUMMARY:")
        print(f"   Total estimated cost: ${total_cost:.4f} USD")
        print(f"   Total data processed: {total_bytes:,} bytes ({total_tb:.6f} TB)")
        print()
        
        # Cost categorization
        if total_cost < 0.01:
            print("💚 COST LEVEL: Very Low (< $0.01)")
        elif total_cost < 0.05:
            print("💙 COST LEVEL: Low (< $0.05)")
        elif total_cost < 0.25:
            print("💛 COST LEVEL: Moderate (< $0.25)")
        elif total_cost < 1.00:
            print("🧡 COST LEVEL: High (< $1.00)")
        else:
            print("❤️ COST LEVEL: Very High (≥ $1.00)")
        
        print()
        print("📊 QUERY RECOMMENDATIONS:")
        
        # High-value, low-cost queries
        high_value_queries = [
            'executive_summary', 'geographic_performance', 'campaign_roi', 
            'product_performance', 'customer_lifetime_value'
        ]
        
        print("   Essential queries (high business value):")
        for result in results:
            if result['name'] in high_value_queries:
                print(f"   ✅ {result['name']} (${result['cost']:.4f})")
        
        print()
        expensive_queries = [r for r in results if r['cost'] > total_cost * 0.2]
        if expensive_queries:
            print("   Expensive queries (>20% of total cost):")
            for result in expensive_queries:
                print(f"   💰 {result['name']} (${result['cost']:.4f})")
    
    if errors:
        print()
        print("❌ ERRORS ENCOUNTERED:")
        for error in errors:
            print(f"   {error}")
    
    print()
    print("=" * 60)
    print("Next steps:")
    print("1. Review the cost estimates above")
    print("2. Decide which queries to include in execution")
    print("3. Run the query execution script with selected queries")
    print("=" * 60)

if __name__ == "__main__":
    main()
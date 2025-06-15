"""
Data Warehouse Transformation DAG
Transforms staging data into star schema (dimensions + fact table)
SQL files stored in Composer environment bucket
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

# Configuration
PROJECT_ID = 'data-demo-etl'
STAGING_DATASET = 'staging_area'
WAREHOUSE_DATASET = 'data_warehouse'
LOCATION = 'europe-west1'
COMPOSER_BUCKET = 'europe-west1-etl-demo-env-2c8d8f3d-bucket'
SQL_FOLDER = 'sql/transforms'

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 13),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
}

def read_sql_from_gcs(sql_file_name):
    """Read SQL file from Composer environment bucket"""
    try:
        gcs_hook = GCSHook()
        sql_content = gcs_hook.download(
            bucket_name=COMPOSER_BUCKET,
            object_name=f"{SQL_FOLDER}/{sql_file_name}"
        )
        return sql_content.decode('utf-8')
    except Exception as e:
        logging.error(f"Failed to read SQL file {sql_file_name}: {e}")
        raise

# DAG Definition
dag = DAG(
    'transformation_dag',
    default_args=default_args,
    description='Transform staging data to star schema',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['transformation', 'star-schema', 'data-warehouse']
)

def log_transformation_start(**context):
    """Log transformation process start"""
    logging.info(f"Starting transformation process at {datetime.now()}")
    logging.info(f"Processing data from {STAGING_DATASET} to {WAREHOUSE_DATASET}")
    logging.info(f"Reading SQL from bucket: {COMPOSER_BUCKET}/{SQL_FOLDER}")
    return "transformation_started"

def log_transformation_complete(**context):
    """Log transformation process completion and validate record counts"""
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_ID)
    tables = ['dim_customers', 'dim_products', 'dim_campaigns', 'fact_sales_transactions']
    counts = {}
    
    for table in tables:
        query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{WAREHOUSE_DATASET}.{table}`"
        result = client.query(query).result()
        counts[table] = list(result)[0].count
    
    logging.info("Transformation completed successfully!")
    logging.info(f"Record counts: {counts}")
    
    total_records = sum(counts.values())
    if total_records == 0:
        raise ValueError("No records found in warehouse tables!")
    
    return counts

# Sensors to check staging data availability
check_customers_data = BigQueryTableExistenceSensor(
    task_id='check_customers_staging',
    project_id=PROJECT_ID,
    dataset_id=STAGING_DATASET,
    table_id='customers',
    dag=dag,
    timeout=300,
    poke_interval=30
)

check_products_data = BigQueryTableExistenceSensor(
    task_id='check_products_staging',
    project_id=PROJECT_ID,
    dataset_id=STAGING_DATASET,
    table_id='products',
    dag=dag,
    timeout=300,
    poke_interval=30
)

check_campaigns_data = BigQueryTableExistenceSensor(
    task_id='check_campaigns_staging',
    project_id=PROJECT_ID,
    dataset_id=STAGING_DATASET,
    table_id='marketing_campaigns',
    dag=dag,
    timeout=300,
    poke_interval=30
)

# Log start
log_start = PythonOperator(
    task_id='log_transformation_start',
    python_callable=log_transformation_start,
    dag=dag
)

# Dimension transformations (read SQL from GCS)
transform_customers = BigQueryInsertJobOperator(
    task_id='transform_dim_customers',
    configuration={
        "query": {
            "query": "{{ task_instance.xcom_pull(task_ids='read_customers_sql') }}",
            "useLegacySql": False,
        }
    },
    location=LOCATION,
    dag=dag,
    retries=3
)

transform_products = BigQueryInsertJobOperator(
    task_id='transform_dim_products',
    configuration={
        "query": {
            "query": "{{ task_instance.xcom_pull(task_ids='read_products_sql') }}",
            "useLegacySql": False,
        }
    },
    location=LOCATION,
    dag=dag,
    retries=3
)

transform_campaigns = BigQueryInsertJobOperator(
    task_id='transform_dim_campaigns',
    configuration={
        "query": {
            "query": "{{ task_instance.xcom_pull(task_ids='read_campaigns_sql') }}",
            "useLegacySql": False,
        }
    },
    location=LOCATION,
    dag=dag,
    retries=3
)

transform_transactions = BigQueryInsertJobOperator(
    task_id='transform_fact_transactions',
    configuration={
        "query": {
            "query": "{{ task_instance.xcom_pull(task_ids='read_transactions_sql') }}",
            "useLegacySql": False,
        }
    },
    location=LOCATION,
    dag=dag,
    retries=3,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

# SQL reading tasks
read_customers_sql = PythonOperator(
    task_id='read_customers_sql',
    python_callable=lambda: read_sql_from_gcs('dim_customers_transform.sql'),
    dag=dag
)

read_products_sql = PythonOperator(
    task_id='read_products_sql',
    python_callable=lambda: read_sql_from_gcs('dim_products_transform.sql'),
    dag=dag
)

read_campaigns_sql = PythonOperator(
    task_id='read_campaigns_sql',
    python_callable=lambda: read_sql_from_gcs('dim_campaigns_transform.sql'),
    dag=dag
)

read_transactions_sql = PythonOperator(
    task_id='read_transactions_sql',
    python_callable=lambda: read_sql_from_gcs('fact_sales_transactions_transform.sql'),
    dag=dag
)

# Log completion
log_complete = PythonOperator(
    task_id='log_transformation_complete',
    python_callable=log_transformation_complete,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

# Task Dependencies
# Check staging data availability
[check_customers_data, check_products_data, check_campaigns_data] >> log_start

# Read SQL files in parallel
log_start >> [read_customers_sql, read_products_sql, read_campaigns_sql, read_transactions_sql]

# Transform dimensions (depend on their respective SQL read tasks)
read_customers_sql >> transform_customers
read_products_sql >> transform_products
read_campaigns_sql >> transform_campaigns

# Transform fact table (depends on all dimensions and its SQL)
[transform_customers, transform_products, transform_campaigns, read_transactions_sql] >> transform_transactions

# Complete
transform_transactions >> log_complete
"""
Data Warehouse Transformation DAG - WITH LINEAGE TRACKING
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

# Import lineage tracking
from utils.lineage_tracking import LineageTracker, log_lineage_execution

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
    description='Transform staging data to star schema with lineage tracking',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['transformation', 'star-schema', 'data-warehouse', 'lineage']
)

@log_lineage_execution
def log_transformation_start(**context):
    """Log transformation process start with lineage tracking"""
    logging.info(f"Starting transformation process at {datetime.now()}")
    logging.info(f"Processing data from {STAGING_DATASET} to {WAREHOUSE_DATASET}")
    logging.info(f"Reading SQL from bucket: {COMPOSER_BUCKET}/{SQL_FOLDER}")
    
    # Initialize lineage tracking for transformation pipeline
    tracker = LineageTracker()
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    
    # Log pipeline start in lineage system
    execution_id = tracker.log_execution_start(
        dag_id=dag_id,
        task_id='transformation_pipeline',
        run_id=run_id,
        source_table=f"{STAGING_DATASET}.*",
        target_table=f"{WAREHOUSE_DATASET}.*"
    )
    
    return {"status": "transformation_started", "execution_id": execution_id}

@log_lineage_execution
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
    
    # Capture final lineage relationships for this execution
    tracker = LineageTracker()
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    
    # Log staging to warehouse relationships
    lineage_mappings = [
        ('TGT_STAGING_CUSTOMERS', 'TGT_DIM_CUSTOMERS', 'TRANS_DIM_CUST_003'),
        ('TGT_STAGING_PRODUCTS', 'TGT_DIM_PRODUCTS', 'TRANS_DIM_PROD_003'),
        ('TGT_STAGING_CAMPAIGNS', 'TGT_DIM_CAMPAIGNS', 'TRANS_DIM_CAMP_003'),
        ('TGT_STAGING_TRANSACTIONS', 'TGT_FACT_TRANSACTIONS', 'TRANS_FACT_003')
    ]
    
    for source_id, target_id, trans_id in lineage_mappings:
        tracker.capture_transformation_lineage(
            pipeline_name='Data Warehouse Transformation',
            dag_id=dag_id,
            task_id=task_id,
            source_id=source_id,
            target_id=target_id,
            transformation_id=trans_id,
            relationship_type='TRANSFORMED',
            confidence_score=0.98
        )
    
    return {"counts": counts, "total_records": total_records}

def capture_dimension_lineage(dimension_name: str, **context):
    """Capture lineage for dimension transformation"""
    tracker = LineageTracker()
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    
    # Map dimensions to their lineage IDs
    lineage_mapping = {
        'customers': ('TGT_STAGING_CUSTOMERS', 'TGT_DIM_CUSTOMERS', 'TRANS_DIM_CUST_003'),
        'products': ('TGT_STAGING_PRODUCTS', 'TGT_DIM_PRODUCTS', 'TRANS_DIM_PROD_003'),
        'campaigns': ('TGT_STAGING_CAMPAIGNS', 'TGT_DIM_CAMPAIGNS', 'TRANS_DIM_CAMP_003')
    }
    
    if dimension_name in lineage_mapping:
        source_id, target_id, trans_id = lineage_mapping[dimension_name]
        
        relationship_id = tracker.capture_transformation_lineage(
            pipeline_name='Data Warehouse Transformation',
            dag_id=dag_id,
            task_id=task_id,
            source_id=source_id,
            target_id=target_id,
            transformation_id=trans_id,
            relationship_type='TRANSFORMED',
            confidence_score=0.98
        )
        
        logging.info(f"Captured lineage for {dimension_name}: {relationship_id}")
    
    return f"lineage_captured_{dimension_name}"

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

# Log start with lineage
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

# Lineage capture tasks for each dimension
capture_customers_lineage = PythonOperator(
    task_id='capture_customers_lineage',
    python_callable=lambda **context: capture_dimension_lineage('customers', **context),
    dag=dag
)

capture_products_lineage = PythonOperator(
    task_id='capture_products_lineage',
    python_callable=lambda **context: capture_dimension_lineage('products', **context),
    dag=dag
)

capture_campaigns_lineage = PythonOperator(
    task_id='capture_campaigns_lineage',
    python_callable=lambda **context: capture_dimension_lineage('campaigns', **context),
    dag=dag
)

# Log completion with lineage
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
read_customers_sql >> transform_customers >> capture_customers_lineage
read_products_sql >> transform_products >> capture_products_lineage
read_campaigns_sql >> transform_campaigns >> capture_campaigns_lineage

# Transform fact table (depends on all dimensions and its SQL)
[capture_customers_lineage, capture_products_lineage, capture_campaigns_lineage, read_transactions_sql] >> transform_transactions

# Complete
transform_transactions >> log_complete
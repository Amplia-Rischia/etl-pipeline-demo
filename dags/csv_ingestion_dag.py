"""
CSV Ingestion DAG - WITH LINEAGE TRACKING
/dags/csv_ingestion_dag.py

Extract customer CSV from source bucket → Data Lake → BigQuery staging
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
import pandas as pd
from io import StringIO
import logging

from utils.datalake_utils import DataLakeManager, DataValidator
from config.validation_schemas import CUSTOMER_SCHEMA
from utils.lineage_tracking import LineageTracker, log_lineage_execution

# Configuration
SOURCE_BUCKET = "synthetic-data-csv-data-demo-etl"
DATALAKE_BUCKET = "etl-demo-datalake-data-demo-etl"
PROJECT_ID = "data-demo-etl"
DATASET_ID = "staging_area"
TABLE_ID = "customers"

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2025, 6, 12),
    'retries': 1,
    'execution_timeout': timedelta(minutes=30),
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'csv_ingestion_pipeline',
    default_args=default_args,
    description='Simple CSV ingestion pipeline with lineage tracking',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['csv', 'customers', 'lineage']
)

@log_lineage_execution
def extract_csv(**context):
    """Extract latest CSV from source bucket - WITH LINEAGE"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(SOURCE_BUCKET)
    
    # Find latest customer CSV
    blobs = list(bucket.list_blobs(prefix="synthetic_customers_"))
    latest_blob = max(blobs, key=lambda b: b.time_created)
    
    # Read CSV
    csv_content = latest_blob.download_as_text()
    df = pd.read_csv(StringIO(csv_content))
    
    logging.info(f"Extracted {len(df)} records from {latest_blob.name}")
    
    return {
        'data': df.to_json(orient='records'),
        'source_file': latest_blob.name,
        'record_count': len(df)  # This will be captured by lineage decorator
    }

@log_lineage_execution
def validate_transform(**context):
    """Validate and add metadata - WITH LINEAGE"""
    ti = context['ti']
    result = ti.xcom_pull(task_ids='extract_csv')
    
    df = pd.read_json(result['data'])
    
    # Validate schema
    validator = DataValidator()
    validation = validator.validate_csv_schema(df, CUSTOMER_SCHEMA)
    
    if not validation['valid']:
        raise Exception(f"Validation failed: {validation}")
    
    # Add metadata
    df['ingestion_timestamp'] = datetime.utcnow()
    df['ingestion_timestamp'] = df['ingestion_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    df['source_file'] = result['source_file']
    
    logging.info(f"Validated and transformed {len(df)} records")
    
    return {
        'data': df.to_json(orient='records'),
        'record_count': len(df)
    }

@log_lineage_execution
def load_to_datalake(**context):
    """Load to data lake raw layer - WITH LINEAGE"""
    ti = context['ti']
    data_result = ti.xcom_pull(task_ids='validate_transform')
    data = data_result['data'] if isinstance(data_result, dict) else data_result
    
    df = pd.read_json(data)
    
    # Upload to data lake
    datalake = DataLakeManager(DATALAKE_BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"customers_{timestamp}.csv"
    
    datalake.upload_csv(df, 'raw', filename)
    
    logging.info(f"Uploaded {len(df)} records to data lake")
    
    return {
        'filename': filename,
        'record_count': len(df)
    }

def capture_csv_lineage(**context):
    """Capture lineage for CSV ingestion pipeline"""
    tracker = LineageTracker()
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    
    relationship_id = tracker.capture_transformation_lineage(
        pipeline_name='CSV Ingestion Pipeline',
        dag_id=dag_id,
        task_id=task_id,
        source_id='SRC_CSV_CUSTOMERS',
        target_id='TGT_STAGING_CUSTOMERS',
        transformation_id='TRANS_CUST_001',
        relationship_type='DIRECT_COPY',
        confidence_score=1.0
    )
    
    logging.info(f"Captured CSV ingestion lineage: {relationship_id}")
    return f"lineage_captured_csv_{relationship_id}"

# Tasks
extract_task = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_transform',
    python_callable=validate_transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_datalake',
    python_callable=load_to_datalake,
    dag=dag
)

create_table = BigQueryCreateEmptyTableOperator(
    task_id='create_bq_table',
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    schema_fields=[
        {"name": "customer_id", "type": "STRING"},
        {"name": "first_name", "type": "STRING"},
        {"name": "last_name", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "country", "type": "STRING"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP"},
        {"name": "source_file", "type": "STRING"}
    ],
    exists_ok=True,
    dag=dag
)

load_bq = GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket=DATALAKE_BUCKET,
    source_objects=['raw/{{ ti.xcom_pull(task_ids="load_to_datalake")["filename"] if ti.xcom_pull(task_ids="load_to_datalake") is not none else ti.xcom_pull(task_ids="load_to_datalake") }}'],
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}',
    write_disposition='WRITE_APPEND',
    source_format='CSV',
    skip_leading_rows=1,
    dag=dag
)

# Lineage capture task
capture_lineage_task = PythonOperator(
    task_id='capture_csv_lineage',
    python_callable=capture_csv_lineage,
    dag=dag
)

# Flow WITH LINEAGE
extract_task >> validate_task >> load_task >> create_table >> load_bq >> capture_lineage_task
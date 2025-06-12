"""
CSV Ingestion DAG - Simplified for Demo
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
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'csv_ingestion_pipeline',
    default_args=default_args,
    description='Simple CSV ingestion pipeline',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['csv', 'customers']
)

def extract_csv(**context):
    """Extract latest CSV from source bucket"""
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
        'record_count': len(df)
    }

def validate_transform(**context):
    """Validate and add metadata"""
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
    df['source_file'] = result['source_file']
    
    logging.info(f"Validated and transformed {len(df)} records")
    
    return df.to_json(orient='records')

def load_to_datalake(**context):
    """Load to data lake raw layer"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='validate_transform')
    
    df = pd.read_json(data)
    
    # Upload to data lake
    datalake = DataLakeManager(DATALAKE_BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"customers_{timestamp}.csv"
    
    datalake.upload_csv(df, 'raw', filename)
    
    logging.info(f"Uploaded {len(df)} records to data lake")
    
    return filename

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
    source_objects=['raw/{{ ti.xcom_pull(task_ids="load_to_datalake") }}'],
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}',
    write_disposition='WRITE_APPEND',
    source_format='CSV',
    skip_leading_rows=1,
    dag=dag
)

# Flow
extract_task >> validate_task >> load_task >> create_table >> load_bq
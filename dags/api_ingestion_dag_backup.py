"""
API Ingestion DAG - Products API
/dags/api_ingestion_dag.py

Extract products from API → Data Lake → BigQuery staging
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import requests
import json
import pandas as pd
import logging
from io import StringIO

from utils.datalake_utils import DataLakeManager, DataValidator
from config.validation_schemas import PRODUCT_SCHEMA

# Configuration
API_ENDPOINT = "https://europe-west1-data-demo-etl.cloudfunctions.net/products-api"
DATALAKE_BUCKET = "etl-demo-datalake-data-demo-etl"
PROJECT_ID = "data-demo-etl"
DATASET_ID = "staging_area"
TABLE_ID = "products"

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2025, 6, 12),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'api_ingestion_pipeline',
    default_args=default_args,
    description='Ingest products from API to data lake and BigQuery',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    tags=['api', 'products']
)

def extract_api_data(**context):
    """Extract all products from API with pagination"""
    all_products = []
    page = 1
    page_size = 50
    
    while True:
        try:
            # API request with pagination
            params = {
                'page': page,
                'limit': page_size
            }
            
            response = requests.get(API_ENDPOINT, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle API response structure from documentation
            if data.get('status') != 'success':
                raise Exception(f"API returned error status: {data}")
            
            products = data.get('data', [])  # Products are in 'data' field
            
            if not products:
                break
                
            all_products.extend(products)
            logging.info(f"Retrieved {len(products)} products from page {page}")
            
            # Check pagination from response
            pagination = data.get('pagination', {})
            has_next = pagination.get('has_next', False)
            
            if not has_next:
                break
                
            page += 1
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {str(e)}")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON response: {str(e)}")
    
    logging.info(f"Total products extracted: {len(all_products)}")
    
    return {
        'products': all_products,
        'total_count': len(all_products),
        'extraction_timestamp': datetime.utcnow().isoformat()
    }

def validate_transform_api(**context):
    """Validate and transform API data"""
    ti = context['ti']
    api_data = ti.xcom_pull(task_ids='extract_api')
    
    products = api_data['products']
    
    # Validate schema
    validator = DataValidator()
    validation = validator.validate_json_structure(products, PRODUCT_SCHEMA)
    
    if not validation['valid']:
        raise Exception(f"API validation failed: {validation}")
    
    # Transform to DataFrame for consistency
    df = pd.DataFrame(products)
    
    # Add metadata
    df['ingestion_timestamp'] = datetime.utcnow()
    df['api_source'] = API_ENDPOINT
    df['extraction_timestamp'] = api_data['extraction_timestamp']
    
    logging.info(f"Validated and transformed {len(df)} products")
    
    return {
        'products_json': products,  # Keep original JSON
        'products_df': df.to_json(orient='records'),  # For BigQuery
        'validation_result': validation
    }

def load_api_to_datalake(**context):
    """Load API data to data lake raw layer"""
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='validate_transform_api')
    
    # Upload JSON to data lake
    datalake = DataLakeManager(DATALAKE_BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"products_{timestamp}.json"
    
    # Upload original JSON structure
    datalake.upload_json(transformed_data['products_json'], 'raw', filename)
    
    logging.info(f"Uploaded API data to data lake: {filename}")
    
    return {
        'filename': filename,
        'record_count': len(transformed_data['products_json'])
    }

def prepare_api_for_bigquery(**context):
    """Prepare flattened CSV for BigQuery load"""
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='validate_transform_api')
    
    # Convert to DataFrame and flatten
    df = pd.read_json(transformed_data['products_df'])
    
    # Create CSV for BigQuery
    datalake = DataLakeManager(DATALAKE_BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"products_flat_{timestamp}.csv"
    
    # Upload flattened CSV
    datalake.upload_csv(df, 'processed', csv_filename)
    
    logging.info(f"Created flattened CSV for BigQuery: {csv_filename}")
    
    return csv_filename

# Tasks
extract_task = PythonOperator(
    task_id='extract_api',
    python_callable=extract_api_data,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_transform_api',
    python_callable=validate_transform_api,
    dag=dag
)

load_datalake_task = PythonOperator(
    task_id='load_api_to_datalake',
    python_callable=load_api_to_datalake,
    dag=dag
)

prepare_bq_task = PythonOperator(
    task_id='prepare_for_bigquery',
    python_callable=prepare_api_for_bigquery,
    dag=dag
)

create_table = BigQueryCreateEmptyTableOperator(
    task_id='create_products_table',
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    schema_fields=[
        {"name": "product_id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "category", "type": "STRING"},
        {"name": "price", "type": "FLOAT"},
        {"name": "description", "type": "STRING"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP"},
        {"name": "api_source", "type": "STRING"},
        {"name": "extraction_timestamp", "type": "TIMESTAMP"}
    ],
    exists_ok=True,
    dag=dag
)

load_bq = GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket=DATALAKE_BUCKET,
    source_objects=['processed/{{ ti.xcom_pull(task_ids="prepare_for_bigquery") }}'],
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}',
    write_disposition='WRITE_TRUNCATE',  # Replace data for API
    source_format='CSV',
    skip_leading_rows=1,
    dag=dag
)

# Flow
extract_task >> validate_task >> load_datalake_task >> prepare_bq_task >> create_table >> load_bq
"""
Firestore Ingestion DAG - Updated with JSON Direct Loading
/dags/firestore_ingestion_dag.py

Extract campaigns and transactions from Firestore → Data Lake → BigQuery staging
Uses JSON-direct loading for transactions to avoid CSV conversion issues
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import firestore, bigquery, storage
import pandas as pd
import json
import logging

from utils.datalake_utils import DataLakeManager, DataValidator
from config.validation_schemas import CAMPAIGN_SCHEMA, TRANSACTION_SCHEMA

# Configuration
PROJECT_ID = "data-demo-etl"
DATALAKE_BUCKET = "etl-demo-datalake-data-demo-etl"
DATASET_ID = "staging_area"
CAMPAIGNS_TABLE = "marketing_campaigns"
TRANSACTIONS_TABLE = "sales_transactions"

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2025, 6, 12),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'firestore_ingestion_pipeline',
    default_args=default_args,
    description='Ingest Firestore collections using JSON-direct loading',
    schedule_interval=timedelta(hours=8),
    catchup=False,
    tags=['firestore', 'campaigns', 'transactions']
)

def extract_firestore_campaigns(**context):
    """Extract marketing campaigns from Firestore"""
    db = firestore.Client(project=PROJECT_ID)
    
    campaigns = []
    collection_ref = db.collection('marketing_campaigns')
    
    docs = collection_ref.stream()
    
    for doc in docs:
        campaign_data = doc.to_dict()
        campaign_data['document_id'] = doc.id
        campaigns.append(campaign_data)
    
    logging.info(f"Extracted {len(campaigns)} marketing campaigns")
    
    return {
        'campaigns': campaigns,
        'count': len(campaigns),
        'extraction_timestamp': datetime.utcnow().isoformat()
    }

def extract_firestore_transactions(**context):
    """Extract all sales transactions from Firestore with batching"""
    db = firestore.Client(project=PROJECT_ID)
    
    transactions = []
    collection_ref = db.collection('sales_transactions')
    
    # Handle large collection with batching
    batch_size = 1000
    last_doc = None
    batch_count = 0
    
    while True:
        query = collection_ref.order_by('transaction_date').limit(batch_size)
        
        if last_doc:
            query = query.start_after(last_doc)
        
        docs = list(query.stream())
        
        if not docs:
            break
            
        for doc in docs:
            transaction_data = doc.to_dict()
            transaction_data['document_id'] = doc.id
            transactions.append(transaction_data)
        
        last_doc = docs[-1]
        batch_count += 1
        logging.info(f"Processed batch {batch_count}: {len(docs)} transactions (total: {len(transactions)})")
        
        # Safety break
        if batch_count > 60:
            break
    
    logging.info(f"Extracted {len(transactions)} sales transactions")
    
    return {
        'transactions': transactions,
        'count': len(transactions),
        'extraction_timestamp': datetime.utcnow().isoformat()
    }

def validate_transform_campaigns(**context):
    """Validate and transform campaign data"""
    ti = context['ti']
    campaign_data = ti.xcom_pull(task_ids='extract_campaigns')
    
    campaigns = campaign_data['campaigns']
    
    # Validate schema
    validator = DataValidator()
    validation = validator.validate_json_structure(campaigns, CAMPAIGN_SCHEMA)
    
    if not validation['valid']:
        logging.warning(f"Campaign validation issues: {validation}")
    
    # Transform timestamps and add metadata
    for campaign in campaigns:
        # Convert Firestore timestamps to ISO strings
        for field in ['start_date', 'end_date', 'created_date', 'updated_date']:
            if field in campaign and hasattr(campaign[field], 'isoformat'):
                campaign[field] = campaign[field].isoformat()
        
        # Add ingestion metadata
        campaign['ingestion_timestamp'] = datetime.utcnow().isoformat()
        campaign['source_collection'] = 'marketing_campaigns'
    
    logging.info(f"Transformed {len(campaigns)} campaigns")
    
    return {
        'campaigns': campaigns,
        'validation_result': validation
    }

def validate_transform_transactions(**context):
    """Validate and transform transaction data"""
    ti = context['ti']
    transaction_data = ti.xcom_pull(task_ids='extract_transactions')
    
    transactions = transaction_data['transactions']
    
    # Validate schema
    validator = DataValidator()
    validation = validator.validate_json_structure(transactions, TRANSACTION_SCHEMA)
    
    if not validation['valid']:
        logging.warning(f"Transaction validation issues: {validation}")
    
    # Transform timestamps and add metadata
    for transaction in transactions:
        # Convert Firestore timestamps to ISO strings
        for field in ['transaction_date', 'created_date', 'updated_date']:
            if field in transaction and hasattr(transaction[field], 'isoformat'):
                transaction[field] = transaction[field].isoformat()
        
        # Handle null campaign_id for BigQuery compatibility
        if transaction.get('campaign_id') is None:
            transaction['campaign_id'] = ''
        
        # Add ingestion metadata
        transaction['ingestion_timestamp'] = datetime.utcnow().isoformat()
        transaction['source_collection'] = 'sales_transactions'
    
    logging.info(f"Transformed {len(transactions)} transactions")
    
    return {
        'transactions': transactions,
        'validation_result': validation
    }

def load_campaigns_to_datalake(**context):
    """Load campaigns to data lake"""
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='validate_transform_campaigns')
    
    campaigns = transformed_data['campaigns']
    
    # Upload to data lake
    datalake = DataLakeManager(DATALAKE_BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"campaigns_{timestamp}.json"
    
    datalake.upload_json(campaigns, 'raw', filename)
    
    logging.info(f"Uploaded {len(campaigns)} campaigns to data lake")
    
    return {
        'filename': filename,
        'record_count': len(campaigns)
    }

def load_transactions_to_datalake(**context):
    """Load transactions to data lake"""
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='validate_transform_transactions')
    
    transactions = transformed_data['transactions']
    
    # Upload to data lake
    datalake = DataLakeManager(DATALAKE_BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"transactions_{timestamp}.json"
    
    datalake.upload_json(transactions, 'raw', filename)
    
    logging.info(f"Uploaded {len(transactions)} transactions to data lake")
    
    return {
        'filename': filename,
        'record_count': len(transactions)
    }

def prepare_campaigns_for_bigquery(**context):
    """Convert campaigns to CSV for BigQuery (keeping existing approach)"""
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='validate_transform_campaigns')
    
    campaigns = transformed_data['campaigns']
    df = pd.DataFrame(campaigns)
    
    # Upload CSV for BigQuery
    datalake = DataLakeManager(DATALAKE_BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"campaigns_flat_{timestamp}.csv"
    
    datalake.upload_csv(df, 'processed', csv_filename)
    
    logging.info(f"Created campaigns CSV for BigQuery: {csv_filename}")
    
    return csv_filename

def prepare_transactions_for_bigquery(**context):
    """Convert transactions to newline-delimited JSON for BigQuery"""
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='validate_transform_transactions')
    
    transactions = transformed_data['transactions']
    
    # Convert to newline-delimited JSON
    newline_delimited_json = ''
    for transaction in transactions:
        newline_delimited_json += json.dumps(transaction) + '\n'
    
    # Upload to data lake processed layer
    storage_client = storage.Client()
    bucket = storage_client.bucket(DATALAKE_BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"transactions_newline_{timestamp}.json"
    
    blob = bucket.blob(f'processed/{filename}')
    blob.upload_from_string(newline_delimited_json, content_type='application/json')
    
    logging.info(f"Created transactions newline-delimited JSON: {filename}")
    
    return filename

# Task definitions
extract_campaigns_task = PythonOperator(
    task_id='extract_campaigns',
    python_callable=extract_firestore_campaigns,
    dag=dag
)

extract_transactions_task = PythonOperator(
    task_id='extract_transactions',
    python_callable=extract_firestore_transactions,
    dag=dag
)

validate_campaigns_task = PythonOperator(
    task_id='validate_transform_campaigns',
    python_callable=validate_transform_campaigns,
    dag=dag
)

validate_transactions_task = PythonOperator(
    task_id='validate_transform_transactions',
    python_callable=validate_transform_transactions,
    dag=dag
)

load_campaigns_datalake_task = PythonOperator(
    task_id='load_campaigns_to_datalake',
    python_callable=load_campaigns_to_datalake,
    dag=dag
)

load_transactions_datalake_task = PythonOperator(
    task_id='load_transactions_to_datalake',
    python_callable=load_transactions_to_datalake,
    dag=dag
)

prepare_campaigns_bq_task = PythonOperator(
    task_id='prepare_campaigns_for_bigquery',
    python_callable=prepare_campaigns_for_bigquery,
    dag=dag
)

prepare_transactions_bq_task = PythonOperator(
    task_id='prepare_transactions_for_bigquery',
    python_callable=prepare_transactions_for_bigquery,
    dag=dag
)

# BigQuery table creation
create_campaigns_table = BigQueryCreateEmptyTableOperator(
    task_id='create_campaigns_table',
    dataset_id=DATASET_ID,
    table_id=CAMPAIGNS_TABLE,
    schema_fields=[
        {"name": "campaign_id", "type": "STRING"},
        {"name": "conversions", "type": "INTEGER"},
        {"name": "impressions", "type": "INTEGER"},
        {"name": "end_date", "type": "TIMESTAMP"},
        {"name": "budget", "type": "FLOAT"},
        {"name": "status", "type": "STRING"},
        {"name": "channel", "type": "STRING"},
        {"name": "target_audience", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "created_date", "type": "TIMESTAMP"},
        {"name": "conversion_rate", "type": "FLOAT"},
        {"name": "updated_date", "type": "TIMESTAMP"},
        {"name": "clicks", "type": "INTEGER"},
        {"name": "actual_spend", "type": "FLOAT"},
        {"name": "start_date", "type": "TIMESTAMP"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP"},
        {"name": "source_collection", "type": "STRING"}
    ],
    exists_ok=True,
    dag=dag
)

create_transactions_table = BigQueryCreateEmptyTableOperator(
    task_id='create_transactions_table',
    dataset_id=DATASET_ID,
    table_id=TRANSACTIONS_TABLE,
    schema_fields=[
        {"name": "transaction_id", "type": "STRING"},
        {"name": "customer_id", "type": "STRING"},
        {"name": "product_id", "type": "STRING"},
        {"name": "campaign_id", "type": "STRING"},
        {"name": "amount", "type": "FLOAT"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "unit_price", "type": "FLOAT"},
        {"name": "discount_amount", "type": "FLOAT"},
        {"name": "tax_amount", "type": "FLOAT"},
        {"name": "transaction_date", "type": "TIMESTAMP"},
        {"name": "status", "type": "STRING"},
        {"name": "payment_method", "type": "STRING"},
        {"name": "shipping_address", "type": "STRING"},
        {"name": "order_notes", "type": "STRING"},
        {"name": "created_date", "type": "TIMESTAMP"},
        {"name": "updated_date", "type": "TIMESTAMP"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP"},
        {"name": "source_collection", "type": "STRING"}
    ],
    exists_ok=True,
    dag=dag
)

# BigQuery load tasks
load_campaigns_bq = GCSToBigQueryOperator(
    task_id='load_campaigns_to_bigquery',
    bucket=DATALAKE_BUCKET,
    source_objects=['processed/{{ ti.xcom_pull(task_ids="prepare_campaigns_for_bigquery") }}'],
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.{CAMPAIGNS_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    source_format='CSV',
    skip_leading_rows=1,
    dag=dag
)

load_transactions_bq = GCSToBigQueryOperator(
    task_id='load_transactions_to_bigquery',
    bucket=DATALAKE_BUCKET,
    source_objects=['processed/{{ ti.xcom_pull(task_ids="prepare_transactions_for_bigquery") }}'],
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.{TRANSACTIONS_TABLE}',
    write_disposition='WRITE_TRUNCATE',
    source_format='NEWLINE_DELIMITED_JSON',  # Changed from CSV to JSON
    dag=dag
)

# Task Dependencies
# Campaigns flow (unchanged - still uses CSV)
extract_campaigns_task >> validate_campaigns_task >> load_campaigns_datalake_task >> prepare_campaigns_bq_task >> create_campaigns_table >> load_campaigns_bq

# Transactions flow (updated - now uses JSON direct)
extract_transactions_task >> validate_transactions_task >> load_transactions_datalake_task >> prepare_transactions_bq_task >> create_transactions_table >> load_transactions_bq
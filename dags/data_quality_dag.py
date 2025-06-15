"""
Data Quality DAG
Comprehensive data quality checks for the data warehouse
SQL files stored in Composer environment bucket
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

# Configuration
PROJECT_ID = 'data-demo-etl'
WAREHOUSE_DATASET = 'data_warehouse'
QUALITY_DATASET = 'data_quality_results'
LOCATION = 'europe-west1'
COMPOSER_BUCKET = 'europe-west1-etl-demo-env-2c8d8f3d-bucket'
SQL_FOLDER = 'sql/warehouse'

# Default arguments
default_args = {
    'owner': 'data-quality-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 15),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=30),
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
    'data_quality_dag',
    default_args=default_args,
    description='Comprehensive data quality monitoring for data warehouse',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['data-quality', 'monitoring', 'warehouse']
)

def log_quality_start(**context):
    """Log data quality process start"""
    logging.info(f"Starting data quality checks at {datetime.now()}")
    logging.info(f"Checking data warehouse: {PROJECT_ID}.{WAREHOUSE_DATASET}")
    logging.info(f"Reading SQL from bucket: {COMPOSER_BUCKET}/{SQL_FOLDER}")
    return "quality_checks_started"

def log_quality_complete(**context):
    """Log data quality process completion and analyze results"""
    logging.info("Data quality checks completed successfully!")
    
    # Could add logic here to:
    # - Send alerts for failed checks
    # - Generate quality reports
    # - Update monitoring dashboards
    
    return "quality_checks_completed"

def validate_quality_results(**context):
    """Validate overall data quality and determine system health"""
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Check if quality results table exists and has recent results
    try:
        query = f"""
        SELECT 
            check_type,
            check_status,
            COUNT(*) as check_count
        FROM `{PROJECT_ID}.{QUALITY_DATASET}.quality_results` 
        WHERE DATE(check_timestamp) = CURRENT_DATE()
        GROUP BY check_type, check_status
        ORDER BY check_type, check_status
        """
        results = client.query(query).result()
        
        quality_summary = {}
        for row in results:
            if row.check_type not in quality_summary:
                quality_summary[row.check_type] = {}
            quality_summary[row.check_type][row.check_status] = row.check_count
        
        logging.info(f"Quality Summary: {quality_summary}")
        
        # Determine overall system health
        has_critical_issues = any(
            'FAIL' in checks or 'CRITICAL' in checks 
            for checks in quality_summary.values()
        )
        
        system_health = 'UNHEALTHY' if has_critical_issues else 'HEALTHY'
        logging.info(f"Overall System Health: {system_health}")
        
        return {
            'quality_summary': quality_summary,
            'system_health': system_health
        }
        
    except Exception as e:
        logging.warning(f"Could not validate quality results: {e}")
        return {'system_health': 'UNKNOWN'}

# Create quality results dataset and table
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

create_quality_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_quality_dataset',
    dataset_id=QUALITY_DATASET,
    location=LOCATION,
    exists_ok=True,
    dag=dag
)

create_quality_table = BigQueryCreateEmptyTableOperator(
    task_id='create_quality_table',
    dataset_id=QUALITY_DATASET,
    table_id='quality_results',
    schema_fields=[
        {"name": "table_name", "type": "STRING"},
        {"name": "check_type", "type": "STRING"},
        {"name": "check_status", "type": "STRING"},
        {"name": "check_timestamp", "type": "TIMESTAMP"},
        {"name": "metric_value", "type": "FLOAT"},
        {"name": "threshold_value", "type": "FLOAT"},
        {"name": "status_description", "type": "STRING"},
        {"name": "recommended_action", "type": "STRING"}
    ],
    exists_ok=True,
    dag=dag
)

# Log start
log_start = PythonOperator(
    task_id='log_quality_start',
    python_callable=log_quality_start,
    dag=dag
)

# SQL reading tasks
read_uniqueness_sql = PythonOperator(
    task_id='read_uniqueness_sql',
    python_callable=lambda: read_sql_from_gcs('uniqueness_checks.sql'),
    dag=dag
)

read_completeness_sql = PythonOperator(
    task_id='read_completeness_sql',
    python_callable=lambda: read_sql_from_gcs('completeness_checks.sql'),
    dag=dag
)

read_integrity_sql = PythonOperator(
    task_id='read_integrity_sql',
    python_callable=lambda: read_sql_from_gcs('referential_integrity_checks.sql'),
    dag=dag
)

read_freshness_sql = PythonOperator(
    task_id='read_freshness_sql',
    python_callable=lambda: read_sql_from_gcs('freshness_monitoring.sql'),
    dag=dag
)

read_volume_sql = PythonOperator(
    task_id='read_volume_sql',
    python_callable=lambda: read_sql_from_gcs('volume_anomaly_detection.sql'),
    dag=dag
)

# Data quality check tasks
run_uniqueness_checks = BigQueryInsertJobOperator(
    task_id='run_uniqueness_checks',
    configuration={
        "query": {
            "query": "{{ task_instance.xcom_pull(task_ids='read_uniqueness_sql') }}",
            "useLegacySql": False,
        }
    },
    location=LOCATION,
    dag=dag,
    retries=2
)

run_completeness_checks = BigQueryInsertJobOperator(
    task_id='run_completeness_checks',
    configuration={
        "query": {
            "query": "{{ task_instance.xcom_pull(task_ids='read_completeness_sql') }}",
            "useLegacySql": False,
        }
    },
    location=LOCATION,
    dag=dag,
    retries=2
)

run_integrity_checks = BigQueryInsertJobOperator(
    task_id='run_integrity_checks',
    configuration={
        "query": {
            "query": "{{ task_instance.xcom_pull(task_ids='read_integrity_sql') }}",
            "useLegacySql": False,
        }
    },
    location=LOCATION,
    dag=dag,
    retries=2
)

run_freshness_checks = BigQueryInsertJobOperator(
    task_id='run_freshness_checks',
    configuration={
        "query": {
            "query": "{{ task_instance.xcom_pull(task_ids='read_freshness_sql') }}",
            "useLegacySql": False,
        }
    },
    location=LOCATION,
    dag=dag,
    retries=2
)

run_volume_checks = BigQueryInsertJobOperator(
    task_id='run_volume_checks',
    configuration={
        "query": {
            "query": "{{ task_instance.xcom_pull(task_ids='read_volume_sql') }}",
            "useLegacySql": False,
        }
    },
    location=LOCATION,
    dag=dag,
    retries=2
)

# Validation and completion
validate_results = PythonOperator(
    task_id='validate_quality_results',
    python_callable=validate_quality_results,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE  # Run even if some checks fail
)

log_complete = PythonOperator(
    task_id='log_quality_complete',
    python_callable=log_quality_complete,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)

# Task Dependencies
# Setup
create_quality_dataset >> create_quality_table >> log_start

# Read SQL files in parallel
log_start >> [read_uniqueness_sql, read_completeness_sql, read_integrity_sql, read_freshness_sql, read_volume_sql]

# Run quality checks (depend on their respective SQL read tasks)
read_uniqueness_sql >> run_uniqueness_checks
read_completeness_sql >> run_completeness_checks
read_integrity_sql >> run_integrity_checks
read_freshness_sql >> run_freshness_checks
read_volume_sql >> run_volume_checks

# Validation and completion
[run_uniqueness_checks, run_completeness_checks, run_integrity_checks, run_freshness_checks, run_volume_checks] >> validate_results >> log_complete
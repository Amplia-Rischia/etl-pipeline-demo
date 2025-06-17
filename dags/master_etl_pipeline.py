"""
Master ETL Pipeline Orchestration DAG
Task 5.1: Create Master Orchestration DAG

Orchestrates the complete ETL pipeline with quasi-parallel ingestion,
sequential transformation, and basic quality gates.
Resource-optimized for small Composer environments.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import logging

# Configuration
PROJECT_ID = "data-demo-etl"
LOCATION = "europe-west1"

# Timeout configuration - 50 minutes per DAG
DAG_TIMEOUT = timedelta(minutes=60)
TASK_TIMEOUT = timedelta(minutes=50)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 15),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': DAG_TIMEOUT,  # 50-minute timeout per DAG
    'task_timeout': TASK_TIMEOUT       # 10-minute timeout per task
}

# Master DAG Definition
dag = DAG(
    'master_etl_pipeline',
    default_args=default_args,
    description='Master orchestration pipeline with quasi-parallel ingestion and quality gates',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['master', 'orchestration', 'etl-pipeline']
)

def validate_pipeline_prerequisites(**context):
    """Validate that all prerequisite conditions are met"""
    client = bigquery.Client(project=PROJECT_ID)
    
    # Check if datasets exist
    datasets = ['staging_area', 'data_warehouse', 'data_quality_results']
    for dataset in datasets:
        try:
            client.get_dataset(f"{PROJECT_ID}.{dataset}")
            logging.info(f"Dataset {dataset} exists ✅")
        except Exception as e:
            raise Exception(f"Dataset {dataset} not found: {e}")
    
    logging.info("All prerequisites validated successfully")
    return "prerequisites_validated"

def validate_ingestion_results(**context):
    """Lightweight data quality gate - check tables exist and log record counts"""
    client = bigquery.Client(project=PROJECT_ID)
    
    tables = ['customers', 'products', 'marketing_campaigns', 'sales_transactions']
    actual_counts = {}
    
    for table in tables:
        try:
            query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.staging_area.{table}`"
            result = list(client.query(query).result())[0]
            actual_counts[table] = result.count
            logging.info(f"Table {table}: {actual_counts[table]} records")
        except Exception as e:
            logging.error(f"Failed to query table {table}: {e}")
            raise Exception(f"Ingestion validation failed: Table {table} not accessible")
    
    total_actual = sum(actual_counts.values())
    logging.info(f"Ingestion completed successfully: {total_actual} total records across all tables")
    
    return {"status": "ingestion_validated", "counts": actual_counts}

def validate_transformation_results(**context):
    """Check transformation completed successfully"""
    client = bigquery.Client(project=PROJECT_ID)
    
    warehouse_tables = ['dim_customers', 'dim_products', 'dim_campaigns', 'fact_sales_transactions']
    counts = {}
    
    for table in warehouse_tables:
        try:
            query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.data_warehouse.{table}`"
            result = list(client.query(query).result())[0]
            counts[table] = result.count
            logging.info(f"Warehouse table {table}: {counts[table]} records")
        except Exception as e:
            logging.error(f"Failed to query warehouse table {table}: {e}")
            raise Exception(f"Transformation validation failed: Table {table} not accessible")
    
    total_warehouse = sum(counts.values())
    logging.info(f"Transformation completed successfully: {total_warehouse} total records in warehouse")
    
    return {"status": "transformation_validated", "counts": counts}

def log_pipeline_completion(**context):
    """Log successful pipeline completion with metrics"""
    ti = context['ti']
    
    # Get validation results
    ingestion_result = ti.xcom_pull(task_ids='validate_ingestion_quality_gate')
    transformation_result = ti.xcom_pull(task_ids='validate_transformation_quality_gate')
    
    completion_time = datetime.now()
    start_time = context['dag_run'].start_date
    duration = completion_time - start_time
    
    logging.info("=" * 50)
    logging.info("MASTER ETL PIPELINE COMPLETED SUCCESSFULLY")
    logging.info("=" * 50)
    logging.info(f"Execution Duration: {duration}")
    logging.info(f"Staging Records: {sum(ingestion_result['counts'].values()) if ingestion_result else 'N/A'}")
    logging.info(f"Warehouse Records: {sum(transformation_result['counts'].values()) if transformation_result else 'N/A'}")
    logging.info(f"Pipeline Status: SUCCESS ✅")
    logging.info("=" * 50)
    
    return {
        "status": "pipeline_completed",
        "duration_seconds": duration.total_seconds(),
        "completion_time": completion_time.isoformat()
    }

# Task Definitions

# Pipeline start
start_pipeline = DummyOperator(
    task_id='start_master_pipeline',
    dag=dag
)

# Prerequisites validation
validate_prerequisites = PythonOperator(
    task_id='validate_prerequisites',
    python_callable=validate_pipeline_prerequisites,
    dag=dag
)

# Sequential ingestion (true sequential execution)
trigger_csv_ingestion = TriggerDagRunOperator(
    task_id='trigger_csv_ingestion',
    trigger_dag_id='csv_ingestion_pipeline',
    wait_for_completion=True,
    execution_timeout=DAG_TIMEOUT,
    dag=dag
)

trigger_api_ingestion = TriggerDagRunOperator(
    task_id='trigger_api_ingestion',
    trigger_dag_id='api_ingestion_pipeline',
    wait_for_completion=True,
    execution_timeout=DAG_TIMEOUT,
    dag=dag
)

trigger_firestore_ingestion = TriggerDagRunOperator(
    task_id='trigger_firestore_ingestion',
    trigger_dag_id='firestore_ingestion_pipeline',
    wait_for_completion=True,
    execution_timeout=DAG_TIMEOUT,
    dag=dag
)

# Quality gate after ingestion
validate_ingestion_gate = PythonOperator(
    task_id='validate_ingestion_quality_gate',
    python_callable=validate_ingestion_results,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

# Sequential transformation
trigger_transformation = TriggerDagRunOperator(
    task_id='trigger_transformation',
    trigger_dag_id='transformation_dag',
    wait_for_completion=True,
    execution_timeout=DAG_TIMEOUT,
    dag=dag
)

# Quality gate after transformation
validate_transformation_gate = PythonOperator(
    task_id='validate_transformation_quality_gate',
    python_callable=validate_transformation_results,
    dag=dag
)

# Pipeline completion
complete_pipeline = PythonOperator(
    task_id='complete_master_pipeline',
    python_callable=log_pipeline_completion,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

# Failure handling task
handle_pipeline_failure = BashOperator(
    task_id='handle_pipeline_failure',
    bash_command='echo "Pipeline failed - check logs for details" && exit 1',
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED
)

# Task Dependencies - True Sequential Execution

# Start and prerequisites
start_pipeline >> validate_prerequisites

# True sequential ingestion (one after another)
validate_prerequisites >> trigger_csv_ingestion
trigger_csv_ingestion >> trigger_api_ingestion  
trigger_api_ingestion >> trigger_firestore_ingestion

# Quality gate after all ingestion
trigger_firestore_ingestion >> validate_ingestion_gate

# Transformation after quality gate passes
validate_ingestion_gate >> trigger_transformation

# Final quality gate and completion
trigger_transformation >> validate_transformation_gate >> complete_pipeline

# Failure handling (connects to all major tasks)
[trigger_csv_ingestion, trigger_api_ingestion, trigger_firestore_ingestion, 
 validate_ingestion_gate, trigger_transformation, validate_transformation_gate] >> handle_pipeline_failure
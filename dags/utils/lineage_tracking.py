# lineage_tracking.py
# Add this to dags/utils/ folder and import in DAGs

"""
Lineage Tracking Utilities
Functions to capture data lineage during pipeline execution
"""

from google.cloud import bigquery
import logging
from datetime import datetime
from typing import Dict, Any, Optional

class LineageTracker:
    def __init__(self, project_id: str = 'data-demo-etl'):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.lineage_dataset = 'data_warehouse'
    
    def log_execution_start(self, 
                           dag_id: str, 
                           task_id: str, 
                           run_id: str,
                           source_table: str = None,
                           target_table: str = None) -> str:
        """Log the start of a lineage-tracked execution"""
        
        execution_id = f"{dag_id}_{task_id}_{run_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        query = f"""
        INSERT INTO `{self.project_id}.{self.lineage_dataset}.lineage_execution_log`
        (execution_id, dag_id, task_id, run_id, source_table, target_table, 
         execution_status, start_time, lineage_captured)
        VALUES 
        ('{execution_id}', '{dag_id}', '{task_id}', '{run_id}', 
         '{source_table or ""}', '{target_table or ""}', 'RUNNING', 
         CURRENT_TIMESTAMP(), FALSE)
        """
        
        try:
            self.client.query(query).result()
            logging.info(f"Logged execution start: {execution_id}")
            return execution_id
        except Exception as e:
            logging.error(f"Failed to log execution start: {e}")
            return execution_id
    
    def log_execution_complete(self, 
                             execution_id: str,
                             records_processed: int = 0,
                             status: str = 'SUCCESS',
                             error_message: str = None):
        """Log the completion of a lineage-tracked execution"""
        
        query = f"""
        UPDATE `{self.project_id}.{self.lineage_dataset}.lineage_execution_log`
        SET 
            execution_status = '{status}',
            end_time = CURRENT_TIMESTAMP(),
            execution_duration_seconds = TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND),
            records_processed = {records_processed},
            error_message = '{error_message or ""}',
            lineage_captured = TRUE
        WHERE execution_id = '{execution_id}'
        """
        
        try:
            self.client.query(query).result()
            logging.info(f"Logged execution complete: {execution_id} - {status}")
        except Exception as e:
            logging.error(f"Failed to log execution complete: {e}")
    
    def capture_transformation_lineage(self,
                                     pipeline_name: str,
                                     dag_id: str,
                                     task_id: str,
                                     source_id: str,
                                     target_id: str,
                                     transformation_id: str,
                                     relationship_type: str = 'TRANSFORMED',
                                     confidence_score: float = 0.95):
        """Capture real-time lineage during transformation execution"""
        
        relationship_id = f"RT_{dag_id}_{task_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        query = f"""
        INSERT INTO `{self.project_id}.{self.lineage_dataset}.lineage_relationships`
        (relationship_id, source_id, target_id, transformation_id, relationship_type,
         confidence_score, pipeline_name, dag_id, task_id, execution_date, is_active)
        VALUES
        ('{relationship_id}', '{source_id}', '{target_id}', '{transformation_id}',
         '{relationship_type}', {confidence_score}, '{pipeline_name}', '{dag_id}',
         '{task_id}', CURRENT_TIMESTAMP(), TRUE)
        """
        
        try:
            self.client.query(query).result()
            logging.info(f"Captured transformation lineage: {relationship_id}")
            return relationship_id
        except Exception as e:
            logging.error(f"Failed to capture transformation lineage: {e}")
            return None

# Convenience functions for use in DAGs
def create_lineage_tracker():
    """Factory function to create lineage tracker"""
    return LineageTracker()

def log_lineage_execution(func):
    """Decorator to automatically log lineage execution"""
    def wrapper(*args, **kwargs):
        context = kwargs.get('context', {})
        dag_id = context.get('dag').dag_id if context.get('dag') else 'unknown'
        task_id = context.get('task').task_id if context.get('task') else 'unknown'
        run_id = context.get('run_id', 'unknown')
        
        tracker = create_lineage_tracker()
        execution_id = tracker.log_execution_start(dag_id, task_id, run_id)
        
        try:
            result = func(*args, **kwargs)
            
            # Try to extract record count from result
            records_processed = 0
            if isinstance(result, dict):
                records_processed = result.get('record_count', result.get('count', 0))
            elif isinstance(result, str) and 'count' in kwargs:
                records_processed = kwargs.get('count', 0)
            
            tracker.log_execution_complete(execution_id, records_processed, 'SUCCESS')
            return result
            
        except Exception as e:
            tracker.log_execution_complete(execution_id, 0, 'FAILED', str(e))
            raise
    
    return wrapper
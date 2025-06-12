"""
Simple Data Lake Utilities for Demo
/dags/utils/datalake_utils.py

Basic functionality for data lake operations
"""

import json
import logging
from datetime import datetime
from typing import Any
from google.cloud import storage
import pandas as pd

logger = logging.getLogger(__name__)


class DataLakeManager:
    """Simple data lake manager for demo"""
    
    def __init__(self, bucket_name: str = "etl-demo-datalake-data-demo-etl"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        
    def upload_csv(self, data: pd.DataFrame, layer: str, filename: str) -> str:
        """Upload CSV data to specified layer"""
        blob_path = f"{layer}/{filename}"
        blob = self.bucket.blob(blob_path)
        
        csv_data = data.to_csv(index=False)
        blob.upload_from_string(csv_data, content_type='text/csv')
        
        logger.info(f"Uploaded CSV: gs://{self.bucket_name}/{blob_path}")
        return f"gs://{self.bucket_name}/{blob_path}"
    
    def upload_json(self, data: Any, layer: str, filename: str) -> str:
        """Upload JSON data to specified layer"""
        blob_path = f"{layer}/{filename}"
        blob = self.bucket.blob(blob_path)
        
        json_data = json.dumps(data, default=str, indent=2)
        blob.upload_from_string(json_data, content_type='application/json')
        
        logger.info(f"Uploaded JSON: gs://{self.bucket_name}/{blob_path}")
        return f"gs://{self.bucket_name}/{blob_path}"
    
    def list_files(self, layer: str) -> list:
        """List files in a layer"""
        blobs = list(self.bucket.list_blobs(prefix=f"{layer}/"))
        return [blob.name for blob in blobs if not blob.name.endswith('.keep')]


class DataValidator:
    """Simple data validation for demo"""
    
    @staticmethod
    def validate_csv_schema(df: pd.DataFrame, expected_columns: list) -> dict:
        """Validate CSV has expected columns"""
        actual_columns = df.columns.tolist()
        missing = [col for col in expected_columns if col not in actual_columns]
        extra = [col for col in actual_columns if col not in expected_columns]
        
        return {
            "valid": len(missing) == 0,
            "missing_columns": missing,
            "extra_columns": extra,
            "record_count": len(df)
        }
    
    @staticmethod
    def validate_json_structure(data: list, required_keys: list) -> dict:
        """Validate JSON has required keys"""
        if not data:
            return {"valid": False, "error": "No data"}
        
        first_item = data[0] if isinstance(data, list) else data
        missing_keys = [key for key in required_keys if key not in first_item]
        
        return {
            "valid": len(missing_keys) == 0,
            "missing_keys": missing_keys,
            "record_count": len(data) if isinstance(data, list) else 1
        }


class DataMonitor:
    """Simple monitoring for data ingestion"""
    
    def __init__(self):
        self.storage_client = storage.Client()
    
    def log_ingestion_event(self, source: str, layer: str, status: str, 
                           record_count: int = 0, error_msg: str = None):
        """Log ingestion events"""
        timestamp = datetime.utcnow().isoformat()
        event = {
            "timestamp": timestamp,
            "source": source,
            "layer": layer,
            "status": status,
            "record_count": record_count,
            "error": error_msg
        }
        
        logger.info(f"Ingestion Event: {event}")
        return event
    
    def check_data_freshness(self, bucket_name: str, layer: str, 
                           max_age_hours: int = 24) -> dict:
        """Check if data is fresh enough"""
        bucket = self.storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=f"{layer}/"))
        
        if not blobs:
            return {"fresh": False, "reason": "No data found"}
        
        latest_blob = max(blobs, key=lambda b: b.time_created)
        age_hours = (datetime.utcnow().replace(tzinfo=None) - 
                    latest_blob.time_created.replace(tzinfo=None)).total_seconds() / 3600
        
        return {
            "fresh": age_hours <= max_age_hours,
            "age_hours": age_hours,
            "latest_file": latest_blob.name
        }
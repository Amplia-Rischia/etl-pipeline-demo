#!/bin/bash

# BigQuery Dataset Creation Script
# Creates the required datasets for ETL/ELT pipeline including external access

PROJECT_ID="data-demo-etl"
LOCATION="europe-west1"

echo "🔧 Creating BigQuery datasets for ETL/ELT pipeline..."
echo "Project: $PROJECT_ID"
echo "Location: $LOCATION"
echo ""

# Function to create dataset with error handling
create_dataset() {
    local dataset_name=$1
    local description=$2
    
    echo "Creating dataset: $dataset_name"
    
    bq mk \
        --dataset \
        --location=$LOCATION \
        --description="$description" \
        $PROJECT_ID:$dataset_name
    
    if [ $? -eq 0 ]; then
        echo "✅ Successfully created dataset: $dataset_name"
    else
        echo "⚠️  Dataset $dataset_name may already exist or there was an error"
    fi
    echo ""
}

# Create staging area dataset
create_dataset "staging_area" "Staging area for raw data ingestion from CSV, API, and Firestore sources"

# Create data warehouse dataset
create_dataset "data_warehouse" "Production data warehouse with star schema - dimensions and fact tables"

# Create metadata dataset
create_dataset "metadata" "ETL metadata, audit logs, data lineage, and processing statistics"

# Create external access dataset (NEW)
create_dataset "external_access" "External tool access views for Power BI, Tableau, and other BI tools"

echo "🔍 Verifying dataset creation..."
echo "Available datasets:"
bq ls --project_id=$PROJECT_ID

echo ""
echo "📊 Dataset details:"
bq show --project_id=$PROJECT_ID staging_area
echo ""
bq show --project_id=$PROJECT_ID data_warehouse
echo ""
bq show --project_id=$PROJECT_ID metadata
echo ""
bq show --project_id=$PROJECT_ID external_access

echo ""
echo "✅ BigQuery dataset setup completed!"
echo ""
echo "📋 Next steps:"
echo "1. Verify datasets are in europe-west1 location"
echo "2. Test permissions with sample query"
echo "3. Configure Airflow connections to these datasets"
echo "4. Set up data lake structure in GCS"

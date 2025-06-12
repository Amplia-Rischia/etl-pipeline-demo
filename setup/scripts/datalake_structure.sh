#!/bin/bash

# Data Lake Structure Setup Script
# Creates GCS bucket structure for raw data lake

PROJECT_ID="data-demo-etl"
BUCKET_NAME="data-demo-etl-data-lake"
LOCATION="europe-west1"

echo "🏞️  Setting up Data Lake structure in GCS..."
echo "Project: $PROJECT_ID"
echo "Bucket: $BUCKET_NAME"
echo "Location: $LOCATION"
echo ""

# Function to create bucket with error handling
create_bucket() {
    echo "Creating data lake bucket: $BUCKET_NAME"
    
    gsutil mb -p $PROJECT_ID -c STANDARD -l $LOCATION gs://$BUCKET_NAME/
    
    if [ $? -eq 0 ]; then
        echo "✅ Successfully created bucket: $BUCKET_NAME"
    else
        echo "⚠️  Bucket $BUCKET_NAME may already exist or there was an error"
    fi
    echo ""
}

# Function to create folder structure
create_folder_structure() {
    echo "Creating data lake folder structure..."
    
    # Create folder structure with placeholder files
    echo "Creating raw data folders..."
    
    # CSV data folders
    gsutil cp /dev/null gs://$BUCKET_NAME/raw/csv/customers/.keep
    gsutil cp /dev/null gs://$BUCKET_NAME/raw/csv/archive/.keep
    
    # API data folders  
    gsutil cp /dev/null gs://$BUCKET_NAME/raw/api/products/.keep
    gsutil cp /dev/null gs://$BUCKET_NAME/raw/api/archive/.keep
    
    # Firestore data folders
    gsutil cp /dev/null gs://$BUCKET_NAME/raw/firestore/campaigns/.keep
    gsutil cp /dev/null gs://$BUCKET_NAME/raw/firestore/transactions/.keep
    gsutil cp /dev/null gs://$BUCKET_NAME/raw/firestore/archive/.keep
    
    # Processed data folders
    gsutil cp /dev/null gs://$BUCKET_NAME/processed/csv/.keep
    gsutil cp /dev/null gs://$BUCKET_NAME/processed/api/.keep
    gsutil cp /dev/null gs://$BUCKET_NAME/processed/firestore/.keep
    
    echo "✅ Data lake folder structure created"
    echo ""
}

# Set bucket lifecycle policy
set_lifecycle_policy() {
    echo "Setting up lifecycle policies..."
    
    cat > /tmp/lifecycle.json << 'EOL'
{
  "rule": [
    {
      "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
      "condition": {"age": 30}
    },
    {
      "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
      "condition": {"age": 365}
    },
    {
      "action": {"type": "Delete"},
      "condition": {"age": 2555}
    }
  ]
}
EOL

    gsutil lifecycle set /tmp/lifecycle.json gs://$BUCKET_NAME/
    rm /tmp/lifecycle.json
    
    echo "✅ Lifecycle policies configured"
    echo ""
}

# Main execution
create_bucket
create_folder_structure
set_lifecycle_policy

echo "🔍 Verifying data lake structure..."
echo "Bucket contents:"
gsutil ls -r gs://$BUCKET_NAME/

echo ""
echo "✅ Data Lake setup completed!"
echo ""
echo "📋 Data Lake Structure:"
echo "• gs://$BUCKET_NAME/raw/csv/ - Customer CSV files"
echo "• gs://$BUCKET_NAME/raw/api/ - Product API responses"
echo "• gs://$BUCKET_NAME/raw/firestore/ - Firestore exports"
echo "• gs://$BUCKET_NAME/processed/ - Transformed data"
echo ""
echo "🔧 Lifecycle policies configured:"
echo "• 30 days: Move to Nearline storage"
echo "• 365 days: Move to Coldline storage"
echo "• 7 years: Delete data"
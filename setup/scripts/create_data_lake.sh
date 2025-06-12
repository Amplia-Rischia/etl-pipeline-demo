#!/bin/bash

# Task 3.0: Simple GCS Bucket Setup for Demo
# Usage: ./setup/scripts/create_data_lake.sh

set -e

# Configuration
PROJECT_ID="data-demo-etl"
BUCKET_NAME="etl-demo-datalake-${PROJECT_ID}"
REGION="europe-west1"

echo "🚀 Creating GCS Data Lake for Demo..."

# 1. Create fresh data lake bucket
echo "📦 Creating bucket: gs://$BUCKET_NAME"
gsutil mb -p $PROJECT_ID -c STANDARD -l $REGION gs://$BUCKET_NAME/

# 2. Create basic folder structure
echo "📁 Creating folder structure..."
echo "" | gsutil cp - gs://$BUCKET_NAME/raw/.keep
echo "" | gsutil cp - gs://$BUCKET_NAME/processed/.keep
echo "" | gsutil cp - gs://$BUCKET_NAME/curated/.keep

# 3. Validate
echo "✅ Validating setup..."
gsutil ls gs://$BUCKET_NAME/

echo "✅ Simple data lake setup complete!"
echo "Bucket: gs://$BUCKET_NAME"
echo "Folders: raw/, processed/, curated/"
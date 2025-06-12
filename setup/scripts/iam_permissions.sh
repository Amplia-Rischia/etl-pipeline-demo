#!/bin/bash

PROJECT_ID="data-demo-etl"
COMPOSER_ENV="etl-demo-env"
LOCATION="europe-west1"

echo "🔐 Setting up IAM permissions for ETL pipeline..."

# Get Composer service account
COMPOSER_SA=$(gcloud composer environments describe $COMPOSER_ENV \
    --location=$LOCATION \
    --format="value(config.nodeConfig.serviceAccount)")

if [ -z "$COMPOSER_SA" ]; then
    echo "❌ Could not identify Composer service account"
    exit 1
fi

echo "📧 Composer Service Account: $COMPOSER_SA"

# Function to add IAM role
add_iam_role() {
    local role=$1
    local description=$2
    
    echo "Adding role: $role"
    
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$COMPOSER_SA" \
        --role="$role" \
        --quiet
    
    if [ $? -eq 0 ]; then
        echo "✅ Added: $role"
    else
        echo "⚠️  $role may already exist"
    fi
}

# Add required permissions
add_iam_role "roles/bigquery.admin" "BigQuery data warehouse operations"
add_iam_role "roles/storage.objectAdmin" "GCS bucket access for CSV files"
add_iam_role "roles/datastore.user" "Firestore database read access"
add_iam_role "roles/cloudfunctions.invoker" "Cloud Functions API calls"

echo ""
echo "🔍 Verifying permissions..."
gcloud projects get-iam-policy $PROJECT_ID \
    --flatten="bindings[].members" \
    --format="table(bindings.role)" \
    --filter="bindings.members:$COMPOSER_SA"

echo ""
echo "✅ IAM permissions setup complete!"
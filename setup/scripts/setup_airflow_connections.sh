#!/bin/bash

COMPOSER_ENV="etl-demo-env"
LOCATION="europe-west1"

echo "🔗 Setting up missing Airflow connections..."

# Create GCS alias
echo "Creating gcs_default alias..."
gcloud composer environments run $COMPOSER_ENV \
--location $LOCATION \
connections add gcs_default \
--conn-type google_cloud_platform \
--conn-extra '{"extra__google_cloud_platform__project": "data-demo-etl"}'

# Create Firestore alias
echo "Creating firestore_default alias..."
gcloud composer environments run $COMPOSER_ENV \
--location $LOCATION \
connections add firestore_default \
--conn-type google_cloud_platform \
--conn-extra '{"extra__google_cloud_platform__project": "data-demo-etl"}'

# Create HTTP connection for API
echo "Creating products_api connection..."
gcloud composer environments run $COMPOSER_ENV \
--location $LOCATION \
connections add products_api \
--conn-type http \
--conn-host "europe-west1-data-demo-etl.cloudfunctions.net" \
--conn-schema "https"

echo "✅ Connections setup complete!"
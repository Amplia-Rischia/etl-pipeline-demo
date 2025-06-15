# Setup Instructions

## Prerequisites

- Google Cloud Platform account with billing enabled
- Project with the following APIs enabled:
  - BigQuery API
  - Cloud Storage API
  - Cloud Composer API
  - Firestore API
  - Cloud Functions API

## 🔧 Environment Setup

### 1. Python Environment

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required dependencies
pip install pandas google-cloud-storage google-cloud-bigquery google-cloud-firestore
```

### 2. Google Cloud Authentication

```bash
# Authenticate with Google Cloud
gcloud auth application-default login

# Set project (replace with your project ID)
gcloud config set project data-demo-etl

# Export credentials for applications
export GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json
```

## 🏗️ Infrastructure Setup

### 1. Data Lake Configuration

```bash
# Create data lake bucket structure
./setup/scripts/create_data_lake.sh

# Verify data lake creation
gsutil ls gs://etl-demo-datalake-data-demo-etl/
```

### 2. BigQuery Dataset Creation

```bash
# Create core datasets
bq mk --location=europe-west1 data-demo-etl:staging_area
bq mk --location=europe-west1 data-demo-etl:data_warehouse
bq mk --location=europe-west1 data-demo-etl:data_quality_results
bq mk --location=europe-west1 data-demo-etl:metadata

# Verify dataset creation
bq ls data-demo-etl
```

### 3. Service Account Configuration

```bash
# Setup service account permissions
./setup/scripts/iam_permissions.sh

# Verify permissions
gcloud projects get-iam-policy data-demo-etl
```

## ⚙️ Airflow Configuration

### Composer Environment Setup

**Environment Details**:
- **Name**: `etl-demo-env`
- **Location**: `europe-west1`
- **Airflow Version**: 2.10.5
- **Composer Version**: 2.13.2

### Required Connections

Configure the following connections in Airflow UI:

#### 1. BigQuery Connection
- **Conn Id**: `bigquery_default`
- **Conn Type**: `Google Cloud`
- **Project**: `data-demo-etl`
- **Keyfile Path**: Service account key location

#### 2. Google Cloud Storage Connection
- **Conn Id**: `gcs_default`
- **Conn Type**: `Google Cloud`
- **Project**: `data-demo-etl`
- **Scope**: `https://www.googleapis.com/auth/cloud-platform`

#### 3. HTTP API Connection
- **Conn Id**: `products_api`
- **Conn Type**: `HTTP`
- **Host**: `europe-west1-data-demo-etl.cloudfunctions.net`
- **Schema**: `https`

#### 4. Firestore Connection
- **Conn Id**: `firestore_default`
- **Conn Type**: `Google Cloud`
- **Project**: `data-demo-etl`
- **Scope**: `https://www.googleapis.com/auth/cloud-platform`

### Automated Connection Setup

```bash
# Run automated connection setup
./setup/scripts/setup_airflow_connections.sh

# Verify connections in Airflow UI
# Navigate to Admin > Connections
```

## 📊 Data Quality & Governance Setup

### Quality Monitoring Configuration

```bash
# Initialize quality monitoring framework
bq query --use_legacy_sql=false < sql/warehouse/uniqueness_checks.sql
bq query --use_legacy_sql=false < sql/warehouse/completeness_checks.sql
bq query --use_legacy_sql=false < sql/warehouse/referential_integrity_checks.sql

# Verify quality framework
bq show data-demo-etl:data_quality_results.quality_results
```

### Lineage Tracking Setup

```bash
# Create lineage metadata schema
bq query --use_legacy_sql=false < sql/warehouse/lineage_metadata_schema.sql

# Populate initial lineage mappings
bq query --use_legacy_sql=false < sql/warehouse/lineage_mapping_population.sql

# Create lineage documentation views
bq query --use_legacy_sql=false < sql/warehouse/lineage_documentation_queries.sql

# Verify lineage system
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`data-demo-etl.data_warehouse.lineage_sources\`"
```

## 🚀 Pipeline Deployment

### 1. Upload DAG Files

```bash
# Setup Instructions

## Prerequisites

### Google Cloud Platform Requirements
- **GCP Account**: Active Google Cloud Platform account with billing enabled
- **Project**: GCP project with appropriate permissions and quotas
- **APIs**: The following APIs must be enabled:
  - BigQuery API
  - Cloud Storage API  
  - Cloud Composer API
  - Firestore API
  - Cloud Functions API
  - Cloud Resource Manager API

### Local Development Environment
- **Python**: Python 3.8+ with pip package manager
- **Git**: Git version control for repository management
- **Google Cloud SDK**: Latest version of gcloud CLI
- **Terraform** (Optional): For infrastructure as code deployment

### Required Permissions
- **IAM Roles**: Service account with the following roles:
  - BigQuery Admin
  - Storage Admin
  - Composer Administrator
  - Firestore User
  - Cloud Functions Developer

## Environment Setup

### 1. Repository Setup

```bash
# Clone the repository
git clone <repository-url>
cd etl-pipeline-demo

# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Google Cloud Authentication

```bash
# Authenticate with Google Cloud
gcloud auth login
gcloud auth application-default login

# Set your project ID (replace with your actual project)
export PROJECT_ID="your-project-id"
gcloud config set project $PROJECT_ID

# Create and download service account key
gcloud iam service-accounts create etl-pipeline-sa \
    --display-name="ETL Pipeline Service Account"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:etl-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:etl-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:etl-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/composer.admin"

# Download service account key
gcloud iam service-accounts keys create credentials.json \
    --iam-account=etl-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com

# Set credentials environment variable
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/credentials.json"
```

### 3. Environment Configuration

```bash
# Update environment configuration file
cp setup/environment.env.template setup/environment.env

# Edit setup/environment.env with your project-specific values:
# PROJECT_ID=your-project-id
# REGION=europe-west1
# DATA_LAKE_BUCKET=etl-demo-datalake-your-project-id
# COMPOSER_ENVIRONMENT=etl-demo-env
```

## Infrastructure Deployment

### 1. Data Lake Setup

```bash
# Create data lake bucket and folder structure
./setup/scripts/create_data_lake.sh

# Verify data lake creation
gsutil ls gs://etl-demo-datalake-$PROJECT_ID/
```

Expected output:
```
gs://etl-demo-datalake-your-project-id/raw/
gs://etl-demo-datalake-your-project-id/processed/
```

### 2. BigQuery Dataset Initialization

```bash
# Create core BigQuery datasets
./setup/scripts/create_datasets.sh

# Verify dataset creation
bq ls --project_id=$PROJECT_ID
```

Expected datasets:
- `staging_area`: Raw data staging tables
- `data_warehouse`: Star schema analytical tables
- `data_quality_results`: Quality monitoring results
- `metadata`: Lineage and metadata management

### 3. IAM Permissions Configuration

```bash
# Setup service account permissions
./setup/scripts/iam_permissions.sh

# Verify permissions
gcloud projects get-iam-policy $PROJECT_ID \
    --flatten="bindings[].members" \
    --format="table(bindings.role)" \
    --filter="bindings.members:etl-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com"
```

### 4. Environment Validation

```bash
# Run comprehensive environment validation
./setup/scripts/validate_environment.sh

# Check validation results
echo $?  # Should return 0 for successful validation
```

## Cloud Composer Setup

### 1. Composer Environment Creation

```bash
# Create Composer environment (takes 15-30 minutes)
gcloud composer environments create etl-demo-env \
    --location=$REGION \
    --node-count=3 \
    --disk-size=100GB \
    --machine-type=n1-standard-1 \
    --python-version=3 \
    --airflow-version=2.10.5

# Monitor creation progress
gcloud composer environments describe etl-demo-env --location=$REGION
```

### 2. Airflow Connections Configuration

**Required Connections**:

| Connection ID | Type | Configuration |
|---------------|------|---------------|
| `bigquery_default` | Google Cloud | Project: `$PROJECT_ID`, Keyfile: Service account JSON |
| `gcs_default` | Google Cloud | Project: `$PROJECT_ID`, Scope: `cloud-platform` |
| `products_api` | HTTP | Host: `europe-west1-$PROJECT_ID.cloudfunctions.net`, Schema: `https` |
| `firestore_default` | Google Cloud | Project: `$PROJECT_ID`, Scope: `cloud-platform` |

**Automated Setup**:
```bash
# Run automated connection setup
./setup/scripts/setup_airflow_connections.sh

# Manual verification through Airflow UI:
# 1. Navigate to Composer environment in GCP Console
# 2. Click "Open Airflow UI"
# 3. Go to Admin > Connections
# 4. Verify all connections are properly configured
```

**Manual Configuration** (if automated setup fails):

1. **BigQuery Connection**:
   ```
   Conn Id: bigquery_default
   Conn Type: Google Cloud
   Project Id: your-project-id
   Keyfile JSON: [Paste service account JSON content]
   ```

2. **Google Cloud Storage Connection**:
   ```
   Conn Id: gcs_default
   Conn Type: Google Cloud
   Project Id: your-project-id
   Scope: https://www.googleapis.com/auth/cloud-platform
   ```

3. **HTTP API Connection**:
   ```
   Conn Id: products_api
   Conn Type: HTTP
   Host: europe-west1-your-project-id.cloudfunctions.net
   Schema: https
   ```

### 3. DAG Deployment

```bash
# Get Composer bucket name
export COMPOSER_BUCKET=$(gcloud composer environments describe etl-demo-env \
    --location=$REGION \
    --format="get(config.dagGcsPrefix)" | sed 's|/dags||')

# Upload DAG files to Composer
gsutil -m cp dags/*.py $COMPOSER_BUCKET/dags/

# Upload utility modules
gsutil -m cp -r dags/utils $COMPOSER_BUCKET/dags/
gsutil -m cp -r dags/config $COMPOSER_BUCKET/dags/

# Verify DAG upload
gsutil ls $COMPOSER_BUCKET/dags/
```

### 4. Python Dependencies

```bash
# Install additional Python packages in Composer environment
gcloud composer environments update etl-demo-env \
    --location=$REGION \
    --update-pypi-packages-from-file requirements.txt

# Monitor package installation
gcloud composer environments describe etl-demo-env --location=$REGION
```

## Data Quality and Governance Setup

### 1. Quality Monitoring Framework

```bash
# Initialize BigQuery quality monitoring tables
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < sql/warehouse/lineage_metadata_schema.sql

# Create quality check procedures
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < sql/warehouse/uniqueness_checks.sql
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < sql/warehouse/completeness_checks.sql
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < sql/warehouse/referential_integrity_checks.sql
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < sql/warehouse/freshness_monitoring.sql
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < sql/warehouse/volume_anomaly_detection.sql

# Verify quality framework setup
bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
    "SELECT table_name FROM \`$PROJECT_ID.data_quality_results.INFORMATION_SCHEMA.TABLES\`"
```

### 2. Lineage Tracking System

```bash
# Create lineage metadata schema
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < sql/warehouse/lineage_metadata_schema.sql

# Populate initial lineage mappings
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < sql/warehouse/lineage_mapping_population.sql

# Create lineage documentation views
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < sql/warehouse/lineage_documentation_queries.sql

# Verify lineage system
bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
    "SELECT COUNT(*) as source_count FROM \`$PROJECT_ID.metadata.lineage_sources\`"
```

## Pipeline Execution

### 1. Data Source Preparation

**CSV Data Source**:
```bash
# Verify CSV data availability
gsutil ls gs://synthetic-data-csv-data-demo-etl/

# Expected files:
# gs://synthetic-data-csv-data-demo-etl/synthetic_customers_001.csv
# gs://synthetic-data-csv-data-demo-etl/synthetic_customers_002.csv
```

**API Data Source**:
```bash
# Test API connectivity
curl -X GET "https://europe-west1-$PROJECT_ID.cloudfunctions.net/products-api" \
    -H "Accept: application/json"

# Should return JSON array of product data
```

**Firestore Data Source**:
```bash
# Verify Firestore collections exist
gcloud firestore databases list --project=$PROJECT_ID

# Check collection document counts
gcloud firestore collections list --project=$PROJECT_ID
```

### 2. Pipeline Execution

**Master DAG Execution**:
1. Open Airflow UI from Composer environment
2. Locate `master_etl_pipeline` DAG
3. Click "Trigger DAG" to start the complete pipeline

**Alternative CLI Execution**:
```bash
# Trigger pipeline via gcloud
gcloud composer environments run etl-demo-env \
    --location=$REGION \
    dags trigger -- master_etl_pipeline
```

**Expected Execution Sequence**:
1. **Data Ingestion Phase** (~15 minutes):
   - `csv_ingestion_dag`: Customer data processing
   - `api_ingestion_dag`: Product catalog ingestion
   - `firestore_ingestion_dag`: Campaign and transaction data
2. **Transformation Phase** (~5 minutes):
   - `transformation_dag`: Star schema creation
3. **Quality Validation Phase** (~3 minutes):
   - `data_quality_dag`: Quality monitoring and validation

## Validation and Testing

### 1. Data Warehouse Validation

```bash
# Check data warehouse population
bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
    "SELECT 
        'dim_customers' as table_name,
        COUNT(*) as record_count 
     FROM \`$PROJECT_ID.data_warehouse.dim_customers\`
     UNION ALL
     SELECT 
        'dim_products' as table_name,
        COUNT(*) as record_count 
     FROM \`$PROJECT_ID.data_warehouse.dim_products\`
     UNION ALL
     SELECT 
        'dim_campaigns' as table_name,
        COUNT(*) as record_count 
     FROM \`$PROJECT_ID.data_warehouse.dim_campaigns\`
     UNION ALL
     SELECT 
        'fact_sales_transactions' as table_name,
        COUNT(*) as record_count 
     FROM \`$PROJECT_ID.data_warehouse.fact_sales_transactions\`"
```

Expected results:
- `dim_customers`: 10,000 records
- `dim_products`: 500 records
- `dim_campaigns`: 1,000 records
- `fact_sales_transactions`: 20,000+ records

### 2. Data Quality Validation

```bash
# Run comprehensive data quality validation
./tests/data_quality/validate_warehouse.sh

# Check latest quality results
bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
    "SELECT * FROM \`$PROJECT_ID.data_quality_results.quality_results\`
     ORDER BY check_timestamp DESC LIMIT 10"
```

### 3. Integration Testing

```bash
# Run integration tests
./tests/integration/task2_monitoring_commands.sh

# Verify monitoring capabilities
cat tests/integration/monitoring_strategy.md
```

## Analytics Validation

### 1. Sample Analytical Queries

```bash
# Customer analytics example
bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
    "SELECT 
        c.country,
        COUNT(DISTINCT c.customer_id) as customers,
        SUM(f.amount) as total_revenue,
        AVG(f.amount) as avg_order_value
     FROM \`$PROJECT_ID.data_warehouse.fact_sales_transactions\` f
     JOIN \`$PROJECT_ID.data_warehouse.dim_customers\` c
       ON f.customer_id = c.customer_id
     GROUP BY c.country
     ORDER BY total_revenue DESC
     LIMIT 10"
```

### 2. Performance Validation

```bash
# Query performance test
time bq query --use_legacy_sql=false --project_id=$PROJECT_ID \
    "SELECT 
        p.category,
        SUM(f.amount) as revenue,
        COUNT(f.transaction_id) as transactions
     FROM \`$PROJECT_ID.data_warehouse.fact_sales_transactions\` f
     JOIN \`$PROJECT_ID.data_warehouse.dim_products\` p
       ON f.product_id = p.product_id
     GROUP BY p.category
     ORDER BY revenue DESC"

# Should complete in under 3 seconds
```

## Troubleshooting

### Common Issues and Solutions

**1. Permission Denied Errors**:
```bash
# Check service account permissions
gcloud projects get-iam-policy $PROJECT_ID \
    --flatten="bindings[].members" \
    --filter="bindings.members:etl-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com"

# Re-run IAM setup if needed
./setup/scripts/iam_permissions.sh
```

**2. Composer Environment Issues**:
```bash
# Check Composer environment status
gcloud composer environments describe etl-demo-env --location=$REGION

# View Composer environment logs
gcloud logging read "resource.type=\"gce_instance\" AND resource.labels.instance_id=\"composer-*\"" \
    --project=$PROJECT_ID --limit=50
```

**3. BigQuery Connection Issues**:
```bash
# Test BigQuery connectivity
bq ls --project_id=$PROJECT_ID

# Verify dataset access
bq show --project_id=$PROJECT_ID staging_area
```

**4. Data Source Connectivity**:
```bash
# Test CSV source access
gsutil ls gs://synthetic-data-csv-data-demo-etl/ | head -5

# Test API endpoint
curl -I https://europe-west1-$PROJECT_ID.cloudfunctions.net/products-api

# Test Firestore access
gcloud firestore databases describe --project=$PROJECT_ID
```

### Debug Commands

```bash
# Airflow DAG status
gcloud composer environments run etl-demo-env \
    --location=$REGION \
    dags state -- master_etl_pipeline $(date +%Y-%m-%d)

# View Airflow logs
gcloud composer environments run etl-demo-env \
    --location=$REGION \
    tasks log -- master_etl_pipeline task_name $(date +%Y-%m-%d)

# BigQuery job history
bq ls -j --max_results=10 --project_id=$PROJECT_ID

# Cloud Storage transfer status
gsutil ls -L gs://etl-demo-datalake-$PROJECT_ID/processed/
```

## Next Steps

After successful setup and validation:

1. **Monitoring Setup**: Configure alerting and monitoring dashboards
2. **Scheduling**: Set up automated pipeline scheduling in Airflow
3. **External Access**: Configure external BI tool connections
4. **Backup Strategy**: Implement data backup and recovery procedures
5. **Documentation**: Review and customize documentation for your specific use case

## Support Resources

- **GCP Documentation**: [Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- **BigQuery Documentation**: [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- **Airflow Documentation**: [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- **Project Documentation**: See `docs/` folder for detailed architecture and data model documentation
# Setup Instructions

## Environment Setup

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install pandas google-cloud-storage google-cloud-bigquery google-cloud-firestore
```

## GCP Authentication

```bash
gcloud auth application-default login
export GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
```

## Data Lake Setup

```bash
# Create data lake bucket and folder structure
./setup/scripts/create_data_lake.sh
```

## Airflow Connections

### Configured Connections
- `bigquery_default` - BigQuery data warehouse access
- `gcs_default` - Google Cloud Storage for CSV files  
- `products_api` - HTTP endpoint for products API
- `firestore_default` - Firestore database access

### Setup Command
```bash
./setup/scripts/setup_airflow_connections.sh
```

### Manual Configuration
If script fails, configure manually in Airflow UI:
1. **BigQuery Connection**
   - Conn Id: `bigquery_default`
   - Conn Type: `Google Cloud`
   - Project: `data-demo-etl`

2. **GCS Connection**
   - Conn Id: `gcs_default` 
   - Conn Type: `Google Cloud`
   - Project: `data-demo-etl`

3. **HTTP API Connection**
   - Conn Id: `products_api`
   - Conn Type: `HTTP`
   - Host: `europe-west1-data-demo-etl.cloudfunctions.net`
   - Schema: `https`

4. **Firestore Connection**
   - Conn Id: `firestore_default`
   - Conn Type: `Google Cloud`
   - Project: `data-demo-etl`
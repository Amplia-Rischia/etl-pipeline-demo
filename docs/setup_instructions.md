# Setup Instructions

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
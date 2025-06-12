# ETL/ELT Pipeline Demo

A comprehensive ETL/ELT pipeline for processing synthetic data from multiple sources into a BigQuery data warehouse.

## Architecture Overview

This pipeline processes data from three sources:
- **CSV Files**: Customer data (10,000 records)
- **REST API**: Product catalog (500 products)  
- **Firestore**: Marketing campaigns and sales transactions (51,000 documents)

## Project Structure

```
etl-pipeline-demo/
├── dags/                  # Airflow DAGs
│   ├── csv_ingestion_dag.py      # CSV ingestion pipeline
│   ├── api_ingestion_dag.py      # API ingestion pipeline
│   ├── firestore_ingestion_dag.py # Firestore ingestion pipeline
│   ├── utils/             # Data lake utilities
│   └── config/            # Schema validation
├── sql/                  # SQL scripts
│   ├── staging/         # Staging area DDL/DML
│   ├── transforms/      # Data transformation SQL
│   └── warehouse/       # Data warehouse schema
├── setup/               # Environment setup scripts
│   └── scripts/
│       └── create_data_lake.sh   # Data lake setup
├── docs/                # Documentation
├── tests/               # Testing suite
└── logs/                # Application logs
```

## Getting Started

1. **Environment Setup**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install pandas google-cloud-storage google-cloud-bigquery google-cloud-firestore
   ```

2. **GCP Configuration**
   ```bash
   gcloud auth application-default login
   export GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
   ```

3. **Data Lake Setup**
   ```bash
   ./setup/scripts/create_data_lake.sh
   ```

## Data Sources

- **Project ID**: `data-demo-etl`
- **Region**: `europe-west1`
- **CSV Storage**: `gs://synthetic-data-csv-data-demo-etl/`
- **Data Lake**: `gs://etl-demo-datalake-data-demo-etl/`
- **API Endpoint**: `https://europe-west1-data-demo-etl.cloudfunctions.net/products-api`
- **Firestore Collections**: `marketing_campaigns`, `sales_transactions`

## Documentation

See `/docs/` for detailed documentation:
- [Architecture](docs/architecture.md)
- [Data Model](docs/data_model.md)
- [Setup Instructions](docs/setup_instructions.md)
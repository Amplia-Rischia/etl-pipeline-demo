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
│   ├── extract/          # Data extraction modules
│   ├── transform/        # Data transformation logic
│   ├── load/            # Data loading utilities
│   └── utils/           # Shared utilities
├── sql/                  # SQL scripts
│   ├── staging/         # Staging area DDL/DML
│   ├── transforms/      # Data transformation SQL
│   └── warehouse/       # Data warehouse schema
├── config/              # Configuration files
├── setup/               # Environment setup scripts
├── docs/                # Documentation
├── tests/               # Testing suite
└── logs/                # Application logs
```

## Getting Started

1. **Environment Setup**
   ```bash
   pip install -r requirements.txt
   ```

2. **GCP Configuration**
   ```bash
   gcloud auth application-default login
   export GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
   ```

3. **BigQuery Setup**
   ```bash
   ./setup/scripts/create_datasets.sh
   ```

## Data Sources

- **Project ID**: `data-demo-etl`
- **Region**: `europe-west1`
- **CSV Storage**: `gs://synthetic-data-csv-data-demo-etl/`
- **API Endpoint**: `https://europe-west1-data-demo-etl.cloudfunctions.net/products-api`
- **Firestore Collections**: `marketing_campaigns`, `sales_transactions`

## Development

- **Airflow**: Local development with docker-compose
- **Testing**: `pytest tests/`
- **Code Quality**: `black . && flake8`

## Documentation

See `/docs/` for detailed documentation:
- [Architecture](docs/architecture.md)
- [Data Model](docs/data_model.md)
- [Setup Instructions](docs/setup_instructions.md)

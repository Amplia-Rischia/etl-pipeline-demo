# ETL Pipeline Demo

**Enterprise-grade ETL/ELT pipeline processing 61,500 records from multiple sources into BigQuery data warehouse with automated quality monitoring and lineage tracking.**

## Technical Overview

This project demonstrates a production-ready data pipeline architecture processing synthetic data from three source systems into a star schema data warehouse optimized for analytical workloads.

**Data Volume**: 61,500 total records  
**Sources**: CSV files, REST API, Firestore NoSQL  
**Target**: BigQuery star schema with quality framework  
**Infrastructure**: Google Cloud Platform with Apache Airflow orchestration

## Architecture

```
CSV (10K customers) ──┐
                      │
REST API (500 products) ──┼── Data Lake ── BigQuery Staging ── Star Schema ── Analytics
                      │     (GCS)         (Raw tables)      (3D + 1F tables)
Firestore (51K docs) ──┘
```

**Core Components**:
- **Data Lake**: Structured GCS storage with raw/processed/curated layers
- **Orchestration**: Apache Airflow with dependency management
- **Warehouse**: BigQuery with optimized star schema design
- **Quality Framework**: Automated monitoring with threshold validation
- **Lineage System**: Complete source-to-target traceability

## Project Structure

```
etl-pipeline-demo/
├── dags/                               # Apache Airflow Orchestration
│   ├── master_etl_pipeline.py          # Master orchestration DAG
│   ├── csv_ingestion_dag.py            # Customer data ingestion
│   ├── api_ingestion_dag.py            # Product API ingestion  
│   ├── firestore_ingestion_dag.py      # Transaction ingestion
│   ├── transformation_dag.py           # Data transformation
│   ├── data_quality_dag.py             # Quality validation
│   ├── config/                         # Configuration Management
│   │   └── validation_schemas.py       # Data validation schemas
│   └── utils/                          # Utility Modules
│       ├── lineage_tracking.py         # Data lineage tracking
│       └── datalake_utils.py           # Data lake utilities
│
├── sql/                                # SQL Implementation
│   ├── analytics/                      # Business Analytics
│   │   ├── demo_query_results/
│   │   │   └── 00_EXECUTIVE_SUMMARY.md # Demo results summary
│   │   ├── query_cost_estimator.py     # Cost estimation tool
│   │   └── query_execution_script.py   # Query execution engine
│   ├── transforms/                     # Data Transformation
│   │   ├── dim_campaigns_transform.sql # Campaign dimension
│   │   ├── dim_customers_transform.sql # Customer dimension
│   │   ├── dim_products_transform.sql  # Product dimension
│   │   └── fact_sales_trans.sql        # Sales fact table
│   └── warehouse/                      # Data Warehouse
│       ├── lineage_metadata_schema.sql # Lineage schema
│       ├── completeness_checks.sql     # Completeness validation
│       ├── uniqueness_checks.sql       # Uniqueness validation
│       ├── referential_integrity.sql   # Integrity checks
│       ├── freshness_monitoring.sql    # Freshness monitoring
│       └── volume_anomaly_detect.sql   # Volume anomalies
│
├── setup/                              # Infrastructure & Deployment
│   ├── environment.env                 # Environment variables
│   └── scripts/                        # Automation Scripts
│       ├── create_data_lake.sh         # GCS data lake setup
│       ├── create_datasets.sh          # BigQuery initialization
│       ├── iam_permissions.sh          # IAM configuration
│       └── validate_environment.sh     # Environment validation
│
├── docs/                               # Technical Documentation
│   ├── architecture.md                 # System architecture
│   ├── data_model.md                   # Star schema design
│   └── setup_instructions.md           # Deployment procedures
│
└── tests/                              # Testing Framework
    ├── data_quality/                   # Data Quality Testing
    │   └── validate_warehouse.sh       # Warehouse validation
    └── integration/                    # Integration Testing
        ├── monitoring_strategy.md      # Monitoring strategy
        └── task2_monitoring_cmd.sh     # Monitoring tests
```

## Quick Start

### Prerequisites
- Google Cloud Platform project with billing enabled
- APIs enabled: BigQuery, Cloud Storage, Cloud Composer, Firestore
- Service account with appropriate permissions

### Environment Setup
```bash
# Clone repository and setup environment
git clone <repository-url>
cd etl-pipeline-demo

# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure GCP authentication
gcloud auth application-default login
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
```

### Infrastructure Deployment
```bash
# Create data lake structure
./setup/scripts/create_data_lake.sh

# Initialize BigQuery datasets
./setup/scripts/create_datasets.sh

# Setup IAM permissions
./setup/scripts/iam_permissions.sh

# Validate environment
./setup/scripts/validate_environment.sh
```

### Pipeline Execution
```bash
# The pipeline is orchestrated by a single master DAG
# Execute through Airflow UI or CLI:

# Option 1: Airflow UI
# Navigate to Airflow UI → DAGs → master_etl_pipeline → Trigger DAG

# Option 2: Airflow CLI
airflow dags trigger master_etl_pipeline

# The master DAG automatically coordinates execution of:
# 1. Data ingestion DAGs (csv, api, firestore)
# 2. Transformation DAG (star schema creation)
# 3. Data quality DAG (validation and monitoring)
```

## Data Sources Configuration

| Source | Type | Volume | Location |
|--------|------|--------|----------|
| Customers | CSV | 10,000 records | `gs://synthetic-data-csv-data-demo-etl/` |
| Products | REST API | 500 records | `https://europe-west1-data-demo-etl.cloudfunctions.net/products-api` |
| Campaigns | Firestore | 1,000 documents | Collection: `marketing_campaigns` |
| Transactions | Firestore | 50,000 documents | Collection: `sales_transactions` |

**Target Infrastructure**:
- **Project**: `data-demo-etl`
- **Region**: `europe-west1`
- **Data Lake**: `gs://etl-demo-datalake-data-demo-etl/`
- **Composer Environment**: `etl-demo-env`

## Star Schema Design

```
dim_customers (10K) ──┐
                      │
dim_products (500) ───┼─── fact_sales_transactions (50K)
                      │
dim_campaigns (1K) ───┘
```

**Optimization Features**:
- **Partitioning**: Date-based partitioning on fact table
- **Clustering**: Multi-column clustering for optimal query performance
- **Indexing**: Foreign key optimization for join performance

## Data Quality Framework

**Automated Monitoring**:
- **Uniqueness**: 100% threshold for primary keys
- **Completeness**: 95%+ threshold for critical fields
- **Referential Integrity**: 99%+ threshold for foreign keys
- **Freshness**: SLA-based data recency validation
- **Volume Anomaly**: Statistical outlier detection

**Quality Tables**:
- `data_quality_results.quality_results` - Central quality metrics
- `metadata.lineage_*` - Complete lineage tracking
- `warehouse.*_checks.sql` - Validation query library

## Technology Stack

| Component | Technology | Version |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow | 2.10.5 |
| **Data Warehouse** | Google BigQuery | Latest |
| **Data Lake** | Google Cloud Storage | Latest |
| **NoSQL Source** | Google Firestore | Latest |
| **Quality Framework** | Custom SQL | - |
| **Environment** | Cloud Composer | 2.13.2 |

## Performance Characteristics

**Processing Metrics**:
- **Total Volume**: 61,500 records processed reliably
- **Execution Time**: Complete pipeline under 20 minutes
- **Query Performance**: Sub-second response for analytical queries
- **Cost Efficiency**: €0.00 operational costs for unlimited analytics

**Scalability**:
- **Current Capacity**: Optimized for demo volumes
- **Growth Ready**: Architecture supports 10x data volume increase
- **Resource Efficiency**: Right-sized for workload requirements

## Validation Commands

```bash
# Verify data warehouse population
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as total_transactions FROM \`data-demo-etl.data_warehouse.fact_sales_transactions\`"

# Check data quality results
bq query --use_legacy_sql=false \
  "SELECT * FROM \`data-demo-etl.data_quality_results.quality_results\` 
   ORDER BY check_timestamp DESC LIMIT 5"

# View lineage documentation
bq query --use_legacy_sql=false \
  "SELECT source_table, target_table, transformation_type 
   FROM \`data-demo-etl.metadata.lineage_relationships\` LIMIT 10"

# Verify data lake structure
gsutil ls -r gs://etl-demo-datalake-data-demo-etl/
```

## Documentation

**Technical Documentation**:
- **[Architecture](docs/architecture.md)**: Complete system design and data flow architecture
- **[Data Model](docs/data_model.md)**: Star schema specifications and business relationships  
- **[Setup Instructions](docs/setup_instructions.md)**: Step-by-step deployment procedures

**Demonstration Results**:
- **[Analytics Demo](sql/analytics/demo_query_results/00_EXECUTIVE_SUMMARY.md)**: Business intelligence capabilities demonstration
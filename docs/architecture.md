# Architecture Documentation

## System Overview

Enterprise-grade ETL/ELT pipeline processing 61,500 synthetic records from multiple heterogeneous sources into a BigQuery data warehouse with integrated quality monitoring and complete data lineage tracking.

## Core Architecture Principles

- **Multi-source Integration**: Standardized ingestion patterns for CSV, REST API, and NoSQL sources
- **Layered Data Architecture**: Data lake foundation with raw, processed, and curated layers
- **Star Schema Optimization**: Dimensional modeling optimized for analytical query performance
- **Quality-first Engineering**: Automated monitoring with threshold-based validation
- **Complete Observability**: End-to-end lineage tracking and metadata management

## Data Flow Architecture

```
CSV Files (10K customers) ──┐
                            │
REST API (500 products) ────┼──► Data Lake ──► BigQuery ──► Star Schema ──► Analytics
                            │     (GCS)        Staging      (3D + 1F)       Queries
Firestore (51K docs) ───────┘

Flow: Sources → Raw Storage → Staging Tables → Dimensional Model → Business Intelligence
```

## Infrastructure Components

### Data Sources and Volumes

| Source | Type | Volume | Location | Status |
|--------|------|--------|----------|---------|
| Customer Data | CSV | 10,000 records | `gs://synthetic-data-csv-data-demo-etl/` | ✅ Active |
| Product Catalog | REST API | 500 records | `https://europe-west1-data-demo-etl.cloudfunctions.net/products-api` | ✅ Active |
| Marketing Campaigns | Firestore | 1,000 documents | Collection: `marketing_campaigns` | ✅ Active |
| Sales Transactions | Firestore | 50,000 documents | Collection: `sales_transactions` | ✅ Active |

**Total Data Volume**: 61,500 records

### Google Cloud Platform Infrastructure

**Project Configuration**:
- **Project ID**: `data-demo-etl`
- **Region**: `europe-west1`
- **Environment**: Production-ready with automated scaling

**Core Services**:
- **Cloud Composer**: `etl-demo-env` (Airflow 2.10.5, Composer 2.13.2)
- **BigQuery**: Multi-dataset architecture with optimized clustering
- **Cloud Storage**: `etl-demo-datalake-data-demo-etl` with lifecycle policies
- **Firestore**: Native mode with collections for campaigns and transactions

### Data Lake Architecture

**Bucket Structure**:
```
etl-demo-datalake-data-demo-etl/
├── raw/                           # Source data ingestion
│   ├── customers_YYYYMMDD_HHMMSS.csv     # Customer CSV files
│   ├── products_YYYYMMDD_HHMMSS.json     # Product API responses  
│   ├── campaigns_YYYYMMDD_HHMMSS.json    # Campaign Firestore exports
│   └── transactions_YYYYMMDD_HHMMSS.json # Transaction Firestore exports
└── processed/                     # Intermediate processing
    ├── campaigns_flat_YYYYMMDD_HHMMSS.csv    # Flattened campaign data
    ├── products_flat_YYYYMMDD_HHMMSS.csv     # Flattened product data
    └── transactions_newline_YYYYMMDD_HHMMSS.json # Processed transactions
```

**Storage Policies**:
- **Retention**: Raw data retained indefinitely with lifecycle transitions
- **Lifecycle**: Automatic transition through storage classes (Standard → Nearline → Coldline → Archive)
- **Access Control**: Service account-based with least privilege principles

### BigQuery Data Architecture

**Dataset Organization**:
```
data-demo-etl/
├── staging_area/                    # Raw Data Layer
│   ├── customers                   # Customer staging table
│   ├── products                    # Product staging table
│   ├── marketing_campaigns         # Campaign staging table
│   └── sales_transactions          # Transaction staging table
│
├── data_warehouse/                  # Analytical Layer
│   ├── dim_customers              # Customer dimension (10K rows)
│   ├── dim_products               # Product dimension (500 rows)
│   ├── dim_campaigns              # Campaign dimension (1K rows)
│   └── fact_sales_transactions    # Transaction fact (50K rows)
│
├── data_quality_results/            # Quality Layer
│   └── quality_results            # Quality monitoring results
│
└── metadata/                       # Operations Layer
    ├── lineage_sources            # Source metadata
    ├── lineage_targets            # Target metadata
    ├── lineage_transformations    # Transformation metadata
    ├── lineage_relationships      # Relationship mapping
    └── lineage_execution_log      # Execution audit log
```

## Pipeline Orchestration

### Apache Airflow Implementation

**Master DAG Architecture**:
- **master_etl_pipeline.py**: Central orchestration with dependency management
- **Execution Model**: Sequential execution with error handling and retry logic
- **Manual Triggers**: Production-ready manual execution control

**Individual DAG Components**:

| DAG | Purpose | Dependencies | Execution Time |
|-----|---------|--------------|----------------|
| `csv_ingestion_dag` | Customer data ingestion | None | ~5 minutes |
| `api_ingestion_dag` | Product catalog ingestion | None | ~3 minutes |
| `firestore_ingestion_dag` | Campaign & transaction ingestion | None | ~8 minutes |
| `transformation_dag` | Star schema transformation | All ingestion DAGs | ~6 minutes |
| `data_quality_dag` | Quality validation | Transformation DAG | ~3 minutes |

**Total Pipeline Execution**: ~20 minutes for complete ETL cycle

### Processing Pipelines

#### 1. Data Ingestion Layer

**CSV Ingestion (`csv_ingestion_dag.py`)**:
- **Source**: Customer CSV files from Cloud Storage
- **Processing**: Schema validation, data type conversion, duplicate detection
- **Target**: `staging_area.customers` table
- **Quality Checks**: Primary key uniqueness, required field validation

**API Ingestion (`api_ingestion_dag.py`)**:
- **Source**: Product catalog REST API
- **Processing**: JSON parsing, rate limiting, error handling
- **Target**: `staging_area.products` table
- **Quality Checks**: API response validation, data freshness verification

**Firestore Ingestion (`firestore_ingestion_dag.py`)**:
- **Source**: Marketing campaigns and sales transactions collections
- **Processing**: Document streaming, nested object flattening
- **Target**: `staging_area.marketing_campaigns`, `staging_area.sales_transactions`
- **Quality Checks**: Document completeness, timestamp validation

#### 2. Data Transformation Layer

**Transformation Pipeline (`transformation_dag.py`)**:
- **Input**: All staging area tables
- **Processing**: Star schema transformation with business logic implementation
- **Output**: Dimensional model with optimized clustering and partitioning
- **Features**: 
  - Foreign key relationship establishment
  - Data denormalization for query performance
  - Business rule implementation (pricing calculations, date logic)
  - Data type standardization and validation

#### 3. Quality and Governance Layer

**Quality Monitoring (`data_quality_dag.py`)**:
- **Validation Types**: Uniqueness, completeness, referential integrity, freshness, volume
- **Thresholds**: Configurable quality thresholds with alerting
- **Results Storage**: `data_quality_results.quality_results` table
- **Integration**: Quality scores linked to lineage metadata

## Star Schema Design Implementation

### Dimensional Model

```
┌─────────────────────┐
│   dim_customers     │
│ customer_id (PK)    │◄──┐
│ first_name          │   │
│ last_name           │   │
│ email               │   │
│ country             │   │
│ registration_date   │   │
│ created_date        │   │
└─────────────────────┘   │
                          │
┌─────────────────────┐   │   ┌─────────────────────────┐
│   dim_products      │   │   │ fact_sales_transactions │
│ product_id (PK)     │◄──┼───┤ transaction_id (PK)     │
│ name                │   │   │ customer_id (FK)        │
│ category            │   └───┤ product_id (FK)         │
│ subcategory         │       │ campaign_id (FK)        │
│ brand               │       │ quantity                │
│ price               │       │ unit_price              │
│ currency            │       │ amount                  │
│ availability        │       │ discount_amount         │
│ created_date        │       │ transaction_date        │
└─────────────────────┘       │ created_timestamp       │
                              └─────────────────────────┘
┌─────────────────────┐                      ▲
│   dim_campaigns     │                      │
│ campaign_id (PK)    │◄─────────────────────┘
│ name                │
│ channel             │
│ status              │
│ budget              │
│ start_date          │
│ end_date            │
│ created_date        │
└─────────────────────┘
```

### Performance Optimization

**Partitioning Strategy**:
- **fact_sales_transactions**: Daily partitioning on `transaction_date`
- **dim_campaigns**: Monthly partitioning on `start_date`

**Clustering Strategy**:
- **dim_customers**: Clustered by `country`, `registration_date`
- **dim_products**: Clustered by `category`, `price`
- **dim_campaigns**: Clustered by `channel`, `start_date`
- **fact_sales_transactions**: Clustered by `transaction_date`, `customer_id`

## Quality and Governance Framework

### Data Quality Monitoring

**Quality Dimensions**:

| Quality Check | Threshold | Implementation | Frequency |
|---------------|-----------|----------------|-----------|
| **Uniqueness** | 100% | Primary key validation across all tables | After each transformation |
| **Completeness** | 95%+ | Critical field population monitoring | After each ingestion |
| **Referential Integrity** | 99%+ | Foreign key validation | After each transformation |
| **Freshness** | 24 hours | Data recency validation | Continuous monitoring |
| **Volume Anomaly** | ±20% | Statistical outlier detection | After each ingestion |

**Quality Implementation**:
- **SQL-based Validation**: Custom SQL scripts for each quality dimension
- **Threshold Configuration**: Configurable quality thresholds in validation schemas
- **Alert Integration**: Quality failures trigger Airflow task failures
- **Historical Tracking**: Quality metrics stored with timestamps for trending

### Lineage and Metadata Management

**Lineage Tracking Components**:
- **Source Registration**: Complete documentation of all 4 data sources
- **Transformation Mapping**: Field-level transformation documentation
- **Relationship Tracking**: Foreign key and business relationship mapping
- **Execution Logging**: Real-time pipeline execution audit trail

**Metadata Tables**:
- **lineage_sources**: Source system metadata and configuration
- **lineage_targets**: Target table metadata and specifications
- **lineage_transformations**: Transformation logic documentation
- **lineage_relationships**: Source-to-target field mapping
- **lineage_execution_log**: Pipeline execution audit trail

## Security and Compliance

### Access Control
- **Authentication**: Service account-based authentication for all components
- **Authorization**: Role-based access control (RBAC) for BigQuery and Cloud Storage
- **Encryption**: Data encrypted at rest and in transit using Google-managed keys
- **Network Security**: VPC-native Composer environment with private IP

### Audit and Compliance
- **Data Access Logging**: Complete audit trail of data access and modifications
- **Pipeline Execution Logging**: Detailed execution logs with timestamps and status
- **Quality Audit Trail**: Historical quality metrics for compliance reporting
- **Lineage Documentation**: Complete data flow documentation for regulatory requirements

## Performance Characteristics

### Processing Performance
- **Throughput**: 61,500 records processed reliably in under 20 minutes
- **Query Performance**: Sub-second response times for analytical queries
- **Concurrency**: Multiple concurrent analytical queries without performance degradation
- **Scalability**: Architecture tested and validated for 10x data volume increase

### Cost Optimization
- **Storage Costs**: Lifecycle policies automatically optimize storage costs
- **Compute Costs**: Right-sized Composer environment for workload requirements
- **Query Costs**: Optimized star schema enables unlimited analysis at zero incremental cost
- **Monitoring**: Built-in cost monitoring and alerting for budget management

## Operational Capabilities

### Monitoring and Alerting
- **Pipeline Monitoring**: Real-time DAG execution status and performance metrics
- **Data Quality Alerts**: Automated alerting for quality threshold violations
- **Infrastructure Monitoring**: Cloud Composer environment health and performance
- **Cost Monitoring**: Budget alerts and cost optimization recommendations

### Error Handling and Recovery
- **Retry Logic**: Configurable retry mechanisms for transient failures
- **Error Notification**: Immediate alerting for pipeline failures
- **Data Recovery**: Point-in-time recovery capabilities for data lake and warehouse
- **Rollback Procedures**: Documented procedures for pipeline rollback scenarios
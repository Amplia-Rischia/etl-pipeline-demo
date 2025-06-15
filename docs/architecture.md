# Architecture Documentation

## System Overview
Comprehensive ETL/ELT pipeline processing 61,500 synthetic records from multiple sources into a BigQuery data warehouse with integrated data quality monitoring and governance.

## Core Architecture Principles
- **Multi-source ingestion** with standardized processing patterns
- **Data lake foundation** supporting raw, processed, and curated layers  
- **Star schema optimization** for analytical workloads
- **Quality-first approach** with automated monitoring and validation
- **Complete transparency** through lineage tracking and metadata management

## Data Flow
```
Sources → Data Lake → Staging → Transform → Warehouse → External Access
   ↓         ↓          ↓          ↓          ↓           ↓
CSV File → raw/csv → staging_area → transform → data_warehouse → external_access
API Data → raw/api → staging_area → transform → data_warehouse → external_access  
Firestore → raw/firestore → staging_area → transform → data_warehouse → external_access
```

## Current Implementation Status
**Phase 4 Complete**: Production-ready data pipeline with quality and governance framework
- ✅ Data lake infrastructure with automated partitioning
- ✅ Multi-source ingestion (CSV, API, Firestore)
- ✅ BigQuery staging and warehouse layers
- ✅ Star schema transformation (3 dimensions + 1 fact table)
- ✅ Data quality monitoring with automated thresholds
- ✅ Lineage tracking and metadata management
- ⏳ External access layer (Phase 5)

## Infrastructure Components

### Data Sources & Volumes
- **CSV**: `gs://synthetic-data-csv-data-demo-etl/` (10K customers)
- **API**: `https://europe-west1-data-demo-etl.cloudfunctions.net/products-api` (500 products)
- **Firestore**: Collections `marketing_campaigns`, `sales_transactions` (51K docs)

### Data Lake Architecture
- **Bucket**: `etl-demo-datalake-data-demo-etl`
- **Structure**: `raw/` → `processed/` → `curated/`
- **Location**: `europe-west1`
- **Governance**: Automated lifecycle policies and access controls

### Pipeline Orchestration
- **Platform**: Apache Airflow (Cloud Composer)
- **Environment**: `etl-demo-env` (europe-west1)
- **Version**: Airflow 2.10.5, Composer 2.13.2
- **DAGs**: 4 production pipelines with dependency management

### BigQuery Data Architecture
```
staging_area (Raw Data Layer)
├── customers, products, marketing_campaigns, sales_transactions
│
data_warehouse (Analytical Layer)  
├── dim_customers, dim_products, dim_campaigns
├── fact_sales_transactions
│
data_quality_results (Governance Layer)
├── quality_results
│
metadata (Operations Layer)
├── lineage_sources, lineage_targets, lineage_transformations
└── lineage_relationships, lineage_execution_log
```

### Processing Pipelines

#### 1. Data Ingestion Layer
- **csv_ingestion_dag.py**: Customer data → Data Lake → BigQuery
- **api_ingestion_dag.py**: Product catalog → Data Lake → BigQuery  
- **firestore_ingestion_dag.py**: Campaign & transaction data → Data Lake → BigQuery

#### 2. Data Transformation Layer
- **transformation_dag.py**: Staging → Star Schema with quality validation

#### 3. Quality & Governance Layer
- **data_quality_dag.py**: Automated monitoring and alerting
- **Lineage System**: Real-time metadata capture and documentation

## Star Schema Design
```
dim_customers (10K) ──┐
                      │
dim_products (500) ───┼─── fact_sales_transactions (50K)
                      │
dim_campaigns (1K) ───┘
```

**Design Rationale**:
- **Customer Dimension**: Enables segmentation and demographic analysis
- **Product Dimension**: Supports catalog management and performance tracking
- **Campaign Dimension**: Drives marketing ROI and attribution analysis
- **Transaction Fact**: Central business process with comprehensive measures

## Quality & Governance Framework

### Data Quality Monitoring
- **Uniqueness**: Primary key validation across all tables
- **Completeness**: Critical field population monitoring (95%+ threshold)
- **Referential Integrity**: Foreign key validation (99%+ threshold)
- **Freshness**: Data recency within defined SLAs
- **Volume Anomaly**: Record count validation against expected ranges

### Lineage & Metadata Management
- **Source Registration**: 4 data sources with complete documentation
- **Transformation Mapping**: 24 transformation rules with field-level tracking
- **Pipeline Dependencies**: 8 target tables with full relationship mapping
- **Execution Tracking**: Real-time pipeline monitoring and audit logs

### Operational Capabilities
- **Manual Trigger Control**: All DAGs configured for controlled execution
- **Error Handling**: Comprehensive retry and failure notification
- **Performance Monitoring**: Execution timing and resource utilization
- **Scalability**: Designed for 61.5K+ record volumes

## Technology Stack
- **Orchestration**: Apache Airflow (Cloud Composer)
- **Data Warehouse**: Google BigQuery
- **Data Lake**: Google Cloud Storage
- **NoSQL Source**: Google Firestore
- **Quality Framework**: Custom SQL-based monitoring
- **Metadata System**: BigQuery-native lineage tracking

## Security & Compliance
- **Authentication**: Service account-based access control
- **Encryption**: Data encrypted at rest and in transit
- **Access Control**: Role-based permissions across all components
- **Audit Logging**: Complete data access and transformation tracking

## Performance Characteristics
- **Ingestion Throughput**: 61.5K records processed reliably
- **Transformation Speed**: Star schema updates within SLA
- **Query Performance**: Optimized for analytical workloads
- **Resource Efficiency**: Composer environment right-sized for workload

## External Access Readiness
- **Power BI Integration**: Ready for external access dataset
- **Tableau Support**: Optimized views for dashboard consumption
- **API Endpoints**: Framework for custom application integration
- **Security**: External access controls and monitoring prepared
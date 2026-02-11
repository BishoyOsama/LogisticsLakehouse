# Logistics Data Pipeline

A modern data engineering pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) using Apache Airflow, Databricks, dbt, and Kafka for both batch and streaming data processing.

## Architecture Overview

<img src="latest architecture.png" width="100%"/>

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow 3.1.7 |
| Data Platform | Databricks |
| Transformation | dbt (dbt-databricks) |
| Streaming | Apache Kafka |
| Containerization | Docker & Docker Compose |
| Database | PostgreSQL (Airflow metadata) |

## Project Structure

```
airflow/
├── dags/                      # Airflow DAG definitions
│   ├── bronze_layer.py        # Bronze layer ingestion DAG
│   ├── silver_layer.py        # Silver layer transformation DAG
│   ├── dbt_build.py           # dbt Gold layer build DAG
│   ├── produce_messages.py    # Kafka message producer DAG
│   └── trigger_dags.py        # Master DAG orchestrating pipeline
├── databricks_dbt/            # dbt project for Gold layer
│   ├── models/
│   │   ├── dim_customers.sql  # Customer dimension table
│   │   ├── dim_products.sql   # Product dimension table
│   │   ├── fact_orders.sql    # Orders fact table (incremental)
│   │   ├── sources.yml        # Source definitions
│   │   └── schema.yml         # Model documentation & tests
│   ├── tests/                 # Custom dbt tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── datasets/                  # Source data files
│   ├── source_crm/            # CRM system data
│   └── source_erp/            # ERP system data
├── scripts/
│   └── kafka_producer/        # Kafka message producer scripts
├── config/
│   └── airflow.cfg            # Airflow configuration
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Data Flow

### Pipeline Execution Sequence

The `trigger_dags` DAG orchestrates the entire pipeline:

```
produce_messages → bronze_layer → silver_layer → dbt_pipeline
```

1. **Produce Messages**: Generates synthetic order data and publishes to Kafka
2. **Bronze Layer**: Ingests raw data (batch + streaming) into Databricks Bronze tables
3. **Silver Layer**: Cleanses and transforms data into Silver tables
4. **dbt Pipeline**: Builds Gold layer dimensional models

### Medallion Layers

| Layer | Purpose | Processing |
|-------|---------|------------|
| **Bronze** | Raw data ingestion | Batch & Streaming (Kafka) |
| **Silver** | Data cleansing & standardization | Batch & Streaming |
| **Gold** | Business-ready dimensional models | dbt transformations |

### Data Sources

- **CRM System**: Customer info, product info, sales details
- **ERP System**: Customer demographics, locations, product categories
- **Kafka Streaming**: Real-time synthetic order data

### Gold Layer Models (dbt)

| Model | Type | Description |
|-------|------|-------------|
| `dim_customers` | Dimension | Customer master data combining CRM/ERP sources |
| `dim_products` | Dimension | Product catalog with categories |
| `fact_orders` | Fact (Incremental) | Order transactions (static + streaming) |

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Databricks workspace with configured jobs
- Confluent Kafka cluster (or equivalent)

### Environment Setup

1. **Clone the repository**

2. **Create environment file** (`.env`)
   ```env
   AIRFLOW_UID=50000
   ENV_FILE_PATH=.env
   
   # Databricks Configuration
   DATABRICKS_HOST=<your-databricks-workspace-url>
   DATABRICKS_TOKEN=<your-databricks-token>
   
   # Kafka Configuration (set as Airflow Variables)
   # bootstrap_servers, api_key, api_secret, client_id
   ```

3. **Build and start services**
   ```bash
   docker compose up -d
   ```

4. **Access Airflow UI**
   - URL: http://localhost:8080
   - Default credentials: `airflow` / `airflow`

### Airflow Configuration

1. **Create Databricks Connection**
   - Connection ID: `databricks-connection`
   - Connection Type: Databricks
   - Host: Your Databricks workspace URL
   - Token: Personal Access Token

2. **Set Airflow Variables** (for Kafka)
   - `bootstrap_servers`: Kafka broker addresses
   - `api_key`: Confluent Cloud API key
   - `api_secret`: Confluent Cloud API secret
   - `client_id`: Kafka client ID

### Running the Pipeline

1. **Trigger the complete pipeline**
   - Enable and trigger the `trigger_dags` DAG

2. **Or run individual layers**
   - `produce_messages` - Generate streaming data
   - `bronze_layer` - Ingest to Bronze
   - `silver_layer` - Transform to Silver
   - `dbt_pipeline` - Build Gold models

## dbt Commands

Run dbt locally or via the Airflow DAG:

```bash
cd databricks_dbt

# Build all models
dbt build

# Run models only
dbt run

# Test models
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Data Quality Tests

### dbt Tests
- `assert_price_not_less_zero` - Validates price is non-negative
- `assert_price_quantity_eq_sales` - Validates sales_amount = price × quantity
- `assert_ship_date_not_less_order_date` - Validates ship date >= order date
- `built-in tests for each model` - Test for columns built-in test like (not_null, accepted_values, unique, relationships)

### Source Freshness
- Kafka orders monitored for freshness (warn: 1 day, error: 2 days)

## Docker Services

| Service | Port | Description |
|---------|------|-------------|
| Airflow Dag Processor | - | Dag Processor |
| Airflow API Server | 8080 | Airflow UI |
| Airflow Scheduler | - | DAG scheduling |
| Airflow Init      | - | Airflow Initializer |
| PostgreSQL | 5432 | Airflow metadata DB |

## Monitoring

- **Airflow UI**: DAG runs, task logs, execution history
- **Databricks**: Job runs, Spark metrics
- **dbt**: Run results, test outcomes in `target/` directory

## Dependencies

Key Python packages (see `requirements.txt`):
- `apache-airflow-providers-databricks` - Databricks integration
- `dbt-databricks` - dbt Databricks adapter
- `kafka-python` / `confluent-kafka` - Kafka clients
- `faker` - Synthetic data generation
- `pandas` - Data manipulation

## Analytics Integration

The Gold layer data is ready for consumption by BI tools such as Power BI, Tableau, or Looker. Connect using Import mode for cached performance or DirectQuery/Live Connection for real-time analysis.


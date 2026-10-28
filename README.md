# RetailCo_Project
Retail Co Ecommerce End to End Data Engineering Project
# RetailCo Project

**An end-to-end data engineering project for a fictional e-commerce retailer, built on Databricks with a Bronze → Silver → Gold (medallion) architecture, config-driven data quality checks, incremental loading, and automated run logging.**

![Databricks](https://img.shields.io/badge/Databricks-Asset%20Bundles-FF3621?logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Auto%20Loader-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-MERGE%20%2F%20SCD-00ADD8)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Pipeline Layers](#pipeline-layers)
- [Data Quality Framework](#data-quality-framework)
- [Incremental Loading and Logging](#incremental-loading-and-logging)
- [Orchestration](#orchestration)
- [Gold KPIs](#gold-kpis)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Tech Stack](#tech-stack)

---

## Overview

RetailCo ingests raw operational data (customers, products, orders, returns and store events) in mixed file formats, cleans and conforms it into a star-schema model, and publishes business KPI tables ready for dashboards and reporting.

**Highlights**

- **Medallion architecture** on Delta Lake with Unity Catalog volumes and tables
- **Auto Loader** (`cloudFiles`) streaming ingestion with schema evolution, triggered on file arrival
- **Config-driven ingestion and data quality**: add a file or a rule by editing JSON, not code
- **Incremental Silver loads** using a watermark (control) table and Delta `MERGE`
- **Dimensional modelling**: surrogate keys, `is_active` flag for tracking changes on dimensions, hash-based change detection
- **Reusable DQ library** that logs every check (records read, dropped, quality %) to a table
- **Run-level observability** through job, pipeline and data quality log tables
- **Deployed as code** with Databricks Asset Bundles (dev and prod targets)

---

## Architecture

```
                       ┌──────────────────────────────┐
  Raw files            │        BRONZE (Delta)        │
  (Unity Catalog  ───► │  Auto Loader, append-only    │
   Volume)             │  + audit columns, row hash   │
  csv / json /         └──────────────┬───────────────┘
  parquet                             │  incremental (watermark)
                                      ▼
                       ┌──────────────────────────────┐
                       │        SILVER (Delta)        │
                       │  trim/standardize → dedup →  │
                       │  DQ checks → dimension joins │
                       │  → MERGE into dim/fact tables│
                       └──────────────┬───────────────┘
                                      │
                                      ▼
                       ┌──────────────────────────────┐
                       │         GOLD (Delta)         │
                       │  KPI tables per subject area │
                       │  (customers, orders, ...)    │
                       └──────────────────────────────┘

   Cross-cutting: control_table (watermarks) · job_logs · data_quality_logs · pipeline_run_log
```

---

## Data Sources

Raw files land in a Unity Catalog volume under `raw_data/<entity>/`. The entities and their formats are declared in
[`src/config_files/raw_configs/configs.json`](RetailCo_dev/src/config_files/raw_configs/configs.json):

| Entity         | Format  |
|----------------|---------|
| `customers`    | CSV     |
| `products`     | JSON    |
| `orders`       | Parquet |
| `store_events` | JSON    |
| `returns`      | CSV     |

---

## Pipeline Layers

### Bronze: raw ingestion

[`src/Bronze/Bronze_Ingestion.py`](RetailCo_dev/src/Bronze/Bronze_Ingestion.py)

- Loops over the ingestion config and reads each entity with **Auto Loader** (`cloudFiles`), writing append-only Delta with `availableNow` triggers.
- Uses schema inference and `addNewColumns` schema evolution; `products` uses explicit schema hints because of its multi-line JSON.
- Adds audit columns to every record: `Ingested_timestamp`, `file_name`, `folder_path`, `Job_Id`, `Run_id`, plus a SHA-256 `record_hash`.
- Writes per-table ingestion timings to `job_logs`.

### Silver: cleaned, conformed model

[`src/Silver/`](RetailCo_dev/src/Silver)

Each `*_V2.py` notebook follows the same pattern:

1. **Incremental read** of Bronze rows newer than the table's watermark (exits early with a `NO_DATA` log entry if nothing is new)
2. **Trim and standardize** (casing, whitespace, currency symbols, type normalization)
3. **Deduplicate** using a row hash and window functions
4. **Data quality checks** driven by JSON config (see below)
5. **Business transformations**, e.g. `gross_amount`, `net_amount`, `discount_amount`, `delivery_date`
6. **Join to dimensions** to swap natural keys for surrogate keys (`customer_sk`, `product_sk`, `payment_sk`)
7. **Upsert** into the target Delta table with `MERGE` (insert new, update changed by `record_hash`), then advance the watermark and write to `job_logs`

| Table              | Type      | Notes                                           |
|--------------------|-----------|-------------------------------------------------|
| `dim_customers`    | Dimension | Surrogate key, `is_active` change tracking      |
| `dim_products`     | Dimension | Surrogate key, `is_active` change tracking      |
| `dim_payment_mts`  | Dimension | Payment method lookup                           |
| `fact_orders`      | Fact      | Orders with computed revenue and delivery fields |
| `fact_returns`     | Fact      | Returned orders and refunds                     |
| `fact_store_events`| Fact      | Store and platform events                       |

### Gold: business KPIs

[`src/gold/`](RetailCo_dev/src/gold)

One notebook per subject area reads from the Silver model and overwrites KPI tables. See [Gold KPIs](#gold-kpis).

---

## Data Quality Framework

[`src/Silver/utils/retailco_dq.py`](RetailCo_dev/src/Silver/utils/retailco_dq.py) provides a reusable `retailco_dq` class. Rules live in per-table JSON files in
[`src/config_files/silver_configs/`](RetailCo_dev/src/config_files/silver_configs) and can be toggled with `"enabled": true/false`.

Supported checks:

| Check               | Purpose                                              |
|---------------------|------------------------------------------------------|
| `Not_Null`          | Drop records with null values in a column            |
| `datatype_handling` | Cast to `int`, `double`, `date`, etc.; drop failures |
| `pattern`           | Validate prefixes such as `ORD...` with a regex      |
| `greater_than`      | Numeric or date lower bound                          |
| `less_than`         | Numeric or date upper bound (e.g. not in the future) |
| `sequence_ck`       | Value must belong to an allowed list (status, payment method) |

Example rule (from `silver_orders_dq.json`):

```json
{ "check": "greater_than", "col_name": "quantity", "value_to_ck": 0, "enabled": true }
```

Every check writes a row to `data_quality_logs` with the job and run IDs, table, column, check name, records read and records dropped.

---

## Incremental Loading and Logging

| Table              | Purpose                                                                 |
|--------------------|-------------------------------------------------------------------------|
| `control_table`    | Watermark per `(source layer, target layer, table)` for incremental loads |
| `job_logs`         | Rows read and written, runtime and trigger metadata per table           |
| `data_quality_logs`| Per-check results (records read, dropped, quality %)                    |
| `pipeline_run_log` | Job and task level run history (state, duration, errors)                |

Table DDL lives in [`src/Queries/SQL - Table Creation.py`](RetailCo_dev/src/Queries).

---

## Orchestration

The pipeline is defined as a Databricks job in
[`resources/jobs/retailco_job.yml`](RetailCo_dev/resources/jobs/retailco_job.yml) and is triggered automatically **on file arrival** in the `store_events` raw volume.

```
Bronze_ingestion
   ├── dim_customers ─┐
   └── dim_products  ─┴─► fact_orders ─┬─► fact_returns ──────┐
                                       └─► fact_store_events ─┤
                                                              ▼
              customer_kpis · orders_kpi · products_kpi · returns_kpi · store_events_kpi
                                                              │
                                                              ▼
                                                          job_logs
```

Job metadata (run ID, job ID, trigger type and time, retry count) is passed to every task as parameters, so all log tables can be joined on `run_id`.

---

## Gold KPIs

| Area         | KPI tables                                                                                                                       |
|--------------|-----------------------------------------------------------------------------------------------------------------------------------|
| **Customers**| `new_customers_this_month`, `new_customers_90_days`, `new_customers_180_days`, `new_customers_1_year`, `top_5_month_with_highest_customer`, `active_customers_in_each_segment`, `active_customers_in_each_region` |
| **Orders**   | `Monthly_Net_and_gross_revenue`, `orders_shipped_10_days_dayWise`, `orders_delivered_10_days_dayWise`, `top_5_customers_with_orders`, `top_5_products_with_high_orders`, `top_5_products_with_high_quantity_sold`, `top_5_products_with_high_revenue`, `orders_kpi` (fulfillment, cancellation and return rates, average delivery days, total quantity sold) |
| **Products** | `Top_5_selling_products_overall`, plus product and supplier counts |
| **Returns**  | `total_orders_returned`, `top_5_products_returned_most`, `top_5_customers_returned_most`, `total_Amount_refunded_vs_net`       |
| **Store events** | `total_events_stored`, `platform_with_more_customers`                                                                     |

---

## Repository Structure

```
RetailCo_Project/
├── README.md
└── RetailCo_dev/                      # Databricks Asset Bundle
    ├── databricks.yml                 # Bundle config: variables, dev/prod targets
    ├── pyproject.toml                 # Python project and dev dependencies (uv)
    ├── resources/
    │   └── jobs/
    │       ├── retailco_job.yml       # Main medallion pipeline job
    │       └── demo_job.yml           # Demo job
    ├── src/
    │   ├── Bronze/                    # Auto Loader ingestion
    │   ├── Silver/                    # Dimension and fact transformations
    │   │   └── utils/                 # retailco_dq (data quality), logging helpers
    │   ├── gold/                      # KPI notebooks per subject area
    │   ├── Job_logs/                  # Run logging notebooks
    │   ├── Queries/                   # Table creation DDL
    │   └── config_files/
    │       ├── raw_configs/           # Ingestion config (entity + format)
    │       └── silver_configs/        # Data quality rules per table
    └── fixtures/                      # Test data fixtures
```

---

## Getting Started

### Prerequisites

- A Databricks workspace with Unity Catalog enabled
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install) installed and authenticated (`databricks configure`)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) for local development
- Python 3.12

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/Praveenrpk123/RetailCo_Project.git
cd RetailCo_Project/RetailCo_dev

# 2. Install dev dependencies
uv sync --dev

# 3. Authenticate to your workspace
databricks configure
```

### Prepare the workspace

1. Create the catalog and schema referenced in `databricks.yml` (or change the bundle variables to match yours).
2. Create the `raw_data` and `bronze` volumes in that schema.
3. Run the DDL in `src/Queries/SQL - Table Creation.py` to create the `job_logs`, `pipeline_run_log` and `control_table` tables.
4. Upload source files to `/Volumes/<catalog>/<schema>/raw_data/<entity>/`.

### Deploy and run

```bash
# Validate the bundle
databricks bundle validate

# Deploy to the dev target (default)
databricks bundle deploy --target dev

# Run the pipeline job
databricks bundle run DAB_retailco_pipeline_v2

# Deploy to production
databricks bundle deploy --target prod
```

The job also starts automatically when new files arrive in the configured volume path.

---

## Configuration

Bundle variables are set in [`databricks.yml`](RetailCo_dev/databricks.yml) and passed to every notebook as job parameters:

| Variable       | Description                | Dev default              |
|----------------|----------------------------|--------------------------|
| `catalog_name` | Unity Catalog catalog      | `retailco_dev_catalog`   |
| `schema_name`  | Schema for all layers      | `retailCo_dev_schema`    |

To add a new source entity, add an entry to `raw_configs/configs.json`. To add or disable a data quality rule, edit the matching file in `silver_configs/`.

---

## Tech Stack

- **Platform**: Databricks (Jobs, Unity Catalog, Volumes)
- **Processing**: PySpark, Structured Streaming, Auto Loader
- **Storage**: Delta Lake
- **Deployment**: Databricks Asset Bundles (CLI)
- **Tooling**: Python 3.12, uv, pytest, ruff

---

## Author

**Praveen Kumar** · [GitHub](https://github.com/Praveenrpk123)

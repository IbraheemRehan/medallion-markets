# 📈 Medallion Markets: Financial Data Lakehouse & Analytics Warehouse

An end-to-end modern data stack project that ingests stock market and company fundamentals data from Alpha Vantage, lands raw JSON payloads into a BigQuery **Bronze** layer, builds an **SCD Type 2** dimension & point-in-time fact model via **dbt**, enforces data quality with **dbt-expectations**, and orchestrates the entire lineage using **Dagster**.

---

## 🏛️ System Architecture & Medallion Pipeline

```
[ Alpha Vantage API ]
         │
         ▼ (Python / Requests)
[ Raw Ingestion Files ] ──► (Local JSON Storage)
         │
         ▼ (Google BigQuery Load Jobs)
[ Bronze Dataset ] ──────► Raw JSON tables (`overview`, `time_series_daily`)
         │
         ▼ (dbt Views & CTE Transformations)
[ Staging Layer ] ───────► `stg_overview`, `stg_daily_prices`
         │
         ▼ (dbt Marts & Window Functions)
[ Gold / Marts Layer ] ──► `dim_company_overview_scd2` (SCD Type 2)
                         └► `fct_daily_prices` (Point-in-Time Fact Table)
```

---

## ✨ Key Features & Technical Highlights

1. **Immutable Bronze Layer**: Ingests raw JSON data directly into Google BigQuery using native Python Load Jobs (`WRITE_APPEND` / `WRITE_TRUNCATE`).
2. **Robust Staging Cleanups**:
   - Parses complex dynamic key JSON structures using BigQuery `json_keys` with `max_depth = 1`.
   - Safely parses compact ISO timestamp strings (`YYYYMMDDTHHMMSSZ`) using `SAFE.PARSE_TIMESTAMP`.
   - Filters out API rate-limit responses (`"Information": "standard API rate limit..."`) to ensure clean staging data.
3. **SCD Type 2 Dimension Tracking (`dim_company_overview_scd2`)**:
   - Detects attribute changes over time (e.g. `Sector`, `Exchange`, `MarketCap`) using `LAG` and `SUM` window functions.
   - Generates effective validity windows (`dbt_valid_from`, `dbt_valid_to`, `is_current`).
   - Works seamlessly on BigQuery Sandbox (Free Tier) without requiring DML billing permissions.
4. **Point-in-Time Fact Model (`fct_daily_prices`)**:
   - Joins daily stock prices against historical dimension states using valid date range boundaries (`price_date >= date(effective_valid_from)` and `price_date < date(dbt_valid_to)`).
5. **Data Quality & Testing**:
   - Includes 20+ automated tests across `dbt-utils`, `dbt-expectations`, and generic tests (`not_null`, `unique`, `accepted_values`, `expect_column_values_to_be_between`).
   - Includes `dbt source freshness` checks on source tables.
6. **Dagster Orchestration**:
   - Unified execution graph mapping API ingestion, BigQuery loading, and dbt models into a single lineage graph.
   - Uses `@dbt_assets` and `CustomDagsterDbtTranslator` for automatic asset discovery and dependency mapping.
7. **CI/CD Pipeline**:
   - GitHub Actions workflow (`.github/workflows/dbt_ci.yml`) for automated linting (`sqlfluff`) and build verification against BigQuery CI target.

---

## 🛠️ Tech Stack

* **Orchestration**: Dagster (`dagster-dbt`)
* **Transformation & Data Modeling**: dbt Core (`dbt-bigquery`, `dbt-utils`, `dbt-expectations`)
* **Data Warehouse**: Google BigQuery
* **Data Ingestion**: Python (`google-cloud-bigquery`, `requests`)
* **Data Provider**: Alpha Vantage API
* **CI/CD & Quality**: GitHub Actions, SQLFluff

---

## 🚀 Quickstart & Setup Guide

### 1. Prerequisites & Environment Variables
Copy `.env.example` (or set environment variables):
```ini
ALPHA_VANTAGE_API_KEY=your_api_key
BQ_PROJECT_ID=your_gcp_project_id
BQ_DATASET=bronze
GOOGLE_APPLICATION_CREDENTIALS=path/to/service_account.json
```

### 2. Install Dependencies
```powershell
pip install -r requirements.txt
```

### 3. Run Pipeline via Dagster UI
```powershell
cd dagster_project
dagster dev
```
Navigate to `http://localhost:3000` and click **Materialize All**.

---

## 📄 License
MIT License

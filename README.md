
![Corporate Spend Analytics](https://github.com/DarlyP/Corporate-Spend-and-Vendor-Risk-Analytics-Pipeline/blob/main/Risk%20wallpaper.jpg)

# Corporate Spend and Vendor Risk Analytics Pipeline

---

## Tools

[<img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow" />](https://airflow.apache.org/)
[<img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark" />](https://spark.apache.org/)
[<img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="Postgres" />](https://www.postgresql.org/)
[<img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />](https://www.docker.com/)
[<img src="https://img.shields.io/badge/DBeaver-372923?style=for-the-badge&logoColor=white" alt="DBeaver" />](https://dbeaver.io/)
[<img src="https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=000000" alt="Power BI" />](https://powerbi.microsoft.com/)

---

## Data Source

[<img src="https://img.shields.io/badge/DC%20Open%20Data-E41E2B?style=for-the-badge&logoColor=white" alt="DC Open Data" />](https://opendata.dc.gov/datasets/92842dceac234b9ca1a8266fcfd57de7_50/explore)

---

## Dashboard

**Power BI Dashboard** :  
[Dashboard – Corporate Spend and Vendor Risk Analytics Pipeline](https://drive.google.com/file/d/1oxTfJwkk38d9CfD9S0ei3hF81WgmrNTL/view?usp=sharing)

**Presentation Deck (PDF)** :  
[Presentation – Corporate Spend and Vendor Risk Analytics Pipeline](https://drive.google.com/file/d/19Ha13SNwivKvhx_6bm8f1IBAjTSW0-Il/view?usp=sharing)

---

## Introduction

This project simulates an **end-to-end enterprise analytics pipeline** for corporate card spend / procurement transactions using a modern stack:

- **Airflow** orchestrates jobs (Bronze → Silver → Gold)
- **PySpark** cleans & transforms raw CSV into analytics-ready tables
- **Postgres** becomes the **serving layer** (dim/fact + BI views)
- **Power BI** consumes curated BI views for executive dashboards

The portfolio focus is not only spend reporting, but also **Risk & Early Warning signals**:
- **Outlier detection** (transaction unusually high vs category statistics)
- **New vendor monitoring** (vendor first-seen month)
- **Risk scoring** (simple explainable score for top risky vendors)

---

## Business Context

Corporate spending data often comes from multiple sources with inconsistent formats (different column names, date formats, vendor naming). As transaction volume grows, it becomes difficult for Finance, Procurement, and Audit teams to quickly answer:

- How much are we spending month-to-month, and where is it going?
- Which vendors dominate spend (vendor concentration risk)?
- Which agencies/departments are overspending and in which categories?
- Are there unusual transactions (outliers) or risky vendor patterns that need investigation?

This project solves that by building an automated pipeline plus a BI-ready serving layer.

---

## Deliverables

- **Automated data pipeline** (Airflow + PySpark) producing:
  - **Bronze**: raw CSV landing
  - **Silver**: cleaned & standardized transactions (Parquet + CSV)
  - **Gold**: aggregates + anomaly detection outputs (Parquet + CSV)
- **PostgreSQL Serving Layer**:
  - `stage` schema for raw loads
  - `corp` schema for curated fact/dim + gold tables
  - `bi` schema for Power BI views (star-schema friendly)
- **Power BI Dashboard** (Executive + Agency/Category + Risk & Anomaly)
- **Risk Scoring** module (simple, explainable scoring)

---

## Architecture Overview

**Data Flow**
1. Raw dataset lands in `data/bronze/` (CSV)
2. PySpark cleans/transforms → `data/silver/` (Parquet + CSV folder)
3. PySpark aggregates/anomaly flags → `data/gold/` (Parquet + CSV folder)
4. CSV outputs loaded into Postgres `stage.*` tables
5. SQL transforms stage → `corp.*` curated tables + `bi.*` views
6. Power BI connects to Postgres and reads from `bi.*` views

**Tools**
- Orchestration: **Apache Airflow**
- Processing: **PySpark**
- Serving layer: **PostgreSQL**
- SQL client: **DBeaver**
- BI: **Power BI**

---

## Repository Structure

```
.
├── airflow/
│   ├── dags/
│   │   ├── corporate_spend_silver_dag.py
│   │   ├── corporate_spend_gold_dag.py
│   │   └── (optional) corporate_spend_risk_dag.py
│   ├── Dockerfile
│   └── requirements.txt
├── spark_job/
│   ├── clean_transform.py
│   ├── build_aggregates.py
│   └── (optional) build_risk_scores.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── sql/
│   ├── 00_serving_layer.sql
│   ├── 01_load_corp_from_stage.sql
│   ├── 02_bi_views.sql   (optional if separated)
│   └── 03_indexes.sql
├── docker-compose.yml
└── README.md
````

---

## Setup & Run

### Prerequisites

* Docker + Docker Compose
* Power BI Desktop (Windows)
* DBeaver (optional but recommended)

### Start the stack

```bash
docker compose up -d --build
```

### Persistence Note (IMPORTANT)

* Your Postgres data persists via Docker volume: `postgres_data`.
* If you run:

```bash
docker compose down -v
```

it **deletes the volume**, so all tables/views disappear.
Use instead:

```bash
docker compose down
```

if you want to stop containers without losing data.

---

## Pipeline Steps (Bronze → Silver → Gold)

### Step 1 — Bronze (Raw Landing)

Place raw CSV files in:

```text
data/bronze/*.csv
```

### Step 2 — Silver (Clean & Standardize) — `clean_transform.py`

The Silver job:

* Reads all bronze CSVs
* Standardizes column names across datasets (transaction_id, vendor, agency, date, amount)
* Cleans numeric amount (`$`, commas)
* Parses multiple datetime formats robustly
* Adds engineered fields:

  * `year`, `month`, `yyyymm`
  * `day_of_week`, `is_weekend`
  * `amount_bucket`
  * `category_group` (simple text-based mapping)
* Deduplicates by `transaction_id`
* Writes:

  * `data/silver/spend_clean.parquet`
  * `data/silver/spend_clean_csv/part-*.csv`

Run via Airflow DAG:

* `corporate_spend_silver_dag` → task `spark_clean_transform_silver`

### Step 3 — Gold (Aggregates + Flags) — `build_aggregates.py`

Gold job creates BI-friendly outputs:

1. **Vendor Monthly Spend**

   * `txn_count`, `total_spend`, `avg_spend`
2. **Agency × Category Spend**

   * monthly spend breakdown by agency & category_group
3. **Transaction Anomaly Table**

   * `is_outlier`: outlier rule (example: amount > mean + 3*std by category)
   * `is_new_vendor_month`: first month vendor appears

Writes:

* Parquet outputs under `data/gold/`
* CSV folders:

  * `data/gold/vendor_monthly_spend_csv/`
  * `data/gold/agency_category_spend_csv/`
  * `data/gold/transaction_anomaly_csv/`

Run via Airflow DAG:

* `corporate_spend_gold_dag` → task `spark_build_aggregates_gold`

### Step 4 — Risk Scoring (Explainable)

Risk scoring is designed to be simple and interview-friendly:

Example:

* `risk_score = 70*is_outlier + 30*is_new_vendor_month`

You can implement:

* Option A: a dedicated Spark job + DAG writing `vendor_risk_scores_csv`
* Option B: compute in SQL in Postgres (recommended for BI serving)

---

## PostgreSQL Serving Layer (DBeaver / SQL)

### Schemas

* `stage`: raw imported outputs (landing tables from CSV)
* `corp`: curated fact/dim + gold tables
* `bi`: Power BI views (clean, stable interface)

### Create schemas/tables/views

Run:

* `sql/00_serving_layer.sql`
  This creates:
* `stage.*` raw tables
* `corp.*` curated tables
* `bi.*` views

### Load CSV outputs into `stage.*`

Load the Gold/Silver CSV folders into Postgres staging tables:

* `stage.silver_spend_clean_raw`
* `stage.gold_vendor_monthly_raw`
* `stage.gold_agency_category_raw`
* `stage.gold_anomaly_raw`

(You can use DBeaver Import, or `COPY` if you mount files to the Postgres container.)

### Transform stage → corp (curated)

Run:

* `sql/01_load_corp_from_stage.sql` (recommended)
  This step:
* Upserts dimension tables `corp.dim_agency`, `corp.dim_vendor`
* Loads/refreshes fact table `corp.fact_transactions_clean`
* Loads gold tables `corp.vendor_monthly_spend`, `corp.agency_category_spend`, `corp.transaction_anomaly`
* Builds `corp.vendor_risk_scores`

### Performance indexes

Run:

* `sql/03_indexes.sql`

### BI Views (Power BI reads only these)

Key views (bi schema):

* `bi.v_dim_date` (daily date table)
* `bi.v_dim_month` (monthly date table)
* `bi.v_exec_kpi` (monthly KPI summary)
* `bi.v_vendor_monthly_spend`
* `bi.v_agency_category_spend`
* `bi.v_transaction_anomaly`
* `bi.v_vendor_risk_scores`

---

## Power BI Setup

### Connect to Postgres

**Get Data → PostgreSQL**

* Host: `localhost`
* Port: `5432`
* Database: `airflow` (or your POSTGRES_DB)
* Username/Password: from `.env` or docker-compose defaults

### Load data

Load the **BI views** only (recommended):

* `bi.v_dim_date`
* `bi.v_dim_month`
* `bi.v_exec_kpi`
* `bi.v_vendor_monthly_spend`
* `bi.v_agency_category_spend`
* `bi.v_transaction_anomaly`
* `bi.v_vendor_risk_scores`

> Note: You may choose “Import” mode for speed. DirectQuery is possible but requires more tuning.

---

## Power BI Data Model (Star Schema + Measures)

### Why relationships matter

Correct relationships prevent:

* double counting KPIs,
* inconsistent slicer behavior,
* visuals not syncing.

### Relationship rules used here

**Monthly tables must join to a monthly dimension**
You should NOT use `bi.v_dim_date[yyyymm]` as the “1” side because `v_dim_date` is **daily**, meaning `yyyymm` repeats many times per month.

**Solution**

* Use `bi.v_dim_month[yyyymm]` for monthly joins (unique key per month)
* Use `bi.v_dim_date[date]` for daily joins (unique per day)

### Recommended Relationships

**dim_month → monthly tables**

* `bi.v_dim_month[yyyymm] (1)` → `bi.v_exec_kpi[yyyymm] (*)`
* `bi.v_dim_month[yyyymm] (1)` → `bi.v_vendor_monthly_spend[yyyymm] (*)`
* `bi.v_dim_month[yyyymm] (1)` → `bi.v_agency_category_spend[yyyymm] (*)`
* `bi.v_dim_month[yyyymm] (1)` → `bi.v_vendor_risk_scores[yyyymm] (*)`

**dim_date → anomaly table**

* `bi.v_dim_date[date] (1)` → `bi.v_transaction_anomaly[transaction_date] (*)`

**Relationship Settings**

* Cardinality: **One-to-many (1:*)**
* Cross-filter direction: **Single**
* Active: **Yes**

---

## DAX Measures (Global KPIs)

Create measures in Power BI (example uses `bi_v_exec_kpi` table name as imported):

```DAX
Total Spend = SUM('bi_v_exec_kpi'[total_spend])

Total Txn = SUM('bi_v_exec_kpi'[txn_count])

Active Vendors = SUM('bi_v_exec_kpi'[active_vendors])

Outlier Txn = SUM('bi_v_exec_kpi'[outlier_txn])

New Vendor Txn = SUM('bi_v_exec_kpi'[new_vendor_txn])

Flagged Txn = [Outlier Txn] + [New Vendor Txn]

Avg Spend / Txn = DIVIDE([Total Spend], [Total Txn])
```

These measures act as **global KPI definitions** that every visual can reuse consistently.

---

## Business Value & Impact

This pipeline converts raw transactions into a consistent monitoring system:

* **Faster visibility** into spend and vendor exposure
* **Early warning** for unusual transactions (outliers + new vendor activity)
* **Targeted audit** and better prioritization (risk-based ranking)
* **Procurement leverage**: understand vendor concentration and spend patterns

---

## Limitations & What’s Next

### Limitations

* Vendor normalization is basic (uppercase + trim; still potential duplicates).
* Outlier detection uses simple rules (mean + 3*std), may be sensitive to skew.
* Loading into Postgres is not fully incremental (depends on current load strategy).
* DQ monitoring/alerts are minimal.

### Next Improvements

* Add stronger vendor dedup logic (fuzzy matching + vendor_key).
* Use percentile-based outliers (P95/P99) per category/vendor.
* Implement incremental loads and upserts by partition (`yyyymm`).
* Add data quality thresholds + alerts (row count, null %, duplicates).
* Add drill-through vendor detail pages and tooltips in Power BI.

---

## How to Reproduce End-to-End

1. Start services:

   ```bash
   docker compose up -d --build
   ```
2. Put raw CSV into `data/bronze/`
3. Run Airflow DAGs:

   * `corporate_spend_silver_dag`
   * `corporate_spend_gold_dag`
4. Load CSV outputs into Postgres `stage.*`
5. Run SQL:

   * `00_serving_layer.sql`
   * `01_load_corp_from_stage.sql`
   * `03_indexes.sql`
6. Open Power BI:

   * connect to Postgres
   * load `bi.*` views
   * set relationships + add measures
   * build visuals

---

## Notes

* **Do not run** `docker compose down -v` unless you intentionally want to wipe database storage.
* Power BI relationships:

  * monthly tables must join to **dim_month**
  * anomaly must join to **dim_date**

---

## Disclaimer

This project is for **learning and portfolio purposes** only.
All data used is assumed to be **public / non-sensitive**, and the pipeline is not intended for real production financial decisioning without additional controls, validation, and governance.







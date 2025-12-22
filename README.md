
![Corporate Spend Analytics](https://github.com/<YOUR_GITHUB_USERNAME>/<YOUR_REPO_NAME>/blob/main/assets/cover-corporate-spend.jpg)

# Corporate Spend Analytics Pipeline — Airflow + PySpark + Postgres + Power BI

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

This project is designed to ingest **any public corporate spend CSV** (configurable using `DATASET_CSV_URL` in `.env`).  
➡️ Replace this with your dataset link when publishing your repo:
- **Dataset Link**: `https://...`

---

## Dashboard

**Power BI Report (PBIX)**  
- Put your `.pbix` file in: `powerbi/CorporateSpendAnalytics.pbix`
- Power BI connects to Postgres **BI views** (`bi.*`) for reporting.

**Recommended Pages**
1. **Executive Overview** (Spend trend, KPI cards, Top vendors)
2. **Agency / Department View** (Category spend by agency)
3. **Risk & Anomaly** (Outliers + new vendors + risk score)

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

## Architecture

```
          +-------------------+
          |  Source CSV (URL) |
          +---------+---------+
                    |
                    v

+-------------------+-------------------+
| BRONZE (raw CSV files in /data/bronze)|
+-------------------+-------------------+
                    |
                    v
+-------------------+-------------------+
| SILVER (clean parquet + csv exports)   |
| - spend_clean.parquet                 |
| - spend_clean_csv/                    |
+-------------------+-------------------+
                    |
                    v
+-------------------+-------------------+
| GOLD (aggregates + anomaly tables)    |
| - vendor_monthly_spend                |
| - agency_category_spend               |
| - transaction_anomaly                 |
| - vendor_risk_scores                  |
+-------------------+-------------------+
                    |
                    v
+-------------------+-------------------+
| POSTGRES serving layer                |
| schemas: stage -> corp -> bi          |
| BI connects to bi.* views             |
+---------------------------------------+

````


## Pipeline Steps (Big Picture)

### Step 1 — Setup (Docker + Airflow + Postgres)
- Start services with Docker Compose
- Postgres stores Airflow metadata **and** analytics serving layer (schemas `stage/corp/bi`)

### Step 2 — Bronze (Ingest raw CSV)
- Download / ingest CSV into: `data/bronze/*.csv`

### Step 3 — Silver (Clean + Standardize) — PySpark
- Standardize column names
- Clean numeric amount
- Robust date parsing
- Add engineered features:
  - `year`, `month`, `yyyymm`, `is_weekend`, `amount_bucket`, `category_group`
- Output:
  - `data/silver/spend_clean.parquet`
  - `data/silver/spend_clean_csv/`

### Step 4 — Gold (Aggregates + Risk Flags) — PySpark
From Silver parquet, build:
- **Vendor monthly spend** (txn_count, total_spend, avg_spend)
- **Agency x Category spend**
- **Transaction anomaly** table (outliers + new vendor month flags)

Output:
- `data/gold/vendor_monthly_spend_*`
- `data/gold/agency_category_spend_*`
- `data/gold/transaction_anomaly_*`

### Step 5 — Postgres Serving Layer (SQL + DBeaver)
- Create schemas & tables (stage/corp/bi)
- Load CSV exports into `stage.*`
- Transform into star schema `corp.*`
- Publish Power BI friendly views in `bi.*`

### Step 6 — Risk Scoring (Explainable)
Example scoring:
- `risk_score = 70*is_outlier + 30*is_new_vendor`
Aggregated per vendor-month:
- `corp.vendor_risk_scores`
- `bi.v_vendor_risk_scores`

### Step 7 — Power BI Dashboard
Power BI connects to Postgres **views**:
- `bi.v_vendor_monthly_spend`
- `bi.v_agency_category_spend`
- `bi.v_transaction_anomaly`
- `bi.v_vendor_risk_scores`
- `bi.v_exec_kpi`
- `bi.v_dim_date` + `bi.v_dim_month`

---

## Postgres Serving Layer Design (Recommended)

### Schemas
- `stage` = raw landing tables (loaded from CSV exports)
- `corp` = cleaned star schema (dimensions + facts + gold tables)
- `bi` = reporting views (stable contract for Power BI)

### Key BI Views
- `bi.v_vendor_monthly_spend`
- `bi.v_agency_category_spend`
- `bi.v_transaction_anomaly`
- `bi.v_vendor_risk_scores`
- `bi.v_exec_kpi` *(monthly KPI summary)*
- `bi.v_dim_date` *(daily calendar)*
- `bi.v_dim_month` *(unique yyyymm calendar — IMPORTANT for monthly relationships)*

✅ **Why `v_dim_month` matters?**  
Power BI requires the “1-side” key to be unique.  
`v_dim_date` is daily → `yyyymm` repeats many times → cannot be the 1-side for a monthly relationship.  
So:
- Use `v_dim_date[date]` to relate to `transaction_date`
- Use `v_dim_month[yyyymm]` to relate to monthly tables (`yyyymm`)

---

## Power BI Data Model (Recommended)

### Relationships
1. `bi.v_dim_date[date]` (1) → (many) `bi.v_transaction_anomaly[transaction_date]`
2. `bi.v_dim_month[yyyymm]` (1) → (many)  
   - `bi.v_vendor_monthly_spend[yyyymm]`  
   - `bi.v_agency_category_spend[yyyymm]`  
   - `bi.v_vendor_risk_scores[yyyymm]`  
   - `bi.v_exec_kpi[yyyymm]`

### Measures (Examples)
```DAX
Total Spend = SUM(bi_v_vendor_monthly_spend[total_spend])
Total Txn   = SUM(bi_v_vendor_monthly_spend[txn_count])
Active Vendors = SUM(bi_v_exec_kpi[active_vendors])
Outlier Txn = SUM(bi_v_exec_kpi[outlier_txn])
````

---

## How To Run

### 1) Start services

```bash
docker compose up -d --build
```

### 2) Run Airflow pipelines

* Open Airflow UI: `http://localhost:8080`
* Trigger DAGs in order:

  1. Bronze ingest DAG (if you have it)
  2. `corporate_spend_silver_dag`
  3. `corporate_spend_gold_dag`

### 3) Load to Postgres + create BI views (DBeaver)

* Connect DBeaver:

  * Host: `localhost`
  * Port: `5432`
  * DB: `airflow`
  * User: `airflow`
  * Password: `airflow` *(or from `.env`)*

Run SQL scripts (recommended order):

1. `sql/00_serving_layer.sql` (schemas + stage/corp tables + bi views)
2. `sql/01_load_corp_from_stage.sql` (insert dim/fact/gold from stage)
3. `sql/02_risk_score.sql` (populate vendor risk scores)
4. `sql/03_indexes.sql` (indexes)

### 4) Power BI

* Get Data → PostgreSQL
* Select **Import** (recommended) or DirectQuery
* Load tables/views from schema `bi`
* Build visuals

---

## Persistence Note (IMPORTANT)

If you run:

```bash
docker compose down -v
```

Docker will remove volumes including `postgres_data` → your Postgres schemas/tables/views will be deleted.

✅ To keep Postgres data, use:

```bash
docker compose down
```

or just restart Docker / laptop and run:

```bash
docker compose up -d
```

---

## Folder Structure

```
.
├── airflow/
│   ├── dags/
│   ├── logs/
│   └── Dockerfile
├── spark_job/
│   ├── clean_transform.py
│   └── build_aggregates.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── sql/
│   ├── 00_serving_layer.sql
│   ├── 01_load_corp_from_stage.sql
│   ├── 02_risk_score.sql
│   └── 03_indexes.sql
└── powerbi/
    └── CorporateSpendAnalytics.pbix
```

---

## Conclusion

This repository demonstrates a realistic analytics workflow:

* **Reliable orchestration** (Airflow)
* **Scalable transformation** (Spark)
* **Enterprise serving layer** (Postgres: stage → corp → bi)
* **Business-ready BI** (Power BI executive + risk views)

The key portfolio differentiator is the **Risk & Anomaly layer**, showing how spend analytics can be used as **early warning signals** for:

* suspicious transactions,
* abnormal spend patterns,
* and vendor onboarding risks.

---

## Limitations

* Outlier detection is intentionally simple (mean + 3*std) and should be improved for production (robust stats / quantiles).
* Risk score is explainable but not a predictive model (no supervised learning here).
* Dataset schema may vary across sources; the pipeline assumes a minimum set of required columns.

---

## What’s Next

* Add **time-based monitoring** (monthly drift, outlier trend)
* Add **more anomaly rules**:

  * weekend high-spend
  * sudden vendor spend spikes
  * agency budget threshold alerts
* Optional: deploy dashboards via **Power BI Service** and publish link
* Add **data quality metrics** table + dashboard page

---

## Disclaimer

This project is for **learning and portfolio purposes** only.
All data used is assumed to be **public / non-sensitive**, and the pipeline is not intended for real production financial decisioning without additional controls, validation, and governance.

```
```


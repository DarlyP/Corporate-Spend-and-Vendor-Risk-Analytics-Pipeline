SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'stage'
  AND table_name   = 'silver_spend_clean_raw'
ORDER BY character_maximum_length NULLS LAST, column_name;

ALTER TABLE stage.silver_spend_clean_raw
  ALTER COLUMN agency_name_clean TYPE text;

SELECT COUNT(*) FROM stage.silver_spend_clean_raw;
SELECT COUNT(*) FROM stage.gold_vendor_monthly_raw;
SELECT COUNT(*) FROM stage.gold_agency_category_raw;
SELECT COUNT(*) FROM stage.gold_anomaly_raw;

SELECT COUNT(*) FROM corp.fact_transactions_clean;
SELECT COUNT(*) FROM corp.vendor_monthly_spend;
SELECT COUNT(*) FROM corp.agency_category_spend;
SELECT COUNT(*) FROM corp.transaction_anomaly;
SELECT COUNT(*) FROM corp.vendor_risk_scores;

SELECT * FROM bi.v_vendor_monthly_spend LIMIT 10;
SELECT * FROM bi.v_agency_category_spend LIMIT 10;
SELECT * FROM bi.v_transaction_anomaly LIMIT 10;
SELECT * FROM bi.v_vendor_risk_scores ORDER BY risk_score DESC LIMIT 10;

-- 1) Row counts
SELECT 'fact' AS tbl, COUNT(*) FROM corp.fact_transactions_clean
UNION ALL SELECT 'vendor_monthly', COUNT(*) FROM corp.vendor_monthly_spend
UNION ALL SELECT 'agency_category', COUNT(*) FROM corp.agency_category_spend
UNION ALL SELECT 'anomaly', COUNT(*) FROM corp.transaction_anomaly
UNION ALL SELECT 'risk', COUNT(*) FROM corp.vendor_risk_scores;

-- 2) Top vendors YTD (contoh)
SELECT vendor_name_clean, SUM(total_spend) AS spend
FROM bi.v_vendor_monthly_spend
GROUP BY 1
ORDER BY spend DESC
LIMIT 10;

-- 3) Top risky vendors (buat screenshot portfolio)
SELECT *
FROM bi.v_vendor_risk_scores
ORDER BY risk_score DESC, outlier_txn DESC
LIMIT 20;

-- 4) Latest anomalies
SELECT *
FROM bi.v_transaction_anomaly
WHERE is_outlier = true OR is_new_vendor = true OR is_high_amount = true
ORDER BY transaction_date DESC
LIMIT 50;

-- =========================================
-- 04_bi_dim_date.sql
-- Calendar table view for Power BI
-- =========================================
CREATE SCHEMA IF NOT EXISTS bi;

CREATE OR REPLACE VIEW bi.v_dim_date AS
WITH bounds AS (
  SELECT
    MIN(transaction_date)::date AS min_date,
    MAX(transaction_date)::date AS max_date
  FROM corp.fact_transactions_clean
),
dates AS (
  SELECT generate_series(min_date, max_date, interval '1 day')::date AS date_day
  FROM bounds
)
SELECT
  date_day AS date,
  EXTRACT(YEAR  FROM date_day)::int AS year,
  EXTRACT(MONTH FROM date_day)::int AS month,
  (EXTRACT(YEAR FROM date_day)::int * 100 + EXTRACT(MONTH FROM date_day)::int) AS yyyymm,
  EXTRACT(DAY FROM date_day)::int AS day,
  EXTRACT(DOW FROM date_day)::int AS dow_0_sun,          -- 0=Sunday ... 6=Saturday
  CASE WHEN EXTRACT(DOW FROM date_day) IN (0,6) THEN true ELSE false END AS is_weekend,
  TO_CHAR(date_day, 'Mon') AS month_short,
  TO_CHAR(date_day, 'Month') AS month_name,
  TO_CHAR(date_day, 'YYYY-MM') AS year_month,
  DATE_TRUNC('month', date_day)::date AS month_start
FROM dates;

-- =========================================
-- 05_bi_exec_kpi.sql
-- Executive KPI summary per month (yyyymm)
-- =========================================
CREATE SCHEMA IF NOT EXISTS bi;

CREATE OR REPLACE VIEW bi.v_exec_kpi AS
WITH base AS (
  SELECT
    yyyymm,
    year,
    month,
    SUM(total_spend) AS total_spend,
    SUM(txn_count) AS txn_count,
    COUNT(DISTINCT vendor_name_clean) AS active_vendors
  FROM bi.v_vendor_monthly_spend
  GROUP BY yyyymm, year, month
),
outlier AS (
  SELECT
    yyyymm,
    COUNT(*) FILTER (WHERE is_outlier = true) AS outlier_txn,
    COUNT(*) FILTER (WHERE is_new_vendor = true) AS new_vendor_txn
  FROM bi.v_transaction_anomaly
  GROUP BY yyyymm
)
SELECT
  b.yyyymm,
  b.year,
  b.month,
  b.total_spend,
  b.txn_count,
  b.active_vendors,
  COALESCE(o.outlier_txn, 0) AS outlier_txn,
  COALESCE(o.new_vendor_txn, 0) AS new_vendor_txn
FROM base b
LEFT JOIN outlier o USING (yyyymm)
ORDER BY b.yyyymm;

SELECT table_schema, table_name
FROM information_schema.views
WHERE table_schema = 'bi'
ORDER BY table_name;

CREATE OR REPLACE VIEW bi.v_dim_month AS
SELECT
  yyyymm,
  MIN(year)        AS year,
  MIN(month)       AS month,
  MIN(year_month)  AS year_month,
  MIN(month_name)  AS month_name,
  MIN(month_short) AS month_short,
  MIN(month_start) AS month_start
FROM bi.v_dim_date
GROUP BY yyyymm;

SELECT yyyymm, COUNT(*) c
FROM bi.v_dim_month
GROUP BY yyyymm
HAVING COUNT(*) > 1;

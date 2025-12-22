-- =========================
-- 01_load_corp_from_stage.sql (NEW)
-- stage -> corp (dims/facts/gold/anomaly/risk)
-- =========================

BEGIN;

-- (A) UPSERT DIM AGENCY
INSERT INTO corp.dim_agency (agency_name)
SELECT DISTINCT TRIM(agency_name)
FROM (
  SELECT agency_name FROM stage.silver_spend_clean_raw
  UNION ALL
  SELECT agency_name FROM stage.gold_agency_category_raw
  UNION ALL
  SELECT agency_name FROM stage.gold_anomaly_raw
) s
WHERE agency_name IS NOT NULL AND TRIM(agency_name) <> ''
ON CONFLICT (agency_name) DO NOTHING;

-- (B) UPSERT DIM VENDOR (vendor_key = md5(clean_name))
INSERT INTO corp.dim_vendor (vendor_key, vendor_name_clean)
SELECT DISTINCT
  md5(UPPER(TRIM(vendor_name))) AS vendor_key,
  UPPER(TRIM(vendor_name))      AS vendor_name_clean
FROM (
  SELECT vendor_name FROM stage.silver_spend_clean_raw
  UNION ALL
  SELECT vendor_name FROM stage.gold_vendor_monthly_raw
  UNION ALL
  SELECT vendor_name FROM stage.gold_anomaly_raw
) v
WHERE vendor_name IS NOT NULL AND TRIM(vendor_name) <> ''
ON CONFLICT (vendor_key) DO NOTHING;

-- (C) RELOAD FACT (optional: truncate for clean reruns)
TRUNCATE corp.fact_transactions_clean;

INSERT INTO corp.fact_transactions_clean (
  transaction_id, agency_id, vendor_id,
  transaction_date, year, month, yyyymm,
  amount, mcc_description, description,
  category_group, amount_bucket, is_weekend, day_of_week
)
SELECT
  s.transaction_id,
  a.agency_id,
  dv.vendor_id,
  s.transaction_date,
  COALESCE(s.year, EXTRACT(YEAR FROM s.transaction_date)::int),
  COALESCE(s.month, EXTRACT(MONTH FROM s.transaction_date)::int),
  COALESCE(s.yyyymm, (COALESCE(s.year, EXTRACT(YEAR FROM s.transaction_date)::int) * 100 + COALESCE(s.month, EXTRACT(MONTH FROM s.transaction_date)::int))),
  s.amount,
  s.mcc_description,
  s.description,
  s.category_group,
  s.amount_bucket,
  s.is_weekend,
  s.day_of_week
FROM stage.silver_spend_clean_raw s
JOIN corp.dim_agency a ON a.agency_name = TRIM(s.agency_name)
JOIN corp.dim_vendor dv ON dv.vendor_key = md5(UPPER(TRIM(s.vendor_name)))
WHERE s.transaction_id IS NOT NULL
  AND s.transaction_date IS NOT NULL
  AND s.amount IS NOT NULL;

-- (D) RELOAD GOLD TABLES
TRUNCATE corp.vendor_monthly_spend;
INSERT INTO corp.vendor_monthly_spend (
  yyyymm, year, month, vendor_id, vendor_name_clean,
  txn_count, total_spend, avg_spend
)
SELECT
  (g.year*100 + g.month) AS yyyymm,
  g.year,
  g.month,
  dv.vendor_id,
  dv.vendor_name_clean,
  g.txn_count,
  g.total_spend,
  g.avg_spend
FROM stage.gold_vendor_monthly_raw g
JOIN corp.dim_vendor dv ON dv.vendor_key = md5(UPPER(TRIM(g.vendor_name)));

TRUNCATE corp.agency_category_spend;
INSERT INTO corp.agency_category_spend (
  yyyymm, year, month, agency_id, agency_name,
  category_group, txn_count, total_spend, avg_spend
)
SELECT
  (g.year*100 + g.month) AS yyyymm,
  g.year,
  g.month,
  a.agency_id,
  a.agency_name,
  g.category_group,
  g.txn_count,
  g.total_spend,
  g.avg_spend
FROM stage.gold_agency_category_raw g
JOIN corp.dim_agency a ON a.agency_name = TRIM(g.agency_name);

-- (E) RELOAD ANOMALY + reason + high_amount
TRUNCATE corp.transaction_anomaly;

INSERT INTO corp.transaction_anomaly (
  transaction_id, yyyymm, transaction_date, amount,
  agency_id, vendor_id, category_group,
  is_outlier, is_new_vendor, is_high_amount, anomaly_reason
)
SELECT
  r.transaction_id,
  (r.year*100 + r.month) AS yyyymm,
  r.transaction_date,
  r.amount,
  a.agency_id,
  dv.vendor_id,
  r.category_group,
  (r.is_outlier = 1) AS is_outlier,
  (r.is_new_vendor_month = 1) AS is_new_vendor,
  (r.amount >= 2000) AS is_high_amount,
  CASE
    WHEN (r.is_outlier = 1) AND (r.is_new_vendor_month = 1) THEN 'OUTLIER+NEW_VENDOR'
    WHEN (r.is_outlier = 1) THEN 'OUTLIER'
    WHEN (r.amount >= 2000) THEN 'HIGH_AMOUNT'
    WHEN (r.is_new_vendor_month = 1) THEN 'NEW_VENDOR'
    ELSE NULL
  END AS anomaly_reason
FROM stage.gold_anomaly_raw r
JOIN corp.dim_agency a ON a.agency_name = TRIM(r.agency_name)
JOIN corp.dim_vendor dv ON dv.vendor_key = md5(UPPER(TRIM(r.vendor_name)))
WHERE r.transaction_id IS NOT NULL;

-- (F) BUILD RISK SCORE (simple explainable)
TRUNCATE corp.vendor_risk_scores;

INSERT INTO corp.vendor_risk_scores (
  yyyymm, vendor_id, vendor_name_clean,
  outlier_txn, new_vendor_txn, risk_score
)
SELECT
  ta.yyyymm,
  ta.vendor_id,
  v.vendor_name_clean,
  SUM(CASE WHEN ta.is_outlier THEN 1 ELSE 0 END) AS outlier_txn,
  SUM(CASE WHEN ta.is_new_vendor THEN 1 ELSE 0 END) AS new_vendor_txn,
  (70 * CASE WHEN SUM(CASE WHEN ta.is_outlier THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END)
  + (30 * CASE WHEN SUM(CASE WHEN ta.is_new_vendor THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END)
  AS risk_score
FROM corp.transaction_anomaly ta
JOIN corp.dim_vendor v ON v.vendor_id = ta.vendor_id
GROUP BY ta.yyyymm, ta.vendor_id, v.vendor_name_clean;

COMMIT;

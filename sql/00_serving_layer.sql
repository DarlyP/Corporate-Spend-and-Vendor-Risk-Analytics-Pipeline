-- =========================
-- 00_serving_layer.sql  (NEW)
-- Stage -> Corp -> BI views
-- =========================

-- 0) schemas
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS corp;
CREATE SCHEMA IF NOT EXISTS bi;

-- =========================
-- 1) STAGING TABLES (CSV landing)
-- =========================

-- Silver CSV output (ambil kolom inti saja biar import gampang)
CREATE TABLE IF NOT EXISTS stage.silver_spend_clean_raw (
  transaction_id      TEXT,
  agency_name         TEXT,
  vendor_name         TEXT,
  mcc_description     TEXT,
  description         TEXT,
  transaction_date    DATE,
  year                INT,
  month               INT,
  yyyymm              INT,
  amount              NUMERIC(18,2),
  category_group      TEXT,
  amount_bucket       TEXT,
  day_of_week         INT,
  is_weekend          BOOLEAN
);

-- Gold: vendor monthly
CREATE TABLE IF NOT EXISTS stage.gold_vendor_monthly_raw (
  year        INT,
  month       INT,
  vendor_name TEXT,
  txn_count   INT,
  total_spend NUMERIC(18,2),
  avg_spend   NUMERIC(18,2)
);

-- Gold: agency x category
CREATE TABLE IF NOT EXISTS stage.gold_agency_category_raw (
  year           INT,
  month          INT,
  agency_name    TEXT,
  category_group TEXT,
  txn_count      INT,
  total_spend    NUMERIC(18,2),
  avg_spend      NUMERIC(18,2)
);

-- Gold: anomaly (sesuaikan kalau kolom kamu beda sedikit)
CREATE TABLE IF NOT EXISTS stage.gold_anomaly_raw (
  transaction_id       TEXT,
  transaction_date     DATE,
  agency_name          TEXT,
  vendor_name          TEXT,
  merchant_category    TEXT,
  category_group       TEXT,
  amount               NUMERIC(18,2),
  mean_amount          NUMERIC(18,2),
  std_amount           NUMERIC(18,2),
  is_outlier           INT,
  is_new_vendor_month  INT,
  year                 INT,
  month                INT
);

-- =========================
-- 2) SERVING TABLES in corp (dim/fact/gold/anomaly + risk)
-- =========================

-- Dimensions
CREATE TABLE IF NOT EXISTS corp.dim_agency (
  agency_id   BIGSERIAL PRIMARY KEY,
  agency_name TEXT NOT NULL UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS corp.dim_vendor (
  vendor_id         BIGSERIAL PRIMARY KEY,
  vendor_key        TEXT NOT NULL UNIQUE,   -- dedup key
  vendor_name_clean TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fact (Silver in DB)
CREATE TABLE IF NOT EXISTS corp.fact_transactions_clean (
  transaction_id   TEXT PRIMARY KEY,
  agency_id        BIGINT NOT NULL REFERENCES corp.dim_agency(agency_id),
  vendor_id        BIGINT NOT NULL REFERENCES corp.dim_vendor(vendor_id),
  transaction_date DATE NOT NULL,
  year             INT NOT NULL,
  month            INT NOT NULL,
  yyyymm           INT NOT NULL,
  amount           NUMERIC(18,2) NOT NULL,
  mcc_description  TEXT NULL,
  description      TEXT NULL,
  category_group   TEXT NULL,
  amount_bucket    TEXT NULL,
  is_weekend       BOOLEAN NULL,
  day_of_week      INT NULL,
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Gold tables (BI friendly)
CREATE TABLE IF NOT EXISTS corp.vendor_monthly_spend (
  yyyymm            INT NOT NULL,
  year              INT NOT NULL,
  month             INT NOT NULL,
  vendor_id         BIGINT NOT NULL REFERENCES corp.dim_vendor(vendor_id),
  vendor_name_clean TEXT NOT NULL,
  txn_count         INT NOT NULL,
  total_spend       NUMERIC(18,2) NOT NULL,
  avg_spend         NUMERIC(18,2) NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (yyyymm, vendor_id)
);

CREATE TABLE IF NOT EXISTS corp.agency_category_spend (
  yyyymm           INT NOT NULL,
  year             INT NOT NULL,
  month            INT NOT NULL,
  agency_id        BIGINT NOT NULL REFERENCES corp.dim_agency(agency_id),
  agency_name      TEXT NOT NULL,
  category_group   TEXT NOT NULL,
  txn_count        INT NOT NULL,
  total_spend      NUMERIC(18,2) NOT NULL,
  avg_spend        NUMERIC(18,2) NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (yyyymm, agency_id, category_group)
);

CREATE TABLE IF NOT EXISTS corp.transaction_anomaly (
  transaction_id   TEXT PRIMARY KEY,
  yyyymm           INT NOT NULL,
  transaction_date DATE NOT NULL,
  amount           NUMERIC(18,2) NOT NULL,
  agency_id        BIGINT NOT NULL REFERENCES corp.dim_agency(agency_id),
  vendor_id        BIGINT NOT NULL REFERENCES corp.dim_vendor(vendor_id),
  category_group   TEXT NULL,
  is_outlier       BOOLEAN NOT NULL DEFAULT FALSE,
  is_high_amount   BOOLEAN NOT NULL DEFAULT FALSE,
  is_new_vendor    BOOLEAN NOT NULL DEFAULT FALSE,
  anomaly_reason   TEXT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Risk scores (simple, explainable)
CREATE TABLE IF NOT EXISTS corp.vendor_risk_scores (
  yyyymm            INT NOT NULL,
  vendor_id         BIGINT NOT NULL REFERENCES corp.dim_vendor(vendor_id),
  vendor_name_clean TEXT NOT NULL,
  outlier_txn       INT NOT NULL DEFAULT 0,
  new_vendor_txn    INT NOT NULL DEFAULT 0,
  risk_score        INT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (yyyymm, vendor_id)
);

-- =========================
-- 3) BI VIEWS (Power BI reads from here)
-- =========================

-- Vendor monthly
CREATE OR REPLACE VIEW bi.v_vendor_monthly_spend AS
SELECT
  vms.yyyymm, vms.year, vms.month,
  vms.vendor_name_clean,
  vms.txn_count, vms.total_spend, vms.avg_spend
FROM corp.vendor_monthly_spend vms;

-- Agency x Category
CREATE OR REPLACE VIEW bi.v_agency_category_spend AS
SELECT
  acs.yyyymm, acs.year, acs.month,
  acs.agency_name, acs.category_group,
  acs.txn_count, acs.total_spend, acs.avg_spend
FROM corp.agency_category_spend acs;

-- Anomaly detail (friendly)
CREATE OR REPLACE VIEW bi.v_transaction_anomaly AS
SELECT
  ta.yyyymm,
  ta.transaction_id,
  ta.transaction_date,
  a.agency_name,
  v.vendor_name_clean,
  ta.category_group,
  ta.amount,
  ta.is_outlier,
  ta.is_new_vendor,
  ta.is_high_amount,
  ta.anomaly_reason
FROM corp.transaction_anomaly ta
JOIN corp.dim_agency a ON a.agency_id = ta.agency_id
JOIN corp.dim_vendor v ON v.vendor_id = ta.vendor_id;

-- Vendor risk score
CREATE OR REPLACE VIEW bi.v_vendor_risk_scores AS
SELECT
  vrs.yyyymm,
  vrs.vendor_name_clean,
  vrs.outlier_txn,
  vrs.new_vendor_txn,
  vrs.risk_score
FROM corp.vendor_risk_scores vrs;

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

ANOMALY_PATH = "/opt/airflow/data/gold/transaction_anomaly.parquet"
OUT_DIR = "/opt/airflow/data/gold"
OUT_PARQUET = f"{OUT_DIR}/vendor_risk_scores.parquet"
OUT_CSV_DIR = f"{OUT_DIR}/vendor_risk_scores_csv"

def pick_first_existing_col(df, candidates):
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None

def main():
    spark = (
        SparkSession.builder
        .appName("build_risk_scores")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )

    df = spark.read.parquet(ANOMALY_PATH)

    # Robust terhadap beda nama kolom
    col_vendor = pick_first_existing_col(df, ["vendor_name", "vendor_name_clean"])
    col_amount = pick_first_existing_col(df, ["amount"])
    col_is_outlier = pick_first_existing_col(df, ["is_outlier"])
    col_is_new = pick_first_existing_col(df, ["is_new_vendor_month"])

    missing = [name for name, col in {
        "vendor_name": col_vendor,
        "amount": col_amount,
        "is_outlier": col_is_outlier,
        "is_new_vendor_month": col_is_new
    }.items() if col is None]

    if missing:
        raise ValueError(f"Missing required columns: {missing}. Available: {df.columns}")

    df = df.select(
        F.col(col_vendor).alias("vendor_name"),
        F.col(col_amount).cast("double").alias("amount"),
        F.col(col_is_outlier).cast("int").alias("is_outlier"),
        F.col(col_is_new).cast("int").alias("is_new_vendor_month"),
    )

    vendor = (
        df.groupBy("vendor_name")
          .agg(
              F.count("*").alias("total_txn"),
              F.sum("amount").alias("total_spend"),
              F.avg("amount").alias("avg_amount"),
              F.sum("is_outlier").alias("outlier_txn"),
              F.sum("is_new_vendor_month").alias("new_vendor_txn"),
              F.max("is_outlier").alias("has_outlier"),
              F.max("is_new_vendor_month").alias("is_new_vendor_month_flag"),
          )
          .withColumn("outlier_rate", F.col("outlier_txn") / F.col("total_txn"))
    )

    # spend_rank untuk nambah bobot “spend tinggi”
    w = Window.orderBy(F.col("total_spend"))
    vendor = vendor.withColumn("spend_rank", F.percent_rank().over(w))  # 0..1

    # Risk score sederhana & explainable
    # base: 70 jika pernah outlier, 30 jika vendor baru
    # plus: 0..20 dari spend_rank (spend makin tinggi makin risky)
    # plus: 0..10 dari outlier_rate (semakin sering outlier makin naik)
    vendor = vendor.withColumn(
        "risk_score_raw",
        (F.lit(70) * F.col("has_outlier")) +
        (F.lit(30) * F.col("is_new_vendor_month_flag")) +
        (F.lit(20) * F.col("spend_rank")) +
        (F.lit(10) * F.col("outlier_rate"))
    )

    vendor = vendor.withColumn(
        "risk_score",
        F.least(F.lit(100.0), F.col("risk_score_raw")).cast("double")
    )

    vendor = vendor.select(
        "vendor_name",
        "total_txn",
        "total_spend",
        "avg_amount",
        "outlier_txn",
        "outlier_rate",
        "new_vendor_txn",
        "has_outlier",
        "is_new_vendor_month_flag",
        "risk_score",
    )

    vendor.write.mode("overwrite").parquet(OUT_PARQUET)
    vendor.coalesce(1).write.mode("overwrite").option("header", True).csv(OUT_CSV_DIR)

    print("✅ vendor_risk_scores saved:", OUT_DIR)
    spark.stop()

if __name__ == "__main__":
    main()

import os
from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "/opt/airflow/data/silver/spend_clean.parquet"
GOLD_DIR = "/opt/airflow/data/gold"

def ensure_col(df, name):
    return name in df.columns

def main():
    spark = (
        SparkSession.builder
        .appName("build_aggregates_gold")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )

    # Pastikan folder gold ada
    os.makedirs(GOLD_DIR, exist_ok=True)

    df = spark.read.parquet(SILVER_PATH)

    # ====== Fallback kolom biar robust ======
    vendor_col = "vendor_name_clean" if ensure_col(df, "vendor_name_clean") else "vendor_name"
    agency_col = "agency_name_clean" if ensure_col(df, "agency_name_clean") else "agency_name"
    mcc_col = "mcc_description" if ensure_col(df, "mcc_description") else None
    cat_group_col = "category_group" if ensure_col(df, "category_group") else None

    required = ["transaction_id", "transaction_date", "amount", "year", "month", vendor_col, agency_col]
    missing = [c for c in required if not ensure_col(df, c)]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Columns present: {df.columns}")

    # Pastikan amount numerik untuk stddev/avg
    df = df.withColumn("amount_num", F.col("amount").cast("double"))

    # =========================
    # 1) Vendor Monthly Spend
    # =========================
    vendor_monthly = (
        df.groupBy("year", "month", F.col(vendor_col).alias("vendor_name"))
          .agg(
              F.count("*").alias("txn_count"),
              F.sum("amount_num").alias("total_spend"),
              F.avg("amount_num").alias("avg_spend")
          )
          .orderBy(F.desc("total_spend"))
    )

    # =========================
    # 2) Agency x Category Spend
    # =========================
    # Jika category_group tidak ada, buat default "Other"
    if cat_group_col is None:
        df = df.withColumn("category_group", F.lit("Other"))
        cat_group_col = "category_group"

    agency_category = (
        df.groupBy("year", "month",
                   F.col(agency_col).alias("agency_name"),
                   F.col(cat_group_col).alias("category_group"))
          .agg(
              F.count("*").alias("txn_count"),
              F.sum("amount_num").alias("total_spend"),
              F.avg("amount_num").alias("avg_spend")
          )
          .orderBy(F.desc("total_spend"))
    )

    # =========================
    # 3) Anomaly Flag (simple)
    # Outlier per MCC (atau category_group jika MCC tidak ada)
    # amount > mean + 3*std
    # + flag vendor baru di bulan pertama muncul
    # =========================
    if mcc_col is None:
        # fallback kalau tidak ada mcc_description
        df = df.withColumn("mcc_description", F.col(cat_group_col))
        mcc_col = "mcc_description"

    stats = (
        df.groupBy(F.col(mcc_col).alias("merchant_category"))
          .agg(
              F.avg("amount_num").alias("mean_amount"),
              F.stddev_pop("amount_num").alias("std_amount")
          )
    )

    vendor_first_month = (
        df.groupBy(F.col(vendor_col).alias("vendor_name"))
          .agg(F.min(F.make_date("year", "month", F.lit(1))).alias("first_seen_month"))
    )

    df2 = (
        df.withColumn("merchant_category", F.col(mcc_col))
          .withColumn("vendor_name_norm", F.col(vendor_col))
          .join(stats, on="merchant_category", how="left")
          .join(vendor_first_month.withColumnRenamed("vendor_name", "vendor_name_norm"),
                on="vendor_name_norm", how="left")
          .withColumn("txn_month", F.make_date("year", "month", F.lit(1)))
          .withColumn(
              "is_outlier",
              F.when(
                  F.col("std_amount").isNotNull()
                  & (F.col("amount_num") > (F.col("mean_amount") + 3 * F.col("std_amount"))),
                  F.lit(1),
              ).otherwise(F.lit(0))
          )
          .withColumn(
              "is_new_vendor_month",
              F.when(F.col("txn_month") == F.col("first_seen_month"), F.lit(1)).otherwise(F.lit(0))
          )
    )

    anomaly = df2.select(
        "transaction_id",
        "transaction_date",
        F.col(agency_col).alias("agency_name"),
        F.col("vendor_name_norm").alias("vendor_name"),
        "merchant_category",
        F.col(cat_group_col).alias("category_group"),
        F.col("amount").alias("amount"),
        "mean_amount",
        "std_amount",
        "is_outlier",
        "is_new_vendor_month",
        "year",
        "month",
    )

    # =========================
    # SAVE GOLD (ingat: parquet/csv itu OUTPUT FOLDER, bukan single file)
    # =========================
    vendor_monthly.write.mode("overwrite").parquet(f"{GOLD_DIR}/vendor_monthly_spend.parquet")
    agency_category.write.mode("overwrite").parquet(f"{GOLD_DIR}/agency_category_spend.parquet")
    anomaly.write.mode("overwrite").parquet(f"{GOLD_DIR}/transaction_anomaly.parquet")

    vendor_monthly.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{GOLD_DIR}/vendor_monthly_spend_csv")
    agency_category.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{GOLD_DIR}/agency_category_spend_csv")
    anomaly.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{GOLD_DIR}/transaction_anomaly_csv")

    print("✅ GOLD aggregates created in:", GOLD_DIR)
    spark.stop()

if __name__ == "__main__":
    main()

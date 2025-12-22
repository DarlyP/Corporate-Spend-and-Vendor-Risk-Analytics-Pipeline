import os
from pyspark.sql import SparkSession, functions as F, types as T

BRONZE_DIR = "/opt/airflow/data/bronze"
SILVER_DIR = "/opt/airflow/data/silver"
SILVER_PARQUET = f"{SILVER_DIR}/spend_clean.parquet"
SILVER_CSV_DIR = f"{SILVER_DIR}/spend_clean_csv"


def pick_first_existing_col(df, candidates):
    """Return first column name from candidates that exists in df, else None."""
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def main():
    spark = SparkSession.builder.appName("clean_transform_silver").getOrCreate()

    # Pastikan config berlaku walaupun SparkSession sudah kebentuk
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    os.makedirs(SILVER_DIR, exist_ok=True)

    bronze_path = f"{BRONZE_DIR}/*.csv"
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(bronze_path)
    )

    # Kalau tidak ada file bronze sama sekali -> stop biar jelas
    if len(df.columns) == 0:
        raise FileNotFoundError(f"No CSV found at {bronze_path}. Pastikan bronze sudah ada isinya.")

    # Map kolom
    col_txn_id = pick_first_existing_col(df, ["transaction_id", "objectid", "object_id", "id"])
    col_agency = pick_first_existing_col(df, ["agency_name", "agency", "department", "dept"])
    col_vendor = pick_first_existing_col(df, ["vendor_name", "merchant_name", "supplier", "vendor"])
    col_mcc = pick_first_existing_col(df, ["merchant_category", "mcc_description", "category", "merchant_category_description"])
    col_date = pick_first_existing_col(df, ["transaction_date", "date", "purchase_date", "posted_date"])
    col_amount = pick_first_existing_col(df, ["amount", "transaction_amount", "amt", "dollars"])
    col_desc = pick_first_existing_col(df, ["description", "transaction_description", "item_description"])

    required = {
        "transaction_id": col_txn_id,
        "agency_name": col_agency,
        "vendor_name": col_vendor,
        "transaction_date": col_date,
        "amount": col_amount
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}. "
            f"Columns present: {df.columns}"
        )

    # Select + rename
    df2 = df.select(
        F.col(col_txn_id).cast("string").alias("transaction_id"),
        F.col(col_agency).cast("string").alias("agency_name"),
        F.col(col_vendor).cast("string").alias("vendor_name"),
        (F.col(col_mcc).cast("string").alias("mcc_description") if col_mcc else F.lit(None).cast("string").alias("mcc_description")),
        F.col(col_date).cast("string").alias("transaction_date_raw"),
        F.col(col_amount).cast("string").alias("amount_raw"),
        (F.col(col_desc).cast("string").alias("description") if col_desc else F.lit(None).cast("string").alias("description")),
    )

    # Clean amount
    df2 = df2.withColumn(
        "amount",
        F.regexp_replace(F.col("amount_raw"), r"[$,]", "").cast(T.DecimalType(18, 2))
    )

    # === Robust date parsing (FIX penting) ===
    # 0) trim dulu (ini yang sering bikin regex timezone sebelumnya gagal)
    dt_str = F.trim(F.col("transaction_date_raw"))

    # 1) hapus timezone di akhir, toleran spasi:
    #    contoh: "....+00", "....+00:00", "....Z", "....+00   "
    dt_str = F.regexp_replace(dt_str, r"\s*(Z|\+00:00|\+00)\s*$", "")

    # 2) kalau ada 'T' ubah jadi spasi
    dt_str = F.regexp_replace(dt_str, "T", " ")

    # 3) rapikan spasi
    dt_str = F.trim(F.regexp_replace(dt_str, r"\s+", " "))

    df2 = df2.withColumn(
        "transaction_ts",
        F.coalesce(
            F.to_timestamp(dt_str, "yyyy/MM/dd HH:mm:ss"),
            F.to_timestamp(dt_str, "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(dt_str, "MM/dd/yyyy HH:mm:ss"),
            F.to_timestamp(dt_str, "M/d/yyyy H:m:s"),
            # fallback tanggal saja
            F.to_timestamp(dt_str, "yyyy/MM/dd"),
            F.to_timestamp(dt_str, "yyyy-MM-dd"),
            F.to_timestamp(dt_str, "MM/dd/yyyy"),
            F.to_timestamp(dt_str, "M/d/yyyy"),
        )
    )

    df2 = df2.withColumn("transaction_date", F.to_date("transaction_ts"))

    # Filters valid data
    df2 = df2.filter(F.col("transaction_id").isNotNull())
    df2 = df2.filter(F.col("transaction_date").isNotNull())
    df2 = df2.filter(F.col("amount").isNotNull())
    df2 = df2.filter(F.col("amount") > F.lit(0))

    # Standardize text
    df2 = df2.withColumn("vendor_name_clean", F.upper(F.trim(F.col("vendor_name"))))
    df2 = df2.withColumn("agency_name_clean", F.trim(F.col("agency_name")))

    # Time features
    df2 = df2.withColumn("year", F.year("transaction_date"))
    df2 = df2.withColumn("month", F.month("transaction_date"))
    df2 = df2.withColumn("yyyymm", F.col("year") * 100 + F.col("month"))
    df2 = df2.withColumn("day_of_week", F.dayofweek("transaction_date"))  # 1=Sun..7=Sat
    df2 = df2.withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))

    # Amount buckets
    df2 = df2.withColumn(
        "amount_bucket",
        F.when(F.col("amount") < 100, "<100")
         .when((F.col("amount") >= 100) & (F.col("amount") < 500), "100-499")
         .when((F.col("amount") >= 500) & (F.col("amount") < 2000), "500-1999")
         .otherwise(">=2000")
    )

    # Category group
    mcc = F.lower(F.coalesce(F.col("mcc_description"), F.lit("")))
    df2 = df2.withColumn(
        "category_group",
        F.when(mcc.contains("travel") | mcc.contains("air") | mcc.contains("hotel") | mcc.contains("car"), "Travel")
         .when(mcc.contains("software") | mcc.contains("computer") | mcc.contains("it") | mcc.contains("electronics"), "IT")
         .when(mcc.contains("office") | mcc.contains("stationery") | mcc.contains("supplies"), "Office")
         .when(mcc.contains("food") | mcc.contains("restaurant") | mcc.contains("catering"), "Meals")
         .otherwise("Other")
    )

    # Deduplicate
    df2 = df2.dropDuplicates(["transaction_id"])

    # Write outputs
    df2.write.mode("overwrite").parquet(SILVER_PARQUET)
    df2.coalesce(1).write.mode("overwrite").option("header", True).csv(SILVER_CSV_DIR)

    print(f"✅ Silver parquet written to: {SILVER_PARQUET}")
    print(f"✅ Silver csv folder written to: {SILVER_CSV_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()

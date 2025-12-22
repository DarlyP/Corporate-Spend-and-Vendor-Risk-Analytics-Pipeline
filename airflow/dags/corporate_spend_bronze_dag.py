import os
from datetime import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

# Where data is mounted inside the container
BRONZE_DIR = "/opt/airflow/data/bronze"


def download_bronze_csv(**context):
    """
    Download the CSV from the DC Open Data dataset and save it to data/bronze/
    Filename: purchase_card_YYYYMMDD_HHMMSS.csv
    """
    url = os.environ.get("DATASET_CSV_URL")
    if not url:
        raise ValueError("DATASET_CSV_URL is not set. Check your .env and docker-compose environment.")

    os.makedirs(BRONZE_DIR, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(BRONZE_DIR, f"purchase_card_{ts}.csv")

    # Stream download so it won't eat RAM
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    # Log path to Airflow task logs
    print(f"Saved bronze CSV to: {out_path}")
    return out_path


default_args = {"owner": "airflow"}

with DAG(
    dag_id="corporate_spend_bronze_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",  # you can change to None while testing
    catchup=False,
    tags=["portfolio", "bronze", "dc-opendata"],
) as dag:

    t1_download_bronze = PythonOperator(
        task_id="download_purchase_card_csv_bronze",
        python_callable=download_bronze_csv,
    )

    t1_download_bronze

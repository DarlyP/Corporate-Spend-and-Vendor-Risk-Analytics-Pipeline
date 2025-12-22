from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {"owner": "airflow"}

with DAG(
    dag_id="corporate_spend_silver_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,   # manual trigger
    catchup=False,
    tags=["portfolio", "silver", "spark"],
) as dag:

    t_spark_clean_silver = BashOperator(
        task_id="spark_clean_transform_silver",
        bash_command=(
            "spark-submit "
            "--conf spark.sql.session.timeZone=UTC "
            "--conf spark.sql.legacy.timeParserPolicy=LEGACY "
            "/opt/airflow/spark_jobs/clean_transform.py"
        ),
    )

    t_spark_clean_silver

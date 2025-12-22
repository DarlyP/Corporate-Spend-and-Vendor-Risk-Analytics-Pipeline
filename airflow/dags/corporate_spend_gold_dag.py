from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="corporate_spend_gold_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # manual dulu biar gampang testing
    catchup=False,
    tags=["portfolio", "gold", "spark"],
) as dag:

    t_build_gold = BashOperator(
        task_id="spark_build_aggregates_gold",
        bash_command="spark-submit /opt/airflow/spark_jobs/build_aggregates.py",
    )

    t_build_gold

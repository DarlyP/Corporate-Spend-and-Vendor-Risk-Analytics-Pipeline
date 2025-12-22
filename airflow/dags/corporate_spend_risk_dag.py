from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {"owner": "airflow"}

with DAG(
    dag_id="corporate_spend_risk_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["portfolio", "risk", "gold", "spark"],
) as dag:

    t_build_risk_scores = BashOperator(
        task_id="spark_build_risk_scores",
        bash_command="spark-submit /opt/airflow/spark_jobs/build_risk_scores.py",
    )

    t_build_risk_scores

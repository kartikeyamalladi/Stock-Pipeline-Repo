from __future__ import annotations
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

def task_failure_alert(context):
    ti = context["task_instance"]
    log.error("Task failed | dag=%s task=%s", ti.dag_id, ti.task_id)

def validate_environment():
    import os
    missing = [k for k in ("ALPHA_VANTAGE_API_KEY", "FINNHUB_API_KEY") if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {missing}")
    log.info("Environment validated.")

def run_spark_transform():
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, "/opt/airflow/processing/spark_transform.py"],
        capture_output=True, text=True
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception(f"Spark transform failed:\n{result.stderr}")

def notify_success(**context):
    log.info("Pipeline completed | exec=%s", context["execution_date"])

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="stock_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fetch stock data -> PySpark transform -> partitioned Parquet",
    schedule_interval="0 18 * * 1-5",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "ingestion", "spark"],
) as dag:

    validate_env = PythonOperator(
        task_id="validate_env",
        python_callable=validate_environment,
        retries=0,
    )

    fetch_stocks = BashOperator(
        task_id="fetch_stocks",
        bash_command="cd /opt/airflow && python -m ingestion.async_fetcher --concurrency 5 --log-level INFO",
        execution_timeout=timedelta(minutes=30),
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    spark_transform = PythonOperator(
        task_id="spark_transform",
        python_callable=run_spark_transform,
        execution_timeout=timedelta(minutes=20),
        retries=2,
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        retries=1,
        on_failure_callback=None,
    )

    validate_env >> fetch_stocks >> spark_transform >> notify

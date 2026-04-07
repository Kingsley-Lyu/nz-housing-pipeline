# dags/housing_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Ensure ingestion modules are importable
sys.path.append(os.path.join(os.path.dirname(__file__), "../ingestion"))

from stats_nz import run as run_stats_nz
from rbnz import run as run_rbnz

# -----------------------------
# DAG DEFINITION
# -----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'nz_housing_pipeline',
    default_args=default_args,
    description='Monthly NZ Housing Market ETL pipeline',
    schedule_interval='0 6 1 * *',  # first day of month at 6am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['housing', 'nz'],
) as dag:

    def ingest_stats_nz():
        print("Starting Stats NZ ingestion")
        df = run_stats_nz()
        print(f"Stats NZ rows: {len(df)}")
        return df

    def ingest_rbnz():
        print("Starting RBNZ ingestion")
        df = run_rbnz()
        print(f"RBNZ rows: {len(df)}")
        return df

    # -----------------------------
    # TASKS
    # -----------------------------
    task_stats_nz = PythonOperator(
        task_id='ingest_stats_nz',
        python_callable=ingest_stats_nz
    )

    task_rbnz = PythonOperator(
        task_id='ingest_rbnz',
        python_callable=ingest_rbnz
    )

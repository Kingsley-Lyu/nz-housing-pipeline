# dags/housing_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys
import os

# Ensure ingestion modules are importable
sys.path.append(os.path.join(os.path.dirname(__file__), "../ingestion"))

from stats_nz import run as run_stats_nz
from rbnz import run as run_rbnz
from snowflake_loader import run as run_snowflake_loader

# Absolute path to dbt project — used by BashOperator
DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "../dbt_project")

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

    # -----------------------------
    # STEP 1 — INGEST
    # Download raw data from Stats NZ and RBNZ to local files
    # Both run in parallel
    # -----------------------------
    def ingest_stats_nz():
        print("Starting Stats NZ ingestion")
        df = run_stats_nz()
        print(f"Stats NZ rows: {len(df)}")

    def ingest_rbnz():
        print("Starting RBNZ ingestion")
        df = run_rbnz()
        print(f"RBNZ rows: {len(df)}")

    task_stats_nz = PythonOperator(
        task_id='ingest_stats_nz',
        python_callable=ingest_stats_nz
    )

    task_rbnz = PythonOperator(
        task_id='ingest_rbnz',
        python_callable=ingest_rbnz
    )

    # -----------------------------
    # STEP 2 — LOAD TO SNOWFLAKE
    # Load local files into Snowflake RAW schema
    # Runs after both ingestion tasks complete
    # -----------------------------
    def load_snowflake():
        print("Loading data into Snowflake RAW schema")
        run_snowflake_loader()

    task_load_snowflake = PythonOperator(
        task_id='load_snowflake',
        python_callable=load_snowflake
    )

    # -----------------------------
    # STEP 3 — DBT RUN
    # Transform RAW → STAGING → MARTS in Snowflake
    # -----------------------------
    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .',
    )

    # -----------------------------
    # STEP 4 — DBT TEST
    # Validate mart_affordability output
    # -----------------------------
    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .',
    )

    # -----------------------------
    # TASK DEPENDENCIES
    # Ingest in parallel → Load → Transform → Test
    # -----------------------------
    [task_stats_nz, task_rbnz] >> task_load_snowflake >> task_dbt_run >> task_dbt_test

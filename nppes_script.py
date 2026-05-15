import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Add parent directory to Python path so Airflow can find npi_automate.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import your script
from dags.scripts import npi_automate

# Default arguments
default_args = {
    'owner': 'saurav',
    'depends_on_past': False,
    'retries': False,
}


def extract_data_from_source():
    npi_automate.request_url()


def process_extracted_npi_data():
    npi_automate.process_extracted_npi_data_v2()


def write_manifest(**context):
    npi_automate.write_nppes_manifest(airflow_run_id=context.get('run_id'))


# DAG definition
with DAG(
    dag_id='nppes_npi_data_pipeline',
    default_args=default_args,
    description='Download and process NPI data into parquet',
    schedule_interval='@monthly',  # adjust if needed
    start_date=datetime(2026, 5, 5),
    catchup=False,
    tags=['nppes','npi', 'healthcare', 'etl'],
) as dag:

    extract_data = PythonOperator(
        task_id='extract_nppes_csv_npi_data',
        python_callable=extract_data_from_source
    )

    process_and_dump_data = PythonOperator(
        task_id='process_and_dump_extracted_data',
        python_callable=process_extracted_npi_data
    )

    write_manifest_task = PythonOperator(
        task_id='write_nppes_manifest',
        python_callable=write_manifest
    )

    extract_data >> process_and_dump_data >> write_manifest_task
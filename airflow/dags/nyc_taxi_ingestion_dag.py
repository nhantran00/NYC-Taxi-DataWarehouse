from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="nyc_taxi_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "dataset": Param(
            "taxi_zones",
            enum=["taxi_zones", "yellow_taxi_trips"],
            description="Dataset name from config/datasets.yml",
        ),
        "year": Param("2024", description="Data year"),
        "month": Param(
            "01",
            enum=["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
            description="Data month",
        ),
    },
    tags=["nyc-taxi", "ingestion"],
) as dag:
    ingest_dataset = BashOperator(
        task_id="ingest_dataset",
        bash_command=(
            "cd /opt/airflow/project && "
            "python src/ingest_taxi_data.py "
            "--dataset {{ params.dataset }} "
            "--year {{ params.year }} "
            "--month {{ params.month }}"
        ),
    )

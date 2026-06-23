from pathlib import Path
import argparse
import os

import pandas as pd
import pyarrow.parquet as pq
import requests
from sqlalchemy import create_engine
from sqlalchemy import text
import yaml


BATCH_SIZE = 200000

def load_config(config_path: Path) -> dict:
    with config_path.open('r', encoding='utf-8') as file:
        return yaml.safe_load(file)
    
def find_dataset(config: dict, dataset_name: str) -> dict:
    for dataset in config["datasets"]:
        if dataset["name"] == dataset_name:
            return dataset
    
    raise ValueError(f"Dataset not found: {dataset_name}")


def download_file(source_url: str, local_path: Path) -> None:
    if local_path.exists():
        print(f"File already exists, skipping download: {local_path}")
        return

    local_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(source_url, timeout=60)
    response.raise_for_status()

    local_path.write_bytes(response.content)


def get_file_metadata(dataset: dict, local_path: Path) -> tuple[list[str], int]:
    source_type = dataset["source_type"]

    if source_type == "csv":
        columns = pd.read_csv(local_path, nrows=0).columns.tolist()
        row_count = 0

        for chunk in pd.read_csv(local_path, chunksize=BATCH_SIZE):
            row_count += len(chunk)

        return columns, row_count

    if source_type == "parquet":
        parquet_file = pq.ParquetFile(local_path)
        columns = parquet_file.schema.names
        row_count = parquet_file.metadata.num_rows

        return columns, row_count

    raise ValueError(f"Unsupported source_type: {source_type}")


def validate_data(dataset: dict, columns: list[str], row_count: int) -> None:
    if row_count == 0:
        raise ValueError(f"Dataset {dataset['name']} has no rows")

    required_columns = dataset.get("required_columns", [])
    missing_columns = [
        column for column in required_columns
        if column not in columns
    ]

    if missing_columns:
        raise ValueError(
            f"Dataset {dataset['name']} is missing required columns: {missing_columns}"
        )


def build_postgres_url() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "nyc_taxi")
    user = os.getenv("POSTGRES_USER", "nyc_taxi")
    password = os.getenv("POSTGRES_PASSWORD", "nyc_taxi")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def add_partition_columns(dataset: dict, dataframe: pd.DataFrame, params: dict) -> pd.DataFrame:
    partition_columns = dataset.get("partition_by", [])

    if not partition_columns:
        return dataframe

    dataframe = dataframe.copy()

    for column in partition_columns:
        dataframe[column] = params[column]

    return dataframe


def iter_dataframes(dataset: dict, local_path: Path):
    source_type = dataset["source_type"]

    if source_type == "csv":
        yield from pd.read_csv(local_path, chunksize=BATCH_SIZE)
        return

    if source_type == "parquet":
        parquet_file = pq.ParquetFile(local_path)

        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
            yield batch.to_pandas()

        return

    raise ValueError(f"Unsupported source_type: {source_type}")


def load_to_postgres(dataset: dict, local_path: Path, params: dict) -> int:
    target_schema = dataset["target_schema"]
    target_table = dataset["target_table"]
    load_strategy = dataset["load_strategy"]

    engine = create_engine(build_postgres_url())

    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))

    total_rows = 0
    if_exists = load_strategy

    for dataframe in iter_dataframes(dataset, local_path):
        dataframe = add_partition_columns(dataset, dataframe, params)

        dataframe.to_sql(
            name=target_table,
            con=engine,
            schema=target_schema,
            if_exists=if_exists,
            index=False,
            chunksize=10000,
            method="multi",
        )

        total_rows += len(dataframe)
        if_exists = "append"
        print(f"Loaded rows so far: {total_rows}")

    return total_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC Taxi Ingestion")

    parser.add_argument("--config", default="config/datasets.yml")

    parser.add_argument("--dataset", required=True)

    parser.add_argument("--year",required=True)

    parser.add_argument("--month", required=True)
    
    return parser.parse_args()

def main() -> None:
    args = parse_args()

    config = load_config(Path(args.config))
    dataset = find_dataset(config, args.dataset)

    if not dataset.get("enabled", True):
        print(f"Dataset is disabled: {args.dataset}")
        return

    params = {
        "year": args.year,
        "month": args.month
    }

    source_url = dataset["source_url"].format(**params)
    local_path = Path(dataset["local_path"].format(**params))

    print(f"Dataset: {dataset['name']}")
    print(f"Source URL: {source_url}")
    print(f"Local path: {local_path}")

    download_file(source_url, local_path)

    columns, row_count = get_file_metadata(dataset, local_path)
    validate_data(dataset, columns, row_count)

    print(f"Rows: {row_count}")
    print(f"Columns: {len(columns)}")

    loaded_rows = load_to_postgres(dataset, local_path, params)

    print(f"Loaded rows: {loaded_rows}")
    print(f"Loaded to PostgreSQL: {dataset['target_schema']}.{dataset['target_table']}")
    print("Ingest completed")

if __name__ == "__main__":
    main()

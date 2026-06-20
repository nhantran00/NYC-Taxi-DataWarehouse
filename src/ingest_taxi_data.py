from pathlib import Path
import argparse

import requests
import yaml

def load_config(config_path: Path) -> dict:
    with config_path.open('r', encoding='utf-8') as file:
        return yaml.safe_load(file)
    
def find_dataset(config: dict, dataset_name: str) -> dict:
    for dataset in config["datasets"]:
        if dataset["name"] == dataset_name:
            return dataset
    
    raise ValueError(f"Dataset not found: {dataset_name}")


def download_file(source_url: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(source_url, timeout=60)
    response.raise_for_status()

    local_path.write_bytes(response.content)

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

    download_file(source_url, local_path)

    print("Ingest completed")

if __name__ == "__main__":
    main()

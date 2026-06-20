# NYC Taxi Data Warehouse

Learning-oriented data warehouse project for NYC Taxi trip data.

## Goal

Build a small end-to-end analytics pipeline:

1. Download or load NYC Taxi raw trip data.
2. Store raw data locally.
3. Transform raw data into analytics-ready tables.
4. Query metrics such as trips, revenue, distance, and pickup zones.
5. Add orchestration and dashboards step by step.

## Planned Architecture

```text
NYC Taxi source data
        |
        v
data/raw
        |
        v
Python ingestion and cleaning
        |
        v
PostgreSQL warehouse
        |
        v
dbt models
        |
        v
Metabase dashboards
```

## Project Layout

```text
.
|-- airflow/       # Airflow DAGs will be added later
|-- config/        # Shared configuration files
|-- data/          # Local raw and processed data
|-- dbt_project/   # dbt project will be added later
|-- docker/        # Docker-related files
|-- sql/           # SQL scripts and warehouse DDL
|-- src/           # Python source code
|-- tests/         # Tests
|-- .env.example
|-- .gitignore
`-- README.md
```

## Current Step

Step 1: Create a clean project foundation.

Next step: add a minimal Python ingestion script for NYC Taxi data.

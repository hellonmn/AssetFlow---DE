# AssetFlow Data Engineering Pipeline

A small batch data engineering project for the AssetFlow asset management domain. It demonstrates ingestion, object storage, PySpark transformations, orchestration, and analytics loading without changing the AssetFlow application repository.

## Stack

- Python and PySpark for batch transforms
- Apache Airflow for scheduling and task monitoring
- AWS S3-compatible object storage (MinIO locally; configurable for AWS S3)
- PostgreSQL for reporting tables
- Docker Compose for local services

## Pipeline

```text
Sample AssetFlow CSVs -> S3 raw/ -> PySpark clean + aggregate -> S3 curated/ (Parquet)
                                                               -> PostgreSQL reporting tables
```

The DAG runs daily and has five visible steps: upload raw CSVs, download them from S3 for Spark, transform with Spark, upload curated Parquet, and upsert analytics into PostgreSQL. The sample assets and maintenance records are synthetic and generated automatically on the first run. You can replace them with CSVs exported from AssetFlow, provided they use the documented column names in the DAG.

## Run locally

Prerequisites: Docker Desktop with Docker Compose enabled.

1. Copy `.env.example` to `.env` (the defaults are only for local development).
2. From this directory, build and initialize the containers:

   ```bash
   docker compose build
   docker compose up airflow-init
   docker compose up -d
   ```

3. Open Airflow at [http://localhost:8080](http://localhost:8080), sign in with `admin` / `admin`, and unpause `assetflow_daily_batch_pipeline`.
4. Trigger the DAG once from the Airflow UI. Local object storage is available at [http://localhost:9001](http://localhost:9001) (credentials in `.env`). PostgreSQL is exposed on port `5433`.
5. Query the output tables:

   ```sql
   SELECT * FROM asset_utilization_daily ORDER BY department;
   SELECT * FROM maintenance_summary_monthly ORDER BY snapshot_month, department;
   ```

## Use AWS S3

Set `S3_ENDPOINT_URL` to an empty value, `S3_BUCKET` to an existing bucket, and provide AWS credentials through the environment (prefer an IAM role in deployed environments). The pipeline writes original input files under `raw/` and the transformed Parquet files under `curated/`. Spark uses a local working directory for processing before the curated files are uploaded.

## Data fields

- `assets.csv`: `asset_id,category,department,status,purchase_date`
- `maintenance.csv`: `request_id,asset_id,department,status,cost,request_date`

The reporting output calculates daily allocated asset utilization by department and monthly maintenance counts and costs by department. Writes use conflict keys so reruns update the same reporting period rather than duplicate it.

## Resume summary

Built a scheduled batch ETL pipeline with Apache Airflow and PySpark to ingest asset and maintenance data, store raw files in S3-compatible object storage, generate Parquet analytics, and upsert utilization and maintenance summaries into PostgreSQL.

## Notes

- Airflow's metadata database is separate from the analytics PostgreSQL database.
- The local credentials and Airflow admin password are demo defaults; change them before any shared deployment.
- This is intentionally a small batch project: it does not add streaming, a warehouse, or a cloud deployment configuration.


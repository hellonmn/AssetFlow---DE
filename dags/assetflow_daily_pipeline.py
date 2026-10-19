from __future__ import annotations

import os
from datetime import datetime, timedelta

import boto3
import psycopg2
from botocore.config import Config
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession, functions as F


DATA_DIR = "/opt/airflow/data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
ASSETS_FILE = os.path.join(RAW_DIR, "assets.csv")
MAINTENANCE_FILE = os.path.join(RAW_DIR, "maintenance.csv")


def storage_client():
    endpoint = os.getenv("S3_ENDPOINT_URL")
    options = {
        "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(s3={"addressing_style": "path"}),
    }
    if endpoint:
        options["endpoint_url"] = endpoint
    return boto3.client("s3", **options)


def ensure_sample_data():
    """Create deterministic sample CSV data once when no source files exist."""
    os.makedirs(RAW_DIR, exist_ok=True)
    if os.path.exists(ASSETS_FILE) and os.path.exists(MAINTENANCE_FILE):
        return

    assets = [
        ("A-1001", "Laptop", "Engineering", "allocated", "2024-01-10"),
        ("A-1002", "Monitor", "Engineering", "available", "2024-02-05"),
        ("A-1003", "Laptop", "Finance", "allocated", "2023-11-20"),
        ("A-1004", "Vehicle", "Operations", "maintenance", "2022-06-15"),
        ("A-1005", "Desk", "Operations", "available", "2024-04-01"),
        ("A-1006", "Laptop", "Finance", "allocated", "2024-03-12"),
        ("A-1007", "Projector", "People", "available", "2023-08-19"),
        ("A-1008", "Phone", "Engineering", "allocated", "2024-05-22"),
    ]
    maintenance = [
        ("M-501", "A-1001", "Engineering", "completed", "125.50", "2025-01-15"),
        ("M-502", "A-1004", "Operations", "completed", "480.00", "2025-01-28"),
        ("M-503", "A-1003", "Finance", "open", "0", "2025-02-03"),
        ("M-504", "A-1008", "Engineering", "completed", "65.25", "2025-02-11"),
        ("M-505", "A-1005", "Operations", "open", "0", "2025-02-20"),
    ]
    with open(ASSETS_FILE, "w", encoding="utf-8", newline="") as handle:
        handle.write("asset_id,category,department,status,purchase_date\n")
        handle.writelines(",".join(row) + "\n" for row in assets)
    with open(MAINTENANCE_FILE, "w", encoding="utf-8", newline="") as handle:
        handle.write("request_id,asset_id,department,status,cost,request_date\n")
        handle.writelines(",".join(row) + "\n" for row in maintenance)


def upload_raw_data():
    ensure_sample_data()
    client = storage_client()
    bucket = os.environ["S3_BUCKET"]
    for filename in ("assets.csv", "maintenance.csv"):
        client.upload_file(os.path.join(RAW_DIR, filename), bucket, f"raw/{filename}")


def transform_with_spark():
    """Transform source CSVs with Spark and persist the curated Parquet files."""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("AssetFlowBatchETL")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        assets = (
            spark.read.option("header", True).option("inferSchema", True).csv(ASSETS_FILE)
            .withColumn("department", F.trim(F.col("department")))
            .withColumn("status", F.lower(F.trim(F.col("status"))))
            .dropDuplicates(["asset_id"])
            .filter(F.col("asset_id").isNotNull() & F.col("department").isNotNull())
        )
        utilization = (
            assets.groupBy("department")
            .agg(
                F.countDistinct("asset_id").alias("total_assets"),
                F.countDistinct(F.when(F.col("status") == "allocated", F.col("asset_id"))).alias("allocated_assets"),
            )
            .withColumn("available_assets", F.col("total_assets") - F.col("allocated_assets"))
            .withColumn(
                "utilization_rate",
                F.round(F.col("allocated_assets") * F.lit(100.0) / F.col("total_assets"), 2),
            )
            .withColumn("snapshot_date", F.current_date())
        )

        maintenance = (
            spark.read.option("header", True).option("inferSchema", True).csv(MAINTENANCE_FILE)
            .withColumn("department", F.trim(F.col("department")))
            .withColumn("status", F.lower(F.trim(F.col("status"))))
            .withColumn("cost", F.col("cost").cast("double"))
            .withColumn("request_date", F.to_date("request_date"))
            .dropDuplicates(["request_id"])
            .filter(F.col("request_id").isNotNull() & F.col("department").isNotNull())
        )
        maintenance_monthly = (
            maintenance.withColumn("snapshot_month", F.trunc("request_date", "month"))
            .groupBy("snapshot_month", "department")
            .agg(
                F.countDistinct("request_id").alias("request_count"),
                F.countDistinct(F.when(F.col("status") == "completed", F.col("request_id"))).alias("completed_count"),
                F.round(F.sum(F.coalesce("cost", F.lit(0.0))), 2).alias("total_cost"),
                F.round(F.avg(F.when(F.col("status") == "completed", F.col("cost"))), 2).alias("average_cost"),
            )
        )

        os.makedirs(PROCESSED_DIR, exist_ok=True)
        for name, frame in (("asset_utilization", utilization), ("maintenance_summary", maintenance_monthly)):
            output = os.path.join(PROCESSED_DIR, name)
            if os.path.exists(output):
                import shutil
                shutil.rmtree(output)
            frame.coalesce(1).write.mode("overwrite").parquet(output)
    finally:
        spark.stop()


def stage_raw_files_from_s3():
    """Download the raw S3 objects into the task's shared working directory."""
    client = storage_client()
    bucket = os.environ["S3_BUCKET"]
    os.makedirs(RAW_DIR, exist_ok=True)
    for filename in ("assets.csv", "maintenance.csv"):
        client.download_file(bucket, f"raw/{filename}", os.path.join(RAW_DIR, filename))


def upload_curated_parquet():
    client = storage_client()
    bucket = os.environ["S3_BUCKET"]
    for dataset in ("asset_utilization", "maintenance_summary"):
        for filename in _parquet_files(dataset):
            key = f"curated/{dataset}/{os.path.basename(filename)}"
            client.upload_file(filename, bucket, key)


def load_to_postgres():
    """Upsert the curated Spark outputs into PostgreSQL reporting tables."""
    import pyarrow.parquet as pq

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "assetflow_analytics"),
        user=os.getenv("POSTGRES_USER", "assetflow"),
        password=os.getenv("POSTGRES_PASSWORD", "assetflow"),
    )
    try:
        with connection, connection.cursor() as cursor:
            for parquet_file in _parquet_files("asset_utilization"):
                table = pq.read_table(parquet_file)
                for row in table.to_pylist():
                    snapshot = row["snapshot_date"]
                    if isinstance(snapshot, datetime):
                        snapshot = snapshot.date()
                    cursor.execute(
                        """INSERT INTO asset_utilization_daily
                        (snapshot_date, department, total_assets, allocated_assets, available_assets, utilization_rate)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (snapshot_date, department) DO UPDATE SET
                        total_assets=EXCLUDED.total_assets, allocated_assets=EXCLUDED.allocated_assets,
                        available_assets=EXCLUDED.available_assets, utilization_rate=EXCLUDED.utilization_rate""",
                        (snapshot, row["department"], row["total_assets"], row["allocated_assets"],
                         row["available_assets"], row["utilization_rate"]),
                    )
            for parquet_file in _parquet_files("maintenance_summary"):
                table = pq.read_table(parquet_file)
                for row in table.to_pylist():
                    month = row["snapshot_month"]
                    if isinstance(month, datetime):
                        month = month.date()
                    cursor.execute(
                        """INSERT INTO maintenance_summary_monthly
                        (snapshot_month, department, request_count, completed_count, total_cost, average_cost)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (snapshot_month, department) DO UPDATE SET
                        request_count=EXCLUDED.request_count, completed_count=EXCLUDED.completed_count,
                        total_cost=EXCLUDED.total_cost, average_cost=EXCLUDED.average_cost""",
                        (month, row["department"], row["request_count"], row["completed_count"],
                         row["total_cost"] or 0, row["average_cost"] or 0),
                    )
    finally:
        connection.close()


def _parquet_files(dataset):
    folder = os.path.join(PROCESSED_DIR, dataset)
    return [os.path.join(folder, filename) for filename in os.listdir(folder) if filename.endswith(".parquet")]


with DAG(
    dag_id="assetflow_daily_batch_pipeline",
    description="Upload AssetFlow sample data, transform it with PySpark, and load analytics into PostgreSQL.",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "assetflow", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["assetflow", "pyspark", "postgres"],
) as dag:
    raw_to_s3 = PythonOperator(task_id="upload_raw_csvs_to_s3", python_callable=upload_raw_data)
    stage_raw = PythonOperator(task_id="download_raw_csvs_from_s3", python_callable=stage_raw_files_from_s3)
    transform = PythonOperator(task_id="transform_and_write_parquet_with_spark", python_callable=transform_with_spark)
    curated_to_s3 = PythonOperator(task_id="upload_curated_parquet_to_s3", python_callable=upload_curated_parquet)
    load = PythonOperator(task_id="upsert_analytics_into_postgres", python_callable=load_to_postgres)

    raw_to_s3 >> stage_raw >> transform >> curated_to_s3 >> load


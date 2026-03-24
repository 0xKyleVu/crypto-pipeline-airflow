import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


GCP_PROJECT = os.getenv("GCP_PROJECT", "learn-airflow-crypto-project")
GCS_BUCKET = os.getenv("GCS_BUCKET", "crypto-exchange-pipeline")
GCS_RAW_DATA_PATH = os.getenv("GCS_RAW_DATA_PATH", "raw_data/crypto_raw_data")
GCS_TRANSFORMED_PATH = os.getenv(
    "GCS_TRANSFORMED_PATH", "transformed_data/crypto_transformed_data"
)
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "crypto_db")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "tbl_crypto")

RAW_FILENAME = "crypto_data.json"
TRANSFORMED_FILENAME = "transformed_data.csv"

BQ_SCHEMA = [
    {"name": "id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "current_price", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "image", "type": "STRING", "mode": "NULLABLE"},
    {"name": "market_cap", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_volume", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "last_updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


def _fetch_data_from_api():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": False,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    with open(RAW_FILENAME, "w", encoding="utf-8") as file:
        json.dump(data, file)


def _transform_data():
    with open(RAW_FILENAME, "r", encoding="utf-8") as file:
        data = json.load(file)

    transformed_data = []
    for item in data:
        transformed_data.append(
            {
                "id": item["id"],
                "symbol": item["symbol"],
                "name": item["name"],
                "current_price": item["current_price"],
                "image": item.get("image"),
                "market_cap": item.get("market_cap"),
                "total_volume": item.get("total_volume"),
                "last_updated": item.get("last_updated"),
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

    pd.DataFrame(transformed_data).to_csv(TRANSFORMED_FILENAME, index=False)


default_args = {
    "owner": "data-engineering-course",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="crypto_exchange_pipeline",
    default_args=default_args,
    description="Fetch crypto market data from CoinGecko, store in GCS, and load to BigQuery",
    schedule=timedelta(minutes=10),
    start_date=datetime(2026, 3, 19),
    catchup=False,
    tags=["crypto", "gcp", "bigquery"],
)


fetch_data_task = PythonOperator(
    task_id="fetch_data_from_api",
    python_callable=_fetch_data_from_api,
    dag=dag,
)

create_bucket_task = GCSCreateBucketOperator(
    task_id="create_bucket",
    bucket_name=GCS_BUCKET,
    storage_class="STANDARD",
    location="US",
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

upload_raw_data_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id="upload_raw_data_to_gcs",
    src=RAW_FILENAME,
    dst=f"{GCS_RAW_DATA_PATH}_{{{{ ts_nodash }}}}.json",
    bucket=GCS_BUCKET,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id="transform_data",
    python_callable=_transform_data,
    dag=dag,
)

upload_transformed_data_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id="upload_transformed_data_to_gcs",
    src=TRANSFORMED_FILENAME,
    dst=f"{GCS_TRANSFORMED_PATH}_{{{{ ts_nodash }}}}.csv",
    bucket=GCS_BUCKET,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

create_bigquery_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id="create_bigquery_dataset",
    dataset_id=BIGQUERY_DATASET,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

create_bigquery_table_task = BigQueryCreateTableOperator(
    task_id="create_bigquery_table",
    dataset_id=BIGQUERY_DATASET,
    table_id=BIGQUERY_TABLE,
    table_resource={
        "tableReference": {
            "projectId": GCP_PROJECT,
            "datasetId": BIGQUERY_DATASET,
            "tableId": BIGQUERY_TABLE,
        },
        "schema": {"fields": BQ_SCHEMA},
    },
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

load_to_bigquery_task = GCSToBigQueryOperator(
    task_id="load_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=[f"{GCS_TRANSFORMED_PATH}_{{{{ ts_nodash }}}}.csv"],
    destination_project_dataset_table=f"{GCP_PROJECT}:{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
    source_format="CSV",
    schema_fields=BQ_SCHEMA,
    write_disposition="WRITE_TRUNCATE",
    skip_leading_rows=1,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

(
    fetch_data_task
    >> create_bucket_task
    >> upload_raw_data_to_gcs_task
    >> transform_data_task
    >> upload_transformed_data_to_gcs_task
    >> create_bigquery_dataset_task
    >> create_bigquery_table_task
    >> load_to_bigquery_task
)

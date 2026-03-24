# Architecture

## Pipeline flow

1. `fetch_data_from_api`: Pull market data from CoinGecko.
2. `upload_raw_data_to_gcs`: Upload raw JSON to GCS.
3. `transform_data`: Build normalized CSV from JSON.
4. `upload_transformed_data_to_gcs`: Upload transformed CSV to GCS.
5. `create_bigquery_dataset`: Ensure destination dataset exists.
6. `create_bigquery_table`: Ensure destination table exists.
7. `load_to_bigquery`: Load CSV from GCS to BigQuery.

## Components

- **Orchestration**: Apache Airflow
- **Source**: CoinGecko REST API
- **Storage**: Google Cloud Storage
- **Warehouse**: BigQuery

## Design notes

- Object names include `ts_nodash` for run-level traceability.
- Load mode is `WRITE_TRUNCATE` to keep latest snapshot.
- Runtime configuration is environment-driven for portability.
